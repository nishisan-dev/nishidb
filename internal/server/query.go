package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
)

// handleQuery serve o POST /v1/query do §67-69.
//
// A resposta é uma MOLDURA NDJSON: a primeira linha descreve as colunas, as do
// meio são as rows exatamente como o §69 as mostra, e a última é o fecho. A
// moldura existe por uma razão estrutural do streaming: o status HTTP é
// enviado junto com o primeiro byte, então uma consulta que falha na linha
// 10.000 não tem mais status para usar. Sem uma linha de fecho, o cliente não
// consegue separar "acabou" de "quebrou" de "a conexão caiu".
//
// A consequência é deliberada: depois que o schema sai, o status é sempre 200 e
// TODO desfecho vai no corpo. Antes dele, nada foi escrito e o erro sai como
// status + JSON, com a posição do trecho ofensor quando existe.
//
// Nada do resultado é acumulado (§128): o StreamWriter tem buffer fixo em
// bytes, e o lote de query.resultBatchSize apenas decide de quanto em quanto o
// buffer é descarregado no fio.
func (s *Servidor) handleQuery(w http.ResponseWriter, r *http.Request) {
	inicio := time.Now()

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocolo.Falha{
			Code:    protocolo.CodMetodo,
			Message: fmt.Sprintf("%s não é aceito em /v1/query; use POST", r.Method),
		})
		return
	}
	if ct := r.Header.Get("Content-Type"); !ehJSON(ct) {
		s.recusa(w, r, http.StatusUnsupportedMediaType, datasetDesconhecido, protocolo.Falha{
			Code:    protocolo.CodMediaType,
			Message: fmt.Sprintf("Content-Type %q não é aceito; use %s", ct, protocolo.ContentTypeJSON),
		})
		return
	}

	req, falha, status := s.leRequisicao(w, r)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}

	d, ok := s.porNome[req.Dataset]
	if !ok {
		// O nome NÃO entra na métrica: ele é entrada do cliente, e um label
		// alimentado por entrada do cliente é uma série nova por requisição.
		s.recusa(w, r, http.StatusNotFound, datasetDesconhecido, protocolo.Falha{
			Code:    protocolo.CodDatasetDesconhecido,
			Message: fmt.Sprintf("dataset %q não está registrado neste servidor", req.Dataset),
		})
		return
	}

	params, err := protocolo.Valores(req.Parameters)
	if err != nil {
		s.recusa(w, r, http.StatusBadRequest, d.nome, protocolo.Falha{
			Code: protocolo.CodBadParameter, Message: err.Error(),
		})
		return
	}

	tetoDoRequest, falha := tetoDeLinhas(req, d.sch.MaxRows)
	if falha != nil {
		s.recusa(w, r, http.StatusBadRequest, d.nome, *falha)
		return
	}
	ctx, cancela, falha := s.contextoDaConsulta(r, req, d.sch.Timeout)
	if falha != nil {
		s.recusa(w, r, http.StatusBadRequest, d.nome, *falha)
		return
	}
	defer cancela()

	// A vaga é tomada DEPOIS de toda validação barata e ANTES de tocar o
	// Store: recusar cedo custa nada, e o §81 é explícito em preferir rejeitar
	// consulta a comprometer a ingestão.
	select {
	case s.vagas <- struct{}{}:
		defer func() { <-s.vagas }()
	default:
		w.Header().Set("Retry-After", "1")
		s.recusa(w, r, http.StatusServiceUnavailable, d.nome, protocolo.Falha{
			Code: protocolo.CodMuitasQueries,
			Message: fmt.Sprintf("as %d vagas de consulta simultânea estão ocupadas (runtime.query.maxConcurrent)",
				s.cfg.MaxConcurrent()),
		})
		return
	}

	encerra := s.met.admite(d.nome)
	defer encerra()
	defer func() { s.met.observa(d.nome, time.Since(inicio).Seconds()) }()

	it, err := s.eng.Execute(ctx, d.nome, req.SQL, params)
	if err != nil {
		status, falha := traduzErroDeAbertura(err, req.SQL)
		s.recusa(w, r, status, d.nome, falha)
		return
	}
	defer it.Close()

	colunas := it.Columns()
	if nome, colide := colunaReservada(colunas); colide {
		// Uma coluna chamada como a chave de moldura tornaria a row
		// indistinguível de uma linha de controle. A recusa é aqui, antes do
		// primeiro byte, porque depois dele já não haveria como recusar nada.
		s.recusa(w, r, http.StatusBadRequest, d.nome, protocolo.Falha{
			Code: protocolo.CodColunaReservada,
			Message: fmt.Sprintf("a coluna de saída %q colide com a chave reservada da resposta; "+
				"renomeie com um alias na projeção", nome),
		})
		return
	}

	s.transmite(w, r, d, req, it, colunas, tetoDoRequest, inicio)
}

// ehJSON aceita application/json com ou sem parâmetros (charset, por exemplo).
func ehJSON(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.EqualFold(strings.TrimSpace(ct), protocolo.ContentTypeJSON)
}

// leRequisicao decodifica o corpo com teto de tamanho e SEM tolerar chave
// desconhecida.
//
// A estritez é o oposto da doutrina do nishidb.yaml, e de propósito: o arquivo
// de configuração precisa aceitar seções de fases futuras para não obrigar o
// operador a manter dois arquivos, mas aqui um `maxRow` com typo aceito em
// silêncio faria o cliente acreditar que limitou o resultado. O caminho é
// versionado (/v1) justamente para que campo novo venha com contrato novo.
func (s *Servidor) leRequisicao(w http.ResponseWriter, r *http.Request) (protocolo.Requisicao, *protocolo.Falha, int) {
	var req protocolo.Requisicao
	r.Body = http.MaxBytesReader(w, r.Body, protocolo.MaxCorpoRequisicao)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var grande *http.MaxBytesError
		if errors.As(err, &grande) {
			return req, &protocolo.Falha{
				Code:    protocolo.CodCorpoGrande,
				Message: fmt.Sprintf("corpo acima do teto de %d bytes", protocolo.MaxCorpoRequisicao),
			}, http.StatusRequestEntityTooLarge
		}
		return req, &protocolo.Falha{
			Code:    protocolo.CodBadRequest,
			Message: "corpo JSON inválido: " + err.Error(),
		}, http.StatusBadRequest
	}
	switch {
	case req.Dataset == "":
		return req, &protocolo.Falha{Code: protocolo.CodBadRequest, Message: `"dataset" é obrigatório`}, http.StatusBadRequest
	case strings.TrimSpace(req.SQL) == "":
		return req, &protocolo.Falha{Code: protocolo.CodBadRequest, Message: `"sql" é obrigatório`}, http.StatusBadRequest
	}
	return req, nil, http.StatusOK
}

// tetoDeLinhas resolve o maxRows do request contra o do dataset.
//
// O request pode pedir MENOS, nunca mais: o teto do dataset é proteção do
// servidor, não uma sugestão. Pedir mais é recusado em vez de reduzido em
// silêncio — um clamp calado faria o cliente acreditar que recebeu tudo o que
// pediu.
func tetoDeLinhas(req protocolo.Requisicao, tetoDoDataset int) (int, *protocolo.Falha) {
	if req.MaxRows == nil {
		return 0, nil // sem teto próprio; vale o do dataset, aplicado pelo motor
	}
	n := *req.MaxRows
	switch {
	case n <= 0:
		return 0, &protocolo.Falha{
			Code:    protocolo.CodBadRequest,
			Message: fmt.Sprintf(`"maxRows" precisa ser positivo (recebido %d)`, n),
		}
	case n > tetoDoDataset:
		return 0, &protocolo.Falha{
			Code: protocolo.CodMaxRowsAcimaDoTeto,
			Message: fmt.Sprintf(`"maxRows" %d acima do teto efetivo do dataset (%d); peça menos ou ajuste query.maxRows`,
				n, tetoDoDataset),
		}
	}
	return n, nil
}

// contextoDaConsulta aplica o timeoutMs do request, que também só pode
// ENCURTAR: o menor prazo vence porque o executor deriva do contexto recebido.
func (s *Servidor) contextoDaConsulta(r *http.Request, req protocolo.Requisicao, tetoDoDataset time.Duration) (
	context.Context, context.CancelFunc, *protocolo.Falha,
) {
	if req.TimeoutMs == nil {
		return r.Context(), func() {}, nil
	}
	ms := *req.TimeoutMs
	switch {
	case ms <= 0:
		return nil, nil, &protocolo.Falha{
			Code:    protocolo.CodBadRequest,
			Message: fmt.Sprintf(`"timeoutMs" precisa ser positivo (recebido %d)`, ms),
		}
	case time.Duration(ms)*time.Millisecond > tetoDoDataset:
		return nil, nil, &protocolo.Falha{
			Code: protocolo.CodBadRequest,
			Message: fmt.Sprintf(`"timeoutMs" %d acima do teto do dataset (%d ms); peça menos ou ajuste query.timeout`,
				ms, tetoDoDataset.Milliseconds()),
		}
	}
	ctx, cancela := context.WithTimeout(r.Context(), time.Duration(ms)*time.Millisecond)
	return ctx, cancela, nil
}

// colunaReservada procura na projeção uma coluna com o nome da chave de
// moldura.
func colunaReservada(cols []executor.ColMeta) (string, bool) {
	for _, c := range cols {
		if c.Name == protocolo.ChaveMoldura {
			return c.Name, true
		}
	}
	return "", false
}

// transmite emite a moldura completa: schema, rows e fecho.
func (s *Servidor) transmite(
	w http.ResponseWriter, r *http.Request, d *dataset, req protocolo.Requisicao,
	it executor.RowIterator, colunas []executor.ColMeta, tetoDoRequest int, inicio time.Time,
) {
	log := s.log.Dataset(d.nome)

	cols := make([]protocolo.Coluna, len(colunas))
	for i, c := range colunas {
		cols[i] = protocolo.ColunaDe(c.Name, c.Type, c.Nullable)
	}

	w.Header().Set("Content-Type", protocolo.ContentTypeNDJSON)
	flusher, _ := w.(http.Flusher)
	// O schema sai ANTES de qualquer row, e é o que compromete o status em
	// 200: daqui para a frente todo desfecho é uma linha de corpo.
	if err := protocolo.EscreveLinha(w, protocolo.LinhaSchema{
		Nishidb: protocolo.MolduraSchema,
		Dataset: d.nome,
		Columns: cols,
	}); err != nil {
		// O cliente sumiu antes da primeira linha. Não há a quem responder e
		// não há dado perdido: só o registro de que a consulta não chegou a ser
		// entregue.
		s.met.erro(d.nome, protocolo.CodInterno)
		log.Warn("query.escrita_falhou", "o cliente fechou a conexão antes do schema", "erro", err.Error())
		return
	}

	// O schema vai ao fio SOZINHO, e não junto do primeiro lote.
	//
	// Custa um chunk por consulta e paga o ResultSetMetaData imediato: sem
	// isto, uma query que varre muito e emite pouco — full scan com WHERE
	// seletivo — deixaria o cliente sem nenhum byte até o primeiro lote de
	// resultBatchSize fechar, e uma ferramenta de SQL não teria como desenhar
	// as colunas do grid enquanto isso. Também adianta a detecção de cliente
	// que já foi embora.
	if flusher != nil {
		flusher.Flush()
	}

	lote := s.cfg.ResultBatchSize()
	sw := executor.NewStreamWriter(w, executor.DefaultBufSize)

	var emitidas int64
	truncadoPeloRequest := false
	for it.Next() {
		if err := sw.WriteRow(it.Row()); err != nil {
			s.fechaComErro(w, d, protocolo.CodInterno, emitidas, err, log)
			return
		}
		emitidas++

		if tetoDoRequest > 0 && emitidas >= int64(tetoDoRequest) {
			// O teto do PRÓPRIO cliente: parar aqui não é erro, é o pedido
			// sendo atendido. O iterador fecha no defer.
			truncadoPeloRequest = true
			break
		}
		if int(emitidas)%lote == 0 {
			// Os DOIS flushes, nesta ordem: o do writer move as linhas do
			// buffer de 64 KiB para o ResponseWriter, e o do http entrega o
			// chunk ao fio. Só o primeiro deixaria os bytes parados no
			// servidor; só o segundo não teria o que entregar.
			if err := sw.Flush(); err != nil {
				s.fechaComErro(w, d, protocolo.CodInterno, emitidas, err, log)
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

	if err := sw.Flush(); err != nil {
		s.fechaComErro(w, d, protocolo.CodInterno, emitidas, err, log)
		return
	}
	if err := it.Err(); err != nil {
		s.fechaComErro(w, d, codigoDeErroDeQuery(err), emitidas, err, log)
		return
	}

	if err := protocolo.EscreveLinha(w, protocolo.LinhaFim{
		Nishidb:   protocolo.MolduraFim,
		Rows:      emitidas,
		Truncated: truncadoPeloRequest,
	}); err != nil {
		log.Warn("query.escrita_falhou", "o cliente fechou a conexão antes do fecho", "erro", err.Error())
		return
	}
	if flusher != nil {
		flusher.Flush()
	}

	log.Info("query.concluida", "consulta atendida",
		"rows", emitidas,
		"truncado", truncadoPeloRequest,
		"duracao_ms", time.Since(inicio).Milliseconds())
	// O SQL só em DEBUG: ele pode carregar literais com dado do cliente, e o
	// §95 proíbe registrar payload por default.
	if log.Enabled(slog.LevelDebug) {
		log.Debug("query.sql", "texto da consulta atendida", "sql", req.SQL, "remoto", r.RemoteAddr)
	}
}

// fechaComErro escreve o fecho de falha DEPOIS de rows já emitidas.
//
// Nunca troca o status — ele já foi enviado com o schema. É por isso que o
// formato precisa de uma linha de fecho: sem ela, este caminho seria
// indistinguível de um resultado que simplesmente acabou.
func (s *Servidor) fechaComErro(w http.ResponseWriter, d *dataset, codigo string, emitidas int64, causa error, log *logging.Logger) {
	s.met.erro(d.nome, codigo)
	if err := protocolo.EscreveLinha(w, protocolo.LinhaErro{
		Nishidb: protocolo.MolduraErro,
		Code:    codigo,
		Rows:    emitidas,
		Message: causa.Error(),
	}); err != nil {
		log.Warn("query.escrita_falhou", "não foi possível entregar o fecho de erro", "erro", err.Error())
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	log.Error("query.falhou", "a consulta falhou depois de começar a responder",
		"codigo", codigo, "rows", emitidas, "erro", causa.Error())
}

// recusa responde um erro que aconteceu ANTES da primeira linha: status HTTP de
// verdade e corpo JSON único.
//
// dataset é o nome REGISTRADO, ou datasetDesconhecido quando a falha aconteceu
// antes de resolvê-lo — nunca o nome cru vindo do request.
func (s *Servidor) recusa(w http.ResponseWriter, r *http.Request, status int, dataset string, falha protocolo.Falha) {
	s.met.erro(dataset, falha.Code)

	w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(protocolo.CorpoErro{Error: falha}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo de erro", "erro", err.Error())
	}
	// A mensagem NÃO vai para o log: ela pode conter o texto da query, e com
	// ele literais com dado do cliente. O código classifica, e o cliente já
	// recebeu a prosa.
	s.log.Warn("query.recusada", "consulta recusada antes de responder",
		"codigo", falha.Code, "status", status, "remoto", r.RemoteAddr)
}

// traduzErroDeAbertura mapeia a falha do Execute — que acontece antes de
// qualquer leitura de dado — em status e código.
func traduzErroDeAbertura(err error, sql string) (int, protocolo.Falha) {
	switch {
	case errors.Is(err, parser.ErrSyntax):
		f := protocolo.Falha{Code: protocolo.CodSintaxe, Message: err.Error()}
		var se *lexer.SyntaxError
		if errors.As(err, &se) {
			f.Line, f.Column = posicao(sql, se.Pos)
		}
		return http.StatusBadRequest, f
	case errors.Is(err, binder.ErrBind):
		f := protocolo.Falha{Code: protocolo.CodBind, Message: err.Error()}
		var be *binder.Error
		if errors.As(err, &be) {
			f.Line, f.Column = posicao(sql, be.Pos)
		}
		return http.StatusBadRequest, f
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, protocolo.Falha{Code: protocolo.CodTimeout, Message: err.Error()}
	case errors.Is(err, storage.ErrDatasetCorrupt), errors.Is(err, encoding.ErrCorrupt):
		return http.StatusInternalServerError, protocolo.Falha{Code: protocolo.CodDatasetCorrompido, Message: err.Error()}
	default:
		// Inclui executor.ErrInvalidPlan, que é bug do binder/planner e não do
		// cliente: 500 é a resposta honesta.
		return http.StatusInternalServerError, protocolo.Falha{Code: protocolo.CodInterno, Message: err.Error()}
	}
}

// codigoDeErroDeQuery classifica a falha que aparece DEPOIS das rows, no
// it.Err().
func codigoDeErroDeQuery(err error) string {
	switch {
	case errors.Is(err, executor.ErrMaxRowsExceeded):
		return protocolo.CodMaxRows
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return protocolo.CodTimeout
	case errors.Is(err, storage.ErrDatasetCorrupt), errors.Is(err, encoding.ErrCorrupt):
		return protocolo.CodDatasetCorrompido
	default:
		return protocolo.CodInterno
	}
}

// posicao converte o offset em linha e coluna 1-based, como o CLI já faz ao
// desenhar o cursor sob o trecho ofensor.
func posicao(sql string, pos int) (int, int) {
	if pos < 0 || pos > len(sql) {
		return 0, 0
	}
	linha, inicio := 1, 0
	for i := 0; i < pos && i < len(sql); i++ {
		if sql[i] == '\n' {
			linha++
			inicio = i + 1
		}
	}
	return linha, pos - inicio + 1
}
