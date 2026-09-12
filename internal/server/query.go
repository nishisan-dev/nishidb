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
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/budget"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
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
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.reject(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeMethod,
			Message: fmt.Sprintf("%s não é aceito em /v1/query; use POST", r.Method),
		})
		return
	}
	if ct := r.Header.Get("Content-Type"); !isJSON(ct) {
		s.reject(w, r, http.StatusUnsupportedMediaType, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeMediaType,
			Message: fmt.Sprintf("Content-Type %q não é aceito; use %s", ct, protocol.ContentTypeJSON),
		})
		return
	}

	req, fail, status := s.readRequisicao(w, r)
	if fail != nil {
		s.reject(w, r, status, datasetDesconhecido, *fail)
		return
	}

	d, ok := s.byNome[req.Dataset]
	if !ok {
		// O nome NÃO entra na métrica: ele é entrada do cliente, e um label
		// alimentado por entrada do cliente é uma série nova por requisição.
		//
		// O complemento sai de binder.DottedDatasetNameHint, a MESMA função que
		// o bind usa, e não de uma segunda redação da regra. Ele é necessário
		// aqui porque este é o ponto de recusa que o cliente JDBC alcança: o
		// driver extrai o nome do FROM e o manda no corpo, então
		// `FROM "telecom.service_view"` chega como o dataset
		// `telecom.service_view`, que não está registrado — e a recusa acontece
		// ANTES de o SQL ser analisado, de modo que o bind, onde a dica já
		// existia, nunca roda. Um nome sem ponto não recebe complemento nenhum.
		s.reject(w, r, http.StatusNotFound, datasetDesconhecido, protocol.ErrorDetail{
			Code: protocol.CodeUnknownDataset,
			Message: fmt.Sprintf("dataset %q não está registrado neste servidor%s",
				req.Dataset, binder.DottedDatasetNameHint(req.Dataset)),
		})
		return
	}

	params, err := protocol.Values(req.Parameters)
	if err != nil {
		s.reject(w, r, http.StatusBadRequest, d.nome, protocol.ErrorDetail{
			Code: protocol.CodeBadParameter, Message: err.Error(),
		})
		return
	}

	// Os limites de consulta — `query.maxRows`, `query.timeout` e
	// `query.maxTimeout` — são declarados POR DATASET no YAML, então quem os
	// governa é o dataset que a consulta vai LER, e não o handle por onde ela
	// entrou. Desde que o `FROM <schema>.<tabela>` resolve o dataset dono, os
	// dois podem ser diferentes: aplicar os do handle faria de um dataset com
	// teto alto a porta para ler, sob o teto dele, um dataset cujo YAML declara
	// teto baixo — o YAML deixaria de valer para a tabela de que ele fala.
	//
	// O handle continua governando tudo o mais: é ele que o `d.nome` leva para
	// as métricas, para o log e para o registro de queries em voo.
	limits := d
	if owner, ok := s.byNome[s.eng.TargetDataset(d.nome, req.SQL)]; ok {
		limits = owner
	}

	ceilingOfRequest, fail := rowCeiling(req, limits.sch.MaxRows)
	if fail != nil {
		s.reject(w, r, http.StatusBadRequest, d.nome, *fail)
		return
	}
	ctx, cancel, prazo, fail := s.contextoOfConsulta(r, req, limits.sch)
	if fail != nil {
		s.reject(w, r, http.StatusBadRequest, d.nome, *fail)
		return
	}
	defer cancel()
	// LONGA é quem passa do prazo padrão do dataset, e não quem passa de um
	// número fixo: `query.timeout` é a declaração de quanto tempo uma consulta
	// deste dataset costuma levar, então tudo que a excede é, por definição
	// dele mesmo, fora do comum.
	longa := prazo > limits.sch.Timeout

	// A vaga é tomada DEPOIS de toda validação barata e ANTES de tocar o
	// Store: recusar cedo custa nada, e o §81 é explícito em preferir rejeitar
	// consulta a comprometer a ingestão.
	select {
	case s.vagas <- struct{}{}:
		defer func() { <-s.vagas }()
	default:
		w.Header().Set("Retry-After", "1")
		s.reject(w, r, http.StatusServiceUnavailable, d.nome, protocol.ErrorDetail{
			Code: protocol.CodeTooManyQueries,
			Message: fmt.Sprintf("as %d vagas de consulta simultânea estão ocupadas (runtime.query.maxConcurrent)",
				s.cfg.MaxConcurrent()),
		})
		return
	}

	// A consulta longa toma DUAS vagas: a do pool geral, já tomada acima, e a
	// do sub-pool. É a segunda que garante que as vagas gerais restantes
	// continuem disponíveis para quem responde em milissegundos — sem ela,
	// maxConcurrent relatórios de dez minutos fariam TODA consulta interativa
	// receber 503 durante esses dez minutos.
	//
	// A ordem (geral primeiro, longo depois) importa: se a vaga longa faltar,
	// o defer acima já devolve a geral, e uma consulta longa recusada não
	// deixa vaga geral presa.
	if longa {
		select {
		case s.vagasLongas <- struct{}{}:
			defer func() { <-s.vagasLongas }()
		default:
			// Retry-After maior que o do pool geral, e de propósito: quem
			// ocupa uma vaga longa pode estar lá por minutos, e mandar o
			// cliente voltar em um segundo o treinaria a martelar o servidor.
			w.Header().Set("Retry-After", "30")
			s.met.rejectLonga()
			s.reject(w, r, http.StatusServiceUnavailable, d.nome, protocol.ErrorDetail{
				Code: protocol.CodeTooManyLongQueries,
				Message: fmt.Sprintf(
					"as %d vagas de consulta longa estão ocupadas (runtime.query.maxLongRunning); "+
						"peça um timeoutMs de até %d ms para entrar pelo pool geral",
					s.cfg.MaxLongRunning(), limits.sch.Timeout.Milliseconds()),
			})
			return
		}
	}

	// O orçamento de memória desta consulta é reservado AQUI, ao lado das vagas
	// e antes de tocar o Store — é decisão de admissão, não de meio de stream.
	//
	// Reservado de uma vez, o desfecho é honesto: ou a consulta tem o orçamento
	// dela inteiro antes do primeiro byte, ou recebe 503 com Retry-After. Com
	// reserva conforme o buffer cresce, o `memoryTarget` poderia acabar na linha
	// quarenta mil, e o cliente perderia o resultado por causa da memória de
	// OUTRA consulta que nada tem a ver com a dele.
	//
	// A recusa reusa TOO_MANY_QUERIES, e não ganha código próprio, porque a AÇÃO
	// do cliente é idêntica à do semáforo cheio — esperar e repetir. O que muda
	// é a CAUSA, e ela vai na mensagem, nomeando a chave.
	orc := s.budget.Query(s.cfg.MaxBufferPerQuery())
	if !orc.Admit() {
		w.Header().Set("Retry-After", "1")
		s.reject(w, r, http.StatusServiceUnavailable, d.nome, protocol.ErrorDetail{
			Code: protocol.CodeTooManyQueries,
			Message: fmt.Sprintf(
				"o orçamento de memória de consulta do processo está esgotado (runtime.memoryTarget, %d bytes); "+
					"esta consulta precisa de %d bytes (runtime.query.maxBufferPerQuery)",
				s.cfg.MemoryTarget(), s.cfg.MaxBufferPerQuery()),
		})
		return
	}
	defer orc.Release()

	encerra := s.met.admite(d.nome)
	defer encerra()
	// O registro de queries em voo nasce ao LADO do admite, e não dentro dele,
	// porque as duas coisas respondem a perguntas diferentes: o medidor diz
	// QUANTAS estão correndo, e o registro diz QUAIS e HÁ QUANTO TEMPO. Com o
	// prazo longo virando padrão, "há 3 consultas ativas" deixou de ser
	// resposta — o operador precisa saber se são três point lookups ou um
	// full scan de oito minutos.
	saiDoRecord := s.registerInVoo(d.nome, req.SQL, longa, start)
	defer saiDoRecord()
	defer func() { s.met.observa(d.nome, time.Since(start).Seconds()) }()

	it, err := s.eng.Execute(ctx, d.nome, req.SQL, params)
	if err != nil {
		status, fail := translateOpenError(err, req.SQL)
		s.reject(w, r, status, d.nome, fail)
		return
	}
	defer it.Close()

	columns := it.Columns()
	if name, colide := columnReservada(columns); colide {
		// Uma coluna chamada como a chave de moldura tornaria a row
		// indistinguível de uma linha de controle. A recusa é aqui, antes do
		// primeiro byte, porque depois dele já não haveria como recusar nada.
		s.reject(w, r, http.StatusBadRequest, d.nome, protocol.ErrorDetail{
			Code: protocol.CodeReservedColumn,
			Message: fmt.Sprintf("a coluna de saída %q colide com a chave reservada da resposta; "+
				"renomeie com um alias na projeção", name),
		})
		return
	}

	s.transmite(w, r, d, req, it, columns, ceilingOfRequest, start, orc)
}

// isJSON aceita application/json com ou sem parâmetros (charset, por exemplo).
func isJSON(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.EqualFold(strings.TrimSpace(ct), protocol.ContentTypeJSON)
}

// readRequisicao decodifica o corpo com teto de tamanho e SEM tolerar chave
// desconhecida.
//
// A estritez é o oposto da doutrina do nishidb.yaml, e de propósito: o arquivo
// de configuração precisa aceitar seções de fases futuras para não obrigar o
// operador a manter dois arquivos, mas aqui um `maxRow` com typo aceito em
// silêncio faria o cliente acreditar que limitou o resultado. O caminho é
// versionado (/v1) justamente para que campo novo venha com contrato novo.
func (s *Server) readRequisicao(w http.ResponseWriter, r *http.Request) (protocol.Request, *protocol.ErrorDetail, int) {
	var req protocol.Request
	r.Body = http.MaxBytesReader(w, r.Body, protocol.MaxRequestBody)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var grande *http.MaxBytesError
		if errors.As(err, &grande) {
			return req, &protocol.ErrorDetail{
				Code:    protocol.CodeBodyTooLarge,
				Message: fmt.Sprintf("corpo acima do teto de %d bytes", protocol.MaxRequestBody),
			}, http.StatusRequestEntityTooLarge
		}
		return req, &protocol.ErrorDetail{
			Code:    protocol.CodeBadRequest,
			Message: "corpo JSON inválido: " + err.Error(),
		}, http.StatusBadRequest
	}
	switch {
	case req.Dataset == "":
		return req, &protocol.ErrorDetail{Code: protocol.CodeBadRequest, Message: `"dataset" é obrigatório`}, http.StatusBadRequest
	case strings.TrimSpace(req.SQL) == "":
		return req, &protocol.ErrorDetail{Code: protocol.CodeBadRequest, Message: `"sql" é obrigatório`}, http.StatusBadRequest
	}
	return req, nil, http.StatusOK
}

// rowCeiling resolve o maxRows do request contra o do dataset.
//
// O request pode pedir MENOS, nunca mais: o teto do dataset é proteção do
// servidor, não uma sugestão. Pedir mais é recusado em vez de reduzido em
// silêncio — um clamp calado faria o cliente acreditar que recebeu tudo o que
// pediu.
func rowCeiling(req protocol.Request, ceilingOfDataset int) (int, *protocol.ErrorDetail) {
	if req.MaxRows == nil {
		return 0, nil // sem teto próprio; vale o do dataset, aplicado pelo motor
	}
	n := *req.MaxRows
	switch {
	case n <= 0:
		return 0, &protocol.ErrorDetail{
			Code:    protocol.CodeBadRequest,
			Message: fmt.Sprintf(`"maxRows" precisa ser positivo (recebido %d)`, n),
		}
	case n > ceilingOfDataset:
		return 0, &protocol.ErrorDetail{
			Code: protocol.CodeMaxRowsAboveCeiling,
			Message: fmt.Sprintf(`"maxRows" %d acima do teto efetivo do dataset (%d); peça menos ou ajuste query.maxRows`,
				n, ceilingOfDataset),
		}
	}
	return n, nil
}

// contextoOfConsulta resolve o prazo EFETIVO da consulta e devolve o contexto
// já com ele, mais o próprio prazo — que é o que decide se ela é longa.
//
// O request pode ALONGAR até `query.maxTimeout` e nunca além dele. Enquanto o
// teto era o próprio `query.timeout`, ele acumulava dois papéis incompatíveis:
// dar prazo maior a UM cliente exigia subir o default de TODOS, inclusive o dos
// que nunca chamaram setQueryTimeout — e esses passariam a poder segurar uma
// vaga do §80 por minutos sem nunca terem pedido isso. Com as duas chaves
// separadas, alongar é um ato explícito de quem pede.
//
// A ausência de `timeoutMs` aplica o `Timeout` do dataset EXPLICITAMENTE, com
// um deadline de verdade no contexto, em vez de devolver r.Context() cru e
// deixar o executor pôr o prazo lá embaixo. Não é cerimônia: o executor agora
// respeita o deadline de quem chama (ver executor.Open), então quem não puser
// deadline aqui entrega uma consulta sem prazo nenhum.
func (s *Server) contextoOfConsulta(r *http.Request, req protocol.Request, sch *schema.Compiled) (
	context.Context, context.CancelFunc, time.Duration, *protocol.ErrorDetail,
) {
	prazo := sch.Timeout
	if req.TimeoutMs != nil {
		ms := *req.TimeoutMs
		switch {
		case ms <= 0:
			return nil, nil, 0, &protocol.ErrorDetail{
				Code:    protocol.CodeBadRequest,
				Message: fmt.Sprintf(`"timeoutMs" precisa ser positivo (recebido %d)`, ms),
			}
		case time.Duration(ms)*time.Millisecond > sch.MaxTimeout:
			// A mensagem nomeia `query.maxTimeout`, e não `query.timeout`:
			// mandar o operador mexer no default para atender um pedido pontual
			// é exatamente o conselho errado — subiria o prazo de todo mundo.
			return nil, nil, 0, &protocol.ErrorDetail{
				Code: protocol.CodeBadRequest,
				Message: fmt.Sprintf(
					`"timeoutMs" %d acima do teto do dataset (%d ms); peça menos ou ajuste query.maxTimeout`,
					ms, sch.MaxTimeout.Milliseconds()),
			}
		}
		prazo = time.Duration(ms) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), prazo)
	return ctx, cancel, prazo, nil
}

// columnReservada procura na projeção uma coluna com o nome da chave de
// moldura.
func columnReservada(cols []executor.ColMeta) (string, bool) {
	for _, c := range cols {
		if c.Name == protocol.FrameKey {
			return c.Name, true
		}
	}
	return "", false
}

// transmite emite a moldura completa: schema, rows e fecho.
func (s *Server) transmite(
	w http.ResponseWriter, r *http.Request, d *dataset, req protocol.Request,
	it executor.RowIterator, columns []executor.ColMeta, ceilingOfRequest int, start time.Time,
	orc *budget.PerQuery,
) {
	log := s.log.Dataset(d.nome)

	cols := make([]protocol.Column, len(columns))
	for i, c := range columns {
		cols[i] = protocol.NewColumn(c.Name, c.Type, c.Nullable)
	}

	w.Header().Set("Content-Type", protocol.ContentTypeNDJSON)
	flusher, _ := w.(http.Flusher)
	// O schema sai ANTES de qualquer row, e é o que compromete o status em
	// 200: daqui para a frente todo desfecho é uma linha de corpo.
	if err := protocol.WriteLine(w, protocol.SchemaLine{
		Nishidb: protocol.FrameSchema,
		Dataset: d.nome,
		Columns: cols,
	}); err != nil {
		// O cliente sumiu antes da primeira linha. Não há a quem responder e
		// não há dado perdido: só o registro de que a consulta não chegou a ser
		// entregue.
		s.met.err(d.nome, protocol.CodeInternal)
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
	// O orçamento já foi ADMITIDO em handleQuery, e é devolvido lá, no defer que
	// cobre todos os caminhos de saída. Aqui ele só informa ao writer o teto que
	// ele tem de fazer valer.
	sw := executor.NewStreamWriter(w, executor.DefaultBufSize, orc)

	var emitidas int64
	truncadoPeloRequest := false
	for it.Next() {
		if err := sw.WriteRow(it.Row()); err != nil {
			s.closeWithErr(w, d, s.codigoOfErrOfWrite(err), emitidas, err, log)
			return
		}
		emitidas++

		if ceilingOfRequest > 0 && emitidas >= int64(ceilingOfRequest) {
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
				s.closeWithErr(w, d, s.codigoOfErrOfWrite(err), emitidas, err, log)
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

	if err := sw.Flush(); err != nil {
		s.closeWithErr(w, d, s.codigoOfErrOfWrite(err), emitidas, err, log)
		return
	}
	if err := it.Err(); err != nil {
		s.closeWithErr(w, d, codigoOfErrOfQuery(err), emitidas, err, log)
		return
	}

	if err := protocol.WriteLine(w, protocol.EndLine{
		Nishidb:   protocol.FrameEnd,
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
		"duracao_ms", time.Since(start).Milliseconds())
	// O SQL só em DEBUG: ele pode carregar literais com dado do cliente, e o
	// §95 proíbe registrar payload por default.
	if log.Enabled(slog.LevelDebug) {
		log.Debug("query.sql", "texto da consulta atendida", "sql", req.SQL, "remoto", r.RemoteAddr)
	}
}

// closeWithErr escreve o fecho de falha DEPOIS de rows já emitidas.
//
// Nunca troca o status — ele já foi enviado com o schema. É por isso que o
// formato precisa de uma linha de fecho: sem ela, este caminho seria
// indistinguível de um resultado que simplesmente acabou.
func (s *Server) closeWithErr(w http.ResponseWriter, d *dataset, codigo string, emitidas int64, causa error, log *logging.Logger) {
	s.met.err(d.nome, codigo)
	if err := protocol.WriteLine(w, protocol.ErrorLine{
		Nishidb: protocol.FrameError,
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

// reject responde um erro que aconteceu ANTES da primeira linha: status HTTP de
// verdade e corpo JSON único.
//
// dataset é o nome REGISTRADO, ou datasetDesconhecido quando a falha aconteceu
// antes de resolvê-lo — nunca o nome cru vindo do request.
func (s *Server) reject(w http.ResponseWriter, r *http.Request, status int, dataset string, fail protocol.ErrorDetail) {
	s.met.err(dataset, fail.Code)

	w.Header().Set("Content-Type", protocol.ContentTypeJSON)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(protocol.ErrorBody{Error: fail}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo de erro", "erro", err.Error())
	}
	// A mensagem NÃO vai para o log: ela pode conter o texto da query, e com
	// ele literais com dado do cliente. O código classifica, e o cliente já
	// recebeu a prosa.
	s.log.Warn("query.recusada", "consulta recusada antes de responder",
		"codigo", fail.Code, "status", status, "remoto", r.RemoteAddr)
}

// translateOpenError mapeia a falha do Execute — que acontece antes de
// qualquer leitura de dado — em status e código.
func translateOpenError(err error, sql string) (int, protocol.ErrorDetail) {
	switch {
	case errors.Is(err, parser.ErrSyntax):
		f := protocol.ErrorDetail{Code: protocol.CodeSyntax, Message: err.Error()}
		var se *lexer.SyntaxError
		if errors.As(err, &se) {
			f.Line, f.Column = position(sql, se.Pos)
			// A cláusula não suportada é um erro de sintaxe com um nome
			// melhor, e por isso a distinção mora DENTRO deste case, num
			// campo, e não num case próprio antes dele. Um case novo teria de
			// vir antes do de ErrSyntax para disparar, e trocar a ordem dos
			// dois faria o código novo nunca sair — degradando em silêncio
			// para SYNTAX, que é exatamente o estado que ele veio corrigir.
			if se.Clause != "" {
				f.Code = protocol.CodeUnsupportedClause
			}
		}
		return http.StatusBadRequest, f
	case errors.Is(err, binder.ErrBind):
		f := protocol.ErrorDetail{Code: protocol.CodeBind, Message: err.Error()}
		var be *binder.Error
		if errors.As(err, &be) {
			f.Line, f.Column = position(sql, be.Pos)
		}
		return http.StatusBadRequest, f
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, protocol.ErrorDetail{Code: protocol.CodeTimeout, Message: err.Error()}
	case errors.Is(err, storage.ErrDatasetCorrupt), errors.Is(err, encoding.ErrCorrupt):
		return http.StatusInternalServerError, protocol.ErrorDetail{Code: protocol.CodeDatasetCorrupt, Message: err.Error()}
	case errors.Is(err, storage.ErrExpiredSnapshot):
		// 503, e não 500: o servidor está íntegro e a consulta pode ser
		// repetida — a nova abre um snapshot que ainda não retém nada.
		return http.StatusServiceUnavailable, protocol.ErrorDetail{Code: protocol.CodeSnapshotExpired, Message: err.Error()}
	default:
		// Inclui executor.ErrInvalidPlan, que é bug do binder/planner e não do
		// cliente: 500 é a resposta honesta.
		return http.StatusInternalServerError, protocol.ErrorDetail{Code: protocol.CodeInternal, Message: err.Error()}
	}
}

// codigoOfErrOfWrite classifica a falha do StreamWriter.
//
// Existe porque nem toda falha de escrita é defeito do servidor. A linha que não
// cabe no orçamento de buffer (§80) é o teto de memória sendo APLICADO, com ação
// possível do cliente — projetar menos colunas — e do operador — subir
// runtime.query.maxBufferPerQuery. Publicá-la como INTERNAL mandaria os dois
// procurar um bug que não existe, e é o que acontecia enquanto o writer crescia
// em silêncio: não havia nem erro para classificar.
//
// A recusa também alimenta o contador do PROCESSO, aqui e não no executor: é a
// mesma doutrina de metricsOfQuery — as séries de consulta nascem no protocolo,
// que é quem conhece requisições, e não no motor, que conhece iteradores.
func (s *Server) codigoOfErrOfWrite(err error) string {
	if errors.Is(err, executor.ErrRowTooLarge) {
		s.met.rejectByBudget()
		return protocol.CodeRowTooLarge
	}
	return protocol.CodeInternal
}

// codigoOfErrOfQuery classifica a falha que aparece DEPOIS das rows, no
// it.Err().
func codigoOfErrOfQuery(err error) string {
	switch {
	case errors.Is(err, executor.ErrMaxRowsExceeded):
		return protocol.CodeMaxRows
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return protocol.CodeTimeout
	case errors.Is(err, storage.ErrDatasetCorrupt), errors.Is(err, encoding.ErrCorrupt):
		return protocol.CodeDatasetCorrupt
	case errors.Is(err, storage.ErrExpiredSnapshot):
		// Sai DEPOIS das rows, e é onde ele aparece na prática: o Open não lê
		// nada, então o journal só pode estourar com a consulta já correndo.
		// As linhas já entregues são do universo de T0 e continuam válidas —
		// o que o cliente não pode fazer é tratá-las como resultado completo,
		// e é para isso que a linha de fecho carrega o código.
		return protocol.CodeSnapshotExpired
	default:
		return protocol.CodeInternal
	}
}

// position converte o offset em linha e coluna 1-based, como o CLI já faz ao
// desenhar o cursor sob o trecho ofensor.
func position(sql string, pos int) (int, int) {
	if pos < 0 || pos > len(sql) {
		return 0, 0
	}
	row, start := 1, 0
	for i := 0; i < pos && i < len(sql); i++ {
		if sql[i] == '\n' {
			row++
			start = i + 1
		}
	}
	return row, pos - start + 1
}
