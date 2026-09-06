// As duas rotas MUTANTES do §96 (Fase 11, Task B5, decisão D3 do roadmap) —
// POST /v1/compact e POST /v1/checkpoint. São as PRIMEIRAS rotas do
// protocolo remoto que mudam estado; até aqui tudo era leitura.
//
// As duas são ADMIN-only (registradas em montaMux, autenticacao.go),
// síncronas (a resposta só sai depois do trabalho feito — nenhuma das duas
// enfileira nem devolve 202) e aceitam o MESMO corpo opcional
// {"dataset":"..."}: ausente significa TODOS os datasets, na ordem
// determinística da subida (s.datasets, o mesmo slice que /v1/metadata e
// /v1/stats já percorrem).

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// leRequisicaoDataset decodifica o corpo OPCIONAL de POST /v1/compact e POST
// /v1/checkpoint.
//
// Difere de leRequisicao (query.go) num ponto: corpo AUSENTE (io.EOF na
// primeira leitura, o que um POST sem Content-Length ou com Content-Length:
// 0 produz) não é erro aqui — é o pedido de "todos os datasets", a forma
// mais comum de chamar as duas rotas. leRequisicao não tem esse caso porque
// /v1/query não tem leitura implícita de "todos".
func (s *Servidor) leRequisicaoDataset(w http.ResponseWriter, r *http.Request) (protocol.RequisicaoDataset, *protocol.Falha, int) {
	var req protocol.RequisicaoDataset
	r.Body = http.MaxBytesReader(w, r.Body, protocol.MaxCorpoRequisicao)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return req, nil, http.StatusOK // corpo ausente: aceito, Dataset == ""
		}
		var grande *http.MaxBytesError
		if errors.As(err, &grande) {
			return req, &protocol.Falha{
				Code:    protocol.CodCorpoGrande,
				Message: fmt.Sprintf("corpo acima do teto de %d bytes", protocol.MaxCorpoRequisicao),
			}, http.StatusRequestEntityTooLarge
		}
		return req, &protocol.Falha{
			Code:    protocol.CodBadRequest,
			Message: "corpo JSON inválido: " + err.Error(),
		}, http.StatusBadRequest
	}
	return req, nil, http.StatusOK
}

// datasetsAlvo resolve o campo Dataset do corpo contra s.porNome/s.datasets:
// nome presente filtra para UM dataset (404 se desconhecido, o mesmo código
// de /v1/query e /v1/metadata); nome ausente devolve TODOS, na ordem
// determinística da subida.
func (s *Servidor) datasetsAlvo(nome string) ([]*dataset, *protocol.Falha, int) {
	if nome == "" {
		return s.datasets, nil, http.StatusOK
	}
	d, ok := s.porNome[nome]
	if !ok {
		return nil, &protocol.Falha{
			Code:    protocol.CodDatasetDesconhecido,
			Message: fmt.Sprintf("dataset %q não está registrado neste servidor", nome),
		}, http.StatusNotFound
	}
	return []*dataset{d}, nil, http.StatusOK
}

// erroDeMutacao traduz o erro do storage (Store.Checkpoint ou
// Store.CompactaUmaVez) em status HTTP e código do protocolo. É
// COMPARTILHADA pelas duas rotas porque as duas passam pelo MESMO writer e
// podem encontrar o MESMO modo falho sticky (aposentaSegmento chama
// Store.Checkpoint por dentro, então uma falha de checkpoint é alcançável
// pelas DUAS rotas, não só por /v1/checkpoint).
func erroDeMutacao(err error) (int, protocol.Falha) {
	switch {
	case errors.Is(err, storage.ErrCompactionBusy):
		return http.StatusConflict, protocol.Falha{
			Code: protocol.CodCompactionBusy,
			Message: "uma passada de compaction já está em voo (o laço automático ou outra chamada manual); " +
				"tente novamente em instantes",
		}
	case errors.Is(err, storage.ErrWriterFailed):
		return http.StatusInternalServerError, protocol.Falha{
			Code:    protocol.CodWriterFailed,
			Message: "o writer está em modo falho sticky; reabra o dataset (reinicie o servidor) antes de tentar de novo",
		}
	case errors.Is(err, storage.ErrDatasetCorrupt):
		return http.StatusInternalServerError, protocol.Falha{Code: protocol.CodDatasetCorrompido, Message: err.Error()}
	default:
		return http.StatusInternalServerError, protocol.Falha{Code: protocol.CodInterno, Message: err.Error()}
	}
}

// ErroParcial é a falha de UM dataset no meio de "todos os datasets" (Task
// B5, revisão, Important 3): processaDatasets para no primeiro erro, mas
// não pode ESCONDER que já tocou os datasets anteriores — checkpoint já
// gravou, compaction já moveu disco para eles, e esse efeito colateral
// aconteceu de verdade. Dataset nomeia QUEM falhou; Processados nomeia, NA
// ORDEM em que rodaram, os que tinham tido sucesso ANTES dele.
type ErroParcial struct {
	Dataset     string
	Processados []string
	Err         error
}

// processaDatasets executa fn para cada dataset de alvos, NA ORDEM, e para
// no PRIMEIRO erro — stop-no-primeiro-erro continua sendo a política (uma
// falha de writer/compaction busy no meio da lista não é motivo para tentar
// os próximos, que só reencontrariam o mesmo writer). O que muda (revisão da
// B5, Important 3) é que o erro devolvido nomeia o dataset que falhou E os
// nomes dos que já tinham sido processados com sucesso antes — informação
// que o handler usa para NÃO esconder do operador que o efeito colateral já
// aconteceu para parte da lista.
//
// fn devolve o erro CRU do storage — processaDatasets não traduz status HTTP
// nem código de protocolo, só agrega ordem e nomes; a tradução continua em
// erroDeMutacao, chamada pelo handler com ErroParcial.Err.
func processaDatasets[T any](alvos []*dataset, fn func(*dataset) (T, error)) (processados []T, parcial *ErroParcial) {
	nomes := make([]string, 0, len(alvos))
	for _, d := range alvos {
		v, err := fn(d)
		if err != nil {
			return processados, &ErroParcial{Dataset: d.nome, Processados: nomes, Err: err}
		}
		processados = append(processados, v)
		nomes = append(nomes, d.nome)
	}
	return processados, nil
}

// mensagemParcial decora a mensagem de erroDeMutacao com o dataset que
// falhou e os que já tinham sido processados ANTES dele. "nenhum" quando a
// lista está vazia (falhou no primeiro) — nunca uma lista vazia silenciosa
// que o operador poderia ler como "não sei".
func mensagemParcial(p *ErroParcial, base string) string {
	processados := "nenhum"
	if len(p.Processados) > 0 {
		processados = strings.Join(p.Processados, ", ")
	}
	return fmt.Sprintf("dataset %q: %s (processados com sucesso antes deste: %s)", p.Dataset, base, processados)
}

// handleCheckpoint serve o POST /v1/checkpoint do §96 (ADMIN).
//
// Idempotente por EFEITO, não por resultado: repetir sobre estado inalterado
// custa o stall real da ingestão (Store.Checkpoint toma o mutex do writer de
// verdade) mas não muda manifest nenhum de forma observável. É custo real
// para efeito inócuo — um ADMIN em loop aqui é o mesmo argumento do laço
// automático de checkpoint (store_checkpoint.go): o preço é pago mesmo sem
// nada para checkpointar. Aceito porque é ADMIN, e a doc da rota diz isso.
func (s *Servidor) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.Falha{
			Code:    protocol.CodMetodo,
			Message: fmt.Sprintf("%s não é aceito em /v1/checkpoint; use POST", r.Method),
		})
		return
	}
	req, falha, status := s.leRequisicaoDataset(w, r)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}
	alvos, falha, status := s.datasetsAlvo(req.Dataset)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}

	resultados, parcial := processaDatasets(alvos, func(d *dataset) (protocol.CheckpointDataset, error) {
		inicio := time.Now()
		if err := d.st.Checkpoint(); err != nil {
			return protocol.CheckpointDataset{}, err
		}
		duracao := time.Since(inicio)
		s.log.Dataset(d.nome).Info("checkpoint.manual",
			"checkpoint disparado via POST /v1/checkpoint (ADMIN)", "duracao_ms", duracao.Milliseconds())
		return protocol.CheckpointDataset{Name: d.nome, DurationMs: duracao.Milliseconds()}, nil
	})
	if parcial != nil {
		status, f := erroDeMutacao(parcial.Err)
		f.Message = mensagemParcial(parcial, f.Message)
		s.recusa(w, r, status, parcial.Dataset, f)
		return
	}
	s.respondeJSON(w, protocol.RespostaCheckpoint{Datasets: resultados})
}

// handleCompact serve o POST /v1/compact do §96 (ADMIN).
//
// Chama Store.CompactaUmaVez com o MESMO minimumLiveRatio que o laço
// automático usaria (s.cfg.Compaction(), validado no parse mesmo com
// storage.compaction.enabled: false) — é assim que a rota funciona no modo
// manual puro, o caso de uso central: operador desligou o laço e opera na
// mão. ctx vem de ctxDeMutacao (server.go): um cliente que desiste (timeout,
// conexão caída) cancela a passada em andamento, com segurança (godoc de
// CompactaUmaVez) — e o SERVIDOR encerrando (SIGTERM) faz o mesmo, o que
// r.Context() sozinho não cobre (Important 1 da revisão final).
func (s *Servidor) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.Falha{
			Code:    protocol.CodMetodo,
			Message: fmt.Sprintf("%s não é aceito em /v1/compact; use POST", r.Method),
		})
		return
	}
	req, falha, status := s.leRequisicaoDataset(w, r)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}
	alvos, falha, status := s.datasetsAlvo(req.Dataset)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}
	_, ratio := s.cfg.Compaction()
	ctx, cancela := s.ctxDeMutacao(r)
	defer cancela()

	resultados, parcial := processaDatasets(alvos, func(d *dataset) (protocol.CompactDataset, error) {
		inicio := time.Now()
		res, err := d.st.CompactaUmaVez(ctx, ratio)
		if err != nil {
			return protocol.CompactDataset{}, err
		}
		duracao := time.Since(inicio)
		s.log.Dataset(d.nome).Info("compaction.manual",
			"passada de compaction disparada via POST /v1/compact (ADMIN)",
			"segmentos_aposentados", res.SegmentosAposentados,
			"bytes_lidos", res.BytesLidos, "bytes_escritos", res.BytesEscritos,
			"duracao_ms", duracao.Milliseconds())
		return protocol.CompactDataset{
			Name:            d.nome,
			SegmentsRetired: res.SegmentosAposentados,
			BytesRead:       res.BytesLidos,
			BytesWritten:    res.BytesEscritos,
			DurationMs:      duracao.Milliseconds(),
		}, nil
	})
	if parcial != nil {
		status, f := erroDeMutacao(parcial.Err)
		f.Message = mensagemParcial(parcial, f.Message)
		s.recusa(w, r, status, parcial.Dataset, f)
		return
	}
	s.respondeJSON(w, protocol.RespostaCompact{Datasets: resultados})
}
