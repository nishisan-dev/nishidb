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
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
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
func (s *Servidor) leRequisicaoDataset(w http.ResponseWriter, r *http.Request) (protocolo.RequisicaoDataset, *protocolo.Falha, int) {
	var req protocolo.RequisicaoDataset
	r.Body = http.MaxBytesReader(w, r.Body, protocolo.MaxCorpoRequisicao)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return req, nil, http.StatusOK // corpo ausente: aceito, Dataset == ""
		}
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
	return req, nil, http.StatusOK
}

// datasetsAlvo resolve o campo Dataset do corpo contra s.porNome/s.datasets:
// nome presente filtra para UM dataset (404 se desconhecido, o mesmo código
// de /v1/query e /v1/metadata); nome ausente devolve TODOS, na ordem
// determinística da subida.
func (s *Servidor) datasetsAlvo(nome string) ([]*dataset, *protocolo.Falha, int) {
	if nome == "" {
		return s.datasets, nil, http.StatusOK
	}
	d, ok := s.porNome[nome]
	if !ok {
		return nil, &protocolo.Falha{
			Code:    protocolo.CodDatasetDesconhecido,
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
func erroDeMutacao(err error) (int, protocolo.Falha) {
	switch {
	case errors.Is(err, storage.ErrCompactionBusy):
		return http.StatusConflict, protocolo.Falha{
			Code: protocolo.CodCompactionBusy,
			Message: "uma passada de compaction já está em voo (o laço automático ou outra chamada manual); " +
				"tente novamente em instantes",
		}
	case errors.Is(err, storage.ErrWriterFailed):
		return http.StatusInternalServerError, protocolo.Falha{
			Code:    protocolo.CodWriterFailed,
			Message: "o writer está em modo falho sticky; reabra o dataset (reinicie o servidor) antes de tentar de novo",
		}
	case errors.Is(err, storage.ErrDatasetCorrupt):
		return http.StatusInternalServerError, protocolo.Falha{Code: protocolo.CodDatasetCorrompido, Message: err.Error()}
	default:
		return http.StatusInternalServerError, protocolo.Falha{Code: protocolo.CodInterno, Message: err.Error()}
	}
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
		s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocolo.Falha{
			Code:    protocolo.CodMetodo,
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

	out := protocolo.RespostaCheckpoint{Datasets: make([]protocolo.CheckpointDataset, 0, len(alvos))}
	for _, d := range alvos {
		inicio := time.Now()
		if err := d.st.Checkpoint(); err != nil {
			status, f := erroDeMutacao(err)
			s.recusa(w, r, status, d.nome, f)
			return
		}
		duracao := time.Since(inicio)
		out.Datasets = append(out.Datasets, protocolo.CheckpointDataset{
			Name: d.nome, DurationMs: duracao.Milliseconds(),
		})
		s.log.Dataset(d.nome).Info("checkpoint.manual",
			"checkpoint disparado via POST /v1/checkpoint (ADMIN)", "duracao_ms", duracao.Milliseconds())
	}
	s.respondeJSON(w, out)
}

// handleCompact serve o POST /v1/compact do §96 (ADMIN).
//
// Chama Store.CompactaUmaVez com o MESMO minimumLiveRatio que o laço
// automático usaria (s.cfg.Compaction(), validado no parse mesmo com
// storage.compaction.enabled: false) — é assim que a rota funciona no modo
// manual puro, o caso de uso central: operador desligou o laço e opera na
// mão. ctx é r.Context(): um cliente que desiste (timeout, conexão caída)
// cancela a passada em andamento, com segurança (godoc de CompactaUmaVez).
func (s *Servidor) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocolo.Falha{
			Code:    protocolo.CodMetodo,
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

	out := protocolo.RespostaCompact{Datasets: make([]protocolo.CompactDataset, 0, len(alvos))}
	for _, d := range alvos {
		inicio := time.Now()
		res, err := d.st.CompactaUmaVez(r.Context(), ratio)
		if err != nil {
			status, f := erroDeMutacao(err)
			s.recusa(w, r, status, d.nome, f)
			return
		}
		duracao := time.Since(inicio)
		out.Datasets = append(out.Datasets, protocolo.CompactDataset{
			Name:            d.nome,
			SegmentsRetired: res.SegmentosAposentados,
			BytesRead:       res.BytesLidos,
			BytesWritten:    res.BytesEscritos,
			DurationMs:      duracao.Milliseconds(),
		})
		s.log.Dataset(d.nome).Info("compaction.manual",
			"passada de compaction disparada via POST /v1/compact (ADMIN)",
			"segmentos_aposentados", res.SegmentosAposentados,
			"bytes_lidos", res.BytesLidos, "bytes_escritos", res.BytesEscritos,
			"duracao_ms", duracao.Milliseconds())
	}
	s.respondeJSON(w, out)
}
