package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
)

// handleReplicationCursor is the handshake used by a follower before it
// chooses incremental catch-up or a snapshot.
func (s *Server) handleReplicationCursor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.rejectSimples(w, r, http.MethodGet)
		return
	}
	d, ok := s.byNome[r.URL.Query().Get("dataset")]
	if !ok || d.replication == nil {
		s.reject(w, r, http.StatusNotFound, "", protocol.ErrorDetail{Code: protocol.CodeUnknownDataset, Message: "replication is not enabled for this dataset"})
		return
	}
	s.respondeJSON(w, d.replication.Status())
}

// handleReplicationBatch blocks until a batch is available or the request is
// cancelled. A missing/expired cursor is deliberately a conflict: followers
// must perform the snapshot protocol instead of silently skipping data.
//
// O long-poll espera sob o ctx da requisição composto com o do Run
// (mutationContext): o http.Shutdown não cancela requisição em voo, e é o
// encerramento que a solta. Feed fechado e encerramento respondem 410 (o
// master está saindo; o follower desconecta e retoma o tail na volta), e não
// 409, que o mandaria refazer o snapshot.
//
// Com a conferência de rewind de um destino Kafka pendente, responde 503
// antes de ler o feed (ver rejectVerificationPending).
func (s *Server) handleReplicationBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.rejectSimples(w, r, http.MethodGet)
		return
	}
	d, ok := s.byNome[r.URL.Query().Get("dataset")]
	if !ok || d.replication == nil {
		s.reject(w, r, http.StatusNotFound, "", protocol.ErrorDetail{Code: protocol.CodeUnknownDataset, Message: "replication is not enabled for this dataset"})
		return
	}
	epoch := r.URL.Query().Get("epoch")
	after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	if epoch == "" || err != nil {
		s.reject(w, r, http.StatusBadRequest, d.nome, protocol.ErrorDetail{Code: protocol.CodeBadRequest, Message: "epoch and after are required"})
		return
	}
	if d.replication.VerificationPending() {
		s.rejectVerificationPending(w, r, d.nome)
		return
	}
	ctx, cancel := s.mutationContext(r)
	defer cancel()
	b, release, err := d.replication.Acquire(ctx, epoch, after)
	if err != nil {
		status := http.StatusConflict
		code := protocol.CodeInternal
		if errors.Is(err, replication.ErrClosed) || s.shuttingDown() {
			status = http.StatusGone
		}
		s.reject(w, r, status, d.nome, protocol.ErrorDetail{Code: code, Message: err.Error()})
		return
	}
	defer release()
	s.respondeJSON(w, b)
}

// handleReplicationSnapshot exporta, em streaming, a geração lógica corrente do
// dataset e da tabela histórica irmã (só quando ela é publicada: com
// `publishHistory: false` o snapshot leva só a base). O cursor do feed é lido
// antes da varredura (ver replication.ExportSnapshot), e o follower reaplica o
// tail depois dele.
//
// Mesmo ctx do batch: o encerramento interrompe a exportação em voo, que
// segura snapshots do Store, e a resposta que ainda não começou sai 410.
//
// A conferência de rewind pendente também responde 503 aqui: o snapshot da
// geração restaurada seria descartado logo depois pelo rewind, e o follower
// o refaria inteiro na geração nova.
func (s *Server) handleReplicationSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.rejectSimples(w, r, http.MethodGet)
		return
	}
	d, ok := s.byNome[r.URL.Query().Get("dataset")]
	if !ok || d.replication == nil {
		s.reject(w, r, http.StatusNotFound, "", protocol.ErrorDetail{Code: protocol.CodeUnknownDataset, Message: "replication is not enabled for this dataset"})
		return
	}
	if d.replication.VerificationPending() {
		s.rejectVerificationPending(w, r, d.nome)
		return
	}
	tables := make([]replication.SnapshotTable, 0, 2)
	for _, source := range []*dataset{d, d.publishedHistory()} {
		if source == nil {
			continue
		}
		tables = append(tables, replication.SnapshotTable{Name: source.nome, History: source.ehHistorico, Source: source.st})
	}
	s.streamReplicationSnapshot(w, r, d.nome, d.replication, tables, replicationSnapshotChunkRows)
}

// rejectVerificationPending responde 503 ao follower enquanto um publicador
// Kafka do dataset não terminou a conferência de rewind da subida
// (replication.Stream.VerificationPending). O feed retomado de um fechamento
// limpo pode ser um backup restaurado na MESMA geração: servir o tail dele
// antes da conferência deixaria o follower, que já aplicou sequences acima
// da reabertura, receber como continuação sequences reusadas com outro
// conteúdo — o defeito que o débito 141 registra, e que continua só no
// master sem destino Kafka, onde não há quem confira. Com o rewind, a conferência troca a geração antes
// de soltar a pendência, e o follower recebe o 409 da geração nova.
//
// 503 é a desconexão retentável do follower (Client.Follow): ele espera com
// backoff, guarda o cursor e a retomada do tail, e não pede snapshot por
// causa dele. O GET /v1/replication/cursor continua respondendo. O código é
// o INTERNAL, que as outras recusas destas rotas já usam: o follower decide
// pelo status HTTP, e um código novo obrigaria o SQLSTATE no driver JDBC e
// a exceção no client Python para rotas que nenhum dos dois chama. A
// mensagem é o que distingue a recusa.
func (s *Server) rejectVerificationPending(w http.ResponseWriter, r *http.Request, name string) {
	w.Header().Set("Retry-After", "1")
	s.reject(w, r, http.StatusServiceUnavailable, name, protocol.ErrorDetail{
		Code:    protocol.CodeInternal,
		Message: "the replication feed is waiting for the rewind check of its Kafka destinations (the dataset may be a restored backup); retry later",
	})
}

// handleReplicationResync pede o snapshot sob demanda de um destino Kafka do
// dataset: POST /v1/replication/resync?dataset=<d>&destination=<t>, ADMIN.
// É uma rota própria, e não um método a mais no GET /v1/replication/snapshot
// dos followers: aquela exporta o snapshot pela própria conexão, esta só pede
// ao publicador que o publique no tópico.
//
// Responde 202 só DEPOIS de o pedido estar gravado no `<destino>.resync`
// (Publisher.RequestSnapshot): o pedido sobrevive a um reinício que aconteça
// antes do snapshot, e o operador que recebeu o 202 não precisa pedir de
// novo. A gravação que falha responde 500, e nada ficou pedido.
//
// O 202 sai também com o publicador parado (blocked, failed ou stopped): o
// pedido fica gravado e a próxima subida o atende. O corpo leva o estado do
// publicador, e é por ele que o operador sabe se o `reset` vem agora.
//
// Dataset sem feed e destino que não é um `replication.targets` dele saem 404
// com UNKNOWN_DATASET, o código que as outras rotas de replicação já usam
// para "não há replicação aqui": o protocolo não tem um código genérico de
// recurso inexistente, e um código novo obrigaria o SQLSTATE no driver JDBC e
// a exceção no client Python para uma rota que nenhum dos dois chama. As duas
// recusas se distinguem pela mensagem.
func (s *Server) handleReplicationResync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.rejectSimples(w, r, http.MethodPost)
		return
	}
	query := r.URL.Query()
	name, destination := query.Get("dataset"), query.Get("destination")
	if name == "" || destination == "" {
		s.reject(w, r, http.StatusBadRequest, "", protocol.ErrorDetail{Code: protocol.CodeBadRequest, Message: "dataset and destination are required"})
		return
	}
	d, ok := s.byNome[name]
	if !ok || d.replication == nil {
		s.reject(w, r, http.StatusNotFound, "", protocol.ErrorDetail{Code: protocol.CodeUnknownDataset, Message: "replication is not enabled for this dataset"})
		return
	}
	p := d.publisherOf(destination)
	if p == nil {
		s.reject(w, r, http.StatusNotFound, d.nome, protocol.ErrorDetail{
			Code:    protocol.CodeUnknownDataset,
			Message: fmt.Sprintf("destination %q is not a Kafka replication target of dataset %q", destination, d.nome),
		})
		return
	}
	// O estado é lido ANTES do pedido: o pedido acorda o laço, que pode
	// passar a snapshotting a qualquer instante, e o que o operador precisa é
	// o estado do publicador que recebeu o pedido.
	state := p.Stats().State
	if err := p.RequestSnapshot(); err != nil {
		s.reject(w, r, http.StatusInternalServerError, d.nome, protocol.ErrorDetail{
			Code:    protocol.CodeInternal,
			Message: "the snapshot request could not be recorded: " + err.Error(),
		})
		return
	}
	resp := protocol.ReplicationResyncResponse{Dataset: d.nome, Destination: destination, Requested: true, State: state}
	w.Header().Set("Content-Type", protocol.ContentTypeJSON)
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.log.Warn("replication.resync.response_failed", "the snapshot request was recorded, but the response could not be written", "dataset", d.nome, "error", err.Error())
	}
}

// publisherOf devolve o publicador do destino, ou nil se o destino não é um
// `replication.targets` do dataset.
func (d *dataset) publisherOf(destination string) *replicationkafka.Publisher {
	for _, p := range d.publishers {
		if p.Stats().Destination == destination {
			return p
		}
	}
	return nil
}

// shuttingDown diz se o servidor está encerrando (o ctx do Run cancelado). Sem
// Run (teste que chama o handler direto) é false.
func (s *Server) shuttingDown() bool {
	return s.vidaCtx != nil && s.vidaCtx.Err() != nil
}

// replicationSnapshotChunkRows é quantas linhas cada snapshot curto do
// exportador varre, no máximo, antes de fechar; o exportador também fecha o
// bloco pelo seu teto em bytes. Um snapshot aberto retém o journal de versões
// e congela a expiração oportunista e o piso da retenção, então o bloco é
// curto; e a memória da exportação fica limitada a um bloco, em linhas e em
// bytes, qualquer que seja o tamanho do dataset.
const replicationSnapshotChunkRows = 4096

// replicationSnapshotBufferBytes é o buffer de escrita da resposta do
// snapshot: evita uma escrita na conexão por entrada.
const replicationSnapshotBufferBytes = 64 << 10

// streamReplicationSnapshot escreve o snapshot como um replication.Snapshot em
// JSON, entrada por entrada, sem montar a lista em memória.
//
// Antes do primeiro byte, uma falha responde pelo caminho normal de erro.
// Depois dele o status 200 já saiu, e fechar o JSON com "]}" entregaria um
// snapshot truncado e VÁLIDO — que o follower aplicaria e usaria para podar as
// linhas ausentes, apagando dado. Por isso a falha no meio do stream derruba a
// conexão (http.ErrAbortHandler): o decode do follower falha e ele tenta de
// novo, sem podar nada.
func (s *Server) streamReplicationSnapshot(w http.ResponseWriter, r *http.Request, name string, feed *replication.Stream, tables []replication.SnapshotTable, chunkRows int) {
	out := bufio.NewWriterSize(w, replicationSnapshotBufferBytes)
	started := false
	separator := false
	ctx, cancel := s.mutationContext(r)
	defer cancel()
	err := replication.ExportSnapshot(ctx, feed, tables, chunkRows, func(h replication.SnapshotHeader) error {
		epoch, err := json.Marshal(h.Epoch)
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", protocol.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		started = true
		// O bufio.Writer guarda o primeiro erro e o devolve em toda escrita
		// seguinte: basta conferir a última.
		_, _ = out.WriteString(`{"epoch":`)
		_, _ = out.Write(epoch)
		_, _ = out.WriteString(`,"cursor":`)
		_, _ = out.WriteString(strconv.FormatUint(h.Cursor, 10))
		_, err = out.WriteString(`,"entries":[`)
		return err
	}, func(e replication.Entry) error {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if separator {
			_ = out.WriteByte(',')
		}
		separator = true
		_, err = out.Write(b)
		return err
	})
	if err == nil {
		if _, err = out.WriteString("]}\n"); err == nil {
			err = out.Flush()
		}
	}
	if err == nil {
		return
	}
	if !started {
		status := http.StatusInternalServerError
		if s.shuttingDown() {
			status = http.StatusGone
		}
		s.reject(w, r, status, name, protocol.ErrorDetail{Code: protocol.CodeInternal, Message: err.Error()})
		return
	}
	s.log.Warn("replication.snapshot_aborted", "snapshot interrupted mid-stream; connection aborted", "dataset", name, "error", err.Error())
	panic(http.ErrAbortHandler)
}
