package server

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/types"
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
	b, release, err := d.replication.Acquire(r.Context(), epoch, after)
	if err != nil {
		status := http.StatusConflict
		code := protocol.CodeInternal
		if errors.Is(err, replication.ErrClosed) {
			status = http.StatusGone
		}
		s.reject(w, r, status, d.nome, protocol.ErrorDetail{Code: code, Message: err.Error()})
		return
	}
	defer release()
	s.respondeJSON(w, b)
}

// handleReplicationSnapshot exports a consistent logical generation. The
// Store snapshot freezes row versions while the feed cursor supplies the
// incremental tail. The cursor may be newer than the snapshot; that is safe
// because the follower replays all batches after it.
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
	epoch, cursor := d.replication.Cursor()
	out := replication.Snapshot{Epoch: epoch, Cursor: cursor}
	for _, source := range []*dataset{d, d.irma} {
		if source == nil {
			continue
		}
		if source.ehHistorico {
			out.Entries = append(out.Entries, replication.Entry{
				Table: source.nome, Operation: replication.Retain,
				Record: types.Record{RowID: source.st.RowIDBase()},
			})
		}
		snap := source.st.OpenSnapshot()
		it := source.st.ScanLive(snap)
		var buf []byte
		for it.Next() {
			rec, next, live, e := source.st.GetByRowID(snap, it.RowID(), buf)
			buf = next
			if e != nil {
				_ = it.Close()
				source.st.CloseSnapshot(snap)
				s.reject(w, r, http.StatusInternalServerError, d.nome, protocol.ErrorDetail{Code: protocol.CodeInternal, Message: e.Error()})
				return
			}
			if live {
				out.Entries = append(out.Entries, replication.Entry{Table: source.nome, Operation: replication.Insert, Record: types.Record{RowID: rec.RowID, Sequence: rec.Sequence, Flags: rec.Flags, Key: append([]byte(nil), rec.Key...), Payload: append([]byte(nil), rec.Payload...)}})
			}
		}
		_ = it.Close()
		source.st.CloseSnapshot(snap)
	}
	s.respondeJSON(w, out)
}
