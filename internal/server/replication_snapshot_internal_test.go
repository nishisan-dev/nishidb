package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// failingSnapshotSource entrega linhas grandes e falha no GetByRowID da linha
// failAt, depois de a resposta já ter passado do buffer de escrita.
type failingSnapshotSource struct {
	rows   int
	failAt types.RowID
	open   int
}

func (f *failingSnapshotSource) OpenSnapshot() types.Snapshot {
	f.open++
	return types.Snapshot{Epoch: 1, Instant: time.Unix(1_790_000_000, 0), LimitRowID: types.RowID(f.rows)}
}

func (f *failingSnapshotSource) CloseSnapshot(types.Snapshot) { f.open-- }

func (f *failingSnapshotSource) ScanLiveFrom(snap types.Snapshot, from, limit types.RowID) types.RowIDIterator {
	return &rangeIterator{next: max(from, snap.BaseRowID), limit: min(limit, snap.LimitRowID)}
}

func (f *failingSnapshotSource) GetByRowID(_ types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if id == f.failAt {
		return types.Record{}, buf, false, storage.ErrExpiredSnapshot
	}
	return types.Record{RowID: id, Sequence: 1, Key: []byte(fmt.Sprintf("k%d", id)), Payload: bytes.Repeat([]byte{'x'}, 32<<10)}, buf, true, nil
}

type rangeIterator struct{ next, limit, cur types.RowID }

func (it *rangeIterator) Next() bool {
	if it.next >= it.limit {
		return false
	}
	it.cur = it.next
	it.next++
	return true
}
func (it *rangeIterator) RowID() types.RowID { return it.cur }
func (it *rangeIterator) Err() error         { return nil }
func (it *rangeIterator) Close() error       { return nil }

func newReplicationFeed(t *testing.T) *replication.Stream {
	t.Helper()
	feed, err := replication.Open(t.TempDir(), replication.Options{Interval: 0, MaxMessages: 1000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 64 << 20, MaxDiskBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Resume(replication.Fingerprint{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = feed.Close(nil, 5*time.Second); feed.Wait() })
	return feed
}

func TestHTTPSnapshotAbortsConnectionOnMidStreamError(t *testing.T) {
	feed := newReplicationFeed(t)
	src := &failingSnapshotSource{rows: 10, failAt: 6}
	s := &Server{log: logging.Discard()}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/replication/cursor", func(w http.ResponseWriter, _ *http.Request) {
		s.respondeJSON(w, feed.Status())
	})
	mux.HandleFunc("/v1/replication/snapshot", func(w http.ResponseWriter, r *http.Request) {
		s.streamReplicationSnapshot(w, r, "events", feed, []replication.SnapshotTable{{Name: "events", Source: src}}, 2)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// O status 200 já saiu quando a falha acontece: seis linhas de 32 KiB
	// passam do buffer de escrita antes da linha que falha.
	res, err := srv.Client().Get(srv.URL + "/v1/replication/snapshot?dataset=events")
	if err != nil {
		t.Fatalf("a falha tinha de vir no MEIO do corpo, e não antes do cabeçalho: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, quer 200: o cenário não exercita a falha depois do primeiro byte", res.StatusCode)
	}
	var snap replication.Snapshot
	decodeErr := json.NewDecoder(res.Body).Decode(&snap)
	_ = res.Body.Close()
	if decodeErr == nil {
		t.Fatalf("snapshot truncado decodificou como válido com %d entradas: o follower podaria as linhas ausentes", len(snap.Entries))
	}

	// O follower de verdade: o snapshot quebrado nunca chega ao begin, então
	// nada é aplicado nem podado.
	begun := 0
	replica := replication.NewReplica(func(replication.Snapshot) error { begun++; return nil },
		func(replication.Entry) error { return nil }, nil, nil)
	client := replication.Client{BaseURL: srv.URL, Dataset: "events", HTTP: srv.Client(), Replica: replica}
	if err := client.SyncOnce(context.Background()); err == nil {
		t.Fatal("SyncOnce aceitou um snapshot truncado")
	}
	if begun != 0 {
		t.Fatalf("o follower começou a aplicar %d snapshot(s) truncado(s)", begun)
	}
	if state, _, _ := replica.State(); state != replication.ReplicaEmpty {
		t.Fatalf("estado do follower=%s, quer %s", state, replication.ReplicaEmpty)
	}
	if src.open != 0 {
		t.Fatalf("%d snapshot(s) ficaram abertos depois da falha", src.open)
	}
}

func TestHTTPSnapshotRejectsBeforeFirstByte(t *testing.T) {
	feed := newReplicationFeed(t)
	src := &failingSnapshotSource{rows: 4, failAt: 1}
	s := &Server{log: logging.Discard(), met: registerMetricsOfQuery(metrics.NewRegistry(), nil, nil, nil)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.streamReplicationSnapshot(w, r, "events", feed, []replication.SnapshotTable{{Name: "events", Source: src}}, 10)
	}))
	defer srv.Close()
	res, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status=%d, quer 500 para falha antes do primeiro byte", res.StatusCode)
	}
}

// replicationTestStore abre um Store com os campos id e status no dicionário,
// na mesma ordem em master e follower.
func replicationTestStore(t *testing.T) *storage.Store {
	t.Helper()
	st, err := storage.Open(storage.Options{Dir: t.TempDir(), SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for _, name := range []string{"id", "status"} {
		if _, err := st.FieldDict().LookupOrCreate([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func replicationTestPayload(t *testing.T, st *storage.Store, key, status string) []byte {
	t.Helper()
	var p []byte
	for _, f := range []struct{ name, value string }{{"id", key}, {"status", status}} {
		id, err := st.FieldDict().LookupOrCreate([]byte(f.name))
		if err != nil {
			t.Fatal(err)
		}
		if p, err = encoding.AppendField(p, id, types.Value{Type: types.TypeString, Bytes: []byte(f.value)}); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func replicationLiveState(t *testing.T, st *storage.Store) map[string][]byte {
	t.Helper()
	snap := st.OpenSnapshot()
	defer st.CloseSnapshot(snap)
	out := make(map[string][]byte)
	it := st.ScanLive(snap)
	var buf []byte
	for it.Next() {
		rec, next, live, err := st.GetByRowID(snap, it.RowID(), buf)
		buf = next
		if err != nil {
			t.Fatal(err)
		}
		if live {
			out[string(rec.Key)] = bytes.Clone(rec.Payload)
		}
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestHTTPSnapshotWireCompatibleWithFollower(t *testing.T) {
	var feed *replication.Stream
	master, err := storage.Open(storage.Options{Dir: t.TempDir(), SegmentSize: 1 << 20,
		OnReplicationChange: func(table string, op replication.Operation, rec types.Record, at time.Time) {
			feed.Publish(table, op, rec, at)
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = master.Close() })
	feed = newReplicationFeed(t)
	for _, name := range []string{"id", "status"} {
		if _, err := master.FieldDict().LookupOrCreate([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 20 {
		key := fmt.Sprintf("k%02d", i)
		if _, _, err := master.Put([]byte(key), int64(i), 0, replicationTestPayload(t, master, key, "Start"), nil); err != nil {
			t.Fatal(err)
		}
	}
	d := &dataset{nome: "events", st: master, replication: feed}
	s := &Server{log: logging.Discard(), byNome: map[string]*dataset{"events": d}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/replication/cursor", s.handleReplicationCursor)
	mux.HandleFunc("/v1/replication/snapshot", s.handleReplicationSnapshot)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// O corpo em streaming decodifica no MESMO replication.Snapshot de antes.
	res, err := srv.Client().Get(srv.URL + "/v1/replication/snapshot?dataset=events")
	if err != nil {
		t.Fatal(err)
	}
	var snap replication.Snapshot
	decodeErr := json.NewDecoder(res.Body).Decode(&snap)
	_ = res.Body.Close()
	if decodeErr != nil {
		t.Fatalf("o corpo não decodifica em replication.Snapshot: %v", decodeErr)
	}
	epoch, cursor := feed.Cursor()
	if snap.Epoch != epoch || snap.Cursor != cursor || len(snap.Entries) != 20 {
		t.Fatalf("snapshot epoch=%q cursor=%d entradas=%d, quer %q %d 20", snap.Epoch, snap.Cursor, len(snap.Entries), epoch, cursor)
	}
	for _, e := range snap.Entries {
		if e.Sequence != cursor || e.Table != "events" || e.Operation != replication.Insert || e.At.IsZero() {
			t.Fatalf("entrada fora do contrato: %+v", e)
		}
	}

	// E o follower de verdade aplica: SyncOnce → snapshot → writer do Store.
	follower := replicationTestStore(t)
	replica := replication.NewReplica(nil, follower.ApplyReplication, nil, nil)
	client := replication.Client{BaseURL: srv.URL, Dataset: "events", HTTP: srv.Client(), Replica: replica}
	if err := client.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, gotEpoch, applied := replica.State(); state != replication.ReplicaFollowing || gotEpoch != epoch || applied != cursor {
		t.Fatalf("follower state=%s epoch=%q applied=%d, quer following %q %d", state, gotEpoch, applied, epoch, cursor)
	}
	want, got := replicationLiveState(t, master), replicationLiveState(t, follower)
	if len(got) != len(want) {
		t.Fatalf("follower com %d linhas, master com %d", len(got), len(want))
	}
	for key, payload := range want {
		if !bytes.Equal(got[key], payload) {
			t.Fatalf("key %q divergiu: follower=%x master=%x", key, got[key], payload)
		}
	}
}

// emptySnapshotSource é uma tabela sem linha nenhuma.
type emptySnapshotSource struct{}

func (emptySnapshotSource) OpenSnapshot() types.Snapshot {
	return types.Snapshot{Epoch: 1, Instant: time.Unix(1_790_000_000, 0)}
}
func (emptySnapshotSource) CloseSnapshot(types.Snapshot) {}
func (emptySnapshotSource) ScanLiveFrom(types.Snapshot, types.RowID, types.RowID) types.RowIDIterator {
	return &rangeIterator{}
}
func (emptySnapshotSource) GetByRowID(types.Snapshot, types.RowID, []byte) (types.Record, []byte, bool, error) {
	return types.Record{}, nil, false, nil
}

// Base vazia: o begin sai (200) sem entrada nenhuma, e a histórica falha no
// primeiro bloco. A falha já é "depois do primeiro byte" — nada decodificável
// pode chegar ao follower.
func TestHTTPSnapshotAbortsWhenFailureFollowsBeginWithoutEntries(t *testing.T) {
	feed := newReplicationFeed(t)
	hist := &failingSnapshotSource{rows: 4, failAt: 0}
	s := &Server{log: logging.Discard(), met: registerMetricsOfQuery(metrics.NewRegistry(), nil, nil, nil)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.streamReplicationSnapshot(w, r, "events", feed, []replication.SnapshotTable{
			{Name: "events", Source: emptySnapshotSource{}},
			{Name: "events_history", History: true, Source: hist},
		}, 10)
	}))
	defer srv.Close()
	res, err := srv.Client().Get(srv.URL)
	if err != nil {
		return // conexão derrubada antes de qualquer byte: o esperado
	}
	defer func() { _ = res.Body.Close() }()
	var snap replication.Snapshot
	if derr := json.NewDecoder(res.Body).Decode(&snap); derr == nil && res.StatusCode == http.StatusOK {
		t.Fatalf("status 200 com corpo decodificável (epoch=%q, %d entradas): a conexão não caiu", snap.Epoch, len(snap.Entries))
	}
}

// O snapshot HTTP pedido com o servidor encerrando (o ctx do Run cancelado)
// não exporta nada e responde 410, como o long-poll do batch: o master está
// saindo, e o follower desconecta em vez de tratar a recusa como falha do
// snapshot. Com o servidor no ar, o mesmo handler responde 200 — o par que
// prova que o 410 vem do encerramento.
func TestHTTPSnapshotAnswersGoneWhileShuttingDown(t *testing.T) {
	feed := newReplicationFeed(t)
	for _, tc := range []struct {
		name     string
		shutdown bool
		want     int
	}{{"running", false, http.StatusOK}, {"shutting_down", true, http.StatusGone}} {
		t.Run(tc.name, func(t *testing.T) {
			life, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.shutdown {
				cancel()
			}
			s := &Server{log: logging.Discard(), met: registerMetricsOfQuery(metrics.NewRegistry(), nil, nil, nil), vidaCtx: life}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s.streamReplicationSnapshot(w, r, "events", feed, []replication.SnapshotTable{{Name: "events", Source: emptySnapshotSource{}}}, 10)
			}))
			defer srv.Close()
			res, err := srv.Client().Get(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			_ = res.Body.Close()
			if res.StatusCode != tc.want {
				t.Fatalf("status=%d, quer %d", res.StatusCode, tc.want)
			}
		})
	}
}
