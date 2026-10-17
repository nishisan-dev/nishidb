package replication

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func rec(row uint64, payload string) types.Record {
	return types.Record{RowID: types.RowID(row), Payload: []byte(payload)}
}

func TestSameOperationDeduplicatesButOperationBoundariesDoNot(t *testing.T) {
	s := openTestFeed(t, Options{Interval: time.Minute, MaxMessages: 100, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 3 << 20})
	now := time.Unix(10, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	s.Publish("events", Update, rec(1, "b"), now)
	s.Publish("events", Update, rec(1, "c"), now)
	s.Publish("events", Delete, rec(1, ""), now)
	s.Flush(now, "test")
	b, release, err := s.Acquire(context.Background(), s.epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(b.Entries) != 3 {
		t.Fatalf("entries=%d, want 3", len(b.Entries))
	}
	if b.Entries[0].Operation != Insert || b.Entries[1].Operation != Update || b.Entries[2].Operation != Delete {
		t.Fatalf("unexpected operation order: %#v", b.Entries)
	}
	if got := string(b.Entries[1].Record.Payload); got != "c" {
		t.Fatalf("update payload=%q, want c", got)
	}
}

func TestSameOperationAfterDifferentOperationDoesNotCrossBoundary(t *testing.T) {
	s := openTestFeed(t, Options{Interval: time.Minute, MaxMessages: 100, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 3 << 20})
	now := time.Unix(10, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	s.Publish("events", Update, rec(1, "b"), now)
	s.Publish("events", Insert, rec(1, "c"), now)
	s.Flush(now, "test")
	b, release, err := s.Acquire(context.Background(), s.epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(b.Entries) != 3 {
		t.Fatalf("entries=%d, want 3", len(b.Entries))
	}
	if b.Entries[0].Operation != Insert || b.Entries[1].Operation != Update || b.Entries[2].Operation != Insert {
		t.Fatalf("unexpected operation order: %#v", b.Entries)
	}
}

func TestIntervalFlushWakesFollower(t *testing.T) {
	s := openTestFeed(t, Options{Interval: time.Second, MaxMessages: 100, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 3 << 20})
	epoch, _ := s.Cursor()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready := make(chan *Batch, 1)
	go func() {
		b, release, e := s.Acquire(ctx, epoch, 0)
		if e == nil {
			release()
			ready <- b
		}
	}()
	now := time.Unix(1, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	s.Tick(now.Add(time.Second))
	select {
	case b := <-ready:
		if len(b.Entries) != 1 {
			t.Fatal("unexpected batch")
		}
	case <-ctx.Done():
		t.Fatal("follower was not woken")
	}
}

func TestReplicaSnapshotThenCatchupPublishesOnlyAfterCommit(t *testing.T) {
	var applied []uint64
	committed := false
	r := NewReplica(func(Snapshot) error { return nil }, func(e Entry) error { applied = append(applied, e.Sequence); return nil }, func() error { committed = true; return nil }, nil)
	if err := r.StartSnapshot(Snapshot{Epoch: "e1", Cursor: 2, Entries: []Entry{{Sequence: 0}}}); err != nil {
		t.Fatal(err)
	}
	if committed {
		t.Fatal("generation committed before snapshot finish")
	}
	if err := r.FinishSnapshot(); err != nil {
		t.Fatal(err)
	}
	state, _, _ := r.State()
	if state != ReplicaCatchingUp {
		t.Fatalf("state after snapshot=%s, want catching_up", state)
	}
	b := &Batch{Epoch: "e1", From: 3, To: 4, Entries: []Entry{{Sequence: 4, Operation: Update}}}
	if err := r.ApplyBatch(b); err != nil {
		t.Fatal(err)
	}
	state, epoch, cursor := r.State()
	if !committed || state != ReplicaFollowing || epoch != "e1" || cursor != 4 {
		t.Fatalf("state=%s epoch=%s cursor=%d committed=%v", state, epoch, cursor, committed)
	}
}

func TestReplicaMarksGapForResync(t *testing.T) {
	r := NewReplica(nil, nil, nil, nil)
	if err := r.StartSnapshot(Snapshot{Epoch: "e1", Cursor: 2}); err != nil {
		t.Fatal(err)
	}
	if err := r.FinishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyBatch(&Batch{Epoch: "e1", From: 4, To: 4}); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("err=%v", err)
	}
	state, _, _ := r.State()
	if state != ReplicaResyncRequired {
		t.Fatalf("state=%s", state)
	}
}

func TestClientChoosesSnapshotThenIncrementalBatch(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		switch r.URL.Path {
		case "/v1/replication/cursor":
			_ = enc.Encode(Status{Epoch: "e1", Produced: 2})
		case "/v1/replication/snapshot":
			_ = enc.Encode(Snapshot{Epoch: "e1", Cursor: 1, Entries: []Entry{{Operation: Insert, Record: rec(1, "a")}}})
		case "/v1/replication/batch":
			_ = enc.Encode(Batch{Epoch: "e1", From: 2, To: 2, Entries: []Entry{{Sequence: 2, Operation: Update, Record: rec(1, "b")}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	var applied []Operation
	r := NewReplica(nil, func(e Entry) error { applied = append(applied, e.Operation); return nil }, nil, nil)
	c := Client{BaseURL: srv.URL, Dataset: "events", HTTP: srv.Client(), Replica: r}
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(applied) != 2 || applied[0] != Insert || applied[1] != Update || calls != 5 {
		t.Fatalf("applied=%v calls=%d", applied, calls)
	}
}
