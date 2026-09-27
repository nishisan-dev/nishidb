package storage

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestScanLiveFromHonorsRangeAndSnapshotLimit(t *testing.T) {
	s := openStore(t, Options{})
	var ids []types.RowID
	for i := range 6 {
		key := fmt.Sprintf("k%d", i)
		id, _ := mustPut(t, s, key, int64(i+1), statusPayload(t, s, key, "Start", 1), nil)
		ids = append(ids, id)
	}
	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)
	// Nasce DEPOIS do snapshot: nenhum recorte pode alcançá-la.
	mustPut(t, s, "late", 99, statusPayload(t, s, "late", "Start", 1), nil)

	cases := []struct {
		name        string
		from, limit types.RowID
		want        []types.RowID
	}{
		{"faixa interna", ids[2], ids[4], ids[2:4]},
		{"prefixo", 0, ids[2], ids[:2]},
		{"teto além do snapshot", ids[4], ids[5] + 100, ids[4:]},
	}
	for _, c := range cases {
		got := collectRows(t, s.ScanLiveFrom(snap, c.from, c.limit))
		if !slices.Equal(got, c.want) {
			t.Errorf("%s: ScanLiveFrom(%d, %d)=%v, quer %v", c.name, c.from, c.limit, got, c.want)
		}
	}
	if got, want := collectRows(t, s.ScanLive(snap)), ids; !slices.Equal(got, want) {
		t.Errorf("ScanLive=%v, quer %v", got, want)
	}
}

// racingSource é o Store com uma escrita disparada logo DEPOIS de cada
// OpenSnapshot, com o snapshot já fixado: é a corrida entre a ingestão e a
// bloco que o exportador está prestes a copiar. n é a ordem da abertura, a
// partir de 1.
type racingSource struct {
	*Store
	opened *int
	onOpen func(n int)
}

func (r racingSource) OpenSnapshot() types.Snapshot {
	snap := r.Store.OpenSnapshot()
	*r.opened++
	r.onOpen(*r.opened)
	return snap
}

// convergenceWriter aplica, em ordem determinística, updates, inserts, deletes
// e ressurreições sobre um espaço de keys. Só escreve pelo Put: o feed recebe
// exatamente o que o writer publica.
type convergenceWriter struct {
	s      *Store
	prefix string
	keys   int
	step   int
	seq    int64
	// payloads pré-montados: o dicionário de campos não é tocado fora do
	// goroutine de teste.
	payloads map[string][]byte
}

func newConvergenceWriter(t *testing.T, s *Store, prefix string, keys int) *convergenceWriter {
	w := &convergenceWriter{s: s, prefix: prefix, keys: keys, payloads: make(map[string][]byte)}
	for k := range keys + 64 {
		key := fmt.Sprintf("%s%03d", prefix, k)
		for v := range 8 {
			w.payloads[fmt.Sprintf("%s/%d", key, v)] = statusPayload(t, s, key, fmt.Sprintf("v%d", v), int64(v))
		}
	}
	return w
}

// write faz uma escrita e devolve o erro em vez de chamar t.Fatal: roda
// também fora do goroutine do teste.
func (w *convergenceWriter) write() error {
	w.step++
	w.seq++
	key := fmt.Sprintf("%s%03d", w.prefix, (w.step*7)%w.keys)
	switch w.step % 4 {
	case 0:
		// Insert de uma key nova: rowId ≥ teto dos snapshots já abertos.
		key = fmt.Sprintf("%s%03d", w.prefix, w.keys+w.step/4%64)
		fallthrough
	case 1, 2:
		_, _, err := w.s.Put([]byte(key), w.seq, 0, w.payloads[fmt.Sprintf("%s/%d", key, w.step%8)], nil)
		return err
	default:
		prev, _, presence, err := w.s.State([]byte(key), nil)
		if err != nil || presence != types.PresenceLive {
			return err
		}
		_, _, err = w.s.Put([]byte(key), w.seq, types.FlagTombstone, []byte{}, &prev)
		return err
	}
}

// liveState é o estado vivo corrente do Store, key → payload.
func liveState(t *testing.T, s *Store) map[string][]byte {
	t.Helper()
	snap := s.OpenSnapshot()
	defer s.CloseSnapshot(snap)
	out := make(map[string][]byte)
	it := s.ScanLive(snap)
	var buf []byte
	for it.Next() {
		rec, next, live, err := s.GetByRowID(snap, it.RowID(), buf)
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

func TestChunkedSnapshotPlusTailConvergesUnderConcurrentWrites(t *testing.T) {
	var feed *replication.Stream
	s := openStore(t, Options{OnReplicationChange: func(table string, op replication.Operation, rec types.Record, at time.Time) {
		feed.Publish(table, op, rec, at)
	}})
	var err error
	feed, err = replication.New(replication.Options{Interval: 0, MaxMessages: 1000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	// Dois escritores em espaços de keys disjuntos: um determinístico, colado
	// às aberturas de snapshot e às emissões, e um em goroutine própria.
	sync1 := newConvergenceWriter(t, s, "a", 40)
	async := newConvergenceWriter(t, s, "b", 40)
	for k := range 40 {
		for _, w := range []*convergenceWriter{sync1, async} {
			key := fmt.Sprintf("%s%03d", w.prefix, k)
			mustPut(t, s, key, 0, w.payloads[key+"/0"], nil)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	asyncErr := make(chan error, 1)
	wg.Go(func() {
		for ctx.Err() == nil {
			if err := async.write(); err != nil {
				asyncErr <- err
				return
			}
		}
	})

	var syncErr error
	var opened int
	source := racingSource{Store: s, opened: &opened, onOpen: func(n int) {
		var err error
		if n == 1 {
			// A primeira linha do primeiro bloco muda com o snapshot já
			// aberto: o bloco copia a versão ANTERIOR, e só o tail — a partir
			// de um cursor lido antes do snapshot — traz a nova.
			_, _, err = s.Put([]byte("a000"), 1, 0, sync1.payloads["a000/7"], nil)
		} else {
			err = sync1.write()
		}
		if err != nil && syncErr == nil {
			syncErr = err
		}
	}}
	var header replication.SnapshotHeader
	model := make(map[string][]byte)
	err = replication.ExportSnapshot(context.Background(), feed, []replication.SnapshotTable{{Name: "events", Source: source}}, 3,
		func(h replication.SnapshotHeader) error { header = h; return nil },
		func(e replication.Entry) error {
			model[string(e.Record.Key)] = bytes.Clone(e.Record.Payload)
			return sync1.write()
		})
	cancel()
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if syncErr != nil {
		t.Fatal(syncErr)
	}
	select {
	case err := <-asyncErr:
		t.Fatal(err)
	default:
	}
	if async.step == 0 {
		t.Fatal("o escritor concorrente não escreveu nada: o cenário não exercita a corrida")
	}

	// Tail: tudo depois do cursor, na ordem do feed, como o follower aplica.
	after := header.Cursor
	produced := feed.Status().Produced
	if produced <= after {
		t.Fatal("nenhuma mudança depois do cursor: o cenário não exercita o tail")
	}
	for after < produced {
		actx, acancel := context.WithTimeout(context.Background(), 5*time.Second)
		b, release, err := feed.Acquire(actx, header.Epoch, after)
		acancel()
		if err != nil {
			t.Fatalf("Acquire depois de %d: %v", after, err)
		}
		for _, e := range b.Entries {
			if e.Sequence <= after {
				continue
			}
			switch e.Operation {
			case replication.Insert, replication.Update:
				model[string(e.Record.Key)] = bytes.Clone(e.Record.Payload)
			case replication.Delete:
				delete(model, string(e.Record.Key))
			}
		}
		after = b.To
		release()
	}

	want := liveState(t, s)
	for key, payload := range want {
		got, ok := model[key]
		if !ok {
			t.Errorf("key %q viva no master e ausente no snapshot+tail", key)
			continue
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("key %q divergiu: snapshot+tail=%x, master=%x", key, got, payload)
		}
	}
	for key := range model {
		if _, ok := want[key]; !ok {
			t.Errorf("key %q ficou no snapshot+tail e não existe mais no master", key)
		}
	}
}
