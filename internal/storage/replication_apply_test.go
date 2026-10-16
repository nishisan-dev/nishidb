package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// No follower a compaction roda com TTL e troca payload vencido por lápide sob
// o wmu, enquanto o aplicador da replicação apaga linhas pelo
// ApplyReplication. Os dois montam lápides; se dividirem o buffer do writer
// fora do wmu, o -race acusa a escrita concorrente. Sem -race o teste só prova
// que os dois caminhos terminam sãos.
func TestReplicatedDeleteDoesNotRaceWithCompactionExpiry(t *testing.T) {
	s, rel, _ := storeWithExpirationAndFeed(t, t.TempDir(), 400)

	type sealed struct {
		sec, off uint32
		rec      types.Record
	}
	const rows = 60
	var targets []sealed
	var ids []types.RowID
	for i := range rows {
		key := fmt.Sprintf("exp-%03d", i)
		id, _ := mustPut(t, s, key, 1, payloadWithWrite(t, s, key, "Start", rel.t), nil)
		ids = append(ids, id)
	}
	var deleted []types.RowID
	for i := range rows {
		key := fmt.Sprintf("del-%03d", i)
		id, _ := mustPut(t, s, key, 1, payloadWithWrite(t, s, key, "Start", rel.t), nil)
		deleted = append(deleted, id)
	}
	// Rotaciona até a última linha-alvo ficar num segmento selado.
	lastSec, _ := localizacaoOfRow(t, s, ids[len(ids)-1])
	for i := 0; s.Stats().CurrentSegmentID == lastSec && i < 200; i++ {
		key := fmt.Sprintf("fill-%03d", i)
		mustPut(t, s, key, 1, payloadWithWrite(t, s, key, "B", rel.t), nil)
	}
	if s.Stats().CurrentSegmentID == lastSec {
		t.Fatal("o segmento da última linha-alvo não rotacionou")
	}
	for _, id := range ids {
		sec, off := localizacaoOfRow(t, s, id)
		targets = append(targets, sealed{sec, off, recordIn(t, s, sec, off)})
	}
	rel.advance(2 * time.Hour)

	var wg sync.WaitGroup
	start := make(chan struct{})
	compactErr := make(chan error, 1)
	wg.Go(func() {
		<-start
		for _, x := range targets {
			if _, _, err := s.reapendaSeVivo(x.sec, x.off, x.rec); err != nil {
				compactErr <- err
				return
			}
		}
	})
	applyErr := make(chan error, 1)
	wg.Go(func() {
		<-start
		for i := range rows {
			key := []byte(fmt.Sprintf("del-%03d", i))
			if err := s.ApplyReplication(replication.Entry{Sequence: uint64(i + 2), Operation: replication.Delete, Record: types.Record{Sequence: 2, Key: key}}); err != nil {
				applyErr <- err
				return
			}
		}
	})
	close(start)
	wg.Wait()
	select {
	case err := <-compactErr:
		t.Fatalf("compaction: %v", err)
	case err := <-applyErr:
		t.Fatalf("ApplyReplication: %v", err)
	default:
	}
	for i, id := range deleted {
		if flagsOfLocation(t, s, id)&types.FlagTombstone == 0 {
			t.Fatalf("del-%03d sem lápide depois do delete replicado", i)
		}
	}
	for _, x := range targets {
		if flagsOfLocation(t, s, x.rec.RowID)&types.FlagTombstone == 0 {
			t.Fatalf("a compaction não expirou a linha %d: o cenário não exercita a lápide da compaction", x.rec.RowID)
		}
	}
}
