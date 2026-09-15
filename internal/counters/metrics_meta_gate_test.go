//go:build !race

package counters_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestCounterMetricsMetaTableDoesNotAllocatePerRow: os textos da linha são
// calculados na montagem, o leitor é resolvido uma vez, o uso vem por valor, e
// o payload é montado no buffer do chamador — nada aloca por linha, como nas
// outras tabelas virtuais. O gate alterna entre a linha com `sampledAt` e a
// sem (NULL), que têm larguras diferentes.
func TestCounterMetricsMetaTableDoesNotAllocatePerRow(t *testing.T) {
	_, source, err := counters.TableMetaMetrics(metricsMetaSpecs(), metricsMetaSource())
	if err != nil {
		t.Fatal(err)
	}
	snap := source.OpenSnapshot()
	defer source.CloseSnapshot(snap)
	if got := snap.LimitRowID; got != 4 {
		t.Fatalf("LimitRowID = %d, quero 4 (três métricas)", got)
	}
	var buf []byte
	if _, bb, ok, err := source.GetByRowID(snap, 1, buf); err != nil || !ok {
		t.Fatalf("linha 1: %v %v", ok, err)
	} else {
		buf = bb
	}
	id := types.RowID(0)
	if n := testing.AllocsPerRun(1000, func() {
		id = id%3 + 1
		rec, bb, ok, err := source.GetByRowID(snap, id, buf)
		buf = bb
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha %d: %v %v", id, ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha = %.1f, quer 0", n)
	}
	key := []byte("c|auth_24h")
	if n := testing.AllocsPerRun(1000, func() {
		_, bb, ok, err := source.GetByKey(snap, key, buf)
		buf = bb
		if err != nil || !ok {
			t.Fatalf("point lookup: %v %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/point lookup = %.1f, quer 0", n)
	}
}
