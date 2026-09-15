//go:build !race

package counters_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestTabelaMetaCountersNaoAlocaPorLinha: os textos da linha são calculados
// na montagem, as estatísticas vêm por valor, e o payload é montado no buffer
// do chamador — nada aloca por linha, como nas outras tabelas virtuais.
func TestTabelaMetaCountersNaoAlocaPorLinha(t *testing.T) {
	a := openCounter(t, defByOLT("a"))
	b := openCounter(t, defByPON("b"))
	populaPON(t, a)
	populaPON(t, b)
	_, source, err := counters.TableMeta([]counters.ListedCounter{a, b})
	if err != nil {
		t.Fatal(err)
	}
	snap := source.OpenSnapshot()
	defer source.CloseSnapshot(snap)
	var buf []byte
	if _, bb, ok, err := source.GetByRowID(snap, 1, buf); err != nil || !ok {
		t.Fatalf("linha 1: %v %v", ok, err)
	} else {
		buf = bb
	}
	id := types.RowID(0)
	if n := testing.AllocsPerRun(1000, func() {
		id = id%2 + 1
		rec, bb, ok, err := source.GetByRowID(snap, id, buf)
		buf = bb
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha %d: %v %v", id, ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha = %.1f, quer 0", n)
	}
	key := []byte("b")
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
