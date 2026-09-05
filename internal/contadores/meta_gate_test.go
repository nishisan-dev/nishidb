//go:build !race

package contadores_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/contadores"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestTabelaMetaCountersNaoAlocaPorLinha: os textos da linha são calculados
// na montagem, as estatísticas vêm por valor, e o payload é montado no buffer
// do chamador — nada aloca por linha, como nas outras tabelas virtuais.
func TestTabelaMetaCountersNaoAlocaPorLinha(t *testing.T) {
	a := abreContador(t, defPorOLT("a"))
	b := abreContador(t, defPorPON("b"))
	populaPON(t, a)
	populaPON(t, b)
	_, fonte, err := contadores.TabelaMeta([]contadores.Contado{a, b})
	if err != nil {
		t.Fatal(err)
	}
	snap := fonte.AbreSnapshot()
	defer fonte.FechaSnapshot(snap)
	var buf []byte
	if _, bb, ok, err := fonte.GetByRowID(snap, 1, buf); err != nil || !ok {
		t.Fatalf("linha 1: %v %v", ok, err)
	} else {
		buf = bb
	}
	id := types.RowID(0)
	if n := testing.AllocsPerRun(1000, func() {
		id = id%2 + 1
		rec, bb, ok, err := fonte.GetByRowID(snap, id, buf)
		buf = bb
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha %d: %v %v", id, ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha = %.1f, quer 0", n)
	}
	chave := []byte("b")
	if n := testing.AllocsPerRun(1000, func() {
		_, bb, ok, err := fonte.GetByKey(snap, chave, buf)
		buf = bb
		if err != nil || !ok {
			t.Fatalf("point lookup: %v %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/point lookup = %.1f, quer 0", n)
	}
}
