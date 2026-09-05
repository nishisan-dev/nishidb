//go:build !race

package contadores_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/contadores"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestTabelaDoContadorNaoAlocaPorLinha: a fonte virtual é lida linha a linha
// pelo executor, e a consulta do alarme varre milhares de grupos por ciclo.
// Chave e texto vêm do dicionário (memória estável); o payload é montado no
// buffer do chamador.
func TestTabelaDoContadorNaoAlocaPorLinha(t *testing.T) {
	c := abreContador(t, defPorPON("pon"))
	populaPON(t, c)
	_, fonte, err := contadores.Tabela(c.Def(), c)
	if err != nil {
		t.Fatal(err)
	}
	snap := fonte.AbreSnapshot()
	defer fonte.FechaSnapshot(snap)
	var buf []byte
	// Aquecimento: o buffer cresce na primeira linha.
	if _, b, ok, err := fonte.GetByRowID(snap, 1, buf); err != nil || !ok {
		t.Fatalf("linha 1: %v %v", ok, err)
	} else {
		buf = b
	}
	id := types.RowID(0)
	if n := testing.AllocsPerRun(1000, func() {
		id = id%4 + 1
		rec, b, ok, err := fonte.GetByRowID(snap, id, buf)
		buf = b
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha %d: %v %v", id, ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha = %.1f, quer 0", n)
	}
	chave := []byte("PRCTA_G1M10|1|12")
	if n := testing.AllocsPerRun(1000, func() {
		_, b, ok, err := fonte.GetByKey(snap, chave, buf)
		buf = b
		if err != nil || !ok {
			t.Fatalf("point lookup: %v %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/point lookup = %.1f, quer 0", n)
	}
}
