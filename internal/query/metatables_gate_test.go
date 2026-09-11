//go:build !race

package query_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestTabelaMetaTablesNaoAlocaPorLinha: os textos da linha — o endereço, o
// nome, o schema, o namespace e o kind — são calculados UMA vez, quando o
// snapshot congela o catálogo, e o payload é montado no buffer do chamador.
// Nada aloca por linha, como nas outras tabelas virtuais.
//
// O gate mede o Store diretamente, fora do pipeline: é o único jeito de
// separar a alocação da LINHA da alocação por consulta, que o congelamento do
// catálogo faz uma vez e é legítima.
func TestTabelaMetaTablesNaoAlocaPorLinha(t *testing.T) {
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, dataset, schema.CountersNamespace, "por_olt")
	tableVirtualOfTest(t, h.engine, dataset, meta.Namespace, "counters")
	_, source, err := h.engine.MetaTables(dataset)
	if err != nil {
		t.Fatal(err)
	}
	snap := source.OpenSnapshot()
	defer source.CloseSnapshot(snap)
	// A base, o contador, a `_meta.counters` e a `_meta.fields`: o gate
	// alterna entre linhas de largura diferente (o namespace é NULL só na
	// base), que é onde um caminho que alocasse só numa delas apareceria.
	if got := snap.LimitRowID; got != 5 {
		t.Fatalf("LimitRowID = %d, quero 5 (quatro tabelas)", got)
	}
	var buf []byte
	if _, bb, ok, err := source.GetByRowID(snap, 1, buf); err != nil || !ok {
		t.Fatalf("linha 1: %v %v", ok, err)
	} else {
		buf = bb
	}
	id := types.RowID(0)
	if n := testing.AllocsPerRun(1000, func() {
		id = id%4 + 1
		rec, bb, ok, err := source.GetByRowID(snap, id, buf)
		buf = bb
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha %d: %v %v", id, ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha = %.1f, quer 0", n)
	}
	key := []byte("_meta.counters")
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
