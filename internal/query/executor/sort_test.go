package executor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// bqOrdenada é a query ligada mínima com ORDER BY sobre `nome`.
func bqOrdenada(t *testing.T, c *schema.Compiled) *binder.BoundQuery {
	t.Helper()
	id, nome := campo(t, c, "id"), campo(t, c, "nome")
	return &binder.BoundQuery{
		Dataset: "t",
		Schema:  c,
		Columns: []binder.BoundColumn{{Field: id, Name: "id", Slot: 0}},
		Slots:   binder.Slots{IDs: []types.FieldID{id.ID, nome.ID}},
		OrderBy: []binder.BoundOrderKey{{
			Field: nome, Slot: 1, Kind: binder.CompareBytes, Want: types.TypeString}},
		MaxRows:        100,
		EffectiveLimit: 100,
	}
}

// O orçamento de ordenação RECUSA a query, e não a trunca.
//
// Truncar a ENTRADA do sort daria "as N primeiras por rowId, depois ordenadas",
// que não é prefixo de resposta nenhuma — é resultado errado com cara de certo.
// O teto é forçado por Options porque essa é a única porta que exercita a
// recusa sem gerar um corpus de dezenas de megabytes.
func TestOrcamentoDeOrdenacaoRecusaENaoTrunca(t *testing.T) {
	c := compilaSchema(t)
	st := semeiaParaOrdenar(t, c)
	it, err := executor.Open(context.Background(), bqOrdenada(t, c), planner.FullScan(),
		executor.Options{Store: st, SortMemory: 8})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("o teto foi estourado e a query NÃO falhou: o resultado saiu truncado")
	}
	if !errors.Is(it.Err(), executor.ErrOrcamentoDeOrdenacao) {
		t.Errorf("erro = %v, quer ErrOrcamentoDeOrdenacao", it.Err())
	}
}

// A MESMA query com teto folgado responde inteira e ORDENADA. Sem este, o teste
// acima passaria com um Open quebrado — qualquer erro serviria.
func TestOrcamentoFolgadoOrdenaTudo(t *testing.T) {
	c := compilaSchema(t)
	st := semeiaParaOrdenar(t, c)
	it, err := executor.Open(context.Background(), bqOrdenada(t, c), planner.FullScan(),
		executor.Options{Store: st, SortMemory: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	var ids []string
	for it.Next() {
		ids = append(ids, string(it.Row().Cols()[0].Val.Bytes))
	}
	if it.Err() != nil {
		t.Fatalf("Err: %v", it.Err())
	}
	// nome: c=alfa, a=beta, b=gama ⇒ ordem por nome é c, a, b.
	if len(ids) != 3 || ids[0] != "c" || ids[1] != "a" || ids[2] != "b" {
		t.Errorf("ids = %v, quer [c a b] (ordem por nome)", ids)
	}
}

func semeiaParaOrdenar(t *testing.T, c *schema.Compiled) *fakestorage.Store {
	t.Helper()
	st := fakestorage.New(fakestorage.Indexes{})
	id, nome := campo(t, c, "id"), campo(t, c, "nome")
	for _, r := range []struct{ k, n string }{{"a", "beta"}, {"b", "gama"}, {"c", "alfa"}} {
		var p []byte
		var err error
		primeiro, segundo := id, nome
		vp := types.Value{Type: types.TypeString, Bytes: []byte(r.k)}
		vs := types.Value{Type: types.TypeString, Bytes: []byte(r.n)}
		if segundo.ID < primeiro.ID {
			primeiro, segundo = segundo, primeiro
			vp, vs = vs, vp
		}
		if p, err = encoding.AppendField(p, primeiro.ID, vp); err != nil {
			t.Fatal(err)
		}
		if p, err = encoding.AppendField(p, segundo.ID, vs); err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.Put([]byte(r.k), 1, 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	return st
}
