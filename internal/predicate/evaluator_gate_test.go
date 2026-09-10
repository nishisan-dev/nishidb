//go:build !race

package predicate_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestAvaliadorDePayloadNaoAloca: o avaliador roda a cada evento dentro do
// applyRecord, por contador declarado. Uma alocação aqui é pressão de GC
// permanente numa ingestão 24/7 — e é exatamente o tipo de alocação que só
// aparece com profile na mão, porque nenhum teste de valor a vê.
func TestAvaliadorDePayloadNaoAloca(t *testing.T) {
	c := compila(t, dsContadorYAML)
	prog, slots, err := predicate.CompilaCondicao("x", "authenticated = true AND slot > 0", c)
	if err != nil {
		t.Fatal(err)
	}
	av := predicate.NovoAvaliadorDePayload(prog, slots.IDs)
	payload := payloadDe(t, c, map[string]types.Value{
		"customerId":    {Type: types.TypeString, Bytes: []byte("A")},
		"authenticated": {Type: types.TypeBool, I64: 1},
		"slot":          {Type: types.TypeInt64, I64: 3},
	})
	if n := testing.AllocsPerRun(1000, func() {
		if ok, err := av.Evaluate(payload); err != nil || !ok {
			t.Fatalf("avaliação: %v, %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/avaliação = %.1f, quer 0", n)
	}
}
