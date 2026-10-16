//go:build !race

package predicate_test

import (
	"math"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestLinearColumnComparisonDoesNotAllocate: a condição do alarme é avaliada
// sobre a linha de TODO grupo do contador a cada passada. O caminho da
// comparação entre colunas é novo — dois produtos de 128 bits —, e o gate do
// avaliador de payload não o exercita; por isso ganha o seu. Os valores forçam
// o produto acima de 64 bits e um lado negativo.
func TestLinearColumnComparisonDoesNotAllocate(t *testing.T) {
	c := compile(t, counterRowYAML)
	prog, slots, err := predicate.CompileAlarmCondition("alarms[0].when",
		"total * 4611686018427387904 <= rows AND slot * 3 < total * 2", c)
	if err != nil {
		t.Fatal(err)
	}
	av := predicate.NewAvaliadorOfPayload(prog, slots.IDs)
	payload := payloadOf(t, c, map[string]types.Value{
		"groupKey": {Type: types.TypeString, Bytes: []byte("G")},
		"total":    {Type: types.TypeInt64, I64: 1},
		"rows":     {Type: types.TypeInt64, I64: math.MaxInt64},
		"slot":     {Type: types.TypeInt64, I64: -3},
	})
	if n := testing.AllocsPerRun(1000, func() {
		if ok, err := av.Evaluate(payload); err != nil || !ok {
			t.Fatalf("avaliação: %v, %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/avaliação = %.1f, quero 0", n)
	}
}
