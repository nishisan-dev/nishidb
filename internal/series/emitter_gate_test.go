//go:build !race

package series

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// TestPassadaNaoAlocaPorGrupo: um contador por PON tem milhares de grupos, e a
// passada roda a cada intervalo declarado. Uma alocação por grupo seria lixo
// proporcional ao inventário multiplicado pela frequência — e este é o gate que
// impede que a montagem da linha volte a alocar (a do alarme já regrediu uma
// vez, num mapa indexado por string).
func TestPassadaNaoAlocaPorGrupo(t *testing.T) {
	gs := make([]grupo, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: i * 3, live: true})
	}
	e, _, _ := emitterDeTeste(t, newFakeCounter(t, gs...))
	// Uma passada antes da medida, para que os buffers reusados já tenham
	// nascido: o gate mede o regime permanente.
	if err := e.EmitOnce(); err != nil {
		t.Fatal(err)
	}
	got := testing.AllocsPerRun(50, func() {
		if err := e.EmitOnce(); err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Fatalf("a passada alocou %v vez(es) sobre 64 grupos", got)
	}
}

type measuredReader struct{ *fakeCounter }

func (r measuredReader) MeasureCount(id uint32, _ int, _ int64) int64 { return int64(id) * 30 }

func TestAPassWithMeasuresDoesNotAllocatePerGroup(t *testing.T) {
	var groups []grupo
	for i := range int64(64) {
		groups = append(groups, grupo{olt: "A", slot: i, live: true})
	}
	f := measuredReader{newFakeCounter(t, groups...)}
	e, err := NewEmitter(Options{
		Dataset: "services", Counter: "rates", Columns: []string{"olt", "slot"},
		DisableTotal: true, Measures: []schema.CounterMeasureSpec{{Name: "event_rate", Window: time.Minute, Unit: "second"}},
		Reader: f, Dir: t.TempDir(), File: "rates.ndjson", Every: time.Second, MaxBytes: 1 << 24, Keep: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.EmitOnce(); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(50, func() {
		if err := e.EmitOnce(); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("alocações da emissão com medidas = %v, quero zero", n)
	}
}
