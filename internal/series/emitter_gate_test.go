//go:build !race

package series

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// TestAPassDoesNotAllocatePerGroup: um contador por PON tem milhares de grupos, e a
// passada roda a cada intervalo declarado. Uma alocação por grupo seria lixo
// proporcional ao inventário multiplicado pela frequência — e este é o gate que
// impede que a montagem da linha volte a alocar (a do alarme já regrediu uma
// vez, num mapa indexado por string).
func TestAPassDoesNotAllocatePerGroup(t *testing.T) {
	gs := make([]group, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, group{olt: "OLT_A", slot: i, total: i * 3, live: true})
	}
	e, _, _ := newTestEmitter(t, newFakeCounter(t, gs...))
	// Uma passada antes da medida, para que os buffers reusados já tenham
	// nascido: o gate mede o regime permanente.
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	got := testing.AllocsPerRun(50, func() {
		if err := e.EmitOnce(context.Background(), t0); err != nil {
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
	var groups []group
	for i := range int64(64) {
		groups = append(groups, group{olt: "A", slot: i, live: true})
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
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(50, func() {
		if err := e.EmitOnce(context.Background(), t0); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("alocações da emissão com medidas = %v, quero zero", n)
	}
}

// discardOutput é o destino que não aloca: o gate abaixo mede a passada, e não
// o destino.
type discardOutput struct{ n int }

func (o *discardOutput) Publish(context.Context, []byte, []byte, time.Time) error {
	o.n++
	return nil
}

// TestAPassWithOutputsDoesNotAllocatePerGroup estende o gate da passada ao
// caminho dos destinos: a chave e o corpo Kafka são montados no scratch, e o
// ctx viaja até o Publish sem alocação nenhuma. Um context.WithTimeout ou um
// context.AfterFunc por grupo na passada reprova aqui.
func TestAPassWithOutputsDoesNotAllocatePerGroup(t *testing.T) {
	gs := make([]group, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, group{olt: "OLT_A", slot: i, total: i * 3, live: true})
	}
	out := &discardOutput{}
	e, err := NewEmitter(Options{
		Dataset: "services", Counter: "by_olt", Columns: []string{"olt", "slot"},
		Reader: newFakeCounter(t, gs...), Outputs: []Output{out, out},
		Dir: t.TempDir(), File: "s.ndjson", Every: time.Minute, MaxBytes: 1 << 24, Keep: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := e.EmitOnce(ctx, t0); err != nil {
		t.Fatal(err)
	}
	got := testing.AllocsPerRun(50, func() {
		if err := e.EmitOnce(ctx, t0); err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Fatalf("a passada com destinos alocou %v vez(es) sobre 64 grupos", got)
	}
	// AllocsPerRun roda a função uma vez a mais para aquecer: 52 passadas.
	if want := 64 * 2 * 52; out.n != want {
		t.Fatalf("o destino recebeu %d amostras, quero %d: o gate não exerceu o caminho", out.n, want)
	}
}
