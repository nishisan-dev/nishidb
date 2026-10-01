package counter

import (
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func openMeasuredCounter(t testing.TB) (*Counter, []byte, time.Time) {
	t.Helper()
	def := defByOLT("rates", true, 100)
	def.DisableTotal = true
	def.RetainEmptyGroups = time.Second
	def.WriteInstantField = 5
	def.Measures = []MeasureDefinition{{"rate", time.Minute, time.Second}, {"rate_5m", 5 * time.Minute, time.Minute}}
	c, err := Open(t.TempDir(), def, 1024, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.OpenMeasures(0, false, false); err != nil {
		t.Fatal(err)
	}
	c.ActivateMeasures()
	at := time.Unix(1800000000, 0)
	var p []byte
	for _, field := range []struct {
		id    types.FieldID
		value types.Value
	}{
		{cOLT, types.Value{Type: types.TypeString, Bytes: []byte("A")}},
		{cAuth, types.Value{Type: types.TypeBool, I64: 1}},
		{5, types.Value{Type: types.TypeTimestamp, I64: at.UnixMilli()}},
	} {
		p, err = encoding.AppendField(p, field.id, field.value)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return c, p, at
}

func TestMeasuresExpireAtBucketBoundariesAndRetainEmptyGroups(t *testing.T) {
	c, p, at := openMeasuredCounter(t)
	c.UseClock(func() time.Time { return at })
	for range 70000 {
		if err := c.ApplyEvent(0, p, false, true); err != nil {
			t.Fatal(err)
		}
	}
	for g, p := range c.pairs {
		if pairTotal(p) != 0 {
			t.Fatalf("total desativado escreveu %d na metade alta da palavra do grupo %d", pairTotal(p), g)
		}
	}
	for _, d := range []time.Duration{0, 59 * time.Second, time.Minute - time.Millisecond} {
		if n := c.MeasureCount(1, 0, at.Add(d).UnixMilli()); n != 70000 {
			t.Fatalf("ocorrências em %s = %d", d, n)
		}
	}
	if n := c.MeasureCount(1, 0, at.Add(time.Minute).UnixMilli()); n != 0 {
		t.Fatalf("janela vencida = %d", n)
	}
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	cover(c)
	if n, err := c.RetireEmptyGroups(at.Add(time.Minute)); err != nil || n != 0 {
		t.Fatalf("aposentou grupo com taxa de 5m: %d %v", n, err)
	}
	if n, err := c.RetireEmptyGroups(at.Add(5 * time.Minute)); err != nil || n != 1 {
		t.Fatalf("grupo vazio após janela: %d %v", n, err)
	}
	for _, m := range c.measures {
		if len(m.positions) != 0 || len(m.free) != 1 {
			t.Fatal("posição não liberada")
		}
	}
}

func TestMeasuresConcurrentReadersAndWriter(t *testing.T) {
	c, p, at := openMeasuredCounter(t)
	if err := c.ApplyEvent(0, p, false, true); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last int64
			for range 1000 {
				n := c.MeasureCount(1, 0, at.UnixMilli())
				if n < last {
					t.Errorf("contagem retrocedeu: %d < %d", n, last)
				}
				last = n
			}
		}()
	}
	for range 1000 {
		if err := c.ApplyEvent(0, p, false, true); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if n := c.MeasureCount(1, 0, at.UnixMilli()); n != 1001 {
		t.Fatalf("contagem = %d", n)
	}
}
