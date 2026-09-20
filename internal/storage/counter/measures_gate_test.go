//go:build !race

package counter

import "testing"

func TestMeasuresDoNotAllocatePerEventOrRead(t *testing.T) {
	c, p, at := openMeasuredCounter(t)
	if err := c.ApplyEvent(0, p, false, true); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(1000, func() {
		if err := c.ApplyEvent(0, p, false, true); err != nil {
			t.Fatal(err)
		}
		c.MeasureCount(1, 0, at.UnixMilli())
	}); n != 0 {
		t.Fatalf("alocações por evento/leitura = %v, quero zero", n)
	}
}

func BenchmarkCounterMeasures(b *testing.B) {
	for _, measures := range []bool{false, true} {
		name := "membership"
		if measures {
			name = "two_measures"
		}
		b.Run(name, func(b *testing.B) {
			c, p, _ := openMeasuredCounter(b)
			if err := c.ApplyEvent(0, p, false, measures); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := c.ApplyEvent(0, p, false, measures); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
