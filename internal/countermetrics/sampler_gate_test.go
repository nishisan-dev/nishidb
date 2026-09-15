//go:build !race

package countermetrics

import (
	"fmt"
	"testing"
	"time"
)

// TestSampleOnceAndAggregateDoNotAllocate é o gate de alocação do caminho que
// roda para sempre: a passada varre todos os grupos a cada poucos segundos, e
// o SQL e o alarme leem o agregado por linha. Alocar por grupo em qualquer um
// dos dois é lixo proporcional ao inventário.
//
// O regime medido é o permanente: duas passadas antes da medida (blocos em
// heap e arquivo já nascidos), e o relógio anda meio balde por rodada, então
// metade das passadas medidas fecha balde (grava no mmap e relê os 60 slots) e
// metade não.
func TestSampleOnceAndAggregateDoNotAllocate(t *testing.T) {
	h := newHarness(t)
	const groups = 64
	for i := range groups {
		h.counter.add(fmt.Sprintf("OLT_%02d", i), int64(i))
	}
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	h.clock.advance(time.Second)
	sampleOnce(t, s)

	if n := testing.AllocsPerRun(100, func() {
		h.clock.advance(500 * time.Millisecond)
		if err := s.SampleOnce(); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("SampleOnce alocou %v vez(es) por passada sobre %d grupos", n, groups)
	}

	if n := testing.AllocsPerRun(100, func() {
		for id := uint32(1); id <= groups; id++ {
			if samples, _, _, _ := m.Aggregate(id); samples == 0 {
				t.Fatalf("grupo %d sem valor depois das passadas", id)
			}
		}
	}); n != 0 {
		t.Fatalf("Aggregate alocou %v vez(es) por varredura de %d grupos", n, groups)
	}

	if n := testing.AllocsPerRun(100, func() {
		if _, ok := s.Metric(testCounter, testMetric); !ok {
			t.Fatal("métrica declarada não encontrada")
		}
	}); n != 0 {
		t.Fatalf("Metric alocou %v vez(es) por chamada", n)
	}
}
