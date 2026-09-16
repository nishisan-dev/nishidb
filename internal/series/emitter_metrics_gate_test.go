//go:build !race

package series

import (
	"testing"
)

// TestAPassWithWindowMetricsDoesNotAllocatePerGroup é o gate de alocação do
// caminho NOVO da linha.
//
// Existe separado de TestPassadaNaoAlocaPorGrupo de propósito: o gate antigo
// mede o emissor SEM métrica nenhuma, e continuaria verde com o nome da coluna
// de janela sendo composto por grupo. Um gate que não exerce o caminho que diz
// cobrir é carimbo (débito 53).
//
// Um dos grupos não tem amostra na janela, porque o null é caminho de código
// tanto quanto o valor.
func TestAPassWithWindowMetricsDoesNotAllocatePerGroup(t *testing.T) {
	gs := make([]grupo, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: i * 3, live: true})
	}
	f := newFakeCounter(t, gs...)

	auth5m := &fakeMetricReader{byGroup: make(map[uint32]fakeAggregate, 64)}
	auth24h := &fakeMetricReader{byGroup: make(map[uint32]fakeAggregate, 64)}
	for id := uint32(1); id <= 64; id++ {
		if id == 7 {
			continue // o grupo que a janela não viu: sai com null
		}
		auth5m.byGroup[id] = fakeAggregate{samples: 12, avg: float64(id) * 1.5}
		auth24h.byGroup[id] = fakeAggregate{samples: 300, minimum: int64(id), maximum: int64(id) * 4}
	}

	e, _ := emitterWithMetrics(t, f, windowMetrics(auth5m, auth24h))
	if err := e.EmitOnce(); err != nil {
		t.Fatal(err)
	}
	got := testing.AllocsPerRun(50, func() {
		if err := e.EmitOnce(); err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Fatalf("a passada com métricas de janela alocou %v vez(es) sobre 64 grupos", got)
	}
}
