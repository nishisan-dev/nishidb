//go:build !race

package alarm

import (
	"testing"
)

// testingAllocsPerRun é o AllocsPerRun da stdlib, nomeado aqui para que o gate
// diga o que mede.
var testingAllocsPerRun = testing.AllocsPerRun

// TestPassadaNaoAlocaPorGrupoSaudavel é o gate da guarda que evita criar
// tracker para grupo que não satisfaz a condição.
//
// A guarda NÃO é barreira de comportamento — um tracker criado para um grupo
// saudável morreria no delete da mesma chamada, e o mapa ficaria do mesmo
// tamanho. O que ela evita é a ALOCAÇÃO, e é isso que importa: um contador por
// PON tem milhares de grupos saudáveis, e alocar um tracker para cada um a cada
// passada é lixo proporcional ao inventário, não ao problema. Só um gate de
// alocação prova essa guarda; foi uma mutação que mostrou que nenhum teste de
// comportamento poderia.
func TestPassadaNaoAlocaPorGrupoSaudavel(t *testing.T) {
	gs := make([]grupo, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: 500, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	r, _, _ := runnerDeTeste(t, f, "")
	// Uma passada antes da medida, para que os buffers reusados já tenham
	// nascido: o gate mede o regime permanente, não a primeira vez.
	passada(t, r)
	base := allocsOfPass(t, r)
	if base != 0 {
		t.Fatalf("a passada alocou %v vez(es) por rodada sobre 64 grupos saudáveis", base)
	}
}

func allocsOfPass(t *testing.T, r *Runner) float64 {
	t.Helper()
	return testingAllocsPerRun(100, func() {
		if err := r.EvaluateOnce(); err != nil {
			t.Fatal(err)
		}
	})
}
