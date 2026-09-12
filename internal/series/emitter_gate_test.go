//go:build !race

package series

import "testing"

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
