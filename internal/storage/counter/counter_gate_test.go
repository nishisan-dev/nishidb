//go:build !race

package counter

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestContadorNaoAlocaNoCaminhoQuente: Transition roda em TODA escrita do
// dataset, por contador declarado, dentro do applyRecord. O gate mede o regime
// — grupos já internados, linhas já presentes no overlay (o mapa não cresce),
// alternando grupo e condição para percorrer os dois ramos de delta. Grupo
// novo aloca (mapa, cópia da chave, texto) e é limitado pelo teto; a primeira
// entrada de uma linha no overlay pode crescer o mapa, e isso é populado fora
// da medição, como a agenda faz com o crescimento da região.
func TestContadorNaoAlocaNoCaminhoQuente(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByPON("pon", 100), false)
	const rows = 1 << 12
	ps := [][]byte{
		payload(t, "A", 1, 1, true),
		payload(t, "B", 2, 3, false),
		payload(t, "A", 1, 1, false),
		payload(t, "B", 2, 3, true),
	}
	for r := types.RowID(0); r < rows; r++ {
		if err := c.Transition(r, ps[int(r)%len(ps)], false); err != nil {
			t.Fatal(err)
		}
	}
	var r types.RowID
	i := 0
	if n := testing.AllocsPerRun(2000, func() {
		r = (r + 1) & (rows - 1)
		i = (i + 1) % len(ps)
		if err := c.Transition(r, ps[i], false); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("allocs/transição = %.1f, quer 0", n)
	}
	t.Run("lapide", func(t *testing.T) {
		if n := testing.AllocsPerRun(1000, func() {
			r = (r + 1) & (rows - 1)
			if err := c.Transition(r, nil, true); err != nil {
				t.Fatal(err)
			}
		}); n != 0 {
			t.Errorf("allocs/lápide = %.1f, quer 0", n)
		}
	})
	t.Run("leitura", func(t *testing.T) {
		if n := testing.AllocsPerRun(1000, func() {
			if _, _, _, _, live := c.Group(1); !live {
				t.Fatal("grupo 1 tem de estar vivo")
			}
			if _, ok := c.ByText([]byte("A|1|1")); !ok {
				t.Fatal("texto tem de resolver")
			}
		}); n != 0 {
			t.Errorf("allocs/leitura = %.1f, quer 0", n)
		}
	})
}

// TestScopedTransitionDoesNotAllocate é o gate acima com `scope` declarado: o
// avaliador do scope roda em toda transição, antes da chave e do where, e o
// gate velho não passa por ele. As linhas alternam dentro e fora do escopo,
// então o caminho que sai cedo (WithoutGroup) e o que segue até o grupo são
// medidos os dois.
func TestScopedTransitionDoesNotAllocate(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLTWithScope("spl", nil), false)
	const rows = 1 << 12
	ps := [][]byte{
		scopedPayload(t, "A", 1, true, true),
		scopedPayload(t, "B", 2, false, false),
		scopedPayload(t, "A", 1, false, true),
		scopedPayload(t, "B", 2, true, true),
		scopedPayload(t, "A", 3, true, false),
	}
	for r := types.RowID(0); r < rows; r++ {
		if err := c.Transition(r, ps[int(r)%len(ps)], false); err != nil {
			t.Fatal(err)
		}
	}
	var r types.RowID
	i := 0
	if n := testing.AllocsPerRun(2000, func() {
		r = (r + 1) & (rows - 1)
		i = (i + 1) % len(ps)
		if err := c.Transition(r, ps[i], false); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("allocs/transição com scope = %.1f, quer 0", n)
	}
}
