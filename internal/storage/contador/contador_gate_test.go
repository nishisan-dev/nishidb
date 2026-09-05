//go:build !race

package contador

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestContadorNaoAlocaNoCaminhoQuente: Transiciona roda em TODA escrita do
// dataset, por contador declarado, dentro do applyRecord. O gate mede o regime
// — grupos já internados, linhas já presentes no overlay (o mapa não cresce),
// alternando grupo e condição para percorrer os dois ramos de delta. Grupo
// novo aloca (mapa, cópia da chave, texto) e é limitado pelo teto; a primeira
// entrada de uma linha no overlay pode crescer o mapa, e isso é populado fora
// da medição, como a agenda faz com o crescimento da região.
func TestContadorNaoAlocaNoCaminhoQuente(t *testing.T) {
	c := abreContador(t, t.TempDir(), defPorPON("pon", 100), false)
	const linhas = 1 << 12
	ps := [][]byte{
		payload(t, "A", 1, 1, true),
		payload(t, "B", 2, 3, false),
		payload(t, "A", 1, 1, false),
		payload(t, "B", 2, 3, true),
	}
	for r := types.RowID(0); r < linhas; r++ {
		if err := c.Transiciona(r, ps[int(r)%len(ps)], false); err != nil {
			t.Fatal(err)
		}
	}
	var r types.RowID
	i := 0
	if n := testing.AllocsPerRun(2000, func() {
		r = (r + 1) & (linhas - 1)
		i = (i + 1) % len(ps)
		if err := c.Transiciona(r, ps[i], false); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("allocs/transição = %.1f, quer 0", n)
	}
	t.Run("lapide", func(t *testing.T) {
		if n := testing.AllocsPerRun(1000, func() {
			r = (r + 1) & (linhas - 1)
			if err := c.Transiciona(r, nil, true); err != nil {
				t.Fatal(err)
			}
		}); n != 0 {
			t.Errorf("allocs/lápide = %.1f, quer 0", n)
		}
	})
	t.Run("leitura", func(t *testing.T) {
		if n := testing.AllocsPerRun(1000, func() {
			if _, _, _, _, vivo := c.Grupo(1); !vivo {
				t.Fatal("grupo 1 tem de estar vivo")
			}
			if _, ok := c.PorTexto([]byte("A|1|1")); !ok {
				t.Fatal("texto tem de resolver")
			}
		}); n != 0 {
			t.Errorf("allocs/leitura = %.1f, quer 0", n)
		}
	})
}
