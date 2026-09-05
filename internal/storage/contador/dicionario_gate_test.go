//go:build !race

package contador

import (
	"path/filepath"
	"testing"
)

// TestDicionarioNaoAlocaNoLookup: o writer resolve a chave do grupo a cada
// evento, e o caso comum em regime é a chave JÁ existir. Internar chave nova
// aloca (mapa e cópia da chave) e é limitado pelo teto — o que este gate mede
// é o caminho que roda milhões de vezes por hora.
func TestDicionarioNaoAlocaNoLookup(t *testing.T) {
	d := abreDicionario(t, filepath.Join(t.TempDir(), "grupos.dict"), 100)
	chaves := [][]byte{[]byte("PRCTA_G1M10"), []byte("BR_SPOMO_OLT10")}
	for _, ch := range chaves {
		if _, err := d.Interna(ch); err != nil {
			t.Fatal(err)
		}
	}
	i := 0
	if n := testing.AllocsPerRun(1000, func() {
		i ^= 1
		if _, err := d.Interna(chaves[i]); err != nil {
			t.Fatal(err)
		}
		if _, ok := d.PorChave(chaves[i]); !ok {
			t.Fatal("chave internada tem de resolver")
		}
		if _, vivo := d.Chave(uint32(i + 1)); !vivo {
			t.Fatal("grupo tem de estar vivo")
		}
	}); n != 0 {
		t.Errorf("allocs/lookup = %.1f, quer 0", n)
	}
}
