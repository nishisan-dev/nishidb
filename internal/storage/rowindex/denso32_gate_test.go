//go:build !race

package rowindex

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestDenso32NaoAlocaNoCaminhoQuente: o vetor de grupos é escrito e lido em
// TODA transição de contador, dentro do applyRecord. O gate mede Get e Set
// sobre entradas já cobertas pelo arquivo — a primeira escrita de um bloco
// cresce a região, e isso é I/O, não caminho quente.
func TestDenso32NaoAlocaNoCaminhoQuente(t *testing.T) {
	const linhas = 1 << 16
	d := abreDenso32(t, filepath.Join(t.TempDir(), "g.idx"), linhas)
	for r := types.RowID(0); r < linhas; r++ {
		if err := d.Set(r, uint32(r)); err != nil {
			t.Fatal(err)
		}
	}
	var r types.RowID
	if n := testing.AllocsPerRun(1000, func() {
		r = (r + 1) & (linhas - 1)
		if err := d.Set(r, uint32(r)+1); err != nil {
			t.Fatal(err)
		}
		if _, ok := d.Get(r); !ok {
			t.Fatal("entrada coberta tem de estar gravada")
		}
	}); n != 0 {
		t.Errorf("allocs/transição = %.1f, quer 0", n)
	}
}
