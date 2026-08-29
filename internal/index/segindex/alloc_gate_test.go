//go:build !race

package segindex_test

import (
	"testing"
)

// TestIterarSeladoNaoAlocaPorLinha: o resultado é fatia contígua do mmap, e
// alocação POR LINHA é sinal de cópia indevida. A medida é da ITERAÇÃO, não do
// Lookup: montar a fonte custa um boxing de interface por segmento, que é
// custo de abertura e não de varredura — medi-los juntos esconderia justamente
// a regressão que este gate existe para pegar.
func TestIterarSeladoNaoAlocaPorLinha(t *testing.T) {
	s := abreSeladoComNPostings(t, 1000)
	defer func() {
		if err := s.Fecha(); err != nil {
			t.Errorf("Fecha: %v", err)
		}
	}()
	it, ok := s.Lookup(7, 100)
	if !ok {
		t.Fatal("Lookup não encontrou")
	}
	// visitadas existe para que o gate não possa passar VAZIO: um Lookup que
	// devolvesse uma fonte de zero linhas mediria um laço que não roda, e
	// "AllocsPerRun = 0" viraria carimbo (débito 53). A contagem é lida depois
	// da medida — incrementá-la não aloca.
	visitadas := 0
	media := testing.AllocsPerRun(100, func() {
		for it.Next() {
			_ = it.RowID()
			visitadas++
		}
		it.(interface{ Rebobina() }).Rebobina()
	})
	if media != 0 {
		t.Errorf("AllocsPerRun = %v, quero 0", media)
	}
	if visitadas < 1000 {
		t.Errorf("a varredura medida visitou %d linhas ao todo — o gate mediu um laço vazio", visitadas)
	}
}
