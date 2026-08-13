//go:build race

package query_test

import "testing"

// Contraparte visível dos gates sob -race: o pulo se anuncia em vez de sumir
// em silêncio da rodada com o detector.
func TestScanAllocsIndependenteDoNumeroDeRows(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestLimitAllocsIndependenteDoNumeroDeRows(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
