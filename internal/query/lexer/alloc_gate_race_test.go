//go:build race

package lexer_test

import "testing"

// Contraparte visível dos gates sob -race: o pulo se anuncia em vez de o gate
// sumir em silêncio da rodada com o detector.
func TestZeroAllocs(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestAllocsCrescemComEscapesNaoComTokens(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
