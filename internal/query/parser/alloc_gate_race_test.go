//go:build race

package parser_test

import "testing"

// Contraparte visível do tripwire sob -race: o pulo se anuncia em vez de
// sumir em silêncio da rodada com o detector.
func TestParseAllocsTripwire(t *testing.T) {
	t.Skip("tripwire de alocação não roda sob -race: a instrumentação altera a contagem")
}
