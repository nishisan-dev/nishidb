//go:build race

package materializer

import "testing"

// Contraparte visível sob -race: a instrumentação altera a contagem de
// alocações, e o pulo se anuncia em vez de sumir da rodada.
func TestTTLDecisionDoesNotAllocate(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
