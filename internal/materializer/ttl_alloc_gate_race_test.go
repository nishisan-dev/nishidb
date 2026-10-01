//go:build race

package materializer

import "testing"

// Contrapartes visíveis sob -race: a instrumentação altera a contagem de
// alocações, e o pulo se anuncia em vez de sumir da rodada.
func TestTTLDecisionDoesNotAllocate(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestTTLAnchorDecisionDoesNotAllocate(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
