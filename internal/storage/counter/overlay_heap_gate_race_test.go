//go:build race

package counter

import "testing"

// Contraparte visível do gate de heap do overlay sob -race: a instrumentação
// altera o heap medido, e o pulo se anuncia em vez de sumir da rodada
// (débito 53).
func TestOverlayDrainAfterABuildReturnsTheHeap(t *testing.T) {
	t.Skip("gate de heap não roda sob -race: a instrumentação altera o heap medido")
}
