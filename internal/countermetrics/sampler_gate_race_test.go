//go:build race

package countermetrics

import "testing"

// Contraparte visível do gate de alocação sob -race: a instrumentação altera a
// contagem, e o pulo se anuncia em vez de sumir da rodada (débito 53).
func TestSampleOnceAndAggregateDoNotAllocate(t *testing.T) {
	t.Skip("allocation gate does not run under -race: the instrumentation changes the count")
}
