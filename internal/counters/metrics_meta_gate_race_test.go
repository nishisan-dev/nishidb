//go:build race

package counters_test

import "testing"

// TestCounterMetricsMetaTableDoesNotAllocatePerRow é gate de medida: sob -race
// o runtime aloca por conta própria e o número não é o do produto. Anuncia-se
// como SKIP em vez de sumir da suíte (débito 53).
func TestCounterMetricsMetaTableDoesNotAllocatePerRow(t *testing.T) {
	t.Skip("gate de alocação: só vale sem -race (make test)")
}
