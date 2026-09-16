//go:build race

package series

import "testing"

// Contraparte sob -race: o gate de alocação se anuncia como SKIP explícito em
// vez de sumir da lista de testes.
func TestAPassWithWindowMetricsDoesNotAllocatePerGroup(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
