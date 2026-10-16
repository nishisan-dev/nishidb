//go:build race

package predicate_test

import "testing"

// Contraparte visível do gate de alocação da comparação entre colunas sob
// -race: a instrumentação altera a contagem, e o pulo se anuncia em vez de
// sumir da rodada (débito 53).
func TestLinearColumnComparisonDoesNotAllocate(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
