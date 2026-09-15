//go:build race

package counters_test

import "testing"

// Contraparte visível do gate de alocação da tabela virtual sob -race: a
// instrumentação altera a contagem, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestTabelaDoContadorNaoAlocaPorLinha(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

// TestCounterTableDoesNotAllocatePerRowWithMetrics: mesma contraparte, para o
// gate com as métricas de janela presentes.
func TestCounterTableDoesNotAllocatePerRowWithMetrics(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

// TestCounterTableAtRowCeilingDoesNotAllocatePerRow: mesma contraparte, para o
// gate da linha no teto. A conferência das colunas no teto roda sob -race
// (TestCounterTableAtRowCeilingPublishesEveryColumn); só a contagem pula.
func TestCounterTableAtRowCeilingDoesNotAllocatePerRow(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
