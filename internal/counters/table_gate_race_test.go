//go:build race

package counters_test

import "testing"

// Contraparte visível do gate de alocação da tabela virtual sob -race: a
// instrumentação altera a contagem, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestTabelaDoContadorNaoAlocaPorLinha(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
