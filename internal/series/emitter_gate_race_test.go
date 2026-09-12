//go:build race

package series

import "testing"

// Contraparte visível do gate de alocação da emissão sob -race: a
// instrumentação altera a contagem, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestPassadaNaoAlocaPorGrupo(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
