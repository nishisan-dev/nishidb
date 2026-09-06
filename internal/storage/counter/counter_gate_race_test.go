//go:build race

package counter

import "testing"

// Contraparte visível do gate de alocação do contador sob -race: a
// instrumentação altera a contagem, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestContadorNaoAlocaNoCaminhoQuente(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
