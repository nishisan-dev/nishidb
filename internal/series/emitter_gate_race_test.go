//go:build race

package series

import "testing"

func TestAPassWithMeasuresDoesNotAllocatePerGroup(t *testing.T) {
	t.Skip("gate de alocação exige execução sem instrumentação de corrida")
}

// Contraparte visível do gate de alocação da emissão sob -race: a
// instrumentação altera a contagem, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestAPassDoesNotAllocatePerGroup(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestAPassWithOutputsDoesNotAllocatePerGroup(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
