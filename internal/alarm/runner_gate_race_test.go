//go:build race

package alarm

import "testing"

// Contraparte visível do gate de alocação da passada sob -race: a
// instrumentação altera a contagem, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestPassadaNaoAlocaPorGrupoSaudavel(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestSweepWithQualifyWhenDoesNotAllocatePerGroup(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
