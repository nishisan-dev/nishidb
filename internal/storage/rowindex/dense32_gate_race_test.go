//go:build race

package rowindex

import "testing"

// Contraparte visível do gate de alocação do Denso32 sob -race, pela mesma
// razão da agenda: a instrumentação altera a contagem, e o pulo se anuncia em
// vez de sumir da rodada (débito 53).
func TestDenso32NaoAlocaNoCaminhoQuente(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
