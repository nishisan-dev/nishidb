//go:build race

package executor_test

import "testing"

// Contraparte visível do gate sob -race: o pulo se anuncia em vez de sumir em
// silêncio da rodada com o detector.
func TestTopNReaproveitaOBufferDaEntradaDespejada(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
