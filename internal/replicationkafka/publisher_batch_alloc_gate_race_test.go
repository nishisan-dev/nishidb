//go:build race

package replicationkafka

import "testing"

// Contraparte visível do gate sob -race: o pulo se anuncia em vez de sumir em
// silêncio da rodada com o detector.
func TestRecordHeadersAllocateNothing(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
