//go:build race

package micro

import "testing"

// Contraparte visível dos gates sob -race (ver gates_test.go): o pulo se
// anuncia em vez de o gate sumir em silêncio da rodada com o detector.
func TestGateAllocsApply(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestGateAllocsIngestFake(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
