//go:build race

package radiusgen_test

import "testing"

// Contraparte visível do gate sob -race (ver alloc_gate_test.go): o pulo se
// anuncia em vez de o gate sumir em silêncio da rodada com o detector.
func TestNextNaoAloca(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
