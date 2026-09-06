//go:build race

package cli_test

import "testing"

// Contraparte visível do gate de vazão sob -race: o pulo se anuncia em vez de
// sumir em silêncio da rodada com o detector. O motivo está medido em
// throughput_test.go — sob -race a mutação de checkpoint cai de 1,78x-2,01x para
// 1,12x-1,30x e encosta no ruído de 0,96x-1,13x sem mutação.
func TestVazaoDaIngestaoPeloCLI(t *testing.T) {
	t.Skip("gate de vazão não roda sob -race: a instrumentação infla o Apply e não o checkpoint, " +
		"e a regressão deixa de se separar do ruído")
}
