//go:build race

package kafka

import "testing"

// Contraparte visível do gate sob -race, na mesma doutrina dos gates do
// materializer: a instrumentação do detector altera a contagem de alocações,
// então o gate mediria o instrumento junto com o código. O pulo se ANUNCIA em
// vez de sumir em silêncio da rodada.
func TestPublicarPosicaoNaoAloca(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
