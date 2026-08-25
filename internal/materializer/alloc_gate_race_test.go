//go:build race

package materializer

import "testing"

// Contraparte visível dos gates sob -race: a instrumentação do detector altera
// a contagem de alocações (ela desfaz a elisão da conversão []byte→string em
// lookup de mapa, entre outras), então o gate mede o instrumento junto com o
// código. O pulo se ANUNCIA em vez de sumir em silêncio da rodada.
func TestExtracaoNaoAloca(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestDescobertaNaoAlocaEmRegime(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
