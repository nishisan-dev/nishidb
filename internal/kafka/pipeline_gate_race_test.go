//go:build race

package kafka

import "testing"

// Contraparte visível do gate de alocação sob -race: a instrumentação do
// detector altera a contagem de alocações, então o gate mediria o instrumento
// junto com o código. O pulo se ANUNCIA em vez de sumir em silêncio da rodada —
// gate que desaparece com a ferramenta é carimbo, não gate (débito 53).
//
// Os dois vizinhos deste arquivo — TestAJanelaVemDoLoteDePollNaoDaFila e
// TestAJanelaArredondaParaCima — não medem alocação e por isso NÃO estão aqui:
// eles rodam sob -race normalmente, e duplicá-los como skip esconderia
// cobertura que existe.
func TestEsteiraNaoAlocaNoCaminhoQuente(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
