//go:build race

package rowindex

import "testing"

// Contraparte visível do gate de alocação sob -race: a instrumentação do
// detector altera a contagem, então o gate mediria o instrumento junto com o
// código. O pulo se ANUNCIA em vez de sumir em silêncio da rodada — é a
// doutrina do débito 53, "gate que passa com a ferramenta ausente é carimbo",
// aplicada à ferramenta que muda o que ela mede.
//
// Este arquivo é o primeiro do gênero em internal/storage: até a agenda, o
// pacote não tinha nenhum AllocsPerRun, e portanto nenhuma contraparte a
// declarar.
func TestAgendaNaoAlocaNoCaminhoQuente(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
