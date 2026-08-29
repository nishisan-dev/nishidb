//go:build race

package segindex_test

import "testing"

// Contraparte visível do gate de alocação sob -race: a instrumentação do
// detector altera a contagem, então o gate mediria o instrumento junto com o
// código. O pulo se ANUNCIA em vez de sumir em silêncio da rodada — é a
// doutrina do débito 53, "gate que passa com a ferramenta ausente é carimbo",
// aplicada à ferramenta que muda o que ela mede.
//
// A propriedade medida — zero alocação por linha na varredura do selado — é a
// razão de o Lookup devolver uma fatia do mmap em vez de um slice
// materializado, então perdê-la em silêncio numa rodada com -race apagaria o
// sinal exatamente na configuração em que ninguém repara.
func TestIterarSeladoNaoAlocaPorLinha(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
