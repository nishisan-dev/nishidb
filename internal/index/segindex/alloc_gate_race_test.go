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

// TestSetNoWIPNaoAloca é o caminho quente da ingestão, e a propriedade medida —
// zero alocação por Set — é o que separa este motor do anterior em RSS.
func TestSetNoWIPNaoAloca(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

// TestLookupNoWIPAlocaExatamenteUmaVez afirma IGUALDADE, e não teto: 0 seria
// "não materializou" e 2 seria "o slice cresceu". Perder essa distinção em
// silêncio numa rodada com -race apagaria o sinal exatamente onde ninguém olha.
func TestLookupNoWIPAlocaExatamenteUmaVez(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

// O gate de CUSTO do Set (custo_gate_test.go) NÃO aparece aqui de propósito:
// ele conta sondas, que a instrumentação não altera, e roda nas duas rodadas.
// Uma entrada de SKIP para ele seria a mentira oposta à do débito 53 — anunciar
// como pulado um gate que está rodando.

// A camada de TEMPO do gate de custo (§6.1), essa sim, mede relógio — e o
// detector de corrida multiplica o custo de cada acesso, o que dobraria a
// razão medida por um fator do instrumento e não do código.
func TestARazaoDeTempoDoSetEntreEscalasEhConstante(t *testing.T) {
	t.Skip("gate de tempo não roda sob -race: a instrumentação altera o que ele mede")
}
