package executor

import (
	"bytes"
	"cmp"
	"math"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// compara devolve <0, 0 ou >0 para a ordem TOTAL entre dois valores do mesmo
// tipo físico.
//
// É CÓDIGO NOVO, e as três funções de ordem do predicado (ordemInt, ordemFloat,
// ordemBytes) ficam INTACTAS. A tentação óbvia — reescrevê-las em cima desta —
// não sobrevive ao IEEE-754: ponto flutuante tem um QUARTO desfecho, o
// *unordered*, que um int de três vias não representa. Com NaN à esquerda,
// `!=` é true e `<`, `>` e `=` são todos false — e qualquer ordemFloat derivada
// de um comparador de três vias erra pelo menos um dos quatro. Um teste
// diferencial prova que as duas concordam nos casos não-NaN, que é o que dá
// confiança sem fundir os dois caminhos.
//
// Para a ORDENAÇÃO, ao contrário do predicado, uma ordem total sobre NaN é
// obrigatória — sem ela o sort é instável e pode até entrar em laço. O
// cmp.Compare da stdlib define exatamente isso: NaN antes de tudo, NaN igual a
// NaN, e -0.0 empatando com +0.0 (o desempate por rowID resolve o empate).
func compara(kind binder.CompareKind, a, b types.Value) int {
	switch kind {
	case binder.CompareInt:
		return cmp.Compare(a.I64, b.I64)
	case binder.CompareFloatBits:
		return cmp.Compare(math.Float64frombits(uint64(a.I64)), math.Float64frombits(uint64(b.I64)))
	case binder.CompareBytes:
		return bytes.Compare(a.Bytes, b.Bytes)
	default:
		// CompareUnset e CompareIntAsFloat. O segundo só existe para comparar
		// coluna com LITERAL, e na ordenação não há literal — chegar aqui é bug
		// de bind, e o Open recusa antes (ver montaOrdenador). Empatar tudo em
		// silêncio produziria uma ordem arbitrária com cara de resultado.
		return 0
	}
}
