// Package predicado é o avaliador de expressões booleanas do NishiDB: coleta
// de campos do payload em slots e execução do predicado compilado.
//
// Mora fora do executor porque tem DOIS consumidores: o WHERE de uma consulta
// e a condição de uma regra de TTL. `type = 2` numa regra e `type = 2` numa
// query têm de ser o MESMO código — duas implementações divergiriam em
// silêncio nos casos de coerção, e a divergência apareceria como entidade
// expirando por uma regra que a consulta diz não casar.
package predicate

import (
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Slot é um campo que o avaliador precisa ler de cada row. Os IDs vêm PRONTOS
// do binder, em ordem crescente e sem duplicatas, e ninguém os recalcula:
// dois lugares numerando a mesma coisa divergem, e a divergência aparece como
// uma coluna lendo o valor do campo errado — falha silenciosa.
type Slot struct {
	ID      types.FieldID
	Val     types.Value
	Present bool
}

// NovosSlots monta a lista de slots de um conjunto de ids já ordenado.
func NovosSlots(ids []types.FieldID) []Slot {
	s := make([]Slot, len(ids))
	for i, id := range ids {
		s[i].ID = id
	}
	return s
}

// Coleta preenche os slots a partir do payload em UMA passada, por merge
// linear. Só funciona porque o payload é ordenado por FieldID e sem
// duplicatas — invariante que o Put valida antes de tornar o record durável.
//
// Erro de leitura ABORTA a query: tratar payload corrompido como "pula a row"
// transformaria corrupção em resultado silenciosamente incompleto.
func Coleta(slots []Slot, fr *encoding.FieldReader, payload []byte) error {
	for i := range slots {
		slots[i].Present = false
	}
	fr.Reset(payload)
	i := 0
	for i < len(slots) {
		id, v, ok := fr.Next()
		if !ok {
			// Fim do payload ou erro; o Err() abaixo distingue.
			return fr.Err()
		}
		for i < len(slots) && slots[i].ID < id {
			i++
		}
		if i < len(slots) && slots[i].ID == id {
			slots[i].Val, slots[i].Present = v, true
			i++
		}
	}
	// Saída pelo esgotamento dos slots: os campos restantes do payload não
	// interessam a esta query, e a integridade do record já foi verificada
	// pelo CRC na leitura do segmento.
	return nil
}
