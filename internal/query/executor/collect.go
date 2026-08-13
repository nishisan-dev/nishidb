package executor

import (
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// slot é um campo que a query precisa ler de cada row. Os IDs vêm PRONTOS do
// binder, em ordem crescente e sem duplicatas, e o executor não os recalcula:
// dois lugares numerando a mesma coisa divergem, e a divergência aparece como
// uma coluna lendo o valor do campo errado — falha silenciosa.
type slot struct {
	id      types.FieldID
	val     types.Value
	present bool
}

func novosSlots(ids []types.FieldID) []slot {
	s := make([]slot, len(ids))
	for i, id := range ids {
		s[i].id = id
	}
	return s
}

// coleta preenche os slots a partir do payload em UMA passada, por merge
// linear. Só funciona porque o payload é ordenado por FieldID e sem
// duplicatas — invariante que o Put valida antes de tornar o record durável.
//
// Erro de leitura ABORTA a query: tratar payload corrompido como "pula a row"
// transformaria corrupção em resultado silenciosamente incompleto.
func coleta(slots []slot, fr *encoding.FieldReader, payload []byte) error {
	for i := range slots {
		slots[i].present = false
	}
	fr.Reset(payload)
	i := 0
	for i < len(slots) {
		id, v, ok := fr.Next()
		if !ok {
			// Fim do payload ou erro; o Err() abaixo distingue.
			return fr.Err()
		}
		for i < len(slots) && slots[i].id < id {
			i++
		}
		if i < len(slots) && slots[i].id == id {
			slots[i].val, slots[i].present = v, true
			i++
		}
	}
	// Saída pelo esgotamento dos slots: os campos restantes do payload não
	// interessam a esta query, e a integridade do record já foi verificada
	// pelo CRC na leitura do segmento.
	return nil
}
