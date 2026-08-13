package materializer

import (
	"errors"
	"fmt"

	"slices"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Categorias de erro de ingestão (spec §101). INVALID_JSON vem de
// jsonp.ErrInvalidJSON, SCHEMA_VALIDATION de schema.ErrUnknownField e
// TYPE_CONVERSION de ErrTypeConversion.
var (
	ErrInvalidKey      = errors.New("INVALID_KEY: key ausente ou nula")
	ErrInvalidSequence = errors.New("INVALID_SEQUENCE: sequence ausente ou nula")
)

// patchEntry é um campo do evento, resolvido e tipado. val.Bytes aponta
// para o arena do PatchBuffer.
type patchEntry struct {
	field  *schema.CompiledField
	isNull bool
	val    types.Value
}

// PatchBuffer converte um evento JSON em entries tipadas ordenadas por
// FieldID, prontas para o merge/replace. Todos os buffers são reutilizados
// entre eventos (owned pela goroutine do writer; não é thread-safe).
type PatchBuffer struct {
	entries []patchEntry
	arena   []byte
	key     []byte // janela do arena
	seq     int64
	hasSeq  bool
	parser  jsonp.Parser
}

// Parse processa um evento completo em um único passo: tokeniza, resolve
// campos contra o schema (include/exclude/unknown policy), converte tipos
// para o arena, extrai key e sequence, ordena por FieldID e aplica
// last-wins em chaves JSON duplicadas.
func (b *PatchBuffer) Parse(event []byte, c *schema.Compiled) error {
	b.entries = b.entries[:0]
	b.arena = b.arena[:0]
	b.key = nil
	b.seq = 0
	b.hasSeq = false

	b.parser.Reset(event)
	for {
		f, ok, err := b.parser.Next()
		if err != nil {
			return err // jsonp.ErrInvalidJSON
		}
		if !ok {
			break
		}
		cf, found := c.FieldByName(f.Key)
		if !found {
			// Campo excluído não está compilado: cai aqui e o
			// ResolveUnknown o descarta — uma comparação a menos por campo
			// por evento no hot path.
			cf, err = c.ResolveUnknown(f.Key)
			if err != nil {
				return fmt.Errorf("campo %q: %w", f.Key, err) // SCHEMA_VALIDATION
			}
			if cf == nil {
				continue // excluído, ignorado ou universo fechado
			}
		}

		target := cf.Type
		if cf.IsAuto {
			// Tipo inferido por evento (wire format carrega o tipo por
			// campo; a fixação persistente do primeiro tipo observado é
			// decisão do binder/Apply — ver R11 no design).
			target = InferType(f)
			if target == types.TypeNull {
				target = cf.Type // null: alvo irrelevante, Convert curto-circuita
			}
		}
		val, arena, err := Convert(f, target, b.arena)
		if err != nil {
			return fmt.Errorf("campo %q: %w", f.Key, err) // TYPE_CONVERSION
		}
		b.arena = arena

		isNull := val.Type == types.TypeNull
		switch {
		case cf.IsKey:
			if isNull {
				return fmt.Errorf("%w (campo %q nulo)", ErrInvalidKey, cf.Name)
			}
			b.key = val.Bytes
		case cf.IsSequence:
			if isNull {
				return fmt.Errorf("%w (campo %q nulo)", ErrInvalidSequence, cf.Name)
			}
			b.seq = val.I64
			b.hasSeq = true
		}
		b.entries = append(b.entries, patchEntry{field: cf, isNull: isNull, val: val})
	}

	// Ordena por FieldID; sort ESTÁVEL preserva a ordem do evento entre
	// duplicatas, permitindo o last-wins no compact abaixo.
	slices.SortStableFunc(b.entries, func(a, e patchEntry) int {
		switch {
		case a.field.ID < e.field.ID:
			return -1
		case a.field.ID > e.field.ID:
			return 1
		}
		return 0
	})
	b.entries = dedupLastWins(b.entries)

	if len(b.key) == 0 {
		return ErrInvalidKey
	}
	if c.Seq != nil && !b.hasSeq {
		return ErrInvalidSequence
	}
	return nil
}

// dedupLastWins remove duplicatas de FieldID mantendo a ÚLTIMA ocorrência
// (ordem do evento, preservada pelo sort estável). In-place, O(n).
func dedupLastWins(entries []patchEntry) []patchEntry {
	if len(entries) < 2 {
		return entries
	}
	out := entries[:0]
	for i := 0; i < len(entries); i++ {
		if i+1 < len(entries) && entries[i+1].field.ID == entries[i].field.ID {
			continue // há ocorrência mais recente à frente
		}
		out = append(out, entries[i])
	}
	return out
}
