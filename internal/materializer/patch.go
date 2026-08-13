package materializer

import (
	"errors"
	"fmt"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Categorias de erro de ingestão (spec §101). INVALID_JSON vem de
// jsonp.ErrInvalidJSON, SCHEMA_VALIDATION de schema.ErrUnknownField e
// TYPE_CONVERSION de ErrTypeConversion.
var (
	ErrInvalidKey      = errors.New("INVALID_KEY")
	ErrInvalidSequence = errors.New("INVALID_SEQUENCE")
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
//
// Exceção ao last-wins: key e sequence são capturadas DURANTE a conversão,
// antes do dedup, então a primeira ocorrência inválida aborta o evento mesmo
// havendo uma válida à frente — {"id":null,"id":"A"} é INVALID_KEY, não "A".
// O mesmo vale para erro de conversão de qualquer campo. Adiar o abort para
// depois do dedup exigiria adiar a conversão do evento inteiro: preço alto
// no hot path para socorrer evento malformado.
//
// Após erro, os buffers ficam com estado PARCIAL (entries, arena, key e seq
// do ponto em que parou). O caller deve descartar o evento; nada além do
// erro é observável até o próximo Parse, que reseta tudo.
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

		// R11: o tipo de um campo sem declaração é decidido na PRIMEIRA
		// observação e persistido; as seguintes convertem contra ele, não
		// contra o shape do evento. Inferir por evento faria o mesmo campo ter
		// significados diferentes conforme a ordem de chegada — e nenhuma
		// query sobre ele seria confiável.
		target := cf.EffectiveType()
		if cf.IsAuto && target == types.TypeNull {
			// null JSON não fixa tipo: gravaria "sem tipo" e travaria o campo.
			if t := InferType(f); t != types.TypeNull {
				if err := c.FixAutoType(cf, t); err != nil {
					// Só o CONFLITO é evento ruim: o campo mudou de formato na
					// origem. Ele precisa carregar ErrTypeConversion, senão o
					// classify cai no default e PARA a ingestão por causa de um
					// evento.
					//
					// Qualquer outro erro daqui é do DATASET — falha de I/O ao
					// gravar o hint (disco cheio, EIO) ou bug de programação.
					// Embrulhá-lo em ErrTypeConversion o faria virar "evento
					// ruim": o pipeline pularia em silêncio, e para sempre, todo
					// evento que tocasse um campo auto ainda sem tipo, com
					// Failed=0 no painel. Bug e infra nunca entram no balde de
					// dado ruim.
					if errors.Is(err, fielddict.ErrTypeHintConflict) {
						return fmt.Errorf("campo %q: %w: %w", f.Key, ErrTypeConversion, err)
					}
					return fmt.Errorf("campo %q: %w", f.Key, err) // STORAGE_ERROR
				}
				target = t
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
			switch {
			case isNull:
				return fmt.Errorf("%w: campo %q nulo", ErrInvalidKey, cf.Name)
			case len(val.Bytes) == 0:
				// Key vazia é rejeitada pelo Store real (ErrInvalidArgument);
				// barrar aqui evita o evento morrer só na escrita.
				return fmt.Errorf("%w: campo %q vazio", ErrInvalidKey, cf.Name)
			}
			b.key = val.Bytes
		case cf.IsSequence:
			if isNull {
				return fmt.Errorf("%w: campo %q nulo", ErrInvalidSequence, cf.Name)
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

	if b.key == nil {
		return fmt.Errorf("%w: campo %q ausente no evento", ErrInvalidKey, c.Key.Name)
	}
	if c.Seq != nil && !b.hasSeq {
		return fmt.Errorf("%w: campo %q ausente no evento", ErrInvalidSequence, c.Seq.Name)
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
