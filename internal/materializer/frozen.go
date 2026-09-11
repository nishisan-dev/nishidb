package materializer

import (
	"errors"
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// capturaCongeladas emite as entries das colunas congeladas a partir do que o
// EVENTO trouxe.
//
// Roda depois da extração das derivadas — para que uma congelada possa ter
// origem numa coluna extraída, cujo valor só existe depois daquela passada — e
// antes da ordenação, para que participe do sort e do dedup como qualquer
// outra entry.
//
// Origem ausente do evento, ou presente com null explícito, NÃO emite nada: a
// coluna fica vazia e é preenchida no primeiro evento que trouxer a origem. É
// a semântica de "primeiro valor visto", e é o que torna a feature tolerante a
// fonte que manda evento parcial.
//
// A busca da origem é linear sobre as entries já montadas. Com uma ou duas
// congeladas sobre algumas dezenas de entries é ruído; um dataset que declare
// muitas e cujo bench acuse deve guardar o índice da entry durante o parsing,
// não trocar isto por um mapa alocado por evento.
func (b *PatchBuffer) capturaCongeladas(c *schema.Compiled, agora time.Time) error {
	for _, cf := range c.FrozenColumns {
		if cf.FromClock {
			b.entries = append(b.entries, patchEntry{
				field:     cf,
				val:       types.Value{Type: types.TypeTimestamp, I64: agora.UnixMilli()},
				congelada: true,
			})
			b.count(countCongelada, cf, types.TypeNull)
			continue
		}
		for i := range b.entries {
			e := &b.entries[i]
			if e.field != cf.FrozenFrom || e.isNull {
				continue
			}
			// A congelada de origem AUTO tem FieldID próprio, e portanto tipo
			// próprio a fixar: o valor vem convertido contra o tipo da origem,
			// mas quem lê a coluna depois — binder, planner, executor —
			// consulta o EffectiveType dela. Sem esta fixação a coluna ficaria
			// para sempre sem tipo, gravada com dado dentro e invisível a
			// qualquer WHERE.
			if cf.IsAuto && cf.EffectiveType() == types.TypeNull && e.val.Type != types.TypeNull {
				if err := b.fixaTipo(c, cf, e.val.Type); err != nil {
					if errors.Is(err, errPrecisaOfWriter) {
						return err
					}
					// A classificação é a mesma do laço do Parse, e pela mesma
					// razão: só o CONFLITO é evento ruim.
					if errors.Is(err, fielddict.ErrTypeHintConflict) {
						return fmt.Errorf("campo %q: %w: %w", cf.Name, ErrTypeConversion, err)
					}
					return fmt.Errorf("campo %q: %w", cf.Name, err) // STORAGE_ERROR
				}
			}
			b.entries = append(b.entries, patchEntry{
				field: cf, val: e.val, congelada: true,
			})
			b.count(countCongelada, cf, types.TypeNull)
			break
		}
	}
	return nil
}
