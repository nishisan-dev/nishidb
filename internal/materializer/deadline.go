package materializer

import (
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// stampTTL decide o prazo sobre o payload FINAL desta escrita — o mesmo que
// vai ao Put — e o grava no `_expiresIn` desse payload, no lugar.
//
// O prazo é decidido sobre o record, e só sobre ele, porque é o record que as
// regras descrevem: "A condição vê o estado PÓS-MERGE". Qualquer
// reimplementação desse estado fora do merge diverge dele em algum canto — e
// divergiu em quatro: a congelada (o merge segura o valor anterior, a entry do
// evento traz o novo), a congelada de relógio (entry com o instante corrente
// em TODA escrita), o `merge: sum` (a entry traz a parcela, o record a soma) e
// a presença (sobre a linha vencida e no `mode: replace` o merge não roda, e o
// estado anterior não existe no record novo). Lendo o payload montado, a
// política de escrita inteira já foi aplicada, por quem a define.
//
// O campo entrou no patch com valor provisório (emitTTL), e a largura fixa do
// int64 é o que permite trocar os 8 bytes sem remontar o payload.
func (m *Materializer) stampTTL(payload []byte) error {
	d, err := m.durationOf(payload)
	if err != nil {
		return err
	}
	id := m.schema.TTLField.ID
	m.fr.Reset(payload)
	for {
		fid, _, ok := m.fr.Next()
		if !ok || fid > id {
			if err := m.fr.Err(); err != nil {
				return fmt.Errorf("%w: payload being written is unreadable: %w", ErrInternalState, err)
			}
			return fmt.Errorf("%w: payload being written has no %s", ErrInternalState, schema.TTLFieldName)
		}
		if fid == id {
			if err := m.fr.OverwriteInt64(d.Milliseconds()); err != nil {
				return fmt.Errorf("%w: %w", ErrInternalState, err)
			}
			return nil
		}
	}
}

// durationOf devolve a duração que vale para o record que ESTA escrita grava,
// avaliando as regras, na ordem, sobre o próprio payload final.
//
// Zero significa "não expira": ou porque uma regra `never` casou, ou porque
// nenhuma casou e o dataset não declara regra de fundo.
func (m *Materializer) durationOf(payload []byte) (time.Duration, error) {
	for i := range m.rules {
		r := &m.rules[i]
		if r.Prog != nil {
			slots := m.slots[i]
			if err := m.fillSlots(slots, payload); err != nil {
				return 0, err
			}
			// O contador de mismatch é nil: a ingestão avalia uma regra por
			// evento e não tem onde pendurar um contador por avaliação.
			if !r.Prog.Evaluate(slots, nil) {
				continue
			}
		}
		// Sem programa é a regra de FUNDO, que casa com tudo.
		m.metrics.byTTLRule[i].Add(1)
		return r.Duration, nil
	}
	m.metrics.ttlUnmatched.Add(1)
	return 0, nil
}

// fillSlots colhe, numa passada sobre o payload que a escrita monta, o valor
// de cada campo que a regra cita. É a mesma coleta do WHERE (predicate.Coleta),
// e o erro dela aqui é nosso: o payload acabou de ser montado por este writer,
// então ilegível é inconsistência interna, nunca evento ruim.
func (m *Materializer) fillSlots(slots []predicate.Slot, payload []byte) error {
	if err := predicate.Coleta(slots, &m.fr, payload); err != nil {
		return fmt.Errorf("%w: payload being written is unreadable: %w", ErrInternalState, err)
	}
	return nil
}

// emitTTL acrescenta a entry do campo de sistema com um valor PROVISÓRIO: o
// prazo de verdade só é decidido sobre o payload montado, e stampTTL o grava
// por cima. A entry precisa existir antes do merge para que o campo ocupe o
// seu lugar no payload, e é a mesma em toda escrita.
func (b *PatchBuffer) emitTTL(cf *schema.CompiledField) {
	b.entries = append(b.entries, patchEntry{
		field: cf,
		val:   types.Value{Type: types.TypeInt64},
	})
	// A entry entra depois do sort do Parse, então reordena. Como ela é UMA e
	// o resto já está ordenado, uma inserção por deslocamento basta — e é o
	// que evita um sort O(n log n) por evento.
	for i := len(b.entries) - 1; i > 0 && b.entries[i-1].field.ID > b.entries[i].field.ID; i-- {
		b.entries[i-1], b.entries[i] = b.entries[i], b.entries[i-1]
	}
}
