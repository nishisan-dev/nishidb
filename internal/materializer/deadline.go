package materializer

import (
	"fmt"
	"math"

	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// minMortalTTL é o menor `_expiresIn` que o storage lê como prazo: 1 é o
// sentinela de imortal no índice de prazos, e zero ou negativo são gravados
// como imortal. A linha cuja âncora já venceu recebe ESTE valor, nunca a conta
// crua — que daria ≤ 0 e a deixaria viva para sempre.
const minMortalTTL = int64(rowindex.MinMortalTTL)

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
// política de escrita inteira já foi aplicada, por quem a define. A âncora é
// lida do mesmo payload, pela mesma razão.
//
// O campo entrou no patch com valor provisório (emitTTL), e a largura fixa do
// int64 é o que permite trocar os 8 bytes sem remontar o payload.
func (m *Materializer) stampTTL(payload []byte) error {
	ms, err := m.expiresInOf(payload)
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
			if err := m.fr.OverwriteInt64(ms); err != nil {
				return fmt.Errorf("%w: %w", ErrInternalState, err)
			}
			return nil
		}
	}
}

// expiresInOf devolve o `_expiresIn` do record que ESTA escrita grava — o
// prazo contado a partir do `_writtenAt` dele —, avaliando as regras, na
// ordem, sobre o próprio payload final.
//
// A avaliação de cada regra é: a regra de FUNDO casa sempre; o `when`, se
// houver, tem de casar; e a âncora, se houver, tem de estar PRESENTE no record
// — ausente, a regra não casa e a próxima é tentada.
//
// Zero significa "não expira": ou porque uma regra `never` casou, ou porque
// nenhuma casou e o dataset não declara regra de fundo.
func (m *Materializer) expiresInOf(payload []byte) (int64, error) {
	for i := range m.rules {
		r := &m.rules[i]
		if r.Fallback {
			m.metrics.byTTLRule[i].Add(1)
			return r.Duration.Milliseconds(), nil
		}
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
		if r.Anchor == 0 {
			m.metrics.byTTLRule[i].Add(1)
			return r.Duration.Milliseconds(), nil
		}
		anchor, ok, err := m.instantOf(payload, r.Anchor)
		if err != nil {
			return 0, err
		}
		if !ok {
			// Só conta aqui, depois do `when`: a pergunta que a série
			// responde é "a regra teria decidido, e a âncora faltou".
			m.metrics.ttlAnchorAbsent[i].Add(1)
			continue
		}
		// O instante da escrita é o CARIMBADO no record, e não um relógio
		// relido: é ele que o storage soma ao prazo, e a conta tem de ser
		// função do record para que o replay chegue ao mesmo vencimento.
		writtenAt, ok, err := m.instantOf(payload, m.schema.WriteInstantField.ID)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("%w: payload being written has no %s", ErrInternalState, schema.WriteInstantFieldName)
		}
		ms, outcome := anchoredExpiresIn(anchor, writtenAt, r.Duration.Milliseconds())
		switch outcome {
		case anchorInFuture:
			m.metrics.ttlAnchorInFuture.Add(1)
		case anchorExpiredOnWrite:
			m.metrics.ttlExpiredOnWrite.Add(1)
		}
		m.metrics.byTTLRule[i].Add(1)
		return ms, nil
	}
	m.metrics.ttlUnmatched.Add(1)
	return 0, nil
}

// anchorOutcome classifica a conta de uma regra ancorada, para as métricas.
type anchorOutcome uint8

const (
	anchorInRange anchorOutcome = iota
	// anchorInFuture a âncora é posterior à escrita e foi LIMITADA a ela.
	anchorInFuture
	// anchorExpiredOnWrite o vencimento já passou no instante da escrita.
	anchorExpiredOnWrite
)

// anchoredExpiresIn resolve o prazo de uma regra ancorada, contado a partir da
// escrita: `min(anchor, writtenAt) + d − writtenAt`, em millis, com d > 0.
//
// Âncora no FUTURO é limitada ao instante da escrita (prazo = d): um relógio
// de origem adiantado não pode comprar mais vida do que a regra declara, e a
// métrica própria mostra a origem que manda isso.
//
// Âncora VENCIDA (`anchor + d ≤ writtenAt`) dá minMortalTTL, e nunca a conta
// crua: ≤ 0 é imortal para o storage. Pela mesma razão o resto de 1 ms, que
// cairia no sentinela de imortal, sobe para minMortalTTL.
//
// A conta é feita por COMPARAÇÃO, sem somar a âncora a nada: a âncora vem do
// dado e pode ser qualquer int64, e `anchor + d` estouraria perto de
// MaxInt64. `writtenAt − d` só é calculado com a guarda que o impede de dar a
// volta, e `writtenAt − anchor` só quando os dois estão a menos de d um do
// outro.
func anchoredExpiresIn(anchor, writtenAt, d int64) (int64, anchorOutcome) {
	if anchor > writtenAt {
		return d, anchorInFuture
	}
	if writtenAt >= math.MinInt64+d && anchor <= writtenAt-d {
		return minMortalTTL, anchorExpiredOnWrite
	}
	// writtenAt − d < anchor ≤ writtenAt: a diferença está em [0, d).
	ms := d - (writtenAt - anchor)
	if ms < minMortalTTL {
		ms = minMortalTTL
	}
	return ms, anchorInRange
}

// instantOf devolve o valor temporal (epoch millis) do campo id no payload, e
// ok=false quando o campo está ausente. A passada para no primeiro id maior:
// o payload é ordenado por FieldID.
//
// Valor de outro tipo físico é lido como ausente: o schema garante o tipo
// temporal da âncora, e um valor fora dele não descreve instante nenhum.
func (m *Materializer) instantOf(payload []byte, id types.FieldID) (int64, bool, error) {
	m.fr.Reset(payload)
	for {
		fid, v, ok := m.fr.Next()
		if !ok || fid > id {
			if err := m.fr.Err(); err != nil {
				return 0, false, fmt.Errorf("%w: payload being written is unreadable: %w", ErrInternalState, err)
			}
			return 0, false, nil
		}
		if fid == id {
			if v.Type != types.TypeTimestamp && v.Type != types.TypeDateTime {
				return 0, false, nil
			}
			return v.I64, true, nil
		}
	}
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
