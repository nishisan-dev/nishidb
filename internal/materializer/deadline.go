package materializer

import (
	"sort"
	"time"

	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// durationOf devolve a duração de ociosidade que vale para o estado que ESTA
// escrita vai produzir, avaliando as regras sobre o valor PÓS-MERGE de cada
// campo que elas mencionam:
//
//	valor pós-merge de F = valor de F no patch, se presente e não-null
//	                       senão o valor de F no estado anterior
//	                       senão ausente
//
// A leitura do estado anterior é DIRIGIDA: uma passada de FieldReader que
// colhe só os ids que as regras citam. Ela não é a mesma passada do merge de
// propósito — misturar a coleta com o mergeRecords ganharia uma varredura e
// custaria a clareza do arquivo onde a semântica de null mora. Se o bench
// acusar, é lá que a otimização entra, com medição na mão.
//
// Zero significa "não expira": ou porque uma regra `never` casou, ou porque
// nenhuma casou e o dataset não declara regra de fundo.
func (m *Materializer) durationOf(prevPayload []byte, entries []patchEntry) (time.Duration, error) {
	for i := range m.regras {
		r := &m.regras[i]
		if r.Prog == nil {
			m.metrics.byRegra[i].Add(1)
			return r.Duration, nil // regra de fundo: casa com tudo
		}
		slots := m.slots[i]
		if err := m.preencheSlots(slots, prevPayload, entries); err != nil {
			return 0, err
		}
		// O contador de mismatch é nil: a ingestão avalia uma regra por
		// evento e não tem onde pendurar um contador por avaliação.
		if r.Prog.Evaluate(slots, nil) {
			m.metrics.byRegra[i].Add(1)
			return r.Duration, nil
		}
	}
	m.metrics.withoutRegra.Add(1)
	return 0, nil
}

// preencheSlots resolve o valor pós-merge de cada slot: o patch tem prioridade
// e o null explícito apaga, senão vale o estado anterior.
func (m *Materializer) preencheSlots(slots []predicate.Slot, prevPayload []byte, entries []patchEntry) error {
	for i := range slots {
		slots[i].Present = false
	}
	// 1. O estado anterior, em uma passada; para quando os slots acabam.
	m.fr.Reset(prevPayload)
	i := 0
	for i < len(slots) {
		id, v, ok := m.fr.Next()
		if !ok {
			if err := m.fr.Err(); err != nil {
				return err
			}
			break
		}
		for i < len(slots) && slots[i].ID < id {
			i++
		}
		if i < len(slots) && slots[i].ID == id {
			slots[i].Val, slots[i].Present = v, true
			i++
		}
	}
	// 2. O patch por cima. entries está ordenada por FieldID e sem duplicatas
	// neste ponto (o dedup já rodou), mas o laço não depende disso.
	for j := range entries {
		e := &entries[j]
		k, ok := slotOf(slots, e.field.ID)
		if !ok {
			continue
		}
		if e.isNull {
			slots[k].Present = false
			continue
		}
		slots[k].Val, slots[k].Present = e.val, true
	}
	return nil
}

// slotOf acha o slot de um FieldID por busca binária. Os ids vêm ordenados do
// binder, e a lista é curta — uma regra cita dois ou três campos.
func slotOf(slots []predicate.Slot, id types.FieldID) (int, bool) {
	i := sort.Search(len(slots), func(i int) bool { return slots[i].ID >= id })
	if i < len(slots) && slots[i].ID == id {
		return i, true
	}
	return 0, false
}

// emitePrazo acrescenta a entry do campo de sistema com a duração escolhida.
func (b *PatchBuffer) emitePrazo(cf *schema.CompiledField, d time.Duration) {
	b.entries = append(b.entries, patchEntry{
		field: cf,
		val:   types.Value{Type: types.TypeInt64, I64: d.Milliseconds()},
	})
	// A entry entra depois do sort do Parse, então reordena. Como ela é UMA e
	// o resto já está ordenado, uma inserção por deslocamento basta — e é o
	// que evita um sort O(n log n) por evento.
	for i := len(b.entries) - 1; i > 0 && b.entries[i-1].field.ID > b.entries[i].field.ID; i-- {
		b.entries[i-1], b.entries[i] = b.entries[i], b.entries[i-1]
	}
}
