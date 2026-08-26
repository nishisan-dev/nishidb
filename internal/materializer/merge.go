package materializer

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrInternalState marca inconsistência que NÃO é culpa do evento: estado
// anterior corrompido (veio do storage) ou patch fora de ordem (bug do
// PatchBuffer). Existe separada das 5 categorias §101 porque contabilizar um
// bug nosso como "evento inválido" o esconderia exatamente na métrica que o
// operador olha para decidir que a fonte está ruim.
var ErrInternalState = errors.New("INTERNAL_STATE")

// mergeRecords produz o payload materializado novo a partir do payload
// anterior e das entries do patch (§2.6, spec §22/§24).
//
// Os dois lados chegam ordenados por FieldID, então um merge linear de duas
// listas resolve tudo em O(n+m) com uma única passada e sem estruturas
// auxiliares. Campo do prev que o patch não menciona é copiado CRU — sem
// decodificar e recodificar —, que é o caso comum num patch de accounting
// (35 campos no estado, 8 no evento).
//
// Semântica NULL (D2/§24): entry com isNull remove o campo do estado novo;
// campo ausente do patch preserva o valor anterior. Não existe terceiro
// estado — um patch posterior que também não mencione o campo mantém a
// ausência, e o executor lê ausência como NULL.
//
// Entry CONGELADA é a terceira política de escrita: o campo já presente no
// estado anterior é preservado e o valor novo é descartado. Ela nunca chega
// aqui com isNull — a captura não emite congelada de origem nula.
//
// out é usado no estilo append (como encoding.AppendField): passe out[:0]
// para reaproveitar a capacidade sem herdar o conteúdo. Devolve SEMPRE o
// slice resultante, inclusive no erro, para o caller reter o buffer crescido.
//
// PRÉ-CONDIÇÃO: out e prevPayload não podem compartilhar o mesmo array. O
// merge lê do prev enquanto escreve em out, então o aliasing corrompe a saída
// de forma dependente dos dados — silenciosa quando as larguras coincidem. No
// Apply a separação é estrutural (prevBuf e outBuf são campos distintos); a
// nota existe para o próximo caller, que não terá essa garantia de graça.
//
// A saída sai com FieldIDs estritamente crescentes e sem TypeNull —
// pré-condição do Put, que a revalida antes de tornar o record durável.
// congeladas conta os campos que o ramo congelado preservou. É ponteiro e
// aceita nil porque os testes de unidade do merge não têm métricas a
// alimentar; o Apply passa o contador do dataset.
func mergeRecords(prevPayload []byte, patch []patchEntry, out []byte, congeladas *atomic.Int64) ([]byte, error) {
	var (
		r      encoding.FieldReader
		lastID types.FieldID // 0 é reservado (§32): serve de sentinela inicial
		j      int
		err    error
	)
	r.Reset(prevPayload)
	pid, _, pok := r.Next()

	for pok || j < len(patch) {
		// Campo do prev sem correspondente à frente no patch: cópia crua.
		if pok && (j >= len(patch) || pid < patch[j].field.ID) {
			out = append(out, r.RawField()...)
			pid, _, pok = r.Next()
			continue
		}
		e := patch[j]
		id := e.field.ID
		if id <= lastID {
			return out, fmt.Errorf("%w: patch fora de ordem: campo %d após %d", ErrInternalState, id, lastID)
		}
		lastID = id
		j++
		if pok && pid == id {
			if e.congelada {
				if congeladas != nil {
					congeladas.Add(1)
				}
				// Entry congelada com o campo JÁ no estado: preserva o
				// anterior cru e descarta a nova. É o congelamento inteiro,
				// e é o único lugar onde ele acontece — a política viaja na
				// entry, e nem o replace nem o create precisam saber dela.
				out = append(out, r.RawField()...)
				pid, _, pok = r.Next()
				continue
			}
			// Sobrescrita (ou remoção): o campo anterior é descartado.
			pid, _, pok = r.Next()
		}
		if e.isNull {
			continue // D2: null explícito remove o campo
		}
		if out, err = encoding.AppendField(out, id, e.val); err != nil {
			return out, err
		}
	}

	// A corrupção interrompe o cursor do prev (pok=false) e o loop drena o
	// resto do patch antes de chegar aqui: alguns bytes a mais num evento que
	// já vai ser descartado, em troca de um único ponto de checagem.
	if cerr := r.Err(); cerr != nil {
		return out, fmt.Errorf("%w: estado anterior ilegível: %w", ErrInternalState, cerr)
	}
	return out, nil
}

// encodePatch materializa SÓ os campos do patch, omitindo os nulls. É o
// caminho do replace (§23) e o do create no modo merge — nos dois não há
// estado anterior a preservar, então o resultado é o patch codificado.
//
// Mesmo contrato de out e de saída ordenada do mergeRecords.
func encodePatch(patch []patchEntry, out []byte) ([]byte, error) {
	var (
		lastID types.FieldID
		err    error
	)
	for _, e := range patch {
		id := e.field.ID
		if id <= lastID {
			return out, fmt.Errorf("%w: patch fora de ordem: campo %d após %d", ErrInternalState, id, lastID)
		}
		lastID = id
		if e.isNull {
			continue
		}
		if out, err = encoding.AppendField(out, id, e.val); err != nil {
			return out, err
		}
	}
	return out, nil
}
