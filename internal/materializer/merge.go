package materializer

import (
	"errors"
	"fmt"
	"math"
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
// efeitosDoMerge são os contadores das políticas de escrita que só o merge
// enxerga: o dado gravado não diz quantas vezes um valor foi preservado nem
// somado, e sem contá-los a política é invisível em produção.
//
// É ponteiro e aceita nil porque os testes de unidade do merge não têm
// métricas a alimentar; o Apply passa os do dataset.
type efeitosDoMerge struct {
	congeladas atomic.Int64
	acumuladas atomic.Int64
	// saturadas é o único desfecho em que o valor gravado deixa de ser
	// verdade: a soma bateu no teto do int64 e parou ali.
	saturadas atomic.Int64
}

func mergeRecords(prevPayload []byte, patch []patchEntry, out []byte, ef *efeitosDoMerge) ([]byte, error) {
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
				if ef != nil {
					ef.congeladas.Add(1)
				}
				// Entry congelada com o campo JÁ no estado: preserva o
				// anterior cru e descarta a nova. É o congelamento inteiro,
				// e é o único lugar onde ele acontece — a política viaja na
				// entry, e nem o replace nem o create precisam saber dela.
				out = append(out, r.RawField()...)
				pid, _, pok = r.Next()
				continue
			}
			if e.acumula && !e.isNull {
				// Entry aditiva: o valor gravado é a SOMA da parcela do evento
				// com o que já estava.
				//
				// O valor anterior é redecodificado do RawField em vez de vir
				// do Next() — e isso não é desperdício, é o que mantém o
				// caminho comum de graça. Guardar o valor de TODO campo do
				// estado só para usá-lo neste ramo custou 43% do merge quando
				// medido (415→595 ns/op no BenchmarkMergeRadiusPatch), num
				// laço em que o normal é copiar 35 campos crus e tocar 8.
				// Aqui o custo é uma decodificação por campo aditivo, que é
				// no máximo um punhado por evento.
				if e.val.Type == types.TypeInt64 {
					var ra encoding.FieldReader
					ra.Reset(r.RawField())
					if _, anterior, ok := ra.Next(); ok && anterior.Type == types.TypeInt64 {
						soma := somaSaturada(anterior.I64, e.val.I64)
						if ef != nil {
							ef.acumuladas.Add(1)
							if soma == math.MaxInt64 || soma == math.MinInt64 {
								ef.saturadas.Add(1)
							}
						}
						e.val.I64 = soma
					}
					// Anterior de outro tipo físico não é somável (payload de
					// antes de a coluna ter tipo declarado, ou dado
					// heterogêneo): a parcela apenas sobrescreve, que é o
					// mesmo desfecho que a política default daria.
				}
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

// somaSaturada soma dois int64 parando nos extremos em vez de dar a volta.
//
// Saturar, e não recusar o evento: um contador que só cresce chega no teto em
// algum momento, e recusar ali travaria a entidade PARA SEMPRE — todo evento
// seguinte estouraria igual, e a entidade pararia de receber qualquer outra
// atualização por causa de um campo. Saturado, o valor para de crescer, o
// resto do documento continua entrando, e o operador vê pelo contador.
func somaSaturada(a, b int64) int64 {
	soma := a + b
	// O overflow do int64 é detectável pelo SINAL: a soma só pode divergir do
	// sinal comum das parcelas se deu a volta.
	if (a > 0 && b > 0 && soma < 0) || (a > 0 && b == math.MaxInt64) {
		return math.MaxInt64
	}
	if a < 0 && b < 0 && soma >= 0 {
		return math.MinInt64
	}
	return soma
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
