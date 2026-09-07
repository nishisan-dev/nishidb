package segindex

import (
	"cmp"
	"container/heap"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Union combina as fontes de vários segmentos num iterador ascendente único —
// é a peça que fecha a §3.3 da spec, porque uma consulta por valor pergunta a
// CADA segmento e o executor só sabe consumir um único RowIDIterator.
//
// A estratégia vem dos DADOS — das faixas que as fontes declaram —, e nunca do
// modo do dataset. A diferença importa: um predicado apoiado no modo
// transforma uma premissa quebrada em resultado silenciosamente errado; um
// apoiado no dado degrada para o caminho mais lento e continua certo.
//
// Faixas disjuntas e ordenadas viram CONCATENAÇÃO: zero comparações entre
// fontes, e a ordem crescente é herdada. É o histórico, sempre — o rowId é
// monotônico e os segmentos particionam o espaço.
//
// Faixas sobrepostas viram MERGE k-way por heap, com dedup de cabeças iguais.
// É o materializado, sempre: a mesma linha pode ter posting em dois
// segmentos, porque um update a regrava com o mesmo valor num segmento novo
// sem que nada remova a posting antiga — não existe fase de Clear neste
// desenho (§3.3).
func Union(sources []Source) types.RowIDIterator {
	live := make([]Source, 0, len(sources))
	for _, f := range sources {
		if f.MinRowID() > f.MaxRowID() {
			// Vazia por declaração: não participa da disjunção. Sem este
			// descarte uma fonte vazia com Min=0,Max=0 (em vez do Min>Max que
			// o contrato exige) empurraria vizinhas legitimamente disjuntas
			// para o caminho de merge, mais lento sem necessidade.
			_ = f.Close()
			continue
		}
		live = append(live, f)
	}
	switch len(live) {
	case 0:
		return emptyIter{}
	case 1:
		return live[0]
	}
	slices.SortFunc(live, func(a, b Source) int { return cmp.Compare(a.MinRowID(), b.MinRowID()) })
	if disjoint(live) {
		return &concatIter{sources: live}
	}
	return newMergeIter(live)
}

// disjoint exige separação ESTRITA entre faixas CONSECUTIVAS (já ordenadas
// por MinRowID). Faixas que se TOCAM num rowId comum não são disjuntas:
// concatená-las emitiria esse rowId duas vezes, e o contrato do iterador é
// ESTRITAMENTE crescente — é o que o unionIter do executor conta como
// premissa para um OR sobre índices.
func disjoint(f []Source) bool {
	for i := 1; i < len(f); i++ {
		if f[i-1].MaxRowID() >= f[i].MinRowID() {
			return false
		}
	}
	return true
}

// emptyIter é o iterador da união de zero fontes vivas — todas as fontes de
// entrada eram vazias por declaração, ou a lista veio vazia.
type emptyIter struct{}

func (emptyIter) Next() bool         { return false }
func (emptyIter) RowID() types.RowID { return 0 }
func (emptyIter) Err() error         { return nil }
func (emptyIter) Close() error       { return nil }

// concatIter percorre as fontes em sequência, aproveitando que disjoint() já
// provou que suas faixas não se sobrepõem: não há necessidade de comparar
// RowIDs entre fontes, e a ordem ascendente cai de graça da ordenação por
// MinRowID feita em Union. Zero comparações por linha é a razão de existir
// deste caminho — é o histórico, com centenas de segmentos por consulta.
type concatIter struct {
	sources []Source
	idx     int
	err     error
}

func (c *concatIter) Next() bool {
	for c.idx < len(c.sources) {
		if c.sources[c.idx].Next() {
			return true
		}
		// Erro nunca vira "resultado menor": se a fonte corrente esgotou por
		// falha (Err() != nil), guardamos o erro e PARAMOS aqui — nunca
		// avançamos para a próxima fonte como se nada tivesse acontecido, o
		// que devolveria um resultado truncado sem erro (falso negativo).
		if err := c.sources[c.idx].Err(); err != nil {
			c.err = err
			return false
		}
		c.idx++
	}
	return false
}

func (c *concatIter) RowID() types.RowID { return c.sources[c.idx].RowID() }
func (c *concatIter) Err() error         { return c.err }

// Close fecha TODAS as fontes, inclusive as já esgotadas e as que um erro
// deixou por tocar: vazar iterador de índice num caminho de erro é o tipo de
// coisa que só aparece como descritor esgotado em produção.
func (c *concatIter) Close() error {
	var first error
	for _, f := range c.sources {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// mergeIter faz o merge k-way por heap binário das fontes cujas faixas se
// sobrepõem, emitindo o menor RowID corrente e descartando cabeças iguais
// antes de devolver.
//
// É o caminho do materializado: a mesma linha pode ter posting em dois
// segmentos — o valor x gravado no segmento 5 e regravado com o MESMO x no
// segmento 9, ao atualizar outro campo — porque não existe fase de Clear
// neste desenho. Sem o dedup, o unionIter do executor devolveria essa linha
// duas vezes num OR.
type mergeIter struct {
	sources []Source // TODAS as fontes de entrada — usado só pelo Close.
	h       sourceHeap
	id      types.RowID
	init    bool
	err     error
}

func newMergeIter(sources []Source) *mergeIter {
	return &mergeIter{sources: sources}
}

func (m *mergeIter) Next() bool {
	if !m.init {
		m.init = true
		m.h = make(sourceHeap, 0, len(m.sources))
		// heap.Push mantém o invariante a cada chamada, partindo de um heap
		// vazio (que o satisfaz trivialmente) — não é preciso um heap.Init
		// depois: seria refazer em O(n) um trabalho já feito incrementalmente.
		for _, f := range m.sources {
			m.advance(f)
		}
	}
	if m.err != nil || len(m.h) == 0 {
		return false
	}
	m.id = m.h[0].RowID()
	// Descarta as cabeças iguais ao mínimo escolhido: no materializado a
	// mesma linha pode estar presente em mais de uma fonte com o MESMO
	// rowId, e emiti-la mais de uma vez faria o unionIter do executor
	// devolver a mesma linha duplicada num OR.
	for len(m.h) > 0 && m.h[0].RowID() == m.id {
		m.advance(heap.Pop(&m.h).(Source))
		if m.err != nil {
			return false
		}
	}
	return true
}

// advance tenta puxar a próxima linha de f. Fontes exauridas sem erro somem do
// heap em silêncio; a primeira que falhar interrompe a iteração — guardar o
// primeiro erro e parar é o que evita que um problema no meio vire "resultado
// menor" sem aviso.
func (m *mergeIter) advance(f Source) {
	if f.Next() {
		heap.Push(&m.h, f)
		return
	}
	if err := f.Err(); err != nil && m.err == nil {
		m.err = err
	}
}

func (m *mergeIter) RowID() types.RowID { return m.id }
func (m *mergeIter) Err() error         { return m.err }

// Close fecha TODAS as fontes de entrada, tanto as que ainda estavam no heap
// quanto as já removidas dele por esgotamento — o heap só guarda o que resta
// avançar, m.sources é quem sabe a lista completa.
func (m *mergeIter) Close() error {
	var first error
	for _, f := range m.sources {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// sourceHeap é um heap binário mínimo de Source por RowID() corrente. É o que
// mantém o merge em O(log k) por linha em vez do O(k) de uma varredura
// linear — a diferença que compensa quando k chega às centenas de segmentos
// que o materializado acumula.
type sourceHeap []Source

func (h sourceHeap) Len() int           { return len(h) }
func (h sourceHeap) Less(i, j int) bool { return h[i].RowID() < h[j].RowID() }
func (h sourceHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *sourceHeap) Push(x any)        { *h = append(*h, x.(Source)) }
func (h *sourceHeap) Pop() any {
	old := *h
	n := len(old)
	f := old[n-1]
	*h = old[:n-1]
	return f
}
