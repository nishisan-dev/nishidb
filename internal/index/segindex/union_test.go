package segindex_test

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// marker é o bit que fakeSource e brokenSource usam para sinalizar, DENTRO
// do próprio slice, que um rowId já foi emitido.
//
// Existe porque o literal `fakeSource{10, 20, 30}` — usado sem `&` em todos os
// testes deste arquivo — só compila se Next/RowID/etc. tiverem receiver por
// VALOR (o conjunto de métodos de um tipo valor nunca inclui método com
// receiver ponteiro, mesmo quando o valor é endereçável). Um receiver por
// valor não pode manter um campo de posição comum: a mutação vale só para a
// cópia local do método e se perde ao retornar. O que SOBREVIVE entre
// chamadas por essa mesma via é a mutação de um ELEMENTO do slice — todas as
// cópias do header, inclusive a guardada dentro da interface Source, apontam
// para o MESMO array de apoio. O bit 63 nunca colide com um rowId de teste (o
// maior usado aqui é a centena do gerador aleatório).
const marker = types.RowID(1) << 63

// fakeSource é o dublê de Source sobre um slice já ORDENADO ascendente:
// MinRowID/MaxRowID são o primeiro e o último elemento, sem marca.
type fakeSource []types.RowID

// Next marca o menor elemento ainda sem marca — o próximo da ordem ascendente,
// já que a entrada chega ordenada e sem repetição.
func (f fakeSource) Next() bool {
	for i := range f {
		if f[i]&marker == 0 {
			f[i] |= marker
			return true
		}
	}
	return false
}

// RowID devolve o MAIOR valor já marcado. Como Next() sempre marca o menor
// ainda sem marca e a entrada é ascendente, o maior marcado é exatamente o
// que a última chamada de Next() acabou de marcar — sem precisar de um campo
// de posição à parte, que o receiver por valor não permitiria manter.
func (f fakeSource) RowID() types.RowID {
	var largest types.RowID
	found := false
	for _, v := range f {
		if v&marker != 0 {
			if value := v &^ marker; !found || value > largest {
				largest, found = value, true
			}
		}
	}
	return largest
}

func (f fakeSource) Err() error   { return nil }
func (f fakeSource) Close() error { return nil }

// MinRowID devolve math.MaxUint64 quando o slice é vazio — o elemento neutro
// que o contrato da Source reserva para "não vou emitir nada" (o gerador
// aleatório do teste de propriedade produz slices vazios de vez em quando).
func (f fakeSource) MinRowID() types.RowID {
	if len(f) == 0 {
		return types.RowID(math.MaxUint64)
	}
	return f[0] &^ marker
}

func (f fakeSource) MaxRowID() types.RowID {
	if len(f) == 0 {
		return 0
	}
	return f[len(f)-1] &^ marker
}

// emptyTestSource é uma Source que já nasce vazia por declaração: Min(1) >
// Max(0). É o caso central de TestUneDescartaFontesVazias — provar que Union a
// descarta ANTES de decidir entre concatenação e merge, sem consumi-la.
type emptyTestSource struct{}

func (emptyTestSource) Next() bool            { return false }
func (emptyTestSource) RowID() types.RowID    { return 0 }
func (emptyTestSource) Err() error            { return nil }
func (emptyTestSource) Close() error          { return nil }
func (emptyTestSource) MinRowID() types.RowID { return 1 }
func (emptyTestSource) MaxRowID() types.RowID { return 0 }

// brokenSource emite as linhas de rows e, só depois de esgotá-las, reporta
// err — simula uma fonte que falha DEPOIS de entregar algo, não antes. Usa a
// mesma marcação por bit alto de fakeSource, pelo mesmo motivo: receiver por
// valor exigido pelo literal sem `&`.
type brokenSource struct {
	rows []types.RowID
	err  error
}

func (f brokenSource) Next() bool {
	for i := range f.rows {
		if f.rows[i]&marker == 0 {
			f.rows[i] |= marker
			return true
		}
	}
	return false
}

func (f brokenSource) RowID() types.RowID {
	var largest types.RowID
	found := false
	for _, v := range f.rows {
		if v&marker != 0 {
			if value := v &^ marker; !found || value > largest {
				largest, found = value, true
			}
		}
	}
	return largest
}

// Err devolve o defeito configurado incondicionalmente. Os combinadores só
// consultam Err() depois que Next() devolve false — e nesse ponto todas as
// linhas boas já saíram, que é exatamente o "falha no meio" que o teste quer.
func (f brokenSource) Err() error   { return f.err }
func (f brokenSource) Close() error { return nil }

func (f brokenSource) MinRowID() types.RowID {
	if len(f.rows) == 0 {
		return types.RowID(math.MaxUint64)
	}
	return f.rows[0] &^ marker
}

func (f brokenSource) MaxRowID() types.RowID {
	if len(f.rows) == 0 {
		return 0
	}
	return f.rows[len(f.rows)-1] &^ marker
}

// drain consome o iterador até o fim e falha o teste se ele terminar em erro
// — os testes que usam drain não esperam falha, então um erro aqui é sempre
// quebra de teste, não asserção.
func drain(t *testing.T, it types.RowIDIterator) []types.RowID {
	t.Helper()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iteração: %v", err)
	}
	return out
}

// TestUneConcatenaFaixasDisjuntas: é o caso do histórico, onde o rowId é
// monotônico e os segmentos particionam o espaço. Concatenar custa ZERO
// comparações, contra ~9 por linha de um heap de 500 fontes.
func TestUneConcatenaFaixasDisjuntas(t *testing.T) {
	it := segindex.Union([]segindex.Source{
		fakeSource{10, 20, 30},
		fakeSource{40, 50},
		fakeSource{60},
	})
	if got := segindex.StrategyOf(it); got != "concatenacao" {
		t.Errorf("estratégia = %q, quero \"concatenacao\"", got)
	}
	if got := drain(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40, 50, 60}) {
		t.Errorf("drena = %v", got)
	}
}

// TestUneMergeiaFaixasSobrepostasEDeduplica: é o caso do materializado, onde a
// mesma linha pode ter posting em dois segmentos — gravada com o valor x no
// segmento 5 e regravada com o MESMO x no segmento 9, ao atualizar outro
// campo. Emiti-la duas vezes faria o unionIter do executor devolver a mesma
// linha duplicada num OR.
func TestUneMergeiaFaixasSobrepostasEDeduplica(t *testing.T) {
	it := segindex.Union([]segindex.Source{
		fakeSource{10, 30, 50},
		fakeSource{20, 30, 40},
	})
	if got := segindex.StrategyOf(it); got != "merge" {
		t.Errorf("estratégia = %q, quero \"merge\"", got)
	}
	if got := drain(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40, 50}) {
		t.Errorf("drena = %v, quero sem repetir o 30", got)
	}
}

// TestUneDescartaFontesVazias: uma fonte vazia reporta Min > Max justamente
// para não participar do teste de disjunção. Sem o descarte, uma fonte vazia
// com Min=0,Max=0 empurraria o histórico inteiro para o caminho de merge.
func TestUneDescartaFontesVazias(t *testing.T) {
	it := segindex.Union([]segindex.Source{
		fakeSource{10, 20},
		emptyTestSource{},
		fakeSource{30, 40},
	})
	if got := segindex.StrategyOf(it); got != "concatenacao" {
		t.Errorf("estratégia = %q, quero \"concatenacao\" — a vazia não pode forçar o merge", got)
	}
	if got := drain(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40}) {
		t.Errorf("drena = %v", got)
	}
}

// TestUneNaoConcatenaQuandoAsFaixasSeTocam trava a fronteira exata: faixas
// [10,20] e [20,30] compartilham o 20 e NÃO são disjuntas. Um `<=` no lugar de
// `<` aqui produziria a mesma linha duas vezes, silenciosamente.
func TestUneNaoConcatenaQuandoAsFaixasSeTocam(t *testing.T) {
	it := segindex.Union([]segindex.Source{fakeSource{10, 20}, fakeSource{20, 30}})
	if got := segindex.StrategyOf(it); got != "merge" {
		t.Errorf("estratégia = %q, quero \"merge\"", got)
	}
	if got := drain(t, it); !slices.Equal(got, []types.RowID{10, 20, 30}) {
		t.Errorf("drena = %v, quero o 20 uma vez só", got)
	}
}

// TestUnePropagaOPrimeiroErro: uma fonte que falha no meio não pode virar
// "resultado menor". Falso negativo é o único desfecho que este produto não
// tolera, e resultado truncado sem erro é exatamente isso.
func TestUnePropagaOPrimeiroErro(t *testing.T) {
	broken := brokenSource{rows: []types.RowID{10}, err: errors.New("mmap sumiu")}
	it := segindex.Union([]segindex.Source{fakeSource{1, 2}, broken})
	if got := segindex.StrategyOf(it); got != "concatenacao" {
		t.Fatalf("estratégia = %q, quero \"concatenacao\" — este teste só cobre concatIter.Err()", got)
	}
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("Err() = nil, quero o erro da fonte quebrada")
	}
}

// TestUneMergePropagaOPrimeiroErro é o mesmo requisito de
// TestUnePropagaOPrimeiroErro, mas sobre o caminho de MERGE: lá as faixas são
// disjuntas ([1,2] e [10,10]), então aquele teste só exercita
// concatIter.Err(). Aqui fakeSource{10,20} e a quebrada (contendo 15) TÊM
// faixas sobrepostas, forçando merge — e é exatamente o caminho que o
// dataset materializado sempre usa. "Erro nunca vira resultado menor" é a
// invariante mais dura do produto (resultado truncado sem erro é falso
// negativo), e ela não pode ficar sem prova justamente aqui.
func TestUneMergePropagaOPrimeiroErro(t *testing.T) {
	broken := brokenSource{rows: []types.RowID{15}, err: errors.New("mmap sumiu")}
	it := segindex.Union([]segindex.Source{fakeSource{10, 20}, broken})
	if got := segindex.StrategyOf(it); got != "merge" {
		t.Fatalf("estratégia = %q, quero \"merge\" — este teste não estaria cobrindo o mergeIter", got)
	}
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("Err() = nil, quero o erro da fonte quebrada")
	}
}

// closeTrackingSource embrulha uma Source e registra se Close() foi chamado.
//
// Prova a exigência do dispatch da Tarefa 5: "Close de qualquer combinador
// fecha TODAS as fontes restantes" — inclusive as que a varredura nunca
// consumiu (parou no meio por LIMIT, erro de outra fonte ou cancelamento do
// cliente) e as vazias que Union descarta ANTES de qualquer decisão de
// estratégia. Um dataset em regime abre até 500 fontes por consulta; um
// Close que não fecha todas vaza um mapeamento por fonte esquecida, e isso
// só aparece em produção como esgotamento de descritor, sem apontar a causa.
type closeTrackingSource struct {
	segindex.Source
	closed bool
}

func (f *closeTrackingSource) Close() error {
	f.closed = true
	return f.Source.Close()
}

// TestConcatIterCloseFechaTodasAsFontesInclusiveAsNaoConsumidas trava que o
// Close da concatenação NÃO é seletivo: interromper a varredura no meio
// (parar de chamar Next antes do fim) e só então fechar tem de fechar tudo —
// a fonte corrente E as que a concatenação nunca chegou a tocar.
func TestConcatIterCloseFechaTodasAsFontesInclusiveAsNaoConsumidas(t *testing.T) {
	f1 := &closeTrackingSource{Source: fakeSource{10, 20}}
	f2 := &closeTrackingSource{Source: fakeSource{30, 40}}
	f3 := &closeTrackingSource{Source: fakeSource{50, 60}}
	it := segindex.Union([]segindex.Source{f1, f2, f3})
	if got := segindex.StrategyOf(it); got != "concatenacao" {
		t.Fatalf("estratégia = %q, quero \"concatenacao\"", got)
	}

	// Interrompe no meio: drena f1 por completo (2 linhas) e entra em f2 (1
	// linha) — o suficiente para o cursor da concatenação já ter avançado
	// PARA ALÉM de f1, que esgotou sozinha mas nunca foi fechada
	// individualmente. f3 fica sem ser tocada. Se Close fechasse só
	// `sources[idx:]` (o que sobrou À FRENTE do cursor), f1 escaparia — ela já
	// não está mais "à frente" depois de esgotar, mas também não foi fechada
	// por ninguém até aqui.
	for i, want := range []types.RowID{10, 20, 30} {
		if !it.Next() {
			t.Fatalf("Next() = false na linha %d (queria %d)", i, want)
		}
		if got := it.RowID(); got != want {
			t.Fatalf("RowID() = %d, quero %d", got, want)
		}
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !f1.closed {
		t.Error("f1 (já esgotada, cursor passou dela) não foi fechada")
	}
	if !f2.closed {
		t.Error("f2 (corrente, parcialmente consumida) não foi fechada")
	}
	if !f3.closed {
		t.Error("f3 (nunca consumida) não foi fechada")
	}
}

// TestMergeIterCloseFechaTodasAsFontesInclusiveAsNaoConsumidas é o mesmo
// requisito sobre o caminho de merge: o heap só guarda o que resta avançar, e
// Close precisa conhecer a lista COMPLETA das fontes de entrada para não
// vazar as que nunca entraram no heap ou já saíram dele.
func TestMergeIterCloseFechaTodasAsFontesInclusiveAsNaoConsumidas(t *testing.T) {
	f1 := &closeTrackingSource{Source: fakeSource{10, 30, 50}}
	f2 := &closeTrackingSource{Source: fakeSource{20, 30, 40}}
	it := segindex.Union([]segindex.Source{f1, f2})
	if got := segindex.StrategyOf(it); got != "merge" {
		t.Fatalf("estratégia = %q, quero \"merge\"", got)
	}

	// Drena até f2 esgotar E SAIR DO HEAP sem ser recolocada — f1{10,30,50} e
	// f2{20,30,40} emitem 10,20,30(dedup),40 nessa ordem, e é na quarta linha
	// que f2 esgota sozinha. Dali em diante ela não está mais "no heap": se
	// Close fechasse só o que o heap ainda guarda (em vez da lista completa
	// de entrada), f2 escaparia — exauriu, mas ninguém a fechou.
	for i, want := range []types.RowID{10, 20, 30, 40} {
		if !it.Next() {
			t.Fatalf("Next() = false na linha %d (queria %d)", i, want)
		}
		if got := it.RowID(); got != want {
			t.Fatalf("RowID() = %d, quero %d", got, want)
		}
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !f1.closed {
		t.Error("f1 (ainda no heap, com o 50 por emitir) não foi fechada")
	}
	if !f2.closed {
		t.Error("f2 (já esgotou e saiu do heap) não foi fechada")
	}
}

// TestUneFechaAsFontesVaziasDescartadasAntesDeCombinar prova que descartar
// não é vazar: Union fecha uma fonte vazia no MOMENTO do descarte — antes de
// qualquer decisão de estratégia —, e não deixa essa responsabilidade para o
// Close do combinador, que nem chega a saber que ela existiu.
func TestUneFechaAsFontesVaziasDescartadasAntesDeCombinar(t *testing.T) {
	empty := &closeTrackingSource{Source: emptyTestSource{}}
	f1 := &closeTrackingSource{Source: fakeSource{10, 20}}
	f2 := &closeTrackingSource{Source: fakeSource{30, 40}}
	it := segindex.Union([]segindex.Source{f1, empty, f2})

	// A vazia já tem de estar fechada AQUI, antes de qualquer Next ou Close no
	// iterador devolvido — quem fecha é o descarte de Union, não o Close do
	// combinador.
	if !empty.closed {
		t.Error("a fonte vazia não foi fechada no descarte, antes da decisão de estratégia")
	}

	drain(t, it)
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !f1.closed {
		t.Error("f1 não foi fechada pelo Close do combinador")
	}
	if !f2.closed {
		t.Error("f2 não foi fechada pelo Close do combinador")
	}
}

// TestUneDevolveAFonteDiretaQuandoSoUmaEViva cobre o ramo de fonte única, que
// nenhum outro teste deste arquivo exercita: quando só uma fonte sobra depois
// do descarte de vazias, Union a devolve SEM embrulhar em concatIter nem
// mergeIter. Não é caso de borda — é o caminho quente em produção para
// colunas de alta cardinalidade (ex.: identifier), onde um valor mora em
// praticamente um segmento só, e um ramo sem asserção aí é o lugar errado
// para economizar teste.
func TestUneDevolveAFonteDiretaQuandoSoUmaEViva(t *testing.T) {
	f := &closeTrackingSource{Source: fakeSource{10, 20, 30}}
	it := segindex.Union([]segindex.Source{f})
	if got := segindex.StrategyOf(it); got != "direta" {
		t.Fatalf("estratégia = %q, quero \"direta\"", got)
	}
	if got := drain(t, it); !slices.Equal(got, []types.RowID{10, 20, 30}) {
		t.Errorf("drena = %v, quero o conteúdo da própria fonte", got)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !f.closed {
		t.Error("a fonte única não foi fechada por Close — Union não pode devolvê-la sem que o Close do chamador continue fechando o que ela abriu")
	}
}

// TestUneDevolveVazioQuandoTodasAsFontesSaoVazias fecha o ramo que sobra no
// switch de StrategyOf depois de "direta" ganhar case próprio: zero fontes
// vivas depois do descarte.
func TestUneDevolveVazioQuandoTodasAsFontesSaoVazias(t *testing.T) {
	it := segindex.Union([]segindex.Source{emptyTestSource{}})
	if got := segindex.StrategyOf(it); got != "vazia" {
		t.Errorf("estratégia = %q, quero \"vazia\"", got)
	}
	if got := drain(t, it); got != nil {
		t.Errorf("drena = %v, quero vazio", got)
	}
}

// TestUneEhEquivalenteAUniaoOrdenada compara contra um oráculo trivial sobre
// entrada aleatória: junta tudo, ordena, compacta. É este teste que reprova a
// mutação M3 (concatenar sempre) e a M2 (merge sem dedup) da spec.
func TestUneEhEquivalenteAUniaoOrdenada(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		var sources []segindex.Source
		var want []types.RowID
		for range 1 + rng.IntN(6) {
			var rows []types.RowID
			for range rng.IntN(30) {
				rows = append(rows, types.RowID(rng.IntN(100)))
			}
			slices.Sort(rows)
			rows = slices.Compact(rows)
			want = append(want, rows...)
			sources = append(sources, fakeSource(rows))
		}
		slices.Sort(want)
		want = slices.Compact(want)
		if got := drain(t, segindex.Union(sources)); !slices.Equal(got, want) {
			t.Fatalf("Union = %v, quero %v", got, want)
		}
	}
}
