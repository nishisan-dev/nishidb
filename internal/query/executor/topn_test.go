package executor_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// O schema do top-N tem uma coluna de TEXTO e uma de INTEIRO, e é por isso: as
// duas caem em ramos diferentes do comparador — a de texto guarda uma janela de
// bytes dentro do buffer da entrada, a de inteiro guarda um escalar — e um
// corpus de um tipo só deixaria metade do modo sem oráculo.
const yamlTopN = `
version: 1
name: t
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    nome:
      type: string
    n:
      type: int64
`

func compilaSchemaTopN(tb testing.TB) *schema.Compiled {
	tb.Helper()
	spec, err := schema.Parse([]byte(yamlTopN), schema.BuiltinDefaults)
	if err != nil {
		tb.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

// linhaTopN descreve uma linha do corpus. `temNome` false é AUSÊNCIA de verdade
// (o campo não é gravado), e não string vazia: o modo precisa ver o mesmo que o
// caminho completo vê.
type linhaTopN struct {
	id      string
	nome    string
	temNome bool
	n       int64
}

// corpusTopN é o corpus com EMPATES DE PROPÓSITO.
//
// Sete valores distintos de `nome` para 300 linhas e onze de `n`: quase toda
// comparação empata na primeira chave, e boa parte empata nas duas. É o único
// jeito de o diferencial exercitar o desempate por rowID — com valores únicos,
// dois comparadores diferentes dariam o mesmo resultado e o teste não provaria
// nada.
//
// Uma linha em treze vem SEM `nome`, para que o grupo dos ausentes exista no
// mesmo corpus.
func corpusTopN(n int) []linhaTopN {
	out := make([]linhaTopN, 0, n)
	for i := 0; i < n; i++ {
		l := linhaTopN{
			id:      fmt.Sprintf("k%04d", i),
			nome:    fmt.Sprintf("g%02d", (i*5)%7),
			temNome: i%13 != 0,
			n:       int64((i * 37) % 11),
		}
		out = append(out, l)
	}
	return out
}

func semeiaTopN(tb testing.TB, c *schema.Compiled, linhas []linhaTopN) *fakestorage.Store {
	tb.Helper()
	st := fakestorage.New(fakestorage.Indexes{})
	id, nome, num := campo(tb, c, "id"), campo(tb, c, "nome"), campo(tb, c, "n")
	type campoDaLinha struct {
		id  types.FieldID
		val types.Value
	}
	for _, l := range linhas {
		campos := []campoDaLinha{
			{id.ID, types.Value{Type: types.TypeString, Bytes: []byte(l.id)}},
			{num.ID, types.Value{Type: types.TypeInt64, I64: l.n}},
		}
		if l.temNome {
			campos = append(campos, campoDaLinha{nome.ID, types.Value{Type: types.TypeString, Bytes: []byte(l.nome)}})
		}
		// AppendField exige FieldID crescente: é a mesma invariante que o
		// payload real honra, e sem ela o encoding recusa.
		slices.SortFunc(campos, func(a, b campoDaLinha) int { return int(a.id) - int(b.id) })
		var p []byte
		var err error
		for _, cp := range campos {
			if p, err = encoding.AppendField(p, cp.id, cp.val); err != nil {
				tb.Fatal(err)
			}
		}
		if _, _, err := st.Put([]byte(l.id), 1, 0, p, nil); err != nil {
			tb.Fatal(err)
		}
	}
	return st
}

// idsDosSlots é a lista de campos que a query coleta, em FieldID CRESCENTE.
//
// A ordem não é estética: o `coleta` casa slots e payload por merge linear e só
// funciona com os dois lados ordenados por FieldID. Fixar os índices na mão foi
// como este arquivo quebrou na primeira escrita — o dicionário numera na ordem
// em que compila, e `n` saiu com ID menor que `nome`, então o slot de `nome`
// passou a receber o inteiro.
func idsDosSlots(tb testing.TB, c *schema.Compiled) []types.FieldID {
	tb.Helper()
	ids := []types.FieldID{campo(tb, c, "id").ID, campo(tb, c, "nome").ID, campo(tb, c, "n").ID}
	slices.Sort(ids)
	return ids
}

// slotDe resolve o índice de slot de uma coluna, sempre a partir da lista
// ordenada.
func slotDe(tb testing.TB, c *schema.Compiled, nome string) int {
	tb.Helper()
	id := campo(tb, c, nome).ID
	for i, x := range idsDosSlots(tb, c) {
		if x == id {
			return i
		}
	}
	tb.Fatalf("campo %q fora da lista de slots", nome)
	return -1
}

// bqTopN monta a query ligada: projeta `id`, ordena pelas chaves dadas, e
// carrega LIMIT explícito e OFFSET — que juntos são o que faz o Open escolher o
// caminho rápido.
func bqTopN(tb testing.TB, c *schema.Compiled, chaves []binder.BoundOrderKey, limite, offset int64) *binder.BoundQuery {
	tb.Helper()
	const maxRows = 10_000
	efetivo := limite
	if efetivo > maxRows {
		efetivo = maxRows
	}
	return &binder.BoundQuery{
		Dataset: "t",
		Schema:  c,
		// A projeção é `id` só, mas os slots trazem as três colunas: ordenar
		// por coluna FORA da projeção é caso suportado, e o top-N não pode
		// tratá-lo diferente do caminho completo.
		Columns:        []binder.BoundColumn{{Field: campo(tb, c, "id"), Name: "id", Slot: slotDe(tb, c, "id")}},
		Slots:          binder.Slots{IDs: idsDosSlots(tb, c)},
		OrderBy:        chaves,
		HasLimit:       true,
		Limit:          limite,
		MaxRows:        maxRows,
		EffectiveLimit: efetivo,
		Offset:         offset,
		Timeout:        30 * time.Second,
	}
}

// chaveNome e chaveN resolvem o slot pelo mesmo caminho que o bqTopN.
func chaveNome(tb testing.TB, c *schema.Compiled, desc, nullsFirst bool) binder.BoundOrderKey {
	tb.Helper()
	return binder.BoundOrderKey{
		Field: campo(tb, c, "nome"), Slot: slotDe(tb, c, "nome"), Desc: desc, NullsFirst: nullsFirst,
		Kind: binder.CompareBytes, Want: types.TypeString}
}

func chaveN(tb testing.TB, c *schema.Compiled, desc bool) binder.BoundOrderKey {
	tb.Helper()
	return binder.BoundOrderKey{
		Field: campo(tb, c, "n"), Slot: slotDe(tb, c, "n"), Desc: desc,
		Kind: binder.CompareInt, Want: types.TypeInt64}
}

// resultadoTopN é o que uma execução devolveu: os ids na ordem de saída, mais o
// que as métricas disseram sobre o modo escolhido.
type resultadoTopN struct {
	ids        []string
	usouTopN   int64
	degradouTo int64
}

// executa roda a query com as Options dadas e colhe a saída.
func executa(t *testing.T, st executor.Store, bq *binder.BoundQuery, opts executor.Options) resultadoTopN {
	t.Helper()
	var m executor.Metrics
	opts.Store, opts.Metrics = st, &m
	it, err := executor.Open(context.Background(), bq, planner.FullScan(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	var ids []string
	for it.Next() {
		ids = append(ids, string(it.Row().Cols()[0].Val.Bytes))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	return resultadoTopN{ids: ids, usouTopN: m.TopN.Load(), degradouTo: m.TopNDegradado.Load()}
}

// desligaTopN é o Options que força o ORDER BY COMPLETO.
var desligaTopN = executor.Options{TopNMax: -1}

// TestTopNDaOMesmoResultadoQueOSortCompleto é o teste que decide se o caminho
// rápido está certo.
//
// A MESMA query, sobre o MESMO corpus, pelos DOIS caminhos, com resultado
// IDÊNTICO linha a linha — não "equivalente", idêntico, porque o desempate por
// rowID torna a ordem total e determinística nos dois. Num corpus em que quase
// toda comparação empata, dois comparadores que divergissem no desempate
// produziriam ordens diferentes já na primeira dezena de linhas.
//
// A ASSERÇÃO DE NÃO-VACUIDADE é a metade que importa: sem ela, um bug que
// desligasse o caminho rápido faria o teste comparar o sort completo consigo
// mesmo e passar para sempre. As métricas dizem qual modo cada lado usou, e o
// teste exige que sejam modos DIFERENTES — é o análogo do que
// TestSortExternoDaOMesmoResultadoQueOSortEmMemoria faz ao provar que o teto de
// 512 realmente transborda.
func TestTopNDaOMesmoResultadoQueOSortCompleto(t *testing.T) {
	c := compilaSchemaTopN(t)
	linhas := corpusTopN(300)
	st := semeiaTopN(t, c, linhas)

	casos := []struct {
		nome            string
		keys          []binder.BoundOrderKey
		limit, offset  int64
		rowsEsperadas int
	}{
		{"uma chave asc", []binder.BoundOrderKey{chaveNome(t, c, false, false)}, 25, 0, 25},
		{"uma chave desc", []binder.BoundOrderKey{chaveNome(t, c, true, false)}, 25, 0, 25},
		{"com offset", []binder.BoundOrderKey{chaveNome(t, c, false, false)}, 25, 40, 25},
		{"duas chaves", []binder.BoundOrderKey{chaveN(t, c, true), chaveNome(t, c, false, false)}, 30, 11, 30},
		{"nulls first", []binder.BoundOrderKey{chaveNome(t, c, false, true)}, 20, 3, 20},
		{"limite maior que o corpus", []binder.BoundOrderKey{chaveNome(t, c, false, false)}, 500, 0, 300},
	}

	for _, cs := range casos {
		t.Run(cs.nome, func(t *testing.T) {
			rapido := executa(t, st, bqTopN(t, c, cs.keys, cs.limit, cs.offset), executor.Options{})
			completo := executa(t, st, bqTopN(t, c, cs.keys, cs.limit, cs.offset), desligaTopN)

			if rapido.usouTopN != 1 {
				t.Fatalf("o lado rápido NÃO escolheu o top-N (TopN=%d): os dois lados são o mesmo "+
					"caminho e o diferencial passaria por vacuidade", rapido.usouTopN)
			}
			if completo.usouTopN != 0 {
				t.Fatalf("o lado completo escolheu o top-N (TopN=%d): TopNMax negativo deveria "+
					"tê-lo desligado", completo.usouTopN)
			}
			if rapido.degradouTo != 0 {
				t.Fatalf("o lado rápido DEGRADOU (TopNDegradado=%d): ele terminou pelo caminho "+
					"completo e o diferencial voltou a comparar um caminho consigo mesmo",
					rapido.degradouTo)
			}
			if len(completo.ids) != cs.rowsEsperadas {
				t.Fatalf("%d linhas, quer %d: a montagem não produziu o corpus",
					len(completo.ids), cs.rowsEsperadas)
			}
			if len(rapido.ids) != len(completo.ids) {
				t.Fatalf("top-N devolveu %d linhas, completo devolveu %d", len(rapido.ids), len(completo.ids))
			}
			for i := range completo.ids {
				if rapido.ids[i] != completo.ids[i] {
					t.Fatalf("divergiram na linha %d: top-N=%q completo=%q\n top-N   = %v\n completo= %v",
						i, rapido.ids[i], completo.ids[i], rapido.ids, completo.ids)
				}
			}
		})
	}
}

// TestTopNComOffsetPulaDepoisDeOrdenar prova que o OFFSET entra no TAMANHO do
// heap, e não é aplicado antes de ordenar.
//
// A prova é por concatenação: `LIMIT 40 OFFSET 0` e `LIMIT 20 OFFSET 20` sobre
// a mesma ordem têm de encaixar exatamente — a segunda página é o sufixo da
// primeira. Um heap dimensionado só pelo LIMIT reteria as 20 melhores e depois
// pularia 20, devolvendo VAZIO; um OFFSET aplicado antes de ordenar pularia
// linhas escolhidas por rowId e a página sairia com outras linhas dentro.
func TestTopNComOffsetPulaDepoisDeOrdenar(t *testing.T) {
	c := compilaSchemaTopN(t)
	st := semeiaTopN(t, c, corpusTopN(300))
	chaves := []binder.BoundOrderKey{chaveNome(t, c, false, false)}

	inteira := executa(t, st, bqTopN(t, c, chaves, 40, 0), executor.Options{})
	segunda := executa(t, st, bqTopN(t, c, chaves, 20, 20), executor.Options{})

	if inteira.usouTopN != 1 || segunda.usouTopN != 1 {
		t.Fatalf("um dos lados não usou o top-N (%d, %d)", inteira.usouTopN, segunda.usouTopN)
	}
	if len(inteira.ids) != 40 || len(segunda.ids) != 20 {
		t.Fatalf("páginas de %d e %d linhas, quer 40 e 20", len(inteira.ids), len(segunda.ids))
	}
	for i := range segunda.ids {
		if segunda.ids[i] != inteira.ids[20+i] {
			t.Fatalf("a página com OFFSET 20 divergiu na linha %d: %q, quer %q (o sufixo da "+
				"página inteira)", i, segunda.ids[i], inteira.ids[20+i])
		}
	}
}

// TestTopNOrdenaPorVariasChaves prova que a segunda chave desempata a primeira
// dentro do heap.
//
// A asserção é sobre a SEQUÊNCIA de valores, e não sobre os ids: com `n DESC,
// nome ASC` o `n` tem de cair monotonicamente e, dentro de cada `n`, o `nome`
// tem de subir. Um heap que usasse só a primeira chave passaria num teste de
// contagem e falharia aqui na primeira transição interna.
func TestTopNOrdenaPorVariasChaves(t *testing.T) {
	c := compilaSchemaTopN(t)
	linhas := corpusTopN(300)
	st := semeiaTopN(t, c, linhas)
	porID := map[string]linhaTopN{}
	for _, l := range linhas {
		porID[l.id] = l
	}

	chaves := []binder.BoundOrderKey{chaveN(t, c, true), chaveNome(t, c, false, false)}
	r := executa(t, st, bqTopN(t, c, chaves, 60, 0), executor.Options{})
	if r.usouTopN != 1 {
		t.Fatalf("não usou o top-N (TopN=%d)", r.usouTopN)
	}
	if len(r.ids) != 60 {
		t.Fatalf("%d linhas, quer 60", len(r.ids))
	}

	transicoes := 0
	for i := 1; i < len(r.ids); i++ {
		a, b := porID[r.ids[i-1]], porID[r.ids[i]]
		if b.n > a.n {
			t.Fatalf("linha %d: n subiu de %d para %d num ORDER BY n DESC", i, a.n, b.n)
		}
		if b.n < a.n {
			transicoes++
			continue
		}
		// Mesmo n: o nome tem de subir, e o ausente vai por último.
		switch {
		case a.temNome && !b.temNome:
		case !a.temNome && b.temNome:
			t.Fatalf("linha %d: ausente antes de presente com o mesmo n=%d", i, a.n)
		case a.temNome && b.temNome && b.nome < a.nome:
			t.Fatalf("linha %d: nome caiu de %q para %q com o mesmo n=%d", i, a.nome, b.nome, a.n)
		}
	}
	if transicoes == 0 {
		t.Fatal("nenhuma transição de n no resultado: a segunda chave não foi exercitada")
	}
}

// TestTopNPoeAusentePorUltimoNasDuasDirecoes prova que o modo rápido herda a
// política de ausentes — e a herda por REUSO, não por reescrita.
//
// São três asserções, e as três são necessárias:
//
//   - ASC e DESC sem NULLS FIRST não trazem NENHUM ausente para as primeiras
//     linhas, porque ausente vai por último NAS DUAS direções. É essa a regra
//     que faz `ORDER BY x DESC LIMIT 10` significar "os 10 maiores".
//   - Com NULLS FIRST, as primeiras linhas são TODAS ausentes.
//   - E, em cada variante, o resultado bate com o do caminho completo linha a
//     linha. É essa terceira que prova o REUSO: uma comparaChave copiada e
//     alterada passaria nas duas primeiras e divergiria aqui.
func TestTopNPoeAusentePorUltimoNasDuasDirecoes(t *testing.T) {
	c := compilaSchemaTopN(t)
	linhas := corpusTopN(300)
	st := semeiaTopN(t, c, linhas)
	porID := map[string]linhaTopN{}
	ausentes := 0
	for _, l := range linhas {
		porID[l.id] = l
		if !l.temNome {
			ausentes++
		}
	}
	if ausentes < 15 {
		t.Fatalf("só %d linhas sem nome: o corpus não exercita o grupo dos ausentes", ausentes)
	}

	for _, cs := range []struct {
		nome              string
		desc, nullsFirst  bool
		primeirosAusentes bool
	}{
		{"asc", false, false, false},
		{"desc", true, false, false},
		{"asc nulls first", false, true, true},
		{"desc nulls first", true, true, true},
	} {
		t.Run(cs.nome, func(t *testing.T) {
			chaves := []binder.BoundOrderKey{chaveNome(t, c, cs.desc, cs.nullsFirst)}
			rapido := executa(t, st, bqTopN(t, c, chaves, 10, 0), executor.Options{})
			completo := executa(t, st, bqTopN(t, c, chaves, 10, 0), desligaTopN)
			if rapido.usouTopN != 1 || completo.usouTopN != 0 {
				t.Fatalf("modos iguais nos dois lados (%d, %d)", rapido.usouTopN, completo.usouTopN)
			}
			for i, id := range rapido.ids {
				if temNome := porID[id].temNome; temNome == cs.primeirosAusentes {
					t.Fatalf("linha %d (%s): temNome=%v, quer %v", i, id, temNome, !cs.primeirosAusentes)
				}
			}
			for i := range completo.ids {
				if rapido.ids[i] != completo.ids[i] {
					t.Fatalf("divergiu do caminho completo na linha %d: %q vs %q",
						i, rapido.ids[i], completo.ids[i])
				}
			}
		})
	}
}

// TestTopNDegradaEmVezDeRecusarQuandoOOrcamentoEstoura prova que o teto de
// MEMÓRIA continua valendo dentro do modo rápido, e que a saída dele é
// DEGRADAR, não recusar.
//
// Recusar seria regressão: sem o modo rápido, esta mesma query responde — o
// caminho completo transborda para disco. Um modo novo que a fizesse falhar
// teria piorado o produto para ganhar um benchmark.
//
// O oráculo é o de sempre: o resultado degradado tem de ser IDÊNTICO ao do
// caminho completo. É ele que prova que descartar as linhas que o heap já tinha
// rejeitado não muda a resposta — elas não estavam entre as N melhores de um
// prefixo, então não podiam estar entre as N melhores do total.
func TestTopNDegradaEmVezDeRecusarQuandoOOrcamentoEstoura(t *testing.T) {
	c := compilaSchemaTopN(t)
	st := semeiaTopN(t, c, corpusTopN(300))
	chaves := []binder.BoundOrderKey{chaveNome(t, c, false, false), chaveN(t, c, true)}
	comSpill := storeComSpill{Store: st, dir: t.TempDir()}

	// Teto de 256 bytes: não cabem nem as 30 entradas do heap, então a
	// degradação acontece cedo, com a maior parte do corpus ainda por varrer.
	degradado := executa(t, comSpill, bqTopN(t, c, chaves, 30, 5), executor.Options{SortMemory: 256})
	completo := executa(t, comSpill, bqTopN(t, c, chaves, 30, 5), desligaTopN)

	if degradado.usouTopN != 1 {
		t.Fatalf("a query não começou no top-N (TopN=%d)", degradado.usouTopN)
	}
	// SEM esta asserção o teste passaria por vacuidade num teto folgado demais:
	// os dois lados teriam terminado pelo caminho completo, e a degradação —
	// que é o que está sob teste — nunca teria sido exercitada.
	if degradado.degradouTo != 1 {
		t.Fatalf("TopNDegradado=%d: o teto de 256 bytes NÃO forçou a degradação, "+
			"e este teste não exercita nada", degradado.degradouTo)
	}
	if len(degradado.ids) != 30 {
		t.Fatalf("%d linhas, quer 30", len(degradado.ids))
	}
	for i := range completo.ids {
		if degradado.ids[i] != completo.ids[i] {
			t.Fatalf("degradado divergiu do completo na linha %d: %q vs %q",
				i, degradado.ids[i], completo.ids[i])
		}
	}
}

// E, sem lugar para transbordar, a degradação termina onde o caminho completo
// termina: RECUSANDO. O modo rápido não inventa uma resposta parcial.
func TestTopNDegradadoRecusaQuandoNaoHaOndeTransbordar(t *testing.T) {
	c := compilaSchemaTopN(t)
	st := semeiaTopN(t, c, corpusTopN(300))
	chaves := []binder.BoundOrderKey{chaveNome(t, c, false, false)}

	var m executor.Metrics
	it, err := executor.Open(context.Background(), bqTopN(t, c, chaves, 30, 0), planner.FullScan(),
		executor.Options{Store: st, Metrics: &m, SortMemory: 256})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for it.Next() {
	}
	if !errors.Is(it.Err(), executor.ErrOrcamentoDeOrdenacao) {
		t.Fatalf("erro = %v, quer ErrOrcamentoDeOrdenacao", it.Err())
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if m.TopNDegradado.Load() != 1 {
		t.Fatalf("TopNDegradado=%d: a recusa veio de outro lugar que não a degradação",
			m.TopNDegradado.Load())
	}
}

// TestTopNSoValeDentroDoTetoDeLinhas trava a regra da escolha de modo:
// `OFFSET + limite efetivo` tem de caber no teto.
//
// A primeira asserção é a que importa mais. Um ORDER BY sem LIMIT escrito herda
// o query.maxRows do dataset como limite efetivo, e reter dezenas de milhares
// de linhas num heap não é caminho rápido — é o sort completo com um sift a
// mais por linha e sem saber transbordar.
func TestTopNSoValeDentroDoTetoDeLinhas(t *testing.T) {
	c := compilaSchemaTopN(t)
	st := semeiaTopN(t, c, corpusTopN(60))
	chaves := []binder.BoundOrderKey{chaveNome(t, c, false, false)}

	semLimite := bqTopN(t, c, chaves, 10, 0)
	semLimite.HasLimit = false
	semLimite.EffectiveLimit = semLimite.MaxRows // 10.000, acima do teto padrão
	if r := executa(t, st, semLimite, executor.Options{}); r.usouTopN != 0 {
		t.Errorf("com limite efetivo de %d o top-N foi escolhido (TopN=%d): o teto de linhas "+
			"não está sendo respeitado", semLimite.EffectiveLimit, r.usouTopN)
	}

	// O teto conta OFFSET + limite, e não só o limite: as linhas puladas também
	// precisam ser as certas, então elas ocupam lugar no heap.
	if r := executa(t, st, bqTopN(t, c, chaves, 10, 0), executor.Options{TopNMax: 10}); r.usouTopN != 1 {
		t.Errorf("com teto 10 e LIMIT 10 o top-N não foi escolhido")
	}
	if r := executa(t, st, bqTopN(t, c, chaves, 10, 1), executor.Options{TopNMax: 10}); r.usouTopN != 0 {
		t.Errorf("com teto 10, LIMIT 10 e OFFSET 1 o top-N foi escolhido: o OFFSET não entrou " +
			"na conta do tamanho do heap")
	}
}

// TestTopNValeSemLimiteEscritoQuandoOMaxRowsCabe é a outra metade da regra, e
// existe porque a condição "só com LIMIT ESCRITO" foi tentada e REMOVIDA.
//
// Ela não tinha oráculo: no query.maxRows padrão, de 100 mil, o teto de linhas
// já recusa sozinho, e a única situação em que a condição mudava alguma coisa
// era o dataset com maxRows PEQUENO — onde ela desligava o caminho rápido justo
// onde ele é seguro. Este teste é o oráculo do que ficou no lugar dela: sem
// LIMIT escrito e com maxRows que cabe, o modo é escolhido e o resultado é
// idêntico ao do caminho completo, inclusive saindo truncado pelo teto
// implícito do dataset.
func TestTopNValeSemLimiteEscritoQuandoOMaxRowsCabe(t *testing.T) {
	c := compilaSchemaTopN(t)
	st := semeiaTopN(t, c, corpusTopN(300))
	chaves := []binder.BoundOrderKey{chaveNome(t, c, false, false), chaveN(t, c, true)}

	semLimite := func() *binder.BoundQuery {
		bq := bqTopN(t, c, chaves, 40, 0)
		bq.HasLimit, bq.Limit = false, 0
		bq.MaxRows, bq.EffectiveLimit = 40, 40
		return bq
	}

	// A parada vem do teto IMPLÍCITO, então o iterador publica
	// ErrMaxRowsExceeded DEPOIS das linhas — colher com o `executa`, que falha
	// em qualquer Err, esconderia o resultado atrás do aviso de truncagem.
	colhe := func(opts executor.Options) []string {
		t.Helper()
		var m executor.Metrics
		opts.Store, opts.Metrics = st, &m
		it, err := executor.Open(context.Background(), semLimite(), planner.FullScan(), opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() {
			if err := it.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
		var ids []string
		for it.Next() {
			ids = append(ids, string(it.Row().Cols()[0].Val.Bytes))
		}
		if !errors.Is(it.Err(), executor.ErrMaxRowsExceeded) {
			t.Fatalf("Err = %v, quer ErrMaxRowsExceeded: o corpus não estourou o teto implícito "+
				"e o teste não exercita a truncagem", it.Err())
		}
		esperado := int64(1)
		if opts.TopNMax < 0 {
			esperado = 0
		}
		if got := m.TopN.Load(); got != esperado {
			t.Fatalf("TopN = %d, quer %d (TopNMax=%d)", got, esperado, opts.TopNMax)
		}
		return ids
	}

	rapido, completo := colhe(executor.Options{}), colhe(desligaTopN)
	if len(rapido) != 40 {
		t.Fatalf("%d linhas, quer 40 (o teto implícito do dataset)", len(rapido))
	}
	if len(completo) != len(rapido) {
		t.Fatalf("completo devolveu %d linhas, top-N devolveu %d", len(completo), len(rapido))
	}
	for i := range completo {
		if rapido[i] != completo[i] {
			t.Fatalf("divergiu do caminho completo na linha %d: %q vs %q", i, rapido[i], completo[i])
		}
	}
}
