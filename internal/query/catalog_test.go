package query_test

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// engineWithCountersAndMeta monta o motor do harness (dataset `radius_sessions`
// no schema `telecom`) com dois contadores e as duas tabelas de meta que hoje
// são registradas — `_meta.counters` e `_meta.kafka`. É o retrato do que um
// servidor de verdade publica, que é o que o catálogo tem de descrever.
func engineWithCountersAndMeta(t *testing.T) *query.Engine {
	t.Helper()
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, schema.CountersNamespace, "por_olt")
	tableVirtualOfTest(t, h.engine, schema.CountersNamespace, "por_pon")
	tableVirtualOfTest(t, h.engine, meta.Namespace, "counters")
	tableVirtualOfTest(t, h.engine, meta.Namespace, "kafka")
	return h.engine
}

// catalogColumnsOf devolve os nomes das colunas da tabela pedida, e falha se ela não
// estiver no catálogo — comparar contra uma lista vazia passaria verde com a
// tabela ausente.
func catalogColumnsOf(t *testing.T, tables []query.TableInfo, schemaName, name string) []string {
	t.Helper()
	idx := slices.IndexFunc(tables, func(tb query.TableInfo) bool {
		return tb.Schema == schemaName && tb.Name == name
	})
	if idx < 0 {
		t.Fatalf("%s.%s não está no catálogo: %v", schemaName, name, qualified(tables))
	}
	out := make([]string, 0, len(tables[idx].Columns))
	for _, c := range tables[idx].Columns {
		if c == nil {
			t.Fatalf("tabela %s.%s com coluna nil", schemaName, name)
		}
		out = append(out, c.Name)
	}
	return out
}

// qualified rende o catálogo na forma comparável `KIND:schema.nome`.
func qualified(tables []query.TableInfo) []string {
	out := make([]string, 0, len(tables))
	for _, tb := range tables {
		out = append(out, string(tb.Kind)+":"+tb.Schema+"."+tb.Name)
	}
	return out
}

// TestCatalogoListaABaseOsContadoresEAsTabelasDeMeta: o catálogo é a resposta
// única para "que tabelas este dataset publica" — o /v1/metadata e a
// `_meta.tables` saem os dois daqui nas tarefas seguintes. Duas derivações do
// mesmo fato divergem, e a divergência aparece como uma ferramenta que discorda
// do SQL sem nada estar quebrado.
//
// As tabelas do dataset saem no schema DECLARADO por ele; as de meta, no
// `_meta`, que é o namespace reservado onde elas já respondem SQL hoje.
func TestCatalogoListaABaseOsContadoresEAsTabelasDeMeta(t *testing.T) {
	eng := engineWithCountersAndMeta(t)
	tables, err := eng.Catalog(dataset)
	if err != nil {
		t.Fatal(err)
	}
	got := qualified(tables)
	want := []string{
		"TABLE:telecom.radius_sessions",
		"VIEW:telecom.por_olt",
		"VIEW:telecom.por_pon",
		"VIEW:_meta.counters",
		"VIEW:_meta.fields",
		"VIEW:_meta.kafka",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("catálogo = %v, quero %v", got, want)
	}
}

// TestCatalogoDaTodaTabelaSchemaEColunas: linha sem schema não tem onde ser
// pendurada na árvore do cliente, e linha sem coluna não pode ser aberta.
// `_meta.fields` é a que corre mais risco: ela é fixa do pacote meta e não
// está no registro de tabelas virtuais, então as colunas dela só chegam aqui
// se alguém for buscá-las.
func TestCatalogoDaTodaTabelaSchemaEColunas(t *testing.T) {
	eng := engineWithCountersAndMeta(t)
	tables, err := eng.Catalog(dataset)
	if err != nil {
		t.Fatal(err)
	}
	for _, tb := range tables {
		if tb.Schema == "" {
			t.Errorf("tabela %q sem schema", tb.Name)
		}
		if len(tb.Columns) == 0 {
			t.Errorf("tabela %s.%s sem coluna nenhuma", tb.Schema, tb.Name)
		}
		for _, c := range tb.Columns {
			if c == nil {
				t.Fatalf("tabela %s.%s com coluna nil", tb.Schema, tb.Name)
			}
		}
	}
	// A fixa do pacote meta tem de trazer as colunas DELA, não as do dataset.
	idx := slices.IndexFunc(tables, func(tb query.TableInfo) bool {
		return tb.Schema == meta.Namespace && tb.Name == meta.TableFields
	})
	if idx < 0 {
		t.Fatalf("%s.%s não está no catálogo: %v", meta.Namespace, meta.TableFields, qualified(tables))
	}
	var names []string
	for _, c := range tables[idx].Columns {
		names = append(names, c.Name)
	}
	for _, want := range []string{"name", "type", "auto", "index"} {
		if !slices.Contains(names, want) {
			t.Errorf("coluna %q ausente em %s.%s: %v", want, meta.Namespace, meta.TableFields, names)
		}
	}
}

// TestCatalogoDaACadaViewAsColunasDaPropriaView: cada linha tem de trazer as
// colunas DAQUELA tabela. Conferir só que a lista não está vazia deixa passar
// a troca que publica, para toda view, as colunas do DATASET — e o cliente
// veria, embaixo de cada contador e de cada tabela de meta, colunas que o
// SELECT recusa.
func TestCatalogoDaACadaViewAsColunasDaPropriaView(t *testing.T) {
	eng := engineWithCountersAndMeta(t)
	tables, err := eng.Catalog(dataset)
	if err != nil {
		t.Fatal(err)
	}
	// As colunas do dublê das tabelas virtuais, que NÃO são as do fixture do
	// dataset — é essa diferença que dá poder à comparação.
	wantVirtual := []string{"groupKey", "olt", "total"}
	for _, tb := range []struct{ schemaName, name string }{
		{"telecom", "por_olt"},
		{meta.Namespace, "counters"},
	} {
		if got := catalogColumnsOf(t, tables, tb.schemaName, tb.name); !slices.Equal(got, wantVirtual) {
			t.Errorf("colunas de %s.%s = %v, quero %v", tb.schemaName, tb.name, got, wantVirtual)
		}
	}
	// A base continua com as suas, e nenhuma delas é do dublê.
	base := catalogColumnsOf(t, tables, "telecom", dataset)
	if slices.Equal(base, wantVirtual) {
		t.Fatalf("o fixture do dataset ficou com as colunas do dublê (%v): a comparação acima deixou de provar coisa alguma", base)
	}
	if !slices.Contains(base, "Acct-Status-Type") {
		t.Errorf("colunas da base = %v, sem os campos do fixture", base)
	}
}

// TestCatalogoPublicaONamespaceQueAbreAView: o par (Schema, Name) ainda não é
// o endereço que o FROM aceita — `telecom.por_olt` é recusado hoje e
// `_counters.por_olt` abre. Quem consome o catálogo para executar SQL precisa
// do namespace publicado na linha; sem ele não há como reconstruir o FROM.
func TestCatalogoPublicaONamespaceQueAbreAView(t *testing.T) {
	eng := engineWithCountersAndMeta(t)
	tables, err := eng.Catalog(dataset)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"telecom." + dataset:         "",
		"telecom.por_olt":            schema.CountersNamespace,
		"telecom.por_pon":            schema.CountersNamespace,
		meta.Namespace + ".counters": meta.Namespace,
		meta.Namespace + ".fields":   meta.Namespace,
		meta.Namespace + ".kafka":    meta.Namespace,
	}
	got := make(map[string]string, len(tables))
	for _, tb := range tables {
		got[tb.Schema+"."+tb.Name] = tb.Namespace
	}
	if !maps.Equal(got, want) {
		t.Fatalf("namespaces = %v, quero %v", got, want)
	}
	// O namespace publicado é o que ABRE a tabela: a consulta montada com ele
	// tem de executar.
	for _, tb := range tables {
		if tb.Namespace == "" {
			continue
		}
		from := tb.Namespace + "." + tb.Name
		it, err := eng.Execute(context.Background(), dataset, "SELECT * FROM "+from+" LIMIT 1", nil)
		if err != nil {
			t.Errorf("FROM %s (namespace do catálogo) não abre: %v", from, err)
			continue
		}
		_ = it.Close()
	}
}

// TestCatalogoTemOrdemEstavel: a ordem é do contrato — uma ferramenta que
// confia nela para agrupar a árvore a mostraria embaralhada sem isto. O mapa
// de tabelas virtuais não tem ordem, então a comparação é da lista INTEIRA:
// conferir só o tamanho passaria com a ordem embaralhada.
func TestCatalogoTemOrdemEstavel(t *testing.T) {
	eng := engineWithCountersAndMeta(t)
	first, err := eng.Catalog(dataset)
	if err != nil {
		t.Fatal(err)
	}
	// Várias voltas, porque desordem de mapa não aparece na primeira: com seis
	// tabelas, uma única repetição acertaria a ordem com frequência alta.
	for i := 0; i < 64; i++ {
		again, err := eng.Catalog(dataset)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(qualified(first), qualified(again)) {
			t.Fatalf("volta %d = %v, a primeira = %v", i, qualified(again), qualified(first))
		}
	}
}

// TestCatalogoIncluiTabelaDeMetaRegistradaDepois: o catálogo enumera o que o
// motor sabe ABRIR, e não uma lista escrita à mão — a `_meta.tables` da
// tarefa seguinte entra por aqui, sem tocar neste código.
func TestCatalogoIncluiTabelaDeMetaRegistradaDepois(t *testing.T) {
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, meta.Namespace, "tables")
	got := mustCatalog(t, h.engine)
	want := []string{
		"TABLE:telecom.radius_sessions",
		"VIEW:_meta.fields",
		"VIEW:_meta.tables",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("catálogo = %v, quero %v", got, want)
	}
}

// TestCatalogoNaoListaDuasVezesAFieldsSombreada: o Execute roteia
// `_meta.fields` para a tabela FIXA do pacote meta antes de olhar o registro,
// então um registro homônimo é inalcançável. Listá-lo poria duas linhas
// `_meta.fields` na árvore, uma delas com colunas que nenhuma consulta
// devolve.
func TestCatalogoNaoListaDuasVezesAFieldsSombreada(t *testing.T) {
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, meta.Namespace, meta.TableFields)
	got := mustCatalog(t, h.engine)
	want := []string{
		"TABLE:telecom.radius_sessions",
		"VIEW:_meta.fields",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("catálogo = %v, quero %v", got, want)
	}
}

// TestCatalogoRecusaDatasetDesconhecido: erro que não nomeia o dataset pedido
// manda quem chamou adivinhar qual das chamadas caiu.
func TestCatalogoRecusaDatasetDesconhecido(t *testing.T) {
	h := newHarness(t)
	_, err := h.engine.Catalog("nao_existe")
	if err == nil {
		t.Fatal("dataset não registrado tem de ser recusado")
	}
	if !strings.Contains(err.Error(), "nao_existe") {
		t.Errorf("a recusa tem de nomear o dataset pedido: %v", err)
	}
}

func mustCatalog(t *testing.T, eng *query.Engine) []string {
	t.Helper()
	tables, err := eng.Catalog(dataset)
	if err != nil {
		t.Fatal(err)
	}
	return qualified(tables)
}
