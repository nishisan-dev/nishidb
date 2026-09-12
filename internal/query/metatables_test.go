package query_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// harnessWithMetaTables monta o retrato do que um servidor de verdade publica
// — a base, um contador e as tabelas de meta — e registra a `_meta.tables`.
//
// Ela é registrada ANTES das outras DE PROPÓSITO, e é o contrário da ordem de
// produção (servidor e CLI a registram depois de counters.Register e
// metakafka.Register). As linhas são montadas a cada consulta, então a ordem
// não muda a resposta; registrá-la primeiro é o que faz o teste discriminar
// uma implementação que congelasse o catálogo no registro — ela sairia sem as
// três tabelas seguintes E sem si mesma.
func harnessWithMetaTables(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	if err := h.engine.RegisterMetaTables(dataset); err != nil {
		t.Fatal(err)
	}
	tableVirtualOfTest(t, h.engine, dataset, schema.CountersNamespace, "por_olt")
	tableVirtualOfTest(t, h.engine, dataset, meta.Namespace, "counters")
	tableVirtualOfTest(t, h.engine, dataset, meta.Namespace, "kafka")
	return h
}

// TestMetaTablesListaOCatalogo: é a resposta por SQL para "que tabelas
// existem" — o `_meta` deste produto já responde `fields`, `counters` e
// `kafka`, e faltava a que lista as tabelas.
//
// O `id` é o endereço do FROM, e a lista inteira é comparada: uma tabela a
// mais ou a menos reprova, que é o que separa este teste de um `Contains`.
func TestMetaTablesListaOCatalogo(t *testing.T) {
	h := harnessWithMetaTables(t)
	rows := h.consultaOK("SELECT id, kind FROM _meta.tables ORDER BY id")
	got := strings.Join(chaves(rows, "id"), ",")
	want := "_counters.por_olt,_meta.counters,_meta.fields,_meta.kafka,_meta.tables,radius_sessions"
	if got != want {
		t.Fatalf("id = %s, quero %s", got, want)
	}
	// A base é a ÚNICA TABLE; todo o resto é VIEW. Com o kind fixo em VIEW
	// para todas, a comparação abaixo fica vermelha.
	if got := strings.Join(chaves(rows, "kind"), ","); got != "VIEW,VIEW,VIEW,VIEW,VIEW,TABLE" {
		t.Fatalf("kind = %s (só a tabela base é TABLE)", got)
	}
}

// TestMetaTablesSeInclui: perguntar "que tabelas existem" e não ver a própria
// pergunta listada deixaria o catálogo incompleto na primeira linha — quem
// descobrisse o dataset por esta tabela não saberia que ela existe.
//
// A linha é cobrada pelos dois caminhos: o point lookup pela key e a
// varredura. Os dois têm de concordar; é aqui que uma key ambígua apareceria.
func TestMetaTablesSeInclui(t *testing.T) {
	h := harnessWithMetaTables(t)
	rows := h.consultaOK("SELECT name, schema, kind FROM _meta.tables WHERE id = '_meta.tables'")
	if len(rows) != 1 {
		t.Fatalf("a própria tabela tem de estar listada: %v", rows)
	}
	if rows[0]["name"] != "tables" || rows[0]["schema"] != meta.Namespace || rows[0]["kind"] != "VIEW" {
		t.Fatalf("a linha da própria tabela = %v", rows[0])
	}
	rows = h.consultaOK("SELECT id FROM _meta.tables WHERE name = 'tables'")
	if len(rows) != 1 || rows[0]["id"] != "_meta.tables" {
		t.Fatalf("a varredura tem de achar a mesma linha que a key: %v", rows)
	}
}

// TestMetaTablesPublicaOSchemaDeCadaTabela: o schema é o lugar da tabela na
// árvore do cliente, e os dois grupos caem em lugares DIFERENTES — a base e o
// contador no schema do dataset, as tabelas de meta no `_meta`. Publicar tudo
// no schema do dataset (ou tudo no `_meta`) deixa este teste vermelho.
func TestMetaTablesPublicaOSchemaDeCadaTabela(t *testing.T) {
	h := harnessWithMetaTables(t)
	rows := h.consultaOK("SELECT id, schema FROM _meta.tables ORDER BY id")
	got := strings.Join(chaves(rows, "schema"), ",")
	want := "telecom,_meta,_meta,_meta,_meta,telecom"
	if got != want {
		t.Fatalf("schema = %s, quero %s (o contador e a base em telecom; as de meta em _meta)", got, want)
	}
	// Nenhuma linha sai com schema vazio: a spec exige schemaName de todo
	// dataset, e as tabelas de meta saem no namespace.
	for _, l := range rows {
		if l["schema"] == "" || l["schema"] == "<null>" {
			t.Fatalf("tabela %q sem schema: %v", l["id"], l)
		}
	}
}

// TestMetaTablesNamespaceEhNuloSoNaTabelaBase: o namespace é o qualificador do
// ATALHO — o que abre a tabela no dataset da requisição —, e a base não tem
// atalho com prefixo: ausência, que neste produto é NULL. Sem ele o cliente não
// teria como reconstruir `_counters.por_olt` a partir de `telecom` e `por_olt`.
// O par `schema.tabela` abre as seis, base inclusive, e sai nas colunas
// `schema` e `name`.
func TestMetaTablesNamespaceEhNuloSoNaTabelaBase(t *testing.T) {
	h := harnessWithMetaTables(t)
	rows := h.consultaOK("SELECT id, namespace FROM _meta.tables ORDER BY id")
	got := strings.Join(chaves(rows, "namespace"), ",")
	want := "_counters,_meta,_meta,_meta,_meta,<null>"
	if got != want {
		t.Fatalf("namespace = %s, quero %s", got, want)
	}
	rows = h.consultaOK("SELECT id FROM _meta.tables WHERE namespace IS NULL")
	if len(rows) != 1 || rows[0]["id"] != dataset {
		t.Fatalf("IS NULL tem de achar só a tabela base: %v", rows)
	}
}

// TestMetaTablesContaAsColunasDeCadaTabela: `columns` é a CONTAGEM das
// colunas descritíveis — quem quer os nomes tem a `_meta.fields`.
//
// Os três números são diferentes de propósito: com a contagem trocada por um
// zero, ou pela contagem de outra tabela, a comparação fica vermelha.
func TestMetaTablesContaAsColunasDeCadaTabela(t *testing.T) {
	h := harnessWithMetaTables(t)
	rows := h.consultaOK("SELECT id, columns FROM _meta.tables WHERE id = '_counters.por_olt' OR id = '_meta.tables' OR id = 'radius_sessions' ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("as três tabelas do teste: %v", rows)
	}
	// O dublê de contador declara groupKey, olt e total.
	if rows[0]["columns"] != "3" {
		t.Fatalf("columns do contador = %q, quero 3", rows[0]["columns"])
	}
	// id, name, schema, namespace, kind, columns, description.
	if rows[1]["columns"] != "7" {
		t.Fatalf("columns da própria tabela = %q, quero 7", rows[1]["columns"])
	}
	if want := itoa(int64(len(h.sch.FieldsForMetadata()))); rows[2]["columns"] != want {
		t.Fatalf("columns da base = %q, quero %s", rows[2]["columns"], want)
	}
}

// TestMetaTablesPublicaDescriptionNulaEnquantoNadaADeclara: a coluna existe
// porque o catálogo a carrega, mas HOJE sai NULL em toda linha — nem a spec do
// dataset nem o registro de tabela virtual declaram descrição, e o catálogo
// não inventa uma. O dia em que houver onde declará-la, é este teste que muda.
func TestMetaTablesPublicaDescriptionNulaEnquantoNadaADeclara(t *testing.T) {
	h := harnessWithMetaTables(t)
	rows := h.consultaOK("SELECT id, description FROM _meta.tables")
	for _, l := range rows {
		if l["description"] != "<null>" {
			t.Fatalf("tabela %q com descrição %q: nada declara descrição de tabela hoje", l["id"], l["description"])
		}
	}
	byIsNull := h.consultaOK("SELECT id FROM _meta.tables WHERE description IS NULL")
	if len(byIsNull) != len(rows) || len(rows) == 0 {
		t.Fatalf("IS NULL tem de achar as %d linhas, achou %d", len(rows), len(byIsNull))
	}
}

// TestMetaTablesSeparaHomonimasPeloEndereco: nada impede um contador chamado
// `tables` — a spec só recusa o nome do próprio dataset —, e aí duas tabelas
// têm o mesmo `name` em schemas diferentes. Se a key fosse o nome, o point
// lookup devolveria uma e a varredura devolveria duas: o motor discordando de
// si mesmo conforme o plano escolhido.
func TestMetaTablesSeparaHomonimasPeloEndereco(t *testing.T) {
	h := harnessWithMetaTables(t)
	// O nome vai como literal, e não pela constante da implementação: o teste
	// tem de reprovar se o nome da tabela mudar sem ninguém perceber.
	tableVirtualOfTest(t, h.engine, dataset, schema.CountersNamespace, "tables")

	rows := h.consultaOK("SELECT id, schema FROM _meta.tables WHERE name = 'tables' ORDER BY id")
	if got := strings.Join(chaves(rows, "id"), ","); got != "_counters.tables,_meta.tables" {
		t.Fatalf("homônimas = %s", got)
	}
	if got := strings.Join(chaves(rows, "schema"), ","); got != "telecom,_meta" {
		t.Fatalf("schemas das homônimas = %s", got)
	}
	// E cada uma responde ao point lookup pelo SEU endereço.
	for _, id := range []string{"_counters.tables", "_meta.tables"} {
		one := h.consultaOK("SELECT name FROM _meta.tables WHERE id = '" + id + "'")
		if len(one) != 1 || one[0]["name"] != "tables" {
			t.Fatalf("point lookup de %q = %v", id, one)
		}
	}
}
