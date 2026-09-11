package counters_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// engineWithMeta registra o dataset de mentira e a `_meta.counters` sobre os
// contadores dados — a mesma costura do Register, sem o Store real.
func engineWithMeta(t *testing.T, cs ...*counter.Counter) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte("version: 1\nname: servicos\nmaterialization:\n  key:\n    field: id\nschema:\n  fields:\n    id:\n      type: string\n"), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	eng := query.NewEngine()
	if err := eng.Register("servicos", sc, fakestorage.New(fakestorage.Indexes{})); err != nil {
		t.Fatal(err)
	}
	contados := make([]counters.Contado, 0, len(cs))
	for _, c := range cs {
		contados = append(contados, c)
	}
	msc, source, err := counters.TableMeta(contados)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterVirtual("servicos", meta.Namespace, counters.TableMetaCounters, msc, source); err != nil {
		t.Fatal(err)
	}
	return eng
}

// TestMetaCountersListaOsContadores: uma linha por contador, com a
// definição e o retrato vivo — o que antes só saía pelo `nishidb counters`.
func TestMetaCountersListaOsContadores(t *testing.T) {
	byOLT := openCounter(t, defByOLT("autenticados_por_olt"))
	byPON := openCounter(t, defByPON("autenticados_por_pon"))
	populaPON(t, byOLT)
	populaPON(t, byPON)
	eng := engineWithMeta(t, byOLT, byPON)

	rows := consulta(t, eng, "SELECT name, groupBy, condition, groups, maxGroups, retired, refused, orphans FROM _meta.counters ORDER BY name")
	if got := column(rows, "name"); got != "autenticados_por_olt,autenticados_por_pon" {
		t.Fatalf("name = %s", got)
	}
	if got := column(rows, "groupBy"); got != "olt,olt|slot|port" {
		t.Fatalf("groupBy = %s (o separador é o mesmo do groupKey)", got)
	}
	if got := column(rows, "condition"); got != "authenticated = true,authenticated = true" {
		t.Fatalf("condition = %s", got)
	}
	// populaPON toca duas OLTs e quatro PONs.
	if got := column(rows, "groups"); got != "2,4" {
		t.Fatalf("groups = %s", got)
	}
	if got := column(rows, "maxGroups"); got != "1000,1000" {
		t.Fatalf("maxGroups = %s", got)
	}
	if got := column(rows, "retired") + "/" + column(rows, "refused") + "/" + column(rows, "orphans"); got != "0,0/0,0/0,0" {
		t.Fatalf("retired/refused/orphans = %s", got)
	}

	// bytes é o vetor mais o dicionário: positivo depois de 113 linhas.
	rows = consulta(t, eng, "SELECT name, bytes FROM _meta.counters WHERE bytes > 0")
	if len(rows) != 2 {
		t.Fatalf("bytes > 0 em %d contadores, quero 2: %v", len(rows), rows)
	}
}

// TestMetaCountersPublicaLinhasETotal: os dois agregados do contador saem por
// SQL em tempo constante — antes o mesmo número exigia o `counters --audit`,
// que varre o dataset sob snapshot.
//
// A bancada dá 113 linhas e 110 na condição, e os dois números serem
// DIFERENTES é o que faz o teste discriminar: com rows == total ele passaria
// verde com as duas colunas trocadas.
func TestMetaCountersPublicaLinhasETotal(t *testing.T) {
	byOLT := openCounter(t, defByOLT("autenticados_por_olt"))
	byPON := openCounter(t, defByPON("autenticados_por_pon"))
	populaPON(t, byOLT)
	populaPON(t, byPON)
	eng := engineWithMeta(t, byOLT, byPON)

	rows := consulta(t, eng, "SELECT name, rows, total FROM _meta.counters ORDER BY name")
	if got := column(rows, "rows"); got != "113,113" {
		t.Fatalf("rows = %s, quero 113,113 (toda linha tem as colunas do groupBy)", got)
	}
	if got := column(rows, "total"); got != "110,110" {
		t.Fatalf("total = %s, quero 110,110 (três linhas não estão autenticadas)", got)
	}

	// Nunca NULL, ao contrário das duas da auditoria: zero linhas é um fato
	// medido, e a diferença entre as duas ausências é justamente o que a
	// tabela existe para mostrar.
	vazio := openCounter(t, defByOLT("sem_linha"))
	rows = consulta(t, engineWithMeta(t, vazio), "SELECT name, rows, total FROM _meta.counters WHERE rows = 0 AND total = 0")
	if len(rows) != 1 {
		t.Fatalf("contador sem linha tem de sair com zero, não com NULL: %v", rows)
	}
}

// TestMetaCountersSemContadorEhTabelaVazia: a tabela existe em todo dataset,
// e "nenhum" é uma resposta — não um erro de tabela desconhecida.
func TestMetaCountersSemContadorEhTabelaVazia(t *testing.T) {
	eng := engineWithMeta(t)
	rows := consulta(t, eng, "SELECT name FROM _meta.counters")
	if len(rows) != 0 {
		t.Fatalf("dataset sem contador listou %v", rows)
	}
}

// TestMetaCountersAuditoriaEhNulaAteAPrimeira: zero divergências e "nunca
// auditado" são afirmações diferentes, e as duas colunas da auditoria só
// ganham valor depois de uma auditoria conclusiva.
func TestMetaCountersAuditoriaEhNulaAteAPrimeira(t *testing.T) {
	c := openCounter(t, defByOLT("autenticados_por_olt"))
	populaPON(t, c)
	eng := engineWithMeta(t, c)

	rows := consulta(t, eng, "SELECT auditDivergent, auditAt FROM _meta.counters")
	if len(rows) != 1 || rows[0]["auditDivergent"] != "<null>" || rows[0]["auditAt"] != "<null>" {
		t.Fatalf("antes da auditoria: %v", rows)
	}
	rows = consulta(t, eng, "SELECT name FROM _meta.counters WHERE auditAt IS NULL")
	if len(rows) != 1 {
		t.Fatalf("IS NULL tem de achar o contador nunca auditado: %v", rows)
	}

	c.RecordAudit(3, time.Date(2026, 9, 4, 23, 40, 0, 0, time.UTC))
	rows = consulta(t, eng, "SELECT auditDivergent, auditAt FROM _meta.counters")
	if len(rows) != 1 || rows[0]["auditDivergent"] != "3" || rows[0]["auditAt"] == "<null>" {
		t.Fatalf("depois da auditoria: %v", rows)
	}
	rows = consulta(t, eng, "SELECT name FROM _meta.counters WHERE auditDivergent > 0")
	if len(rows) != 1 {
		t.Fatalf("a divergência tem de ser filtrável: %v", rows)
	}
}

// TestMetaCountersSemWhereTemCondicaoNula: contador sem `where` conta toda
// linha com grupo, e a coluna diz isso por ausência.
func TestMetaCountersSemWhereTemCondicaoNula(t *testing.T) {
	def := defByOLT("servicos_por_olt")
	def.Where = ""
	def.NewEvaluator = nil
	c := openCounter(t, def)
	eng := engineWithMeta(t, c)
	rows := consulta(t, eng, "SELECT name, condition FROM _meta.counters WHERE condition IS NULL")
	if len(rows) != 1 || rows[0]["name"] != "servicos_por_olt" {
		t.Fatalf("condition IS NULL = %v", rows)
	}
}

// TestMetaCountersPointLookupPeloNome: o nome é a key da tabela.
func TestMetaCountersPointLookupPeloNome(t *testing.T) {
	a := openCounter(t, defByOLT("a"))
	b := openCounter(t, defByPON("b"))
	eng := engineWithMeta(t, a, b)
	rows := consulta(t, eng, "SELECT name, groupBy FROM _meta.counters WHERE name = 'b'")
	if len(rows) != 1 || rows[0]["groupBy"] != "olt|slot|port" {
		t.Fatalf("point lookup = %v", rows)
	}
	if rows := consulta(t, eng, "SELECT name FROM _meta.counters WHERE name = 'nao_existe'"); len(rows) != 0 {
		t.Fatalf("nome inexistente devolveu %v", rows)
	}
}

// TestMetaFieldsContinuaRespondendoAoLadoDaCounters: as duas tabelas do
// `_meta` convivem, e a recusa de tabela desconhecida nomeia as duas.
func TestMetaFieldsContinuaRespondendoAoLadoDaCounters(t *testing.T) {
	eng := engineWithMeta(t, openCounter(t, defByOLT("a")))
	rows := consulta(t, eng, "SELECT name FROM _meta.fields")
	if len(rows) == 0 {
		t.Fatal("_meta.fields tem de continuar respondendo")
	}
	_, err := eng.Execute(t.Context(), "servicos", "SELECT x FROM _meta.nada", nil)
	if err == nil || !strings.Contains(err.Error(), "counters, fields") {
		t.Fatalf("a recusa tem de listar counters e fields: %v", err)
	}
}

// TestTabelaMetaRecusaNomeRepetido: dois contadores com o mesmo nome não
// cabem numa tabela cuja key é o nome.
func TestTabelaMetaRecusaNomeRepetido(t *testing.T) {
	a := openCounter(t, defByOLT("a"))
	b := openCounter(t, defByOLT("a"))
	if _, _, err := counters.TableMeta([]counters.Contado{a, b}); err == nil {
		t.Fatal("nome repetido tem de ser recusado")
	}
}
