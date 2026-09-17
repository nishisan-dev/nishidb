package alarmtable_test

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/alarmtable"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

var t0 = time.Date(2026, 9, 12, 3, 14, 0, 0, time.UTC)

// engineWithGroups registra um dataset de mentira e a view sob ele.
func engineWithGroups(t *testing.T, groups func() []alarm.ActiveGroup) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte("version: 1\nname: servicos\nschemaName: telecom\n"+
		"materialization:\n  key:\n    field: id\nschema:\n  fields:\n    id:\n      type: string\n"),
		schema.BuiltinDefaults)
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
	if err := alarmtable.Register(eng, "servicos", alarmtable.Source{Groups: groups}); err != nil {
		t.Fatal(err)
	}
	return eng
}

type row map[string]string

func queryDataset(t *testing.T, eng *query.Engine, sql string) []row {
	t.Helper()
	it, err := eng.Execute(context.Background(), "servicos", sql, nil)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return collect(t, it, sql)
}

// querySchema roda pelo caminho da conexão por schema (ExecuteInSchema),
// onde `FROM alarms` e `FROM <schema>.alarms` resolvem contra a árvore do
// schema, e não contra o dataset por handle.
func querySchema(t *testing.T, eng *query.Engine, sql string) []row {
	t.Helper()
	it, err := eng.ExecuteInSchema(context.Background(), "servicos", sql, nil)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return collect(t, it, sql)
}

func collect(t *testing.T, it executor.RowIterator, sql string) []row {
	t.Helper()
	defer func() { _ = it.Close() }()
	var out []row
	for it.Next() {
		r := row{}
		for _, c := range it.Row().Cols() {
			r[c.Name] = render(c)
		}
		out = append(out, r)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

// render lê a coluna PELO TIPO, e não pela heurística "tem Bytes ou não": uma
// coluna double NULA-ou-não passa pela mesma trilha que string e timestamp, o
// que o teste do valor de disparo (double sobre bits de int64) exige.
func render(c executor.ColView) string {
	if c.Null {
		return "<null>"
	}
	switch c.Val.Type {
	case types.TypeString:
		return string(c.Val.Bytes)
	case types.TypeInt64, types.TypeTimestamp:
		return strconv.FormatInt(c.Val.I64, 10)
	case types.TypeDouble:
		return strconv.FormatFloat(math.Float64frombits(uint64(c.Val.I64)), 'g', -1, 64)
	case types.TypeBool:
		if c.Val.I64 != 0 {
			return "true"
		}
		return "false"
	default:
		return "?"
	}
}

func groupsFixture() []alarm.ActiveGroup {
	return []alarm.ActiveGroup{
		{
			Counter: "autenticados_por_pon", Alarm: "olt_down", GroupKey: "BR_SPOAM_OLT_09|2",
			Firing: false, Since: t0, Condition: "total < 100", SustainedFor: 2 * time.Minute,
			ValueColumn: "total",
		},
		{
			Counter: "autenticados_por_pon", Alarm: "olt_down", GroupKey: "BR_SPOAM_OLT_14|8",
			Firing: true, Since: t0.Add(-time.Hour), Condition: "total < 100", SustainedFor: 2 * time.Minute,
			ValueColumn: "total", FiredAt: t0, FiringValue: 42, HasFiringValue: true,
		},
	}
}

// TestTabelaListaOsAlarmesEmObservacao é o que a tabela existe para responder:
// QUAL equipamento, desde quando, e se o prazo já se cumpriu — o detalhe que as
// séries agregadas não dão.
func TestTabelaListaOsAlarmesEmObservacao(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	got := queryDataset(t, eng, "SELECT id, counter, alarm, groupKey, state, condition, sustainedForMs FROM _counters.alarms")
	if len(got) != 2 {
		t.Fatalf("%d linhas, quero 2", len(got))
	}
	if got[0]["id"] != "autenticados_por_pon|olt_down|BR_SPOAM_OLT_09|2" {
		t.Errorf("id = %q", got[0]["id"])
	}
	if got[0]["state"] != alarmtable.StatePending {
		t.Errorf("state do que ainda conta o prazo = %q", got[0]["state"])
	}
	if got[1]["state"] != alarmtable.StateFiring {
		t.Errorf("state do disparado = %q", got[1]["state"])
	}
	if got[0]["condition"] != "total < 100" || got[0]["sustainedForMs"] != "120000" {
		t.Errorf("a regra não chegou na linha: %+v", got[0])
	}
}

// TestFiltrarPorEstadoFunciona: é o WHERE do SQL sobre a tabela, e é como
// alguém pergunta "o que está disparado agora".
func TestFiltrarPorEstadoFunciona(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	got := queryDataset(t, eng, "SELECT groupKey FROM _counters.alarms WHERE state = 'firing'")
	if len(got) != 1 || got[0]["groupKey"] != "BR_SPOAM_OLT_14|8" {
		t.Fatalf("linhas = %+v", got)
	}
}

// TestOrdenarPorSinceFunciona: o alarme mais antigo primeiro é a pergunta
// natural de quem investiga um incidente.
func TestOrdenarPorSinceFunciona(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	got := queryDataset(t, eng, "SELECT groupKey FROM _counters.alarms ORDER BY since ASC")
	if len(got) != 2 || got[0]["groupKey"] != "BR_SPOAM_OLT_14|8" {
		t.Fatalf("a ordem por since não trouxe o mais antigo primeiro: %+v", got)
	}
}

// TestBuscaPelaKeyFunciona: a key é a coluna `id`, e uma consulta por ela é o
// caminho direto do painel que já sabe o que quer olhar.
func TestBuscaPelaKeyFunciona(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	got := queryDataset(t, eng, "SELECT state FROM _counters.alarms WHERE id = 'autenticados_por_pon|olt_down|BR_SPOAM_OLT_14|8'")
	if len(got) != 1 || got[0]["state"] != alarmtable.StateFiring {
		t.Fatalf("linhas = %+v", got)
	}
}

// TestTabelaVaziaNaoEhErro: um dataset sem alarme declarado responde a lista
// vazia. `SELECT *` nunca pode falhar por falta de configuração.
func TestTabelaVaziaNaoEhErro(t *testing.T) {
	if got := queryDataset(t, engineWithGroups(t, nil), "SELECT id FROM _counters.alarms"); len(got) != 0 {
		t.Fatalf("linhas = %+v", got)
	}
	if got := queryDataset(t, engineWithGroups(t, func() []alarm.ActiveGroup { return nil }), "SELECT id FROM _counters.alarms"); len(got) != 0 {
		t.Fatalf("linhas = %+v", got)
	}
}

// TestSelectEstrelaTrazTodasAsColunas: a projeção completa é o que alguém digita
// primeiro ao explorar a tabela.
func TestSelectEstrelaTrazTodasAsColunas(t *testing.T) {
	got := queryDataset(t, engineWithGroups(t, groupsFixture), "SELECT * FROM _counters.alarms LIMIT 1")
	if len(got) != 1 {
		t.Fatalf("%d linhas", len(got))
	}
	for _, col := range []string{
		"id", "counter", "alarm", "groupKey", "state", "since", "condition", "qualifyCondition", "sustainedForMs",
		"valueColumn", "firedAt", "firingValue",
	} {
		if _, ok := got[0][col]; !ok {
			t.Errorf("a coluna %q não veio no SELECT *: %+v", col, got[0])
		}
	}
}

// TestQualifyConditionColumnIsNullWhenNotDeclared: a coluna traz o
// `qualifyWhen` do alarme que o declara, e NULL — não a string vazia — do que
// não declara. É o que deixa `IS NULL` separar os dois tipos de alarme.
func TestQualifyConditionColumnIsNullWhenNotDeclared(t *testing.T) {
	groups := func() []alarm.ActiveGroup {
		return []alarm.ActiveGroup{
			{
				Counter: "autenticados_por_olt", Alarm: "olt_down", GroupKey: "BR_SPOAM_OLT_09",
				Since: t0, Condition: "total < 100", SustainedFor: 2 * time.Minute, ValueColumn: "total",
			},
			{
				Counter: "autenticados_por_olt", Alarm: "primary_nearly_empty", GroupKey: "BR_SPOAM_OLT_14",
				Firing: true, Since: t0, Condition: "auth_5m_avg < 1", QualifyCondition: "auth_24h_max > 10",
				SustainedFor: 5 * time.Minute, ValueColumn: "auth_5m_avg", FiredAt: t0, FiringValue: 0.6, HasFiringValue: true,
			},
		}
	}
	eng := engineWithGroups(t, groups)
	got := queryDataset(t, eng, "SELECT alarm, qualifyCondition FROM _counters.alarms")
	if len(got) != 2 {
		t.Fatalf("%d linhas, quero 2", len(got))
	}
	if got[0]["qualifyCondition"] != "<null>" {
		t.Errorf("qualifyCondition do alarme sem qualifyWhen = %q, quero NULL", got[0]["qualifyCondition"])
	}
	if got[1]["qualifyCondition"] != "auth_24h_max > 10" {
		t.Errorf("qualifyCondition do alarme qualificado = %q", got[1]["qualifyCondition"])
	}
	nulls := queryDataset(t, eng, "SELECT alarm FROM _counters.alarms WHERE qualifyCondition IS NULL")
	if len(nulls) != 1 || nulls[0]["alarm"] != "olt_down" {
		t.Errorf("IS NULL trouxe %+v, quero só olt_down", nulls)
	}
}

// TestFiringRowCarriesFiredAtAndFiringValue: a linha disparada leva os três,
// e firingValue chega como o double que a captura em internal/alarm produziu —
// mesmo vindo de uma coluna int64 na origem (total).
func TestFiringRowCarriesFiredAtAndFiringValue(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	got := queryDataset(t, eng, "SELECT valueColumn, firedAt, firingValue FROM _counters.alarms WHERE state = 'firing'")
	if len(got) != 1 {
		t.Fatalf("%d linhas, quero 1", len(got))
	}
	if got[0]["valueColumn"] != "total" {
		t.Errorf("valueColumn = %q, quero total", got[0]["valueColumn"])
	}
	if got[0]["firedAt"] != strconv.FormatInt(t0.UnixMilli(), 10) {
		t.Errorf("firedAt = %q, quero %d", got[0]["firedAt"], t0.UnixMilli())
	}
	if got[0]["firingValue"] != "42" {
		t.Errorf("firingValue = %q, quero 42", got[0]["firingValue"])
	}
}

// TestPendingRowHasNullFiredAtAndFiringValue: pendente é "ainda não disparou",
// e firedAt/firingValue têm de ficar NULL — não zero, que mentiria "disparou
// às 00:00 com valor 0".
func TestPendingRowHasNullFiredAtAndFiringValue(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	got := queryDataset(t, eng, "SELECT valueColumn, firedAt, firingValue FROM _counters.alarms WHERE state = 'pending'")
	if len(got) != 1 {
		t.Fatalf("%d linhas, quero 1", len(got))
	}
	if got[0]["valueColumn"] != "total" {
		t.Errorf("valueColumn = %q, quero total (SEMPRE preenchida)", got[0]["valueColumn"])
	}
	if got[0]["firedAt"] != "<null>" {
		t.Errorf("firedAt pendente = %q, quero NULL", got[0]["firedAt"])
	}
	if got[0]["firingValue"] != "<null>" {
		t.Errorf("firingValue pendente = %q, quero NULL", got[0]["firingValue"])
	}
}

// TestFiringValueIsNullWhenAbsent: disparado, mas a coluna do valor estava
// ausente do payload que decidiu — HasFiringValue=false. firedAt ainda entra
// (o disparo aconteceu), só o valor fica NULL.
func TestFiringValueIsNullWhenAbsent(t *testing.T) {
	groups := func() []alarm.ActiveGroup {
		return []alarm.ActiveGroup{
			{
				Counter: "autenticados_por_pon", Alarm: "olt_down", GroupKey: "BR_SPOAM_OLT_14|8",
				Firing: true, Since: t0, Condition: "total < 100", SustainedFor: 2 * time.Minute,
				ValueColumn: "total", FiredAt: t0, HasFiringValue: false,
			},
		}
	}
	eng := engineWithGroups(t, groups)
	got := queryDataset(t, eng, "SELECT firedAt, firingValue FROM _counters.alarms")
	if len(got) != 1 {
		t.Fatalf("%d linhas, quero 1", len(got))
	}
	if got[0]["firedAt"] != strconv.FormatInt(t0.UnixMilli(), 10) {
		t.Errorf("firedAt = %q, o disparo aconteceu e tem de aparecer", got[0]["firedAt"])
	}
	if got[0]["firingValue"] != "<null>" {
		t.Errorf("firingValue sem HasFiringValue = %q, quero NULL", got[0]["firingValue"])
	}
}

// TestTheViewOpensBySchemaQualifiedName prova a promessa central do plano: a
// view é publicada no schema do dataset, e responde tanto pelo nome nu
// (`FROM alarms`, dentro da conexão por schema) quanto pelo endereço
// qualificado (`FROM telecom.alarms`) e pelo atalho (`_counters.alarms`).
func TestTheViewOpensBySchemaQualifiedName(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	for _, sql := range []string{
		"SELECT id FROM alarms",
		"SELECT id FROM telecom.alarms",
		"SELECT id FROM _counters.alarms",
	} {
		got := querySchema(t, eng, sql)
		if len(got) != 2 {
			t.Errorf("%s: %d linhas, quero 2", sql, len(got))
		}
	}
}

// TestMetaAlarmsNoLongerExists: a quebra é deliberada (sem release publicada
// que dependa do contrato antigo) — `_meta.alarms` responde tabela
// inexistente, e não mais a view.
func TestMetaAlarmsNoLongerExists(t *testing.T) {
	eng := engineWithGroups(t, groupsFixture)
	_, err := eng.Execute(context.Background(), "servicos", "SELECT id FROM "+meta.Namespace+".alarms", nil)
	if err == nil {
		t.Fatal("_meta.alarms respondeu sem erro; a tabela foi removida do namespace _meta")
	}
}
