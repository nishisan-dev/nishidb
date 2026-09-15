package metaalarm_test

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/metaalarm"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

var t0 = time.Date(2026, 9, 12, 3, 14, 0, 0, time.UTC)

// motor registra um dataset de mentira e a tabela sob ele.
func motor(t *testing.T, groups func() []alarm.ActiveGroup) *query.Engine {
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
	if err := eng.DeclaraNamespace("servicos", meta.Namespace); err != nil {
		t.Fatal(err)
	}
	if err := metaalarm.Register(eng, "servicos", metaalarm.Source{Groups: groups}); err != nil {
		t.Fatal(err)
	}
	return eng
}

type row map[string]string

func consulta(t *testing.T, eng *query.Engine, sql string) []row {
	t.Helper()
	it, err := eng.Execute(context.Background(), "servicos", sql, nil)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	var out []row
	for it.Next() {
		r := row{}
		for _, c := range it.Row().Cols() {
			if c.Null {
				r[c.Name] = "<null>"
				continue
			}
			switch {
			case len(c.Val.Bytes) > 0:
				r[c.Name] = string(c.Val.Bytes)
			default:
				r[c.Name] = itoa(c.Val.I64)
			}
		}
		out = append(out, r)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func grupos() []alarm.ActiveGroup {
	return []alarm.ActiveGroup{
		{
			Counter: "autenticados_por_pon", Alarm: "olt_down", GroupKey: "BR_SPOAM_OLT_09|2",
			Firing: false, Since: t0, Condition: "total < 100", SustainedFor: 2 * time.Minute,
		},
		{
			Counter: "autenticados_por_pon", Alarm: "olt_down", GroupKey: "BR_SPOAM_OLT_14|8",
			Firing: true, Since: t0.Add(-time.Hour), Condition: "total < 100", SustainedFor: 2 * time.Minute,
		},
	}
}

// TestTabelaListaOsAlarmesEmObservacao é o que a tabela existe para responder:
// QUAL equipamento, desde quando, e se o prazo já se cumpriu — o detalhe que as
// séries agregadas não dão.
func TestTabelaListaOsAlarmesEmObservacao(t *testing.T) {
	eng := motor(t, grupos)
	got := consulta(t, eng, "SELECT id, counter, alarm, groupKey, state, condition, sustainedForMs FROM _meta.alarms")
	if len(got) != 2 {
		t.Fatalf("%d linhas, quero 2", len(got))
	}
	if got[0]["id"] != "autenticados_por_pon|olt_down|BR_SPOAM_OLT_09|2" {
		t.Errorf("id = %q", got[0]["id"])
	}
	if got[0]["state"] != metaalarm.StatePending {
		t.Errorf("state do que ainda conta o prazo = %q", got[0]["state"])
	}
	if got[1]["state"] != metaalarm.StateFiring {
		t.Errorf("state do disparado = %q", got[1]["state"])
	}
	if got[0]["condition"] != "total < 100" || got[0]["sustainedForMs"] != "120000" {
		t.Errorf("a regra não chegou na linha: %+v", got[0])
	}
}

// TestFiltrarPorEstadoFunciona: é o WHERE do SQL sobre a tabela, e é como
// alguém pergunta "o que está disparado agora".
func TestFiltrarPorEstadoFunciona(t *testing.T) {
	eng := motor(t, grupos)
	got := consulta(t, eng, "SELECT groupKey FROM _meta.alarms WHERE state = 'firing'")
	if len(got) != 1 || got[0]["groupKey"] != "BR_SPOAM_OLT_14|8" {
		t.Fatalf("linhas = %+v", got)
	}
}

// TestOrdenarPorSinceFunciona: o alarme mais antigo primeiro é a pergunta
// natural de quem investiga um incidente.
func TestOrdenarPorSinceFunciona(t *testing.T) {
	eng := motor(t, grupos)
	got := consulta(t, eng, "SELECT groupKey FROM _meta.alarms ORDER BY since ASC")
	if len(got) != 2 || got[0]["groupKey"] != "BR_SPOAM_OLT_14|8" {
		t.Fatalf("a ordem por since não trouxe o mais antigo primeiro: %+v", got)
	}
}

// TestBuscaPelaKeyFunciona: a key é a coluna `id`, e uma consulta por ela é o
// caminho direto do painel que já sabe o que quer olhar.
func TestBuscaPelaKeyFunciona(t *testing.T) {
	eng := motor(t, grupos)
	got := consulta(t, eng, "SELECT state FROM _meta.alarms WHERE id = 'autenticados_por_pon|olt_down|BR_SPOAM_OLT_14|8'")
	if len(got) != 1 || got[0]["state"] != metaalarm.StateFiring {
		t.Fatalf("linhas = %+v", got)
	}
}

// TestTabelaVaziaNaoEhErro: um dataset sem alarme declarado responde a lista
// vazia. `SELECT *` nunca pode falhar por falta de configuração.
func TestTabelaVaziaNaoEhErro(t *testing.T) {
	if got := consulta(t, motor(t, nil), "SELECT id FROM _meta.alarms"); len(got) != 0 {
		t.Fatalf("linhas = %+v", got)
	}
	if got := consulta(t, motor(t, func() []alarm.ActiveGroup { return nil }), "SELECT id FROM _meta.alarms"); len(got) != 0 {
		t.Fatalf("linhas = %+v", got)
	}
}

// TestSelectEstrelaTrazTodasAsColunas: a projeção completa é o que alguém digita
// primeiro ao explorar a tabela.
func TestSelectEstrelaTrazTodasAsColunas(t *testing.T) {
	got := consulta(t, motor(t, grupos), "SELECT * FROM _meta.alarms LIMIT 1")
	if len(got) != 1 {
		t.Fatalf("%d linhas", len(got))
	}
	for _, col := range []string{"id", "counter", "alarm", "groupKey", "state", "since", "condition", "qualifyCondition", "sustainedForMs"} {
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
				Since: t0, Condition: "total < 100", SustainedFor: 2 * time.Minute,
			},
			{
				Counter: "autenticados_por_olt", Alarm: "primary_nearly_empty", GroupKey: "BR_SPOAM_OLT_14",
				Firing: true, Since: t0, Condition: "auth_5m_avg < 1", QualifyCondition: "auth_24h_max > 10",
				SustainedFor: 5 * time.Minute,
			},
		}
	}
	eng := motor(t, groups)
	got := consulta(t, eng, "SELECT alarm, qualifyCondition FROM _meta.alarms")
	if len(got) != 2 {
		t.Fatalf("%d linhas, quero 2", len(got))
	}
	if got[0]["qualifyCondition"] != "<null>" {
		t.Errorf("qualifyCondition do alarme sem qualifyWhen = %q, quero NULL", got[0]["qualifyCondition"])
	}
	if got[1]["qualifyCondition"] != "auth_24h_max > 10" {
		t.Errorf("qualifyCondition do alarme qualificado = %q", got[1]["qualifyCondition"])
	}
	nulls := consulta(t, eng, "SELECT alarm FROM _meta.alarms WHERE qualifyCondition IS NULL")
	if len(nulls) != 1 || nulls[0]["alarm"] != "olt_down" {
		t.Errorf("IS NULL trouxe %+v, quero só olt_down", nulls)
	}
}
