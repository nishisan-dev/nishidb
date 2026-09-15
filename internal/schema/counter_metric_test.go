package schema

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// approvedMetricsContract é o contrato do plano
// (planning/metricas-de-janela-e-alarme-qualificavel.md), com o `aggregates`
// da segunda métrica fora da ordem alfabética para que a ordem declarada seja
// distinguível de uma ordenação, e o qualifyWhen com espaços a colapsar.
const approvedMetricsContract = `
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    metrics:
      - name: auth_5m
        type: window
        source: total
        window: 5m
        aggregates: [avg]
      - name: auth_24h
        type: window
        source: total
        window: 24h
        aggregates: [max, min]
    alarms:
      - name: primary_quase_vazia
        qualifyWhen: "  auth_24h_max    >  10 "
        when: "auth_5m_avg < 1"
        sustainedFor: 5m
      - name: olt_down
        when: "total < 100"
        sustainedFor: 2m
`

func TestCounterMetricsContractParsesInDeclaredOrder(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(approvedMetricsContract)), BuiltinDefaults)
	if err != nil {
		t.Fatalf("o contrato aprovado tem de parsear: %v", err)
	}
	c := spec.Counters[0]
	want := []CounterMetricSpec{
		{Name: "auth_5m", Type: MetricTypeWindow, Source: MetricSourceTotal, Window: 5 * time.Minute, Aggregates: []string{"avg"}},
		{Name: "auth_24h", Type: MetricTypeWindow, Source: MetricSourceTotal, Window: 24 * time.Hour, Aggregates: []string{"max", "min"}},
	}
	if !reflect.DeepEqual(c.Metrics, want) {
		t.Fatalf("metrics = %+v\nquero %+v", c.Metrics, want)
	}
	if got := strings.Join(c.Metrics[0].Columns(), ","); got != "auth_5m_avg" {
		t.Errorf("Columns() da primeira = %q, quero auth_5m_avg", got)
	}
	if got := strings.Join(c.Metrics[1].Columns(), ","); got != "auth_24h_max,auth_24h_min" {
		t.Errorf("Columns() da segunda = %q, quero a ordem declarada auth_24h_max,auth_24h_min", got)
	}
	if got := c.Alarms[0].QualifyWhen; got != "auth_24h_max > 10" {
		t.Errorf("qualifyWhen = %q, quero %q normalizado como o when", got, "auth_24h_max > 10")
	}
	if got := c.Alarms[1].QualifyWhen; got != "" {
		t.Errorf("alarme sem qualifyWhen tem de ficar com a string vazia, veio %q", got)
	}
}

func TestCounterWithoutMetricsBlockHasNilMetrics(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters("counters:\n  - name: c\n    groupBy: [olt]\n")), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Counters[0].Metrics != nil {
		t.Fatalf("sem bloco, Metrics tem de ser nil; veio %+v", spec.Counters[0].Metrics)
	}
}

// TestMetricWindowAcceptsTheBoundaries: os limites são inclusivos, e o sufixo
// `d` da gramática de duração vale.
func TestMetricWindowAcceptsTheBoundaries(t *testing.T) {
	cases := map[string]time.Duration{"1m": time.Minute, "30d": 30 * 24 * time.Hour, "1d12h": 36 * time.Hour, "120s": 2 * time.Minute}
	for raw, want := range cases {
		t.Run(raw, func(t *testing.T) {
			block := counterWith("olt", metricsOf(metricItem(map[string]string{"window": raw})))
			spec, err := Parse([]byte(yamlWithCounters(block)), BuiltinDefaults)
			if err != nil {
				t.Fatalf("window %s recusada: %v", raw, err)
			}
			if got := spec.Counters[0].Metrics[0].Window; got != want {
				t.Errorf("window %s = %v, quero %v", raw, got, want)
			}
		})
	}
}

func TestCounterMetricsAcceptUpToTheCeiling(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(counterWith("olt", metricsOf(numberedMetrics(MaxCounterMetrics)...)))), BuiltinDefaults)
	if err != nil {
		t.Fatalf("%d métricas (o teto) recusadas: %v", MaxCounterMetrics, err)
	}
	if got := len(spec.Counters[0].Metrics); got != MaxCounterMetrics {
		t.Fatalf("%d métricas, quero %d", got, MaxCounterMetrics)
	}
}

// TestAlarmNamedAfterCounterColumnIsStillAccepted: a recusa de nome igual a
// coluna vale para a métrica, e NÃO para o alarme — os alarmes foram
// publicados sem ela, e uma configuração que subia não pode passar a ser
// recusada sem ter mudado.
func TestAlarmNamedAfterCounterColumnIsStillAccepted(t *testing.T) {
	alarms := "    alarms:\n" +
		"      - name: total\n        when: \"total = 0\"\n        sustainedFor: 2m\n" +
		"      - name: olt\n        when: \"total = 0\"\n        sustainedFor: 2m\n" +
		"      - name: auth_5m_avg\n        when: \"total = 0\"\n        sustainedFor: 2m\n"
	block := counterWith("olt", alarms+metricsOf(metricItem(map[string]string{"name": "auth_5m"})))
	if _, err := Parse([]byte(yamlWithCounters(block)), BuiltinDefaults); err != nil {
		t.Fatalf("alarme com nome de coluna tem de continuar aceito: %v", err)
	}
}

func TestCounterColumnsListFixedThenGroupByThenGenerated(t *testing.T) {
	cols := CounterColumns([]string{"olt", "slot"}, []CounterMetricSpec{
		{Name: "a", Aggregates: []string{AggregateMax, AggregateAvg}},
		{Name: "b", Aggregates: []string{AggregateMin}},
	})
	got := make([]string, len(cols))
	for i, c := range cols {
		got[i] = c.Name + "@" + c.Origin
	}
	want := []string{"groupKey@", "total@", "olt@groupBy", "slot@groupBy", "a_max@metrics[0]", "a_avg@metrics[0]", "b_min@metrics[1]"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("colunas = %v\nquero %v", got, want)
	}
}

// TestFindCounterColumnCollisionNamesBothOrigins cobre a colisão entre duas
// colunas geradas, que o YAML não alcança (ela exige dois nomes de métrica
// iguais, recusados antes) mas que a função exportada precisa responder por
// quem a chama sem passar pela validação.
func TestFindCounterColumnCollisionNamesBothOrigins(t *testing.T) {
	first, second, found := FindCounterColumnCollision([]string{"olt"}, []CounterMetricSpec{
		{Name: "a", Aggregates: []string{AggregateAvg}},
		{Name: "a", Aggregates: []string{AggregateAvg}},
	})
	if !found {
		t.Fatal("duas colunas a_avg não foram apontadas como colisão")
	}
	if first.Origin != "metrics[0]" || second.Origin != "metrics[1]" || second.Name != "a_avg" {
		t.Fatalf("colisão = %+v × %+v, quero a_avg de metrics[0] × metrics[1]", first, second)
	}
	if _, _, found := FindCounterColumnCollision([]string{"olt"}, []CounterMetricSpec{{Name: "a", Aggregates: []string{AggregateAvg, AggregateMax}}}); found {
		t.Fatal("colunas distintas apontadas como colisão")
	}
}

func TestCounterMetricsRefusals(t *testing.T) {
	alarm := func(name, extra string) string {
		return "    alarms:\n      - name: " + name + "\n        when: \"total = 0\"\n        sustainedFor: 2m\n" + extra
	}
	cases := []struct {
		name, fields, block, want string
	}{
		{"metrics is not a list", "", counterWith("olt", "    metrics:\n      name: a\n"), "counters[0].metrics must be a list of metrics"},
		{"empty list", "", counterWith("olt", "    metrics: []\n"), "counters[0].metrics: empty list"},
		{"above the ceiling", "", counterWith("olt", metricsOf(numberedMetrics(MaxCounterMetrics+1)...)), "9 metrics declared, the ceiling is 8"},
		{"unknown key", "", counterWith("olt", metricsOf(metricItem(nil)+"        windw: 5m\n")), "metrics[0].windw: chave desconhecida"},
		{"name missing", "", counterWith("olt", metricsOf(metricItem(map[string]string{"name": "-"}))), "metrics[0].name required"},
		{"name invalid", "", counterWith("olt", metricsOf(metricItem(map[string]string{"name": "Auth-5m"}))), `metrics[0].name "Auth-5m" invalid`},
		{"name starting with a digit", "", counterWith("olt", metricsOf(metricItem(map[string]string{"name": "5m"}))), `metrics[0].name "5m" invalid (expected [a-z_][a-z0-9_]*`},
		{"name that is only digits", "", counterWith("olt", metricsOf(metricItem(map[string]string{"name": "24"}))), `metrics[0].name "24" invalid`},
		{"name repeated", "", counterWith("olt", metricsOf(metricItem(nil), metricItem(map[string]string{"window": "10m"}))), `counters[0].metrics[1].name "a" repeated (already declared in counters[0].metrics[0])`},
		{"type missing", "", counterWith("olt", metricsOf(metricItem(map[string]string{"type": "-"}))), "metrics[0].type required"},
		{"type unknown", "", counterWith("olt", metricsOf(metricItem(map[string]string{"type": "rate"}))), `metrics[0].type "rate" unknown`},
		{"source missing", "", counterWith("olt", metricsOf(metricItem(map[string]string{"source": "-"}))), "metrics[0].source required"},
		{"source unknown", "", counterWith("olt", metricsOf(metricItem(map[string]string{"source": "rows"}))), `metrics[0].source "rows" unknown`},
		{"window missing", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "-"}))), "metrics[0].window required"},
		{"window unparseable", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "cinco minutos"}))), `metrics[0].window: duração inválida "cinco minutos"`},
		{"window never", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "never"}))), `metrics[0].window "never" is not a window`},
		{"window zero", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "0"}))), `metrics[0].window "0" declares no duration`},
		{"window zero with unit", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "0m"}))), `metrics[0].window "0m" declares no duration`},
		{"window negative", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "-5m"}))), "metrics[0].window: duração deve ser positiva"},
		{"window below the minimum", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "30s"}))), "metrics[0].window 30s is below the minimum of 1m0s"},
		{"window above the maximum", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "30d1m"}))), "metrics[0].window 30d1m is above the maximum of 720h0m0s"},
		{"window not a multiple of 1m", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "90s"}))), "metrics[0].window 90s is not a multiple of 1m"},
		{"window with sub-second remainder", "", counterWith("olt", metricsOf(metricItem(map[string]string{"window": "5m500ms"}))), "metrics[0].window 5m500ms is not a multiple of 1m"},
		{"aggregates missing", "", counterWith("olt", metricsOf(metricItem(map[string]string{"aggregates": "-"}))), "metrics[0].aggregates required"},
		{"aggregates empty", "", counterWith("olt", metricsOf(metricItem(map[string]string{"aggregates": "[]"}))), "metrics[0].aggregates empty"},
		{"aggregates repeated", "", counterWith("olt", metricsOf(metricItem(map[string]string{"aggregates": "[avg, max, avg]"}))), `metrics[0].aggregates: "avg" repeated`},
		{"aggregates unknown", "", counterWith("olt", metricsOf(metricItem(map[string]string{"aggregates": "[avg, sum]"}))), `metrics[0].aggregates: "sum" unknown`},
		{"aggregates not a list", "", counterWith("olt", metricsOf(metricItem(map[string]string{"aggregates": "avg"}))), "metrics[0].aggregates: valor inválido"},
		{"qualifyWhen blank", "", counterWith("olt", alarm("a", "        qualifyWhen: \"   \"\n")), "counters[0].alarms[0].qualifyWhen is blank"},
		{"qualifyWhen empty", "", counterWith("olt", alarm("a", "        qualifyWhen: \"\"\n")), "counters[0].alarms[0].qualifyWhen is blank"},
		{
			"generated column collides with groupBy column", "auth_5m_avg",
			counterWith("auth_5m_avg", metricsOf(metricItem(map[string]string{"name": "auth_5m"}))),
			`counters[0].metrics[0]: column "auth_5m_avg" generated by metrics[0] collides with groupBy column "auth_5m_avg"; rename the metric`,
		},
		{
			"groupBy column collides with fixed total", "total",
			counterWith("total", ""),
			`counters[0].groupBy: groupBy column "total" collides with fixed column "total" of table _counters.<name>; rename the column in the schema`,
		},
		{
			"groupBy column collides with fixed groupKey", "groupKey",
			counterWith("groupKey", ""),
			`counters[0].groupBy: groupBy column "groupKey" collides with fixed column "groupKey" of table _counters.<name>`,
		},
		{
			"metric name collides with groupBy column", "",
			counterWith("olt", metricsOf(metricItem(map[string]string{"name": "olt"}))),
			`counters[0].metrics[0].name "olt" collides with groupBy column "olt"; rename the metric`,
		},
		{
			"metric name collides with fixed column", "",
			counterWith("olt", metricsOf(metricItem(map[string]string{"name": "total"}))),
			`counters[0].metrics[0].name "total" collides with fixed column "total"`,
		},
		{
			"metric name collides with generated column", "",
			counterWith("olt", metricsOf(metricItem(nil), metricItem(map[string]string{"name": "a_avg", "aggregates": "[max]"}))),
			`counters[0].metrics[1].name "a_avg" collides with column "a_avg" generated by metrics[0]`,
		},
		{
			"metric name collides with alarm name", "",
			counterWith("olt", alarm("olt_down", "")+metricsOf(metricItem(map[string]string{"name": "olt_down"}))),
			`counters[0].metrics[0].name "olt_down" collides with the name of alarm counters[0].alarms[0]`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			doc := yamlWithCounters(stringFields(c.fields) + c.block)
			_, err := Parse([]byte(doc), BuiltinDefaults)
			if err == nil {
				t.Fatalf("aceitou:\n%s", c.block)
			}
			if !errors.Is(err, ErrInvalidSpec) {
				t.Errorf("erro %q não embrulha ErrInvalidSpec", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("erro %q não menciona %q", err, c.want)
			}
		})
	}
}

// TestMetricNameAcceptsLetterOrUnderscoreStart: a regra do nome de métrica
// segue o início de identificador do lexer SQL — letra ou `_` — e dígito
// depois do primeiro caractere continua valendo.
func TestMetricNameAcceptsLetterOrUnderscoreStart(t *testing.T) {
	for _, name := range []string{"auth_5m", "a5m", "_auth", "m_24h"} {
		t.Run(name, func(t *testing.T) {
			block := counterWith("olt", metricsOf(metricItem(map[string]string{"name": name})))
			spec, err := Parse([]byte(yamlWithCounters(block)), BuiltinDefaults)
			if err != nil {
				t.Fatalf("nome de métrica %q recusado: %v", name, err)
			}
			if got := spec.Counters[0].Metrics[0].Name; got != name {
				t.Errorf("name = %q, quero %q", got, name)
			}
		})
	}
}

// counterWith monta um bloco `counters:` com um contador `c` agrupado pelas
// colunas dadas (texto de dentro dos colchetes). rest são as chaves
// seguintes, já indentadas com quatro espaços.
func counterWith(groupBy, rest string) string {
	return "counters:\n  - name: c\n    groupBy: [" + groupBy + "]\n" + rest
}

// metricsOf monta a chave `metrics:` com os itens dados.
func metricsOf(items ...string) string {
	return "    metrics:\n" + strings.Join(items, "")
}

// metricItem monta um item válido de `metrics:` com nome `a`. Cada chave pode
// ser substituída pelo valor em overrides, e o valor "-" remove a chave.
func metricItem(overrides map[string]string) string {
	keys := [][2]string{{"name", "a"}, {"type", "window"}, {"source", "total"}, {"window", "5m"}, {"aggregates", "[avg]"}}
	var b strings.Builder
	prefix := "      - "
	for _, kv := range keys {
		value := kv[1]
		if o, ok := overrides[kv[0]]; ok {
			value = o
		}
		if value == "-" {
			continue
		}
		fmt.Fprintf(&b, "%s%s: %s\n", prefix, kv[0], value)
		prefix = "        "
	}
	return b.String()
}

// numberedMetrics monta n itens válidos de `metrics:` com nomes distintos.
func numberedMetrics(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = metricItem(map[string]string{"name": fmt.Sprintf("m%d", i)})
	}
	return out
}

// stringFields declara campos string a mais no schema de yamlWithCounters.
// Funciona porque o texto de yamlWithCounters termina DENTRO de
// schema.fields: o que vier antes de `counters:` continua o mapa de campos.
// Se aquele texto mudar de forma, o YAML deixa de parsear e os casos que usam
// campos a mais falham alto, em vez de passarem sem o campo.
func stringFields(names string) string {
	if names == "" {
		return ""
	}
	var b strings.Builder
	for _, n := range strings.Split(names, ",") {
		fmt.Fprintf(&b, "    %s:\n      type: string\n", n)
	}
	return b.String()
}
