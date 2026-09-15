package counters_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// engineWithMetricsMeta registra o dataset de mentira e a
// `_meta.counter_metrics` sobre os contadores dados — a mesma costura do
// Register, sem o Store real.
func engineWithMetricsMeta(t *testing.T, specs []schema.CounterSpec, src counters.MetricSource) *query.Engine {
	t.Helper()
	eng := engineWithDataset(t)
	sc, store, err := counters.TableMetaMetrics(specs, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterVirtual("servicos", meta.Namespace, counters.TableMetaCounterMetrics, sc, store); err != nil {
		t.Fatal(err)
	}
	return eng
}

// metricsMetaSpecs declara três contadores: `d` com uma métrica, `b` sem
// nenhuma e `c` com duas. A ordem declarada NÃO é a alfabética, nem a dos
// contadores nem a das métricas de `c` (`auth_5m` antes de `auth_24h`), e os
// dois contadores com métrica repetem o nome `auth_5m` — é o que faz a key ser
// o par, e não o nome.
func metricsMetaSpecs() []schema.CounterSpec {
	return []schema.CounterSpec{
		{Name: "d", GroupBy: []string{"olt"}, Metrics: []schema.CounterMetricSpec{
			windowMetric("auth_5m", 5*time.Minute, schema.AggregateAvg),
		}},
		{Name: "b", GroupBy: []string{"olt"}},
		{Name: "c", GroupBy: []string{"olt"}, Metrics: []schema.CounterMetricSpec{
			windowMetric("auth_5m", 5*time.Minute, schema.AggregateAvg),
			windowMetric("auth_24h", 24*time.Hour, schema.AggregateMax, schema.AggregateMin),
		}},
	}
}

// metricsMetaSource publica o uso das três métricas. `c|auth_5m` nunca
// amostrou (sampledAt 0) apesar de ter grupos e bytes.
func metricsMetaSource() fakeMetricSource {
	return fakeMetricSource{
		"d|auth_5m":  {usage: [4]int64{7, 1024, 4096, 1757980800000}},
		"c|auth_5m":  {usage: [4]int64{3, 512, 2048, 0}},
		"c|auth_24h": {usage: [4]int64{3, 600, 8192, 1757980860000}},
	}
}

// TestCounterMetricsMetaListsDefinitionsInDeclaredOrder: uma linha por
// métrica, contadores e métricas na ordem declarada, com a definição inteira e
// as colunas que ela gera.
func TestCounterMetricsMetaListsDefinitionsInDeclaredOrder(t *testing.T) {
	eng := engineWithMetricsMeta(t, metricsMetaSpecs(), metricsMetaSource())

	names, _, _ := columnsOf(t, eng, "SELECT * FROM _meta.counter_metrics")
	sameColumnSet(t, names, "id,counter,name,type,source,windowMs,bucketMs,aggregates,columns,groups,heapBytes,diskBytes,sampledAt")
	rows := consulta(t, eng, "SELECT * FROM _meta.counter_metrics")
	if got, want := column(rows, "id"), "d|auth_5m,c|auth_5m,c|auth_24h"; got != want {
		t.Fatalf("id = %s, quero %s (ordem declarada, não alfabética)", got, want)
	}
	checks := map[string]string{
		"counter":  "d,c,c",
		"name":     "auth_5m,auth_5m,auth_24h",
		"type":     "window,window,window",
		"source":   "total,total,total",
		"windowMs": "300000,300000,86400000",
		"bucketMs": "5000,5000,1440000",
	}
	for col, want := range checks {
		if got := column(rows, col); got != want {
			t.Errorf("%s = %s, quero %s", col, got, want)
		}
	}
	// aggregates e columns têm vírgula dentro: conferidas linha a linha.
	if rows[2]["aggregates"] != "max,min" || rows[2]["columns"] != "auth_24h_max,auth_24h_min" {
		t.Errorf("c|auth_24h: aggregates=%q columns=%q, quero max,min e auth_24h_max,auth_24h_min", rows[2]["aggregates"], rows[2]["columns"])
	}
	if rows[0]["aggregates"] != "avg" || rows[0]["columns"] != "auth_5m_avg" {
		t.Errorf("d|auth_5m: aggregates=%q columns=%q", rows[0]["aggregates"], rows[0]["columns"])
	}
}

// TestCounterMetricsMetaReadsUsageFromMetricSource: o uso vem do leitor, e
// `sampledAt` é NULL enquanto a métrica nunca amostrou.
func TestCounterMetricsMetaReadsUsageFromMetricSource(t *testing.T) {
	eng := engineWithMetricsMeta(t, metricsMetaSpecs(), metricsMetaSource())

	rows := consulta(t, eng, "SELECT id, groups, heapBytes, diskBytes, sampledAt FROM _meta.counter_metrics")
	checks := map[string]string{
		"groups":    "7,3,3",
		"heapBytes": "1024,512,600",
		"diskBytes": "4096,2048,8192",
		"sampledAt": "1757980800000,<null>,1757980860000",
	}
	for col, want := range checks {
		if got := column(rows, col); got != want {
			t.Errorf("%s = %s, quero %s", col, got, want)
		}
	}

	rows = consulta(t, eng, "SELECT * FROM _meta.counter_metrics WHERE counter = 'c'")
	if got := column(rows, "id"); got != "c|auth_5m,c|auth_24h" {
		t.Fatalf("WHERE counter = 'c' = %s", got)
	}

	before := eng.Metrics().PointLookups.Load()
	rows = consulta(t, eng, "SELECT id, columns FROM _meta.counter_metrics WHERE id = 'c|auth_24h'")
	if len(rows) != 1 || rows[0]["columns"] != "auth_24h_max,auth_24h_min" {
		t.Fatalf("point lookup = %v", rows)
	}
	if n := eng.Metrics().PointLookups.Load() - before; n != 1 {
		t.Errorf("%d point lookups; a busca pelo id tem de resolver pela key", n)
	}
	if rows := consulta(t, eng, "SELECT id FROM _meta.counter_metrics WHERE id = 'c|nao_existe'"); len(rows) != 0 {
		t.Fatalf("id inexistente devolveu %v", rows)
	}
}

// TestCounterMetricsMetaIsNullWithoutMetricSource: sem amostrador a definição
// continua respondendo, e o uso sai NULL — zero seria uma medida inventada.
func TestCounterMetricsMetaIsNullWithoutMetricSource(t *testing.T) {
	eng := engineWithMetricsMeta(t, metricsMetaSpecs(), nil)

	rows := consulta(t, eng, "SELECT id, windowMs, groups, heapBytes, diskBytes, sampledAt FROM _meta.counter_metrics")
	if len(rows) != 3 {
		t.Fatalf("%d linhas, quero 3: %v", len(rows), rows)
	}
	for _, col := range []string{"groups", "heapBytes", "diskBytes", "sampledAt"} {
		if got := column(rows, col); got != "<null>,<null>,<null>" {
			t.Errorf("%s sem fonte = %s, quero NULL nas três", col, got)
		}
	}
	if got := column(rows, "windowMs"); got != "300000,300000,86400000" {
		t.Errorf("windowMs sem fonte = %s; a definição não depende do amostrador", got)
	}
	if rows := consulta(t, eng, "SELECT id FROM _meta.counter_metrics WHERE groups IS NULL AND sampledAt IS NULL"); len(rows) != 3 {
		t.Fatalf("IS NULL achou %d linhas, quero 3", len(rows))
	}
}

// TestCounterMetricsMetaWithoutMetricsIsEmptyTable: a tabela existe mesmo sem
// métrica, e "nenhuma" é uma resposta — não um erro de tabela desconhecida.
func TestCounterMetricsMetaWithoutMetricsIsEmptyTable(t *testing.T) {
	for name, specs := range map[string][]schema.CounterSpec{
		"sem contador":          nil,
		"contador sem métricas": {{Name: "b", GroupBy: []string{"olt"}}},
	} {
		t.Run(name, func(t *testing.T) {
			eng := engineWithMetricsMeta(t, specs, nil)
			if rows := consulta(t, eng, "SELECT id FROM _meta.counter_metrics"); len(rows) != 0 {
				t.Fatalf("listou %v", rows)
			}
		})
	}
}

// TestCounterMetricsMetaRefusesMetricDeclaredTwice: a key é o par, e dois
// pares iguais não cabem nela.
func TestCounterMetricsMetaRefusesMetricDeclaredTwice(t *testing.T) {
	specs := []schema.CounterSpec{{Name: "c", GroupBy: []string{"olt"}, Metrics: []schema.CounterMetricSpec{
		windowMetric("auth_5m", 5*time.Minute, schema.AggregateAvg),
		windowMetric("auth_5m", 10*time.Minute, schema.AggregateMax),
	}}}
	if _, _, err := counters.TableMetaMetrics(specs, nil); err == nil {
		t.Fatal("a métrica repetida tem de ser recusada")
	}
}

// TestCounterMetricsMetaRefusesSourceWithoutDeclaredMetric: com fonte, a
// métrica declarada que ela não resolve é erro, pela mesma regra da tabela do
// contador.
func TestCounterMetricsMetaRefusesSourceWithoutDeclaredMetric(t *testing.T) {
	_, _, err := counters.TableMetaMetrics(metricsMetaSpecs(), fakeMetricSource{})
	if err == nil || !strings.Contains(err.Error(), `"auth_5m"`) {
		t.Fatalf("a fonte sem a métrica tem de ser recusada nomeando-a: %v", err)
	}
}
