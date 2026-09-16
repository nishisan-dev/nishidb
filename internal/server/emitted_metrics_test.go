package server

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// metricReaderOfTest é um leitor que nunca foi amostrado. O que se prova aqui
// é a RESOLUÇÃO da fonte, e não o valor: o valor tem os testes de series.
type metricReaderOfTest struct{}

func (metricReaderOfTest) Aggregate(uint32) (uint64, float64, int64, int64) { return 0, 0, 0, 0 }
func (metricReaderOfTest) Usage() (int64, int64, int64, int64)              { return 0, 0, 0, 0 }

// metricSourceOfTest resolve só o que está no mapa, pela mesma chave composta
// que o amostrador usa.
type metricSourceOfTest map[string]counters.MetricReader

func (s metricSourceOfTest) Metric(counter, metric string) (counters.MetricReader, bool) {
	r, ok := s[counter+"|"+metric]
	return r, ok
}

// counterWithTwoMetrics é o contador declarado com duas métricas de janela.
func counterWithTwoMetrics() schema.CounterSpec {
	return schema.CounterSpec{
		Name: "autenticados_por_olt",
		Metrics: []schema.CounterMetricSpec{
			{Name: "auth_5m", Aggregates: []string{schema.AggregateAvg}},
			{Name: "auth_24h", Aggregates: []string{schema.AggregateMax}},
		},
	}
}

func TestEmittedMetricsResolvesEveryDeclaredMetricInOrder(t *testing.T) {
	cs := counterWithTwoMetrics()
	src := metricSourceOfTest{
		"autenticados_por_olt|auth_5m":  metricReaderOfTest{},
		"autenticados_por_olt|auth_24h": metricReaderOfTest{},
	}
	got, err := emittedMetrics(cs, src)
	if err != nil {
		t.Fatalf("resolução: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("%d métricas resolvidas, quero 2", len(got))
	}
	for i, want := range []string{"auth_5m", "auth_24h"} {
		if got[i].Name != want {
			t.Errorf("métrica %d é %q, quero %q: a ordem tem de ser a declarada", i, got[i].Name, want)
		}
		if got[i].Reader == nil {
			t.Errorf("métrica %q sem leitor: a fonte a resolveu", got[i].Name)
		}
	}
}

func TestEmittedMetricsRefusesAMetricTheSourceDoesNotResolve(t *testing.T) {
	// A fonte resolve uma e não a outra. Calar a que falta publicaria para
	// sempre uma coluna null que o operador declarou, e sem dizer por quê.
	src := metricSourceOfTest{"autenticados_por_olt|auth_5m": metricReaderOfTest{}}
	_, err := emittedMetrics(counterWithTwoMetrics(), src)
	if err == nil {
		t.Fatal("a métrica que a fonte não resolve tem de recusar a subida, não virar coluna calada")
	}
	if !strings.Contains(err.Error(), "auth_24h") || !strings.Contains(err.Error(), "autenticados_por_olt") {
		t.Errorf("a recusa não nomeia a métrica e o contador: %v", err)
	}
}

func TestWithoutASourceTheDeclaredColumnsStillGetEmitted(t *testing.T) {
	// Fonte nil é o dataset sem amostrador: as colunas continuam na linha, e
	// saem null. O formato da linha é função do YAML.
	got, err := emittedMetrics(counterWithTwoMetrics(), nil)
	if err != nil {
		t.Fatalf("fonte nil é válida: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("%d métricas, quero as 2 declaradas mesmo sem fonte", len(got))
	}
	for _, m := range got {
		if m.Reader != nil {
			t.Errorf("métrica %q ganhou leitor sem fonte nenhuma", m.Name)
		}
	}
}

func TestACounterWithoutMetricsResolvesToNoColumns(t *testing.T) {
	got, err := emittedMetrics(schema.CounterSpec{Name: "autenticados_por_olt"}, metricSourceOfTest{})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("quem não declara métrica não devia ganhar coluna nenhuma: %v", got)
	}
}
