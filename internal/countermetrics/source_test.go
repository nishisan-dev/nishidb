package countermetrics

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// TestMetricSourceReadsThePublishedAggregate: o adaptador entrega à tabela e ao
// alarme a MESMA métrica que o amostrador publica, e não uma cópia.
func TestMetricSourceReadsThePublishedAggregate(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("olt-a", 7)
	s := h.open(t)
	sampleOnce(t, s)

	src := s.MetricSource()
	if src == nil {
		t.Fatal("amostrador aberto devolveu fonte nil")
	}
	r, ok := src.Metric(testCounter, testMetric)
	if !ok || r == nil {
		t.Fatalf("a fonte não resolveu a métrica declarada: ok=%v leitor=%v", ok, r)
	}
	if r != counters.MetricReader(mustMetric(t, s)) {
		t.Fatal("a fonte devolveu um leitor que não é a métrica do amostrador")
	}
	if samples, avg, minimum, maximum := r.Aggregate(id); samples != 1 || avg != 7 || minimum != 7 || maximum != 7 {
		t.Fatalf("pela fonte: (amostras %d, avg %v, min %d, max %d), quero (1, 7, 7, 7)", samples, avg, minimum, maximum)
	}
}

// TestMetricSourceReturnsNilInterfaceForUndeclaredMetric é a armadilha do nil
// tipado: Sampler.Metric devolve (*Metric)(nil) para nome não declarado, e
// embrulhá-lo na interface daria um leitor NÃO-nil que responde "sem amostra"
// para sempre — a tabela deixaria de recusar a métrica que a fonte não resolve.
func TestMetricSourceReturnsNilInterfaceForUndeclaredMetric(t *testing.T) {
	h := newHarness(t)
	s := h.open(t)
	for _, c := range []struct{ counter, metric string }{
		{testCounter, "undeclared"},
		{"undeclared", testMetric},
	} {
		r, ok := s.MetricSource().Metric(c.counter, c.metric)
		if ok {
			t.Fatalf("Metric(%q, %q) = ok para nome não declarado", c.counter, c.metric)
		}
		if r != nil {
			t.Fatalf("Metric(%q, %q) devolveu interface não-nil (%T) para nome não declarado", c.counter, c.metric, r)
		}
	}
}

// TestNilSamplerHasNilMetricSource: o dataset sem métrica não abre amostrador,
// e a fonte que ele entrega tem de ser o nil de interface que counters e alarm
// reconhecem como "sem fonte".
func TestNilSamplerHasNilMetricSource(t *testing.T) {
	var s *Sampler
	if src := s.MetricSource(); src != nil {
		t.Fatalf("Sampler nil devolveu fonte não-nil (%T)", src)
	}
}

// TestCountersWithMetricsSkipsCountersWithoutMetrics: contador sem `metrics:`
// não entra, e sem nenhum a lista sai vazia — é o que faz o servidor e a CLI
// não abrirem amostrador nenhum.
func TestCountersWithMetricsSkipsCountersWithoutMetrics(t *testing.T) {
	inputs, err := CountersWithMetrics([]schema.CounterSpec{{Name: "a"}, {Name: "b"}}, nil)
	if err != nil {
		t.Fatalf("CountersWithMetrics: %v", err)
	}
	if len(inputs) != 0 {
		t.Fatalf("%d entradas para contadores sem métrica, quero 0", len(inputs))
	}
}

// TestCountersWithMetricsRefusesACounterThatIsNotOpen: a métrica declarada
// num contador que o Store não abriu é recusada nomeando o contador, em vez de
// ficar calada.
func TestCountersWithMetricsRefusesACounterThatIsNotOpen(t *testing.T) {
	specs := []schema.CounterSpec{{
		Name:    "by_olt",
		Metrics: []schema.CounterMetricSpec{metricSpec(testMetric, 0)},
	}}
	_, err := CountersWithMetrics(specs, nil)
	if err == nil || !strings.Contains(err.Error(), `"by_olt"`) {
		t.Fatalf("erro = %v, quero a recusa nomeando o contador by_olt", err)
	}
}
