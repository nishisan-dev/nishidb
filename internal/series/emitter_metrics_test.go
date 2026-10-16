package series

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// fakeAggregate é o publicado de um grupo numa métrica. samples == 0 é o grupo
// que a janela não viu, e é o caso que o null da linha existe para cobrir.
type fakeAggregate struct {
	samples          uint64
	avg              float64
	minimum, maximum int64
}

// fakeMetricReader publica agregados por id de grupo. O mapa é lido, nunca
// escrito, durante a passada: é o que deixa o gate de alocação medir o
// emissor, e não o fake.
type fakeMetricReader struct {
	byGroup map[uint32]fakeAggregate
}

func (r *fakeMetricReader) Aggregate(id uint32) (uint64, float64, int64, int64) {
	a := r.byGroup[id]
	return a.samples, a.avg, a.minimum, a.maximum
}

// Usage existe para provar que um counters.MetricReader, que tem este método a
// mais, é atribuível ao MetricReader daqui sem conversão nenhuma.
func (r *fakeMetricReader) Usage() (int64, int64, int64, int64) { return 0, 0, 0, 0 }

// windowMetrics são duas métricas no molde do que o YAML declara: uma com um
// agregado e outra com dois, para que a ordem das colunas tenha o que provar.
//
// Os parâmetros são MetricReader, e não *fakeMetricReader, para que passar nil
// aqui produza o nil de INTERFACE — um (*fakeMetricReader)(nil) embrulhado
// seria uma interface não-nil, e o emissor o chamaria (é a mesma armadilha que
// countermetrics.metricSource.Metric documenta).
func windowMetrics(auth5m, auth24h MetricReader) []Metric {
	return []Metric{
		{Name: "auth_5m", Aggregates: []string{schema.AggregateAvg}, Reader: auth5m},
		{Name: "auth_24h", Aggregates: []string{schema.AggregateMax, schema.AggregateMin}, Reader: auth24h},
	}
}

// emitterWithMetrics é o newTestEmitter com métricas de janela ligadas.
func emitterWithMetrics(t *testing.T, f *fakeCounter, metrics []Metric) (*Emitter, string) {
	t.Helper()
	dir := t.TempDir()
	clock := &clockOfTest{now: t0}
	e, err := NewEmitter(Options{
		Dataset:  "servicos",
		Counter:  "autenticados_por_pon",
		Columns:  []string{"olt", "slot"},
		Metrics:  metrics,
		Reader:   f,
		Dir:      dir,
		File:     "autenticados.ndjson",
		Every:    30 * time.Second,
		MaxBytes: 1 << 20,
		Keep:     3,
		Now:      clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, dir
}

// linesOfSeries devolve as linhas escritas no arquivo da série.
func linesOfSeries(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "autenticados.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

func TestEmittedLineCarriesTheWindowMetricColumns(t *testing.T) {
	f := newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 812, live: true})
	auth5m := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{1: {samples: 12, avg: 807.5}}}
	auth24h := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{1: {samples: 300, minimum: 780, maximum: 830}}}
	e, dir := emitterWithMetrics(t, f, windowMetrics(auth5m, auth24h))
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	lines := linesOfSeries(t, dir)
	if len(lines) != 1 {
		t.Fatalf("esperava uma linha, veio %d", len(lines))
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("a linha não é JSON válido: %v (%s)", err, lines[0])
	}
	for name, want := range map[string]any{
		"total":        float64(812),
		"auth_5m_avg":  807.5,
		"auth_24h_max": float64(830),
		"auth_24h_min": float64(780),
	} {
		if got[name] != want {
			t.Errorf("a coluna %q saiu %v, esperava %v", name, got[name], want)
		}
	}
}

func TestTheMetricColumnsFollowTheDeclaredOrder(t *testing.T) {
	f := newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 812, live: true})
	auth5m := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{1: {samples: 12, avg: 807.5}}}
	auth24h := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{1: {samples: 300, minimum: 780, maximum: 830}}}
	e, dir := emitterWithMetrics(t, f, windowMetrics(auth5m, auth24h))
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	line := linesOfSeries(t, dir)[0]
	// A ordem é a declarada, e `at` continua sendo o último campo: a linha de
	// antes das métricas é um prefixo desta.
	want := `,"total":812,"rows":812,"auth_5m_avg":807.5,"auth_24h_max":830,"auth_24h_min":780,"at":`
	if !strings.Contains(line, want) {
		t.Errorf("a linha não traz as colunas na ordem declarada\nesperava conter: %s\nveio: %s", want, line)
	}
}

func TestAGroupWithNoSampleInTheWindowGetsNull(t *testing.T) {
	// Dois grupos: o primeiro foi amostrado, o segundo nasceu agora e a janela
	// ainda não o viu.
	f := newFakeCounter(t,
		group{olt: "OLT_A", slot: 1, total: 812, live: true},
		group{olt: "OLT_B", slot: 2, total: 5, live: true},
	)
	auth5m := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{1: {samples: 12, avg: 807.5}}}
	auth24h := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{1: {samples: 300, minimum: 780, maximum: 830}}}
	e, dir := emitterWithMetrics(t, f, windowMetrics(auth5m, auth24h))
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	lines := linesOfSeries(t, dir)
	if len(lines) != 2 {
		t.Fatalf("esperava duas linhas, veio %d", len(lines))
	}
	want := `,"total":5,"rows":5,"auth_5m_avg":null,"auth_24h_max":null,"auth_24h_min":null,"at":`
	if !strings.Contains(lines[1], want) {
		t.Errorf("o grupo sem amostra não saiu com null\nesperava conter: %s\nveio: %s", want, lines[1])
	}
	// null é ausência, e nunca zero: zero é um valor legítimo do contador.
	var got map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &got); err != nil {
		t.Fatal(err)
	}
	if v, ok := got["auth_5m_avg"]; !ok {
		t.Error("a chave auth_5m_avg sumiu da linha; o contrato é chave presente com null")
	} else if v != nil {
		t.Errorf("auth_5m_avg saiu %v, esperava null", v)
	}
}

func TestWithoutAMetricSourceEveryColumnIsNull(t *testing.T) {
	// Reader nil é o dataset sem amostrador: as colunas continuam na linha.
	f := newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 812, live: true})
	e, dir := emitterWithMetrics(t, f, windowMetrics(nil, nil))
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	want := `,"total":812,"rows":812,"auth_5m_avg":null,"auth_24h_max":null,"auth_24h_min":null,"at":`
	if line := linesOfSeries(t, dir)[0]; !strings.Contains(line, want) {
		t.Errorf("sem fonte as colunas deviam sair null\nesperava conter: %s\nveio: %s", want, line)
	}
}

// TestACounterWithoutMetricsEmitsTheFixedColumnsWithRows fixa, LITERAL, a
// linha de quem não declara métricas. Ela ganhou `rows` logo depois de `total`
// — quebra aditiva do contrato da série, registrada para as notas da release:
// o consumidor que lê por nome não muda nada, e quem compara a linha inteira
// passa a ver a chave nova. rows (1000) é diferente de total (812) para que uma
// coluna trocada pela outra não passe.
func TestACounterWithoutMetricsEmitsTheFixedColumnsWithRows(t *testing.T) {
	f := newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 812, rows: 1000, live: true})
	e, dir := emitterWithMetrics(t, f, nil)
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	want := `{"counter":"autenticados_por_pon","groupKey":"OLT_A|1","olt":"OLT_A","slot":1,"total":812,"rows":1000,"at":"2026-09-12T10:30:00.000Z"}`
	if line := linesOfSeries(t, dir)[0]; line != want {
		t.Errorf("a linha de quem não declara métricas não é a do contrato\nesperava: %s\nveio:     %s", want, line)
	}
}

func TestAnUnknownAggregateIsRefusedOnConstruction(t *testing.T) {
	f := newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 1, live: true})
	_, err := NewEmitter(Options{
		Counter:  "autenticados_por_pon",
		Metrics:  []Metric{{Name: "auth_5m", Aggregates: []string{"p99"}}},
		Reader:   f,
		Dir:      t.TempDir(),
		File:     "autenticados.ndjson",
		Every:    30 * time.Second,
		MaxBytes: 1 << 20,
		Keep:     3,
	})
	if err == nil {
		t.Fatal("um agregado sem coluna tem de ser recusado na construção, não calado na linha")
	}
	if !strings.Contains(err.Error(), "p99") {
		t.Errorf("a recusa não nomeia o agregado: %v", err)
	}
}
