package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// windowDataset é o dataset dos testes das métricas de janela.
const windowDataset = "servicos"

// specServicesWithWindowMetrics é o contrato aprovado do ciclo: duas métricas de
// janela no contador por OLT e o alarme que só observa a OLT que já teve gente.
const specServicesWithWindowMetrics = `version: 1
name: servicos
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    authenticated:
      type: boolean
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
        aggregates: [max]
    alarms:
      - name: primary_quase_vazia
        qualifyWhen: "auth_24h_max > 10"
        when: "auth_5m_avg < 1"
        sustainedFor: 5m
`

// specServicesWithMetricsOnTwoCounters declara duas métricas num contador e uma
// no outro: é o que distingue "uma série do mapa de posições por contador" de
// "uma por métrica" e de "uma só no dataset".
const specServicesWithMetricsOnTwoCounters = `version: 1
name: servicos
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    authenticated:
      type: boolean
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
        aggregates: [max]
  - name: servicos_por_olt
    groupBy: [olt]
    metrics:
      - name: all_1h
        type: window
        source: total
        window: 1h
        aggregates: [min]
`

// testClock é o relógio injetado no servidor. Atômico porque os laços o leem
// de outra goroutine antes de o teste pará-los.
type testClock struct{ ns atomic.Int64 }

func newTestClock(at time.Time) *testClock {
	c := &testClock{}
	c.set(at)
	return c
}

func (c *testClock) Now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *testClock) set(at time.Time) { c.ns.Store(at.UnixNano()) }

// serviceEvents devolve n eventos de serviço da OLT, com o customerId
// prefixado pela OLT para que duas OLTs nunca compartilhem a key.
func serviceEvents(olt string, n int, authenticated bool) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf(`{"customerId":"%s-%02d","olt":%q,"authenticated":%v}`, olt, i, olt, authenticated))
	}
	return out
}

// applyWithCounters aplica eventos com o servidor PARADO, abrindo o dataset
// pelo MESMO callback de contadores do servidor: depois da primeira subida o
// manifest declara os contadores, e uma abertura sem o callback é recusada.
func applyWithCounters(t *testing.T, a *env, name string, events []string) {
	t.Helper()
	cfg := a.load()
	spec, err := schema.Load(cfg.DatasetSpecPath(name), cfg.QueryDefaults())
	if err != nil {
		t.Fatal(err)
	}
	comp := counters.NewCompiler(spec)
	st, err := storage.Open(storage.Options{
		Dir:                             cfg.DatasetRootDir(name),
		Dataset:                         name,
		SegmentSize:                     cfg.SegmentSize(),
		IndexedFieldsByName:             schema.IndexedFieldNames(spec),
		CompileCountersCallbackFunction: comp.Callback(),
	})
	if err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.New(comp.Compiled(), st)
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	for i, ev := range events {
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			_ = st.Close()
			t.Fatalf("evento %d: %v", i, r.Err)
		}
	}
	if err := st.Checkpoint(); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// startDrivenServer sobe o servidor com o relógio do teste, espera o laço do
// amostrador e o de alarmes subirem e os PARA: daqui em diante cada passada é
// do teste (drivePass). Devolve o servidor e a função que o encerra.
func startDrivenServer(t *testing.T, a *env, clock *testClock) (*server.Server, func()) {
	t.Helper()
	srv, err := server.Novo(a.load(), server.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	return srv, runDriven(t, srv)
}

// runDriven sobe o Run de um servidor já montado, espera os dois laços subirem
// e os PARA. Separado de startDrivenServer para o teste que precisa ler o
// servidor entre o Novo e o Run — antes de qualquer passada.
func runDriven(t *testing.T, srv *server.Server) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-end; err != nil {
				t.Errorf("Run: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	up := func() bool { return srv.CounterMetricsActive(windowDataset) && srv.AlarmsActive(windowDataset) }
	for i := 0; i < 400 && !up(); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if !up() {
		t.Fatalf("os laços não subiram: amostrador=%v alarmes=%v",
			srv.CounterMetricsActive(windowDataset), srv.AlarmsActive(windowDataset))
	}
	srv.StopSamplingAndAlarmsForTest(windowDataset)
	return stop
}

// engineRows é o queryRows sem HTTP (ver Server.QueryRowsForTest).
func engineRows(t *testing.T, srv *server.Server, sql string) []map[string]any {
	t.Helper()
	lines, err := srv.QueryRowsForTest(windowDataset, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("linha não é JSON: %v: %s", err, line)
		}
		out = append(out, row)
	}
	return out
}

// drivePass leva o relógio ao instante e faz uma passada do amostrador seguida
// de uma varredura de alarmes — a ordem da subida.
func drivePass(t *testing.T, srv *server.Server, clock *testClock, at time.Time) {
	t.Helper()
	clock.set(at)
	if err := srv.SampleCounterMetricsOnceForTest(windowDataset); err != nil {
		t.Fatalf("amostragem em %s: %v", at, err)
	}
	if err := srv.EvaluateAlarmsOnceForTest(windowDataset); err != nil {
		t.Fatalf("varredura em %s: %v", at, err)
	}
}

// queryRows consulta pela rota /v1/query e devolve cada linha decodificada.
func queryRows(t *testing.T, srv *server.Server, sql string) []map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]string{"dataset": windowDataset, "sql": sql})
	if err != nil {
		t.Fatal(err)
	}
	resp := consulta(t, srv, string(body))
	if resp.StatusCode != http.StatusOK {
		defer fecha(t, resp)
		t.Fatalf("%s: status %d: %+v", sql, resp.StatusCode, failOf(t, resp))
	}
	lines, _ := result(t, resp)
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("linha não é JSON: %v: %s", err, line)
		}
		out = append(out, row)
	}
	return out
}

// rowsByGroupKey indexa as linhas pelo groupKey, recusando repetição.
func rowsByGroupKey(t *testing.T, rows []map[string]any) map[string]map[string]any {
	t.Helper()
	out := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		key, ok := row["groupKey"].(string)
		if !ok {
			t.Fatalf("linha sem groupKey: %v", row)
		}
		if _, seen := out[key]; seen {
			t.Fatalf("groupKey %q repetido: %v", key, rows)
		}
		out[key] = row
	}
	return out
}

// wantColumn confere uma coluna da linha; want nil é NULL (ausente ou null).
func wantColumn(t *testing.T, rows map[string]map[string]any, groupKey, column string, want any) {
	t.Helper()
	row, ok := rows[groupKey]
	if !ok {
		t.Fatalf("grupo %q ausente: %v", groupKey, rows)
	}
	got := row[column]
	if got != want {
		t.Fatalf("grupo %q, coluna %s = %v (%T), quero %v (%T)", groupKey, column, got, got, want, want)
	}
}

// onlySample devolve a ÚNICA amostra da série com exatamente os rótulos
// pedidos; nenhuma ou mais de uma é falha.
func onlySample(t *testing.T, samples []amostraExposta, name string, labels map[string]string) amostraExposta {
	t.Helper()
	var found []amostraExposta
	for _, s := range samples {
		if s.Name != name || len(s.Labels) != len(labels) {
			continue
		}
		match := true
		for k, v := range labels {
			if s.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d amostras de %s com os rótulos %v, quero exatamente 1: %+v", len(found), name, labels, found)
	}
	return found[0]
}

// alarmStates devolve groupKey → state de `_counters.alarms`.
func alarmStates(t *testing.T, srv *server.Server) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, row := range queryRows(t, srv, "SELECT groupKey, state FROM _counters.alarms") {
		key, _ := row["groupKey"].(string)
		state, _ := row["state"].(string)
		out[key] = state
	}
	return out
}

// TestWindowMetricsFeedTheTableTheAlarmAndSurviveARestart é a costura ponta a
// ponta do ciclo, com relógio injetado e cada passada conduzida pelo teste.
//
// A ordem das fases é o que torna a restauração DISCRIMINANTE: entre as duas
// subidas todos os serviços desautenticam, então o `auth_24h_max = 12` da
// segunda subida só pode ter vindo do disco — o amostrador daquele processo
// nunca viu total diferente de zero. E é essa restauração que deixa o
// `qualifyWhen` enxergar a OLT que tinha gente e esvaziou; sem ela o alarme
// ficaria cego por 24 h a cada reinício.
func TestWindowMetricsFeedTheTableTheAlarmAndSurviveARestart(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset(windowDataset, specServicesWithWindowMetrics)
	applyWithCounters(t, a, windowDataset,
		append(serviceEvents("OLT_A", 12, true), serviceEvents("OLT_B", 3, true)...))

	t0 := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	clock := newTestClock(t0)
	const tableSQL = "SELECT groupKey, total, auth_5m_avg, auth_24h_max FROM _counters.autenticados_por_olt"

	// Primeira subida: a amostra vira coluna, `_meta.counter_metrics` descreve
	// as duas métricas com o uso, e nenhum alarme (a média ainda é alta).
	srv, stop := startDrivenServer(t, a, clock)
	drivePass(t, srv, clock, t0)
	rows := rowsByGroupKey(t, queryRows(t, srv, tableSQL))
	wantColumn(t, rows, "OLT_A", "auth_5m_avg", 12.0)
	wantColumn(t, rows, "OLT_A", "auth_24h_max", 12.0)
	wantColumn(t, rows, "OLT_B", "auth_5m_avg", 3.0)
	wantColumn(t, rows, "OLT_B", "auth_24h_max", 3.0)

	meta := queryRows(t, srv, "SELECT id, columns, windowMs, bucketMs, groups, sampledAt FROM _meta.counter_metrics ORDER BY id")
	if len(meta) != 2 {
		t.Fatalf("%d linhas em _meta.counter_metrics, quero 2: %v", len(meta), meta)
	}
	for i, want := range []struct {
		id, columns        string
		windowMs, bucketMs float64
	}{
		{"autenticados_por_olt|auth_24h", "auth_24h_max", 86_400_000, 1_440_000},
		{"autenticados_por_olt|auth_5m", "auth_5m_avg", 300_000, 5_000},
	} {
		row := meta[i]
		if row["id"] != want.id || row["columns"] != want.columns || row["windowMs"] != want.windowMs || row["bucketMs"] != want.bucketMs {
			t.Fatalf("_meta.counter_metrics[%d] = %v, quero %+v", i, row, want)
		}
		// groups e sampledAt vêm da fonte das métricas: com o amostrador fora da
		// fiação os dois saem NULL.
		if row["groups"] != 2.0 || row["sampledAt"] == nil {
			t.Fatalf("_meta.counter_metrics[%d] sem o uso do amostrador: %v", i, row)
		}
	}
	if states := alarmStates(t, srv); len(states) != 0 {
		t.Fatalf("alarmes em observação com a média alta: %v", states)
	}
	stop()

	// Com o servidor parado, todo mundo desautentica.
	applyWithCounters(t, a, windowDataset,
		append(serviceEvents("OLT_A", 12, false), serviceEvents("OLT_B", 3, false)...))

	// Segunda subida, dez minutos depois: a janela de 5 min já não tem a
	// amostra antiga, a de 24 h tem.
	restart := t0.Add(10 * time.Minute)
	clock.set(restart)
	srv, err := server.Novo(a.load(), server.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("segunda subida: %v", err)
	}
	// ANTES do Run: nenhum laço subiu e nenhuma passada correu neste processo,
	// então o 12 só pode ter vindo da restauração da abertura. A janela de
	// 5 min, que já passou da amostra gravada, sai NULL.
	rows = rowsByGroupKey(t, engineRows(t, srv, tableSQL))
	wantColumn(t, rows, "OLT_A", "auth_24h_max", 12.0)
	wantColumn(t, rows, "OLT_B", "auth_24h_max", 3.0)
	wantColumn(t, rows, "OLT_A", "auth_5m_avg", nil)
	stop = runDriven(t, srv)
	rows = rowsByGroupKey(t, queryRows(t, srv, tableSQL))
	wantColumn(t, rows, "OLT_A", "total", 0.0)
	wantColumn(t, rows, "OLT_A", "auth_24h_max", 12.0)
	wantColumn(t, rows, "OLT_B", "auth_24h_max", 3.0)
	restored := onlySample(t, coletaSamples(t, srv), metrics.MetCounterMetricRestoredGroups,
		map[string]string{metrics.LabelDataset: windowDataset})
	// As duas OLTs na métrica de 24 h; nenhuma na de 5 min, cuja janela já
	// passou da amostra gravada.
	if restored.Value != 2 {
		t.Fatalf("%s = %v depois do reinício, quero 2", metrics.MetCounterMetricRestoredGroups, restored.Value)
	}

	drivePass(t, srv, clock, restart)
	rows = rowsByGroupKey(t, queryRows(t, srv, tableSQL))
	wantColumn(t, rows, "OLT_A", "auth_5m_avg", 0.0)
	wantColumn(t, rows, "OLT_A", "auth_24h_max", 12.0)
	wantColumn(t, rows, "OLT_B", "auth_5m_avg", 0.0)
	// A OLT_B esvaziou também, mas nunca passou de 10: o `when` vale e ela não
	// é qualificada, então nem entra em observação.
	if states := alarmStates(t, srv); len(states) != 1 || states["OLT_A"] != "pending" {
		t.Fatalf("_counters.alarms = %v, quero só OLT_A pending", states)
	}

	drivePass(t, srv, clock, restart.Add(6*time.Minute))
	if states := alarmStates(t, srv); len(states) != 1 || states["OLT_A"] != "firing" {
		t.Fatalf("_counters.alarms = %v, quero só OLT_A firing depois do prazo", states)
	}
	byAlarm := map[string]string{
		metrics.LabelDataset: windowDataset,
		metrics.LabelCounter: "autenticados_por_olt",
		metrics.LabelAlarm:   "primary_quase_vazia",
	}
	samples := coletaSamples(t, srv)
	if v := onlySample(t, samples, metrics.MetAlarmFiredTotal, byAlarm).Value; v != 1 {
		t.Fatalf("%s = %v, quero 1", metrics.MetAlarmFiredTotal, v)
	}

	// Um dia depois, a janela de 24 h só tem zeros: o alarme disparado resolve
	// por desqualificação, e não porque a condição passou.
	drivePass(t, srv, clock, t0.Add(24*time.Hour+30*time.Minute))
	rows = rowsByGroupKey(t, queryRows(t, srv, tableSQL))
	wantColumn(t, rows, "OLT_A", "auth_24h_max", 0.0)
	if states := alarmStates(t, srv); len(states) != 0 {
		t.Fatalf("_counters.alarms = %v depois da desqualificação, quero vazio", states)
	}
	samples = coletaSamples(t, srv)
	if v := onlySample(t, samples, metrics.MetAlarmDisqualifiedTotal, byAlarm).Value; v != 1 {
		t.Fatalf("%s = %v, quero 1", metrics.MetAlarmDisqualifiedTotal, v)
	}
	if v := onlySample(t, samples, metrics.MetAlarmGroupsFiring, byAlarm).Value; v != 0 {
		t.Fatalf("%s = %v, quero 0", metrics.MetAlarmGroupsFiring, v)
	}
	stop()
}

// TestCounterMetricSeriesArePublishedBeforeTheFirstPass: todas as famílias das
// métricas de janela saem ANTES do Run — antes do Start e da primeira passada
// —, e o `sampling` sai 0 até o laço subir. Sem o zero na subida, "a janela não
// mudou" e "ninguém está amostrando" são o mesmo painel vazio.
//
// Lido do Registry sem HTTP: pela porta, o scrape nunca garante chegar antes
// da primeira passada, que é imediata no Start.
func TestCounterMetricSeriesArePublishedBeforeTheFirstPass(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset(windowDataset, specServicesWithMetricsOnTwoCounters)
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	started := false
	t.Cleanup(func() {
		if !started {
			_ = srv.Close()
		}
	})

	samples := parseExposition(t, srv.MetricsExpositionForTest())
	for _, m := range []struct{ counter, metric string }{
		{"autenticados_por_olt", "auth_5m"},
		{"autenticados_por_olt", "auth_24h"},
		{"servicos_por_olt", "all_1h"},
	} {
		labels := map[string]string{
			metrics.LabelDataset: windowDataset,
			metrics.LabelCounter: m.counter,
			metrics.LabelMetric:  m.metric,
		}
		for _, series := range []struct {
			name string
			typ  metrics.Type
			// zero diz se a série nasce com zero; as de bytes nascem com o
			// tamanho da abertura (arquivo e primeiro bloco em heap).
			zero bool
		}{
			{metrics.MetCounterMetricGroups, metrics.TypeGauge, true},
			{metrics.MetCounterMetricHeapBytes, metrics.TypeGauge, false},
			{metrics.MetCounterMetricDiskBytes, metrics.TypeGauge, false},
			{metrics.MetCounterMetricResetsTotal, metrics.TypeCounter, true},
		} {
			s := onlySample(t, samples, series.name, labels)
			if s.Tipo != series.typ {
				t.Errorf("%s de %s/%s tem tipo %v, quero %v", series.name, m.counter, m.metric, s.Tipo, series.typ)
			}
			if series.zero && s.Value != 0 {
				t.Errorf("%s de %s/%s = %v antes da primeira passada, quero 0", series.name, m.counter, m.metric, s.Value)
			}
		}
	}
	// O mapa de posições é do CONTADOR: uma amostra por contador, sem o rótulo
	// da métrica — inclusive no que declara duas.
	for _, counter := range []string{"autenticados_por_olt", "servicos_por_olt"} {
		s := onlySample(t, samples, metrics.MetCounterMetricPositionMapBytes, map[string]string{
			metrics.LabelDataset: windowDataset,
			metrics.LabelCounter: counter,
		})
		if s.Tipo != metrics.TypeGauge {
			t.Errorf("%s de %s tem tipo %v, quero gauge", s.Name, counter, s.Tipo)
		}
	}
	for _, s := range samples {
		if s.Name == metrics.MetCounterMetricPositionMapBytes && s.Labels[metrics.LabelMetric] != "" {
			t.Errorf("%s publicada por métrica: %v", s.Name, s.Labels)
		}
	}
	byDataset := map[string]string{metrics.LabelDataset: windowDataset}
	for _, name := range []string{
		metrics.MetCounterMetricSampling,
		metrics.MetCounterMetricSampleSeconds,
		metrics.MetCounterMetricRestoredGroups,
	} {
		s := onlySample(t, samples, name, byDataset)
		if s.Tipo != metrics.TypeGauge {
			t.Errorf("%s tem tipo %v, quero gauge", name, s.Tipo)
		}
		if s.Value != 0 {
			t.Errorf("%s = %v antes do Start, quero 0", name, s.Value)
		}
	}

	started = true
	run(t, srv)
	for i := 0; i < 400 && !srv.CounterMetricsActive(windowDataset); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if v := onlySample(t, coletaSamples(t, srv), metrics.MetCounterMetricSampling, byDataset).Value; v != 1 {
		t.Fatalf("%s = %v com o laço de pé, quero 1", metrics.MetCounterMetricSampling, v)
	}
}

// TestDatasetWithoutWindowMetricsOpensNoSampler: não se paga arquivo, goroutine
// nem série para não amostrar nada.
func TestDatasetWithoutWindowMetricsOpensNoSampler(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset(windowDataset, fmt.Sprintf(specServices, windowDataset))
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	if srv.CounterMetricsActive(windowDataset) {
		t.Fatal("subiu o amostrador num dataset sem métrica de janela")
	}
	for _, s := range coletaSamples(t, srv) {
		if strings.HasPrefix(s.Name, "nishidb_counter_metric_") {
			t.Fatalf("série de métrica de janela publicada num dataset sem métrica: %s %v", s.Name, s.Labels)
		}
	}
	dir := filepath.Join(a.cfg.DatasetRootDir(windowDataset), "indexes", "counter_metrics")
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("o diretório %s existe (ou não pôde ser lido: %v) num dataset sem métrica", dir, err)
	}
}
