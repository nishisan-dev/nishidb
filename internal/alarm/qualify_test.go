package alarm

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// windowSample é o publicado de UMA métrica para UM grupo, como o amostrador o
// entrega à tabela.
type windowSample struct {
	samples          uint64
	avg              float64
	minimum, maximum int64
}

// fakeMetricReader satisfaz o MetricReader com valores fixados pelo teste,
// indexados pelo id do grupo. Um amostrador de verdade exigiria relógio, mmap e
// passadas de amostragem para exercitar o que é lógica de alarme.
type fakeMetricReader struct{ byGroup []windowSample }

func (m *fakeMetricReader) Aggregate(id uint32) (uint64, float64, int64, int64) {
	i := int(id) - 1
	if i < 0 || i >= len(m.byGroup) {
		return 0, 0, 0, 0
	}
	s := m.byGroup[i]
	return s.samples, s.avg, s.minimum, s.maximum
}

func (m *fakeMetricReader) Usage() (int64, int64, int64, int64) { return 0, 0, 0, 0 }

func (m *fakeMetricReader) set(id uint32, s windowSample) {
	for len(m.byGroup) < int(id) {
		m.byGroup = append(m.byGroup, windowSample{})
	}
	m.byGroup[id-1] = s
}

// fakeMetricSource resolve a métrica pelo nome.
type fakeMetricSource map[string]*fakeMetricReader

func (s fakeMetricSource) Metric(_, metric string) (counters.MetricReader, bool) {
	r, ok := s[metric]
	if !ok {
		return nil, false
	}
	return r, true
}

func newFakeMetricSource(metrics []schema.CounterMetricSpec) fakeMetricSource {
	src := fakeMetricSource{}
	for _, m := range metrics {
		src[m.Name] = &fakeMetricReader{}
	}
	return src
}

// setWindows publica `auth_5m_avg` e `auth_24h_max` do grupo, com uma amostra.
func (s fakeMetricSource) setWindows(id uint32, avg5m float64, max24h int64) {
	s["auth_5m"].set(id, windowSample{samples: 1, avg: avg5m, minimum: int64(avg5m), maximum: int64(avg5m)})
	s["auth_24h"].set(id, windowSample{samples: 1, avg: float64(max24h), maximum: max24h})
}

// windowMetrics são as três métricas do contador de teste: auth_5m e auth_24h,
// citadas pelo alarme do exemplo, e auth_1h, que ele não cita.
func windowMetrics(auth5m, auth24h, auth1h time.Duration) []schema.CounterMetricSpec {
	window := func(name string, w time.Duration, aggregate string) schema.CounterMetricSpec {
		return schema.CounterMetricSpec{
			Name: name, Type: schema.MetricTypeWindow, Source: schema.MetricSourceTotal,
			Window: w, Aggregates: []string{aggregate},
		}
	}
	return []schema.CounterMetricSpec{
		window("auth_5m", auth5m, schema.AggregateAvg),
		window("auth_24h", auth24h, schema.AggregateMax),
		window("auth_1h", auth1h, schema.AggregateMin),
	}
}

func defaultWindowMetrics() []schema.CounterMetricSpec {
	return windowMetrics(5*time.Minute, 24*time.Hour, time.Hour)
}

// nearlyEmptyAlarm é o alarme que motiva a qualificação: a primária quase vazia,
// só para quem já teve gente no último dia.
func nearlyEmptyAlarm() schema.AlarmSpec {
	return schema.AlarmSpec{
		Name: "primary_nearly_empty", When: "auth_5m_avg < 1", QualifyWhen: "auth_24h_max > 10",
		SustainedFor: 2 * time.Minute,
	}
}

func specWithMetrics(metrics []schema.CounterMetricSpec, alarms ...schema.AlarmSpec) []schema.CounterSpec {
	return []schema.CounterSpec{{
		Name:    "autenticados_por_pon",
		GroupBy: []string{"olt", "slot"},
		Metrics: metrics,
		Alarms:  alarms,
	}}
}

// newQualifiedRunner monta o runner sobre o contador falso com a fonte de
// métricas dada (nil é aceito e vale "sem amostrador").
func newQualifiedRunner(t *testing.T, f *fakeCounter, src counters.MetricSource, specs []schema.CounterSpec,
	statePath string, start time.Time) (*Runner, *sinkOfTest, *clockOfTest) {
	t.Helper()
	defs, err := Compile("telecom", specs, []counter.Definition{defByPON("autenticados_por_pon")})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: start}
	r, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink, StatePath: statePath, Now: clock.Now, Metrics: src})
	if err != nil {
		t.Fatal(err)
	}
	return r, sink, clock
}

// TestQualifiedAlarmOnlyFiresForQualifiedGroups: das duas OLTs vazias, só a que
// teve gente no último dia sustenta e dispara. A que nunca teve ninguém nem
// entra em observação — e, ao se qualificar, conta o prazo a partir dali, sem
// herdar o tempo em que a condição valia sem ela ser elegível.
func TestQualifiedAlarmOnlyFiresForQualifiedGroups(t *testing.T) {
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"),
		grupo{olt: "OLT_A", slot: 1, live: true},
		grupo{olt: "OLT_B", slot: 1, live: true},
	)
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0, 50)
	src.setWindows(2, 0, 3)
	r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), "", t0)

	passada(t, r)
	if got := r.trackedGroups(); got != 1 {
		t.Fatalf("%d grupos em observação, quero 1: o grupo inelegível não pode ser observado", got)
	}
	clock.advance(2 * time.Minute)
	passada(t, r)
	if len(sink.events) != 1 {
		t.Fatalf("%d eventos ao completar o prazo, quero 1: %+v", len(sink.events), sink.events)
	}
	if e := sink.last(); e.State != StateFiring || e.GroupKey != "OLT_A|1" {
		t.Fatalf("evento = %s/%s, quero o disparo de OLT_A|1", e.State, e.GroupKey)
	}

	src.setWindows(2, 0, 30)
	clock.advance(time.Minute)
	passada(t, r)
	if len(sink.events) != 1 {
		t.Fatalf("o grupo que acabou de se qualificar disparou sem sustentar: %+v", sink.last())
	}
	clock.advance(2 * time.Minute)
	passada(t, r)
	if len(sink.events) != 2 || sink.last().GroupKey != "OLT_B|1" || sink.last().State != StateFiring {
		t.Fatalf("o grupo qualificado não disparou depois do prazo: %+v", sink.events)
	}
	if want := t0.Add(3 * time.Minute); !sink.last().Since.Equal(want) {
		t.Errorf("since = %v, quero %v: o prazo conta da qualificação", sink.last().Since, want)
	}
}

// TestPendingGroupThatLosesQualificationReturnsToAbsentWithoutEvent: nunca houve
// disparo, e nada sai — nem resolução, nem contagem nas séries.
func TestPendingGroupThatLosesQualificationReturnsToAbsentWithoutEvent(t *testing.T) {
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, live: true})
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0, 50)
	r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), "", t0)

	passada(t, r)
	if got := r.trackedGroups(); got != 1 {
		t.Fatalf("%d grupos em observação, quero 1", got)
	}
	src.setWindows(1, 0, 5)
	for i := 0; i < 5; i++ {
		clock.advance(time.Minute)
		passada(t, r)
	}
	if len(sink.events) != 0 {
		t.Fatalf("o grupo desqualificado antes do prazo emitiu: %+v", sink.events)
	}
	if got := r.trackedGroups(); got != 0 {
		t.Errorf("%d grupos em observação, quero 0", got)
	}
	if s := r.Stats()[0]; s.Fired != 0 || s.Resolved != 0 || s.Disqualified != 0 || s.Tracked != 0 {
		t.Errorf("estatísticas = %+v; nada aconteceu que contasse", s)
	}
}

// TestFiringGroupThatLosesQualificationResolvesAsDisqualified é a OLT morta há
// mais de 24h: o `auth_24h_max` cai abaixo do limiar com a primária ainda
// vazia, e o alarme resolve como `disqualified` — não como "voltou" — e não
// volta a disparar enquanto não se requalificar.
func TestFiringGroupThatLosesQualificationResolvesAsDisqualified(t *testing.T) {
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, live: true})
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0, 50)
	r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), "", t0)
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	if len(sink.events) != 1 || sink.last().State != StateFiring {
		t.Fatalf("o cenário não disparou: %+v", sink.events)
	}

	src.setWindows(1, 0, 5)
	clock.advance(time.Minute)
	passada(t, r)
	if len(sink.events) != 2 {
		t.Fatalf("%d eventos, quero disparo e resolução", len(sink.events))
	}
	e := sink.last()
	if e.State != StateResolved || e.Reason != ReasonDisqualified {
		t.Fatalf("resolução = %q / %q, quero %q", e.State, e.Reason, ReasonDisqualified)
	}
	if !e.Since.Equal(t0) {
		t.Errorf("since = %v, quero %v", e.Since, t0)
	}
	s := r.Stats()[0]
	if s.Resolved != 1 || s.Disqualified != 1 || s.Fired != 1 || s.Firing != 0 || s.Tracked != 0 {
		t.Errorf("estatísticas = %+v; uma resolução, que é também a desqualificação", s)
	}

	for i := 0; i < 5; i++ {
		clock.advance(2 * time.Minute)
		passada(t, r)
	}
	if len(sink.events) != 2 {
		t.Fatalf("o grupo inelegível voltou a emitir: %+v", sink.last())
	}

	src.setWindows(1, 0, 50)
	clock.advance(time.Minute)
	requalified := clock.now
	passada(t, r)
	if len(sink.events) != 2 {
		t.Fatalf("o grupo requalificado disparou sem sustentar: %+v", sink.last())
	}
	clock.advance(2 * time.Minute)
	passada(t, r)
	if len(sink.events) != 3 || sink.last().State != StateFiring || !sink.last().Since.Equal(requalified) {
		t.Fatalf("depois de requalificar e sustentar, quero um disparo desde %v: %+v", requalified, sink.events)
	}
}

// TestClearedConditionWinsOverDisqualificationInTheSamePass: a OLT voltou e, na
// mesma passada, deixou de ser elegível. O fato relevante é que ela voltou, e a
// desqualificação não conta.
func TestClearedConditionWinsOverDisqualificationInTheSamePass(t *testing.T) {
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, live: true})
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0, 50)
	r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), "", t0)
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)

	src.setWindows(1, 30, 5)
	clock.advance(time.Minute)
	passada(t, r)
	if e := sink.last(); e.State != StateResolved || e.Reason != ReasonConditionCleared {
		t.Fatalf("resolução = %q / %q, quero %q", e.State, e.Reason, ReasonConditionCleared)
	}
	if s := r.Stats()[0]; s.Resolved != 1 || s.Disqualified != 0 {
		t.Errorf("estatísticas = %+v; a resolução foi por condição, não por desqualificação", s)
	}
}

// TestAlarmEventCarriesQualifyConditionAndMetrics: o evento traz a condição de
// qualificação e os números das métricas que decidiram — na ordem declarada,
// sem a coluna sem amostra, e sem nenhuma no grupo aposentado.
func TestAlarmEventCarriesQualifyConditionAndMetrics(t *testing.T) {
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 7, live: true})
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0.5, 50) // auth_1h fica sem amostra: NULL
	plain := schema.AlarmSpec{Name: "olt_down", When: "total < 100", SustainedFor: 2 * time.Minute}
	r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm(), plain), "", t0)
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)

	byAlarm := map[string]Event{}
	for _, e := range sink.events {
		byAlarm[e.Alarm] = e
	}
	fired, ok := byAlarm["primary_nearly_empty"]
	if !ok {
		t.Fatalf("o alarme qualificado não disparou: %+v", sink.events)
	}
	if fired.QualifyCondition != "auth_24h_max > 10" {
		t.Errorf("qualifyCondition = %q", fired.QualifyCondition)
	}
	wantMetrics := []Column{{Name: "auth_5m_avg", Value: "0.5"}, {Name: "auth_24h_max", Value: "50"}}
	assertColumns(t, "métricas do disparo", fired.Metrics, wantMetrics)
	if other, ok := byAlarm["olt_down"]; !ok || other.QualifyCondition != "" {
		t.Errorf("o alarme sem qualifyWhen saiu com qualifyCondition: %+v", other)
	} else {
		// As métricas são do GRUPO, e não da condição: o alarme que não as cita
		// também as leva.
		assertColumns(t, "métricas do alarme sem qualifyWhen", other.Metrics, wantMetrics)
	}

	// Desqualificado: o evento traz o número que desqualificou, e não o do
	// disparo.
	src.setWindows(1, 0.5, 7)
	clock.advance(time.Minute)
	passada(t, r)
	resolved := sink.last()
	if resolved.Reason != ReasonDisqualified {
		t.Fatalf("motivo = %q, quero %q", resolved.Reason, ReasonDisqualified)
	}
	assertColumns(t, "métricas da desqualificação", resolved.Metrics,
		[]Column{{Name: "auth_5m_avg", Value: "0.5"}, {Name: "auth_24h_max", Value: "7"}})

	// Aposentado: a varredura não viu o grupo e não tem linha para dar.
	f.retire("OLT_A", 1)
	clock.advance(time.Minute)
	passada(t, r)
	retired := sink.last()
	if retired.Alarm != "olt_down" || retired.Reason != ReasonGroupRetired {
		t.Fatalf("evento = %s/%s, quero a aposentadoria de olt_down", retired.Alarm, retired.Reason)
	}
	if len(retired.Metrics) != 0 {
		t.Errorf("o grupo aposentado saiu com métricas: %+v", retired.Metrics)
	}
}

func assertColumns(t *testing.T, what string, got, want []Column) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s = %+v, quero %+v", what, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s = %+v, quero %+v", what, got, want)
			return
		}
	}
}

// TestMissingMetricSampleNeverQualifies: NULL compara falso. Sem amostra na
// janela, `auth_24h_max > 10` não qualifica — nem com a fonte presente e a
// coluna sem valor, nem sem fonte nenhuma —, e o grupo disparado cuja amostra
// some é desqualificado.
func TestMissingMetricSampleNeverQualifies(t *testing.T) {
	metrics := defaultWindowMetrics()
	busy := schema.AlarmSpec{Name: "olt_down", When: "total < 100", QualifyWhen: "auth_24h_max > 10", SustainedFor: 2 * time.Minute}

	t.Run("source without sample", func(t *testing.T) {
		f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 50, live: true})
		src := newFakeMetricSource(metrics)
		r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, busy), "", t0)
		for i := 0; i < 5; i++ {
			passada(t, r)
			clock.advance(2 * time.Minute)
		}
		if len(sink.events) != 0 || r.trackedGroups() != 0 {
			t.Fatalf("coluna NULL qualificou: eventos=%+v observados=%d", sink.events, r.trackedGroups())
		}

		// A amostra chega: o prazo começa a contar dali.
		src["auth_24h"].set(1, windowSample{samples: 1, avg: 50, maximum: 50})
		passada(t, r)
		clock.advance(2 * time.Minute)
		passada(t, r)
		if len(sink.events) != 1 || sink.last().State != StateFiring {
			t.Fatalf("com a amostra presente o alarme tinha de disparar: %+v", sink.events)
		}

		// A amostra some com o alarme disparado: NULL desqualifica.
		src["auth_24h"].set(1, windowSample{})
		clock.advance(time.Minute)
		passada(t, r)
		if e := sink.last(); e.Reason != ReasonDisqualified {
			t.Fatalf("motivo = %q, quero %q", e.Reason, ReasonDisqualified)
		}
	})

	t.Run("nil source", func(t *testing.T) {
		f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 50, live: true})
		r, sink, clock := newQualifiedRunner(t, f, nil, specWithMetrics(metrics, busy), "", t0)
		for i := 0; i < 5; i++ {
			passada(t, r)
			clock.advance(2 * time.Minute)
		}
		if len(sink.events) != 0 || r.trackedGroups() != 0 {
			t.Fatalf("sem fonte de métricas o grupo qualificou: eventos=%+v observados=%d", sink.events, r.trackedGroups())
		}
	})
}

// countingEvaluator conta as avaliações do avaliador que embrulha.
type countingEvaluator struct {
	inner counter.Evaluator
	calls int
}

func (c *countingEvaluator) Evaluate(payload []byte) (bool, error) {
	c.calls++
	return c.inner.Evaluate(payload)
}

// TestQualifyWhenIsOnlyEvaluatedWhenTheConditionHolds fixa o custo: com a
// condição falsa o desfecho já está decidido, e avaliar o `qualifyWhen` do
// grupo saudável seria uma segunda avaliação por grupo do inventário. O gate
// de alocação não enxerga isto — avaliar não aloca —, e por isso a contagem.
func TestQualifyWhenIsOnlyEvaluatedWhenTheConditionHolds(t *testing.T) {
	metrics := defaultWindowMetrics()
	gs := make([]grupo, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	src := newFakeMetricSource(metrics)
	for id := uint32(1); id <= 64; id++ {
		src.setWindows(id, 30, 50)
	}
	r, _, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), "", t0)
	ra := r.counters[0].alarms[0]
	counting := &countingEvaluator{inner: ra.qualifier}
	ra.qualifier = counting

	passada(t, r)
	if counting.calls != 0 {
		t.Fatalf("o qualifyWhen foi avaliado %d vez(es) sobre 64 grupos com a condição falsa, quero 0", counting.calls)
	}
	for id := uint32(1); id <= 3; id++ {
		src.setWindows(id, 0, 50)
	}
	clock.advance(time.Minute)
	passada(t, r)
	if counting.calls != 3 {
		t.Fatalf("o qualifyWhen foi avaliado %d vez(es), quero 3: um por grupo com a condição valendo", counting.calls)
	}
}

// TestPersistedStateCarriesTheQualificationSignature: o estado gravado carrega
// a qualificação e o fingerprint das métricas citadas, que é o que a subida
// seguinte compara contra o YAML.
func TestPersistedStateCarriesTheQualificationSignature(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, live: true})
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0, 50)
	r, _, _ := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), path, t0)
	passada(t, r)
	st, err := checkpoint.LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Alarms) != 1 {
		t.Fatalf("estado gravado = %+v", st)
	}
	a := st.Alarms[0]
	if a.QualifyWhen != "auth_24h_max > 10" {
		t.Errorf("qualifyWhen gravado = %q", a.QualifyWhen)
	}
	if want := "auth_24h:window:total:86400000;auth_5m:window:total:300000"; a.MetricsFingerprint != want {
		t.Errorf("fingerprint gravado = %q, quero %q", a.MetricsFingerprint, want)
	}
}

// TestRestoreDropsTheTimerWhenTheQualificationSignatureChanges: o cronômetro
// salvo só vale para a MESMA regra. Trocar ou tirar o `qualifyWhen`, ou mudar a
// janela de uma métrica que uma das condições lê, invalida a medida; mudar a
// janela de uma métrica que o alarme não lê, não.
//
// O caso "same rule" é o controle: sem ele, uma restauração quebrada passaria
// por todos os casos de descarte.
func TestRestoreDropsTheTimerWhenTheQualificationSignatureChanges(t *testing.T) {
	withQualify := func(q string) schema.AlarmSpec {
		a := nearlyEmptyAlarm()
		a.QualifyWhen = q
		return a
	}
	cases := []struct {
		name         string
		specs        []schema.CounterSpec
		wantRestored bool
	}{
		{"same rule", specWithMetrics(defaultWindowMetrics(), nearlyEmptyAlarm()), true},
		{"qualifyWhen changed", specWithMetrics(defaultWindowMetrics(), withQualify("auth_24h_max > 20")), false},
		{"qualifyWhen removed", specWithMetrics(defaultWindowMetrics(), withQualify("")), false},
		{"window of metric cited by qualifyWhen changed", specWithMetrics(windowMetrics(5*time.Minute, 12*time.Hour, time.Hour), nearlyEmptyAlarm()), false},
		{"window of metric cited by when changed", specWithMetrics(windowMetrics(10*time.Minute, 24*time.Hour, time.Hour), nearlyEmptyAlarm()), false},
		{"window of uncited metric changed", specWithMetrics(windowMetrics(5*time.Minute, 24*time.Hour, 2*time.Hour), nearlyEmptyAlarm()), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
			metrics := defaultWindowMetrics()
			f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, live: true})
			src := newFakeMetricSource(metrics)
			src.setWindows(1, 0, 50)
			r1, _, _ := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), path, t0)
			passada(t, r1)

			r2, sink, _ := newQualifiedRunner(t, f, src, c.specs, path, t0.Add(time.Hour))
			passada(t, r2)
			if c.wantRestored {
				if len(sink.events) != 1 || !sink.last().Restored || !sink.last().Since.Equal(t0) {
					t.Fatalf("o cronômetro salvo tinha de valer: %+v", sink.events)
				}
				return
			}
			if len(sink.events) != 0 {
				t.Fatalf("disparou com o cronômetro de outra regra: %+v", sink.events)
			}
		})
	}
}
