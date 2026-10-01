package alarm_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// specServiceState é o contrato aprovado com o service-view: membro pelo
// scope, conta pelo where, alarme de proporção qualificado pelos membros.
const specServiceState = `version: 1
name: service_state
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
    idPrimary:
      type: string
    eligible:
      type: boolean
    authenticated:
      type: boolean
counters:
  - name: spl1
    groupBy: [olt, idPrimary]
    keyFormat: "OLT_SPL_1.{olt}.{idPrimary}"
    scope: "eligible = true AND authenticated IS NOT NULL"
    where: "authenticated = true"
    maxGroups: 1200000
    alarms:
      - name: spl1_auth_drop
        when: "total * 10 <= rows"
        qualifyWhen: "rows >= 6"
        sustainedFor: 2m
`

// capturingSink guarda os eventos e o corpo JSON de cada um — o mesmo que o
// webhook entrega.
type capturingSink struct {
	events []alarm.Event
	bodies []map[string]any
	t      *testing.T
}

func (s *capturingSink) Emit(e alarm.Event) {
	raw, err := json.Marshal(e)
	if err != nil {
		s.t.Fatalf("corpo do evento: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		s.t.Fatal(err)
	}
	s.events = append(s.events, e)
	s.bodies = append(s.bodies, body)
}

type e2eClock struct{ now time.Time }

func (c *e2eClock) Now() time.Time { return c.now }

// serviceStateHarness é o caminho inteiro da ingestão ao evento: YAML →
// schema → contadores compilados → Store real → materializer (merge por campo)
// → contador mantido na escrita → laço de alarme → corpo do evento.
type serviceStateHarness struct {
	t      *testing.T
	st     *storage.Store
	mat    *materializer.Materializer
	runner *alarm.Runner
	sink   *capturingSink
	clock  *e2eClock
	spl    *counter.Counter
}

func newServiceStateHarness(t *testing.T) *serviceStateHarness {
	t.Helper()
	spec, err := schema.Parse([]byte(specServiceState), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	comp := counters.NewCompiler(spec)
	st, err := storage.Open(storage.Options{
		Dir:                             t.TempDir(),
		Dataset:                         spec.Name,
		SegmentSize:                     1 << 20,
		CompileCountersCallbackFunction: comp.Callback(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mat, err := materializer.New(comp.Compiled(), st)
	if err != nil {
		t.Fatal(err)
	}
	h := &serviceStateHarness{t: t, st: st, mat: mat, sink: &capturingSink{t: t},
		clock: &e2eClock{now: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}}
	defs := make([]counter.Definition, 0, len(st.Counters()))
	readers := map[string]counters.Reader{}
	for _, c := range st.Counters() {
		defs = append(defs, c.Def())
		readers[c.Def().Name] = c
		h.spl = c
	}
	compiled, err := alarm.Compile(spec.Name, spec.Counters, defs)
	if err != nil {
		t.Fatal(err)
	}
	if h.runner, err = alarm.New(compiled, readers, alarm.Options{Sink: h.sink, Now: h.clock.Now}); err != nil {
		t.Fatal(err)
	}
	return h
}

// apply ingere um evento pelo materializer, que faz o merge por campo.
func (h *serviceStateHarness) apply(event string) {
	h.t.Helper()
	if r := h.mat.Apply([]byte(event)); r.Err != nil {
		h.t.Fatalf("evento %s: %v", event, r.Err)
	}
}

func (h *serviceStateHarness) pass(after time.Duration) {
	h.t.Helper()
	h.clock.now = h.clock.now.Add(after)
	if err := h.runner.EvaluateOnce(); err != nil {
		h.t.Fatal(err)
	}
}

// group devolve (total, rows) do grupo pelo groupKey.
func (h *serviceStateHarness) group(text string) (int64, int64) {
	h.t.Helper()
	id, ok := h.spl.ByText([]byte(text))
	if !ok {
		h.t.Fatalf("grupo %q não existe", text)
	}
	_, _, total, rows, _ := h.spl.Group(id)
	return total, rows
}

func member(i int, eligible, authenticated bool) string {
	return fmt.Sprintf(`{"customerId":"c%02d","olt":"OLT9","idPrimary":"P1","eligible":%v,"authenticated":%v}`,
		i, eligible, authenticated)
}

// TestScopedProportionAlarmFromIngestionToEventBody é a garantia 12 de ponta a
// ponta: dez membros elegíveis, um autenticado, e três não elegíveis fora da
// conta → firing com total=1, rows=10 no corpo; dois autenticados → resolved
// condition_cleared; de novo um, e o grupo perde membros em massa (eligible
// false) até cinco → resolved disqualified.
//
// No meio, a garantia 3 como FIXAÇÃO de uma propriedade que já existe (o
// contador só recebe o payload pós-merge do record): o patch sem
// `authenticated` sobre o membro autenticado não o tira do escopo, embora o
// scope exija `authenticated IS NOT NULL` — o materializer preservou o campo.
func TestScopedProportionAlarmFromIngestionToEventBody(t *testing.T) {
	h := newServiceStateHarness(t)
	const group = "OLT_SPL_1.OLT9.P1"
	for i := 0; i < 10; i++ {
		h.apply(member(i, true, i == 0))
	}
	for i := 10; i < 13; i++ {
		h.apply(member(i, false, true))
	}
	if total, rows := h.group(group); total != 1 || rows != 10 {
		t.Fatalf("depois da carga: total %d rows %d, quero 1 e 10 — os três inelegíveis não contam", total, rows)
	}
	h.apply(`{"customerId":"c00","olt":"OLT9","idPrimary":"P1","eligible":true}`)
	if total, rows := h.group(group); total != 1 || rows != 10 {
		t.Fatalf("patch sem authenticated: total %d rows %d, quero 1 e 10 — o scope vê o estado pós-merge", total, rows)
	}

	h.pass(0)
	h.pass(2 * time.Minute)
	if len(h.sink.events) != 1 {
		t.Fatalf("%d eventos depois do prazo, quero o disparo", len(h.sink.events))
	}
	body := h.sink.bodies[0]
	if body["event"] != "alarm.firing" || body["total"] != float64(1) || body["rows"] != float64(10) ||
		body["groupKey"] != group || body["condition"] != "total * 10 <= rows" {
		t.Fatalf("corpo do disparo = %v", body)
	}

	h.apply(member(1, true, true))
	h.pass(time.Minute)
	body = h.sink.bodies[len(h.sink.bodies)-1]
	if body["event"] != "alarm.resolved" || body["reason"] != alarm.ReasonConditionCleared ||
		body["total"] != float64(2) || body["rows"] != float64(10) {
		t.Fatalf("corpo da resolução por condição = %v", body)
	}

	h.apply(member(1, true, false))
	h.pass(time.Minute)
	h.pass(2 * time.Minute)
	if e := h.sink.events[len(h.sink.events)-1]; e.State != alarm.StateFiring {
		t.Fatalf("segundo disparo = %q", e.State)
	}
	for i := 0; i < 5; i++ {
		h.apply(member(i, false, i == 0))
	}
	if total, rows := h.group(group); total != 0 || rows != 5 {
		t.Fatalf("depois da saída em massa: total %d rows %d, quero 0 e 5", total, rows)
	}
	h.pass(time.Minute)
	body = h.sink.bodies[len(h.sink.bodies)-1]
	if body["event"] != "alarm.resolved" || body["reason"] != alarm.ReasonDisqualified ||
		body["total"] != float64(0) || body["rows"] != float64(5) {
		t.Fatalf("corpo da desqualificação = %v", body)
	}
}
