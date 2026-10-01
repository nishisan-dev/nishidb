package alarm

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// proportionAlarm é o alarme do pedido do service-view: no máximo um décimo
// dos membros autenticado, sobre grupos com ao menos seis membros.
func proportionAlarm() schema.AlarmSpec {
	return schema.AlarmSpec{Name: "spl1_auth_drop", When: "total * 10 <= rows", QualifyWhen: "rows >= 6",
		SustainedFor: 2 * time.Minute}
}

func proportionRunner(t *testing.T, f *fakeCounter, def counter.Definition, statePath string) (*Runner, *sinkOfTest, *clockOfTest) {
	t.Helper()
	defs, err := Compile("telecom", specWith(proportionAlarm()), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: t0}
	r, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink, StatePath: statePath, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	return r, sink, clock
}

// TestProportionAlarmFiresResolvesAndDisqualifiesOnTotalAndRows: a condição
// compara as duas colunas do MESMO grupo, e o evento traz o par que decidiu —
// disparo com 1 de 10, resolução por condição com 2 de 10, e desqualificação
// quando os membros caem abaixo de seis com a condição ainda valendo.
func TestProportionAlarmFiresResolvesAndDisqualifiesOnTotalAndRows(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"),
		grupo{olt: "OLT_A", slot: 1, total: 1, rows: 10, withRows: true, live: true})
	r, sink, clock := proportionRunner(t, f, defByPON("autenticados_por_pon"), "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	e := sink.last()
	if e.State != StateFiring || e.Total != 1 || e.Rows != 10 {
		t.Fatalf("disparo = %q total %d rows %d, quero firing com 1 e 10", e.State, e.Total, e.Rows)
	}

	f.setCounts("OLT_A", 1, 2, 10)
	clock.advance(time.Minute)
	passada(t, r)
	e = sink.last()
	if e.State != StateResolved || e.Reason != ReasonConditionCleared || e.Total != 2 || e.Rows != 10 {
		t.Fatalf("resolução = %q/%q total %d rows %d, quero condition_cleared com 2 e 10", e.State, e.Reason, e.Total, e.Rows)
	}

	f.setCounts("OLT_A", 1, 1, 10)
	clock.advance(time.Minute)
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	if e = sink.last(); e.State != StateFiring {
		t.Fatalf("segundo disparo = %q", e.State)
	}
	f.setCounts("OLT_A", 1, 0, 5)
	clock.advance(time.Minute)
	passada(t, r)
	e = sink.last()
	if e.State != StateResolved || e.Reason != ReasonDisqualified || e.Total != 0 || e.Rows != 5 {
		t.Fatalf("desqualificação = %q/%q total %d rows %d, quero disqualified com 0 e 5", e.State, e.Reason, e.Total, e.Rows)
	}
}

// TestRowsIsAValueColumnAndABindableColumn: `rows` vale em `value:` (o
// firingValue sai dos membros) e em `enrich.bind` (o parâmetro é o rows do
// payload que decidiu).
func TestRowsIsAValueColumnAndABindableColumn(t *testing.T) {
	spec := proportionAlarm()
	spec.Value = schema.CounterRowsColumn
	spec.Enrich = []schema.EnrichSpec{{Name: "membros", SQL: "SELECT customerId FROM servicos WHERE olt = ? LIMIT 1",
		Bind: []string{schema.CounterRowsColumn}}}
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(spec), []counter.Definition{def})
	if err != nil {
		t.Fatalf("value: rows e bind: [rows] têm de compilar: %v", err)
	}
	a := defs[0].alarms[0]
	if a.valueColumn != schema.CounterRowsColumn {
		t.Fatalf("valueColumn = %q", a.valueColumn)
	}
	f := newFakeCounter(t, def, grupo{olt: "OLT_A", slot: 1, total: 1, rows: 10, withRows: true, live: true})
	r, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f}, Options{Sink: &sinkOfTest{}, Now: (&clockOfTest{now: t0}).Now})
	if err != nil {
		t.Fatal(err)
	}
	params, err := r.paramsOf(&a.enrich[0], f.keys[0], f.texts[0], groupCounts{total: 1, rows: 10})
	if err != nil || len(params) != 1 || params[0].I64 != 10 {
		t.Fatalf("parâmetro ligado a rows = %+v (%v), quero 10", params, err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: t0}
	r, err = New(defs, map[string]counters.Reader{"autenticados_por_pon": f}, Options{Sink: sink, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	if e := sink.last(); e.ValueColumn != schema.CounterRowsColumn || !e.HasFiringValue || e.FiringValue != 10 {
		t.Fatalf("disparo com value: rows = %q %v (has=%v), quero rows 10", e.ValueColumn, e.FiringValue, e.HasFiringValue)
	}
}

// TestChangingOnlyTheScopeInvalidatesTheRestoredTimer: o scope decide a
// população de toda coluna do contador, então ele entra na assinatura da
// regra. Trocar SÓ o scope descarta o cronômetro restaurado; mantê-lo, não; e
// sem scope a assinatura de um alarme sobre `total` continua vazia, como antes
// do scope existir.
func TestChangingOnlyTheScopeInvalidatesTheRestoredTimer(t *testing.T) {
	plain := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(proportionAlarm()), []counter.Definition{plain})
	if err != nil {
		t.Fatal(err)
	}
	if fp := defs[0].alarms[0].metricsFingerprint; fp != "" {
		t.Fatalf("sem scope a assinatura mudou: %q, quero vazia como antes", fp)
	}
	scoped := func(scope string) counter.Definition {
		d := defByPON("autenticados_por_pon")
		d.Scope = scope
		return d
	}
	statePath := filepath.Join(t.TempDir(), "alarms.state")
	f := newFakeCounter(t, plain, grupo{olt: "OLT_A", slot: 1, total: 1, rows: 10, withRows: true, live: true})
	r, _, clock := proportionRunner(t, f, scoped("eligible = true"), statePath)
	passada(t, r)
	clock.advance(time.Minute)
	passada(t, r)
	if r.trackedGroups() != 1 {
		t.Fatalf("o grupo não entrou em observação")
	}
	if err := r.persist(clock.Now()); err != nil {
		t.Fatal(err)
	}

	same, _, _ := proportionRunner(t, f, scoped("eligible = true"), statePath)
	if same.restoredGroups != 1 {
		t.Fatalf("o mesmo scope restaurou %d cronômetros, quero 1", same.restoredGroups)
	}
	other, _, _ := proportionRunner(t, f, scoped("eligible = true AND olt IS NOT NULL"), statePath)
	if other.restoredGroups != 0 {
		t.Fatalf("trocar só o scope restaurou %d cronômetros, quero 0", other.restoredGroups)
	}
}
