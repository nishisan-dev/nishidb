package alarm

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// TestFiringCapturesTheDeclaredValueColumn: o disparo carrega o valor da
// coluna declarada em `value:`, e o tipo dela é o do schema COMPILADO — a
// média de uma métrica de janela é double, e min/max são int64.
func TestFiringCapturesTheDeclaredValueColumn(t *testing.T) {
	metrics := defaultWindowMetrics()
	cases := []struct {
		name   string
		column string
		want   float64
	}{
		{"avg is double", "auth_5m_avg", 0.5},
		{"max is int64", "auth_24h_max", 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 7, live: true})
			src := newFakeMetricSource(metrics)
			src.setWindows(1, 0.5, 50)
			alarm := schema.AlarmSpec{Name: "olt_down", When: "total < 100", Value: c.column, SustainedFor: 2 * time.Minute}
			r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, alarm), "", t0)
			passada(t, r)
			clock.advance(2 * time.Minute)
			passada(t, r)

			e := sink.last()
			if e.State != StateFiring {
				t.Fatalf("estado = %q, quero disparo", e.State)
			}
			if e.ValueColumn != c.column {
				t.Errorf("valueColumn = %q, quero %q", e.ValueColumn, c.column)
			}
			if !e.HasFiringValue {
				t.Fatal("hasFiringValue = false, quero true: a coluna tinha amostra")
			}
			if e.FiringValue != c.want {
				t.Errorf("firingValue = %v, quero %v", e.FiringValue, c.want)
			}
		})
	}
}

// TestValueDefaultsToTotal: sem `value:` declarado, a coluna efetiva é
// `total` — o mesmo comportamento de sempre, agora nomeado.
func TestValueDefaultsToTotal(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 42, live: true})
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)

	e := sink.last()
	if e.State != StateFiring {
		t.Fatalf("estado = %q, quero disparo", e.State)
	}
	if e.ValueColumn != schema.CounterTotalColumn {
		t.Errorf("valueColumn = %q, quero %q", e.ValueColumn, schema.CounterTotalColumn)
	}
	if !e.HasFiringValue || e.FiringValue != 42 {
		t.Errorf("firingValue = %v (has=%v), quero 42 — o total observado na decisão", e.FiringValue, e.HasFiringValue)
	}
}

// swayingTotalReader é um dublê de counters.Reader que devolve, para o MESMO
// grupo, o par (total, rows) da leitura #1 (o que Runner.sweep tem em mãos,
// rc.reader.Group) e o da leitura #2 (a que produz o payload avaliado, dentro
// de counters/table.go, (*source).GetByRowID) DIFERENTES — a única forma de
// provar que o evento lê do payload, e não do que o sweep já tinha.
//
// A contagem é POR ID: a primeira Group(id) de cada passada (ímpar) é o
// sweep, a segunda (par) é a leitura que monta o payload — a MESMA ordem em
// que Runner.sweep e source.GetByRowID chamam o Reader. Por id, e não por
// chamada global, porque o sweep visita todo id até o limite do contador
// (inclusive o aposentado, que a passada só descarta depois de ler), e uma
// contagem global desalinharia o par sweep/payload do grupo que importa.
type swayingTotalReader struct {
	*fakeCounter
	sweepTotal, payloadTotal int64
	sweepRows, payloadRows   int64
	calls                    map[uint32]int
}

func (s *swayingTotalReader) Group(id uint32) ([]byte, []byte, int64, int64, bool) {
	key, text, _, _, live := s.fakeCounter.Group(id)
	if s.calls == nil {
		s.calls = make(map[uint32]int)
	}
	s.calls[id]++
	if s.calls[id]%2 == 1 {
		return key, text, s.sweepTotal, s.sweepRows, live
	}
	return key, text, s.payloadTotal, s.payloadRows, live
}

// TestEventTotalAndRowsAreReadFromTheDecidingPayload: o par que o sweep tem em
// mãos e o par que produziu o payload avaliado vêm de DUAS leituras diferentes
// do contador, e podem divergir entre elas. O evento traz Total, Rows e o
// firingValue do PAYLOAD que decidiu — o par que a condição de fato viu —, no
// disparo e na resolução.
func TestEventTotalAndRowsAreReadFromTheDecidingPayload(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	dbl := &swayingTotalReader{fakeCounter: f, sweepTotal: 999, payloadTotal: 50, sweepRows: 7, payloadRows: 80}
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: t0}
	r, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": dbl}, Options{Sink: sink, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)

	e := sink.last()
	if e.State != StateFiring {
		t.Fatalf("estado = %q, quero disparo", e.State)
	}
	if e.Total != 50 || e.Rows != 80 {
		t.Fatalf("disparo com total %d rows %d, quero 50 e 80 — o par do PAYLOAD que decidiu, não o do sweep (999, 7)",
			e.Total, e.Rows)
	}
	if e.FiringValue != 50 {
		t.Errorf("firingValue = %v, quero 50 — o valor do PAYLOAD que decidiu, não o total do sweep", e.FiringValue)
	}
	dbl.payloadTotal, dbl.payloadRows = 500, 600
	clock.advance(time.Minute)
	passada(t, r)
	e = sink.last()
	if e.State != StateResolved || e.Total != 500 || e.Rows != 600 {
		t.Fatalf("resolução %q com total %d rows %d, quero resolved com 500 e 600 (o payload que resolveu)",
			e.State, e.Total, e.Rows)
	}
}

// TestMissingValueColumnYieldsNoFiringValue: coluna de métrica de janela sem
// amostra ainda sai ausente do payload, e hasFiringValue tem de dizer isso —
// nunca um zero que se pareceria com um valor observado.
func TestMissingValueColumnYieldsNoFiringValue(t *testing.T) {
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 50, live: true})
	src := newFakeMetricSource(metrics) // sem setWindows: auth_5m nunca amostrou.
	alarm := schema.AlarmSpec{Name: "olt_down", When: "total < 100", Value: "auth_5m_avg", SustainedFor: 2 * time.Minute}
	r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, alarm), "", t0)
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)

	e := sink.last()
	if e.State != StateFiring {
		t.Fatalf("estado = %q, quero disparo", e.State)
	}
	if e.ValueColumn != "auth_5m_avg" {
		t.Errorf("valueColumn = %q, quero auth_5m_avg — a coluna declarada, mesmo sem amostra", e.ValueColumn)
	}
	if e.HasFiringValue {
		t.Error("hasFiringValue = true sobre uma coluna sem amostra")
	}
	if e.FiringValue != 0 {
		t.Errorf("firingValue = %v, quero 0 — sem amostra, o campo não pode carregar número nenhum", e.FiringValue)
	}

	// A linha de log segue a mesma regra: a coluna é conhecida (sai), o valor
	// não existiu (não sai) — presente e zerado mentiria "o valor era zero".
	got := emitir(t, e)
	if _, ok := got["valueColumn"]; !ok {
		t.Error("linha de log sem a chave valueColumn: a coluna declarada é conhecida mesmo sem amostra")
	}
	if _, ok := got["firingValue"]; ok {
		t.Errorf("linha de log com a chave firingValue = %v: sem amostra, ela não pode existir", got["firingValue"])
	}
}

// TestResolvedGroupLeavesTheSnapshot: a resolução tira o par (alarme, grupo)
// do retrato — não sobra firedAt nem firingValue de um disparo que já passou.
func TestResolvedGroupLeavesTheSnapshot(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 10, live: true})
	r, _, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	if got := r.ActiveGroups(); len(got) != 1 || !got[0].Firing {
		t.Fatalf("retrato antes da resolução = %+v, quero um disparo", got)
	}
	f.setTotal("OLT_A", 1, 900)
	clock.advance(time.Minute)
	passada(t, r)
	if got := r.ActiveGroups(); len(got) != 0 {
		t.Fatalf("retrato depois da resolução = %+v, quero vazio", got)
	}
}

// TestPendingGroupHasNoFiredAtNorFiringValue: pendente é um estado que ainda
// não disparou, e o retrato não pode inventar um valor para ele. ValueColumn
// é a exceção: é propriedade da REGRA, como Condition, e aparece mesmo
// pending.
func TestPendingGroupHasNoFiredAtNorFiringValue(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 10, live: true})
	r, _, _ := runnerDeTeste(t, f, "")
	passada(t, r)
	got := r.ActiveGroups()
	if len(got) != 1 {
		t.Fatalf("%d grupos no retrato, quero 1", len(got))
	}
	g := got[0]
	if g.Firing {
		t.Fatal("o grupo ainda está pending e já aparece como disparado")
	}
	if !g.FiredAt.IsZero() {
		t.Errorf("firedAt = %v, quero zero: o grupo ainda não disparou", g.FiredAt)
	}
	if g.HasFiringValue || g.FiringValue != 0 {
		t.Errorf("firingValue = %v (has=%v), quero zero/false: o grupo ainda não disparou", g.FiringValue, g.HasFiringValue)
	}
	if g.ValueColumn != schema.CounterTotalColumn {
		t.Errorf("valueColumn = %q, quero %q — propriedade da regra, presente mesmo pending", g.ValueColumn, schema.CounterTotalColumn)
	}
}

// activeGroupOf procura, no retrato, a entrada do grupo dado. t.Helper para
// que a falha aponte para o teste chamador.
func activeGroupOf(t *testing.T, got []ActiveGroup, groupKey string) ActiveGroup {
	t.Helper()
	for _, g := range got {
		if g.GroupKey == groupKey {
			return g
		}
	}
	t.Fatalf("%q não está no retrato: %+v", groupKey, got)
	return ActiveGroup{}
}

// TestFiringValueAndFiredAtStayFrozenWhileFiring: firedAt e firingValue
// descrevem O disparo, não uma amostra corrente do grupo. Enquanto o alarme
// segue disparado — condição ainda valendo, sem transição nenhuma — os dois
// têm de continuar os do disparo, mesmo que a passada seguinte observe outro
// número.
//
// OLT_B entra no cenário só para forçar uma passada DIRTY (ela transiciona de
// Absent para Pending, e é a transição que faz Runner.publishSnapshot
// republicar o retrato): sem uma transição em ALGUM grupo, o retrato de
// OLT_A não seria republicado nesta passada e o teste não provaria nada sobre
// o que Runner.decide grava no tracker de OLT_A — só sobre quando o retrato é
// republicado, que é outra guarda.
func TestFiringValueAndFiredAtStayFrozenWhileFiring(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"),
		grupo{olt: "OLT_A", slot: 1, total: 50, live: true},
		grupo{olt: "OLT_B", slot: 1, total: 500, live: true},
	)
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r) // OLT_A dispara.
	if len(sink.events) != 1 || sink.last().State != StateFiring || sink.last().GroupKey != "OLT_A|1" {
		t.Fatalf("o cenário não disparou como esperado: %+v", sink.events)
	}
	firedAt := sink.last().At

	got := activeGroupOf(t, r.ActiveGroups(), "OLT_A|1")
	if !got.Firing || !got.FiredAt.Equal(firedAt) || got.FiringValue != 50 {
		t.Fatalf("retrato recém-disparado = %+v, quero firing/%v/50", got, firedAt)
	}

	// OLT_A continua valendo a condição (total ainda < 100), com outro
	// número, SEM transição nenhuma: já estava disparado e segue disparado.
	// OLT_B passa a valer a condição PELA PRIMEIRA VEZ: é a transição que
	// força a republicação do retrato nesta passada.
	f.setTotal("OLT_A", 1, 80)
	f.setTotal("OLT_B", 1, 50)
	clock.advance(time.Minute)
	passada(t, r)
	if len(sink.events) != 1 {
		t.Fatalf("a oscilação de OLT_A sem transição emitiu: %+v", sink.events)
	}

	got = activeGroupOf(t, r.ActiveGroups(), "OLT_A|1")
	if !got.Firing {
		t.Fatalf("OLT_A saiu do disparo: %+v", got)
	}
	if !got.FiredAt.Equal(firedAt) {
		t.Errorf("firedAt = %v, quero %v — congelado no instante do disparo", got.FiredAt, firedAt)
	}
	if got.FiringValue != 50 {
		t.Errorf("firingValue = %v, quero 50 — congelado no disparo, não os 80 observados agora", got.FiringValue)
	}
}

// TestChangingValueDoesNotResetTheTimer: `value:` não entra na assinatura da
// regra gravada no estado durável — trocá-lo não pode zerar o cronômetro de
// sustentação de um alarme já em observação.
func TestChangingValueDoesNotResetTheTimer(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 50, live: true})
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0.5, 50)
	plain := schema.AlarmSpec{Name: "olt_down", When: "total < 100", SustainedFor: 2 * time.Minute}

	r1, _, _ := newQualifiedRunner(t, f, src, specWithMetrics(metrics, plain), path, t0)
	passada(t, r1) // entra em observação: since = t0, value implícito = total.

	changed := plain
	changed.Value = "auth_5m_avg"
	r2, sink, _ := newQualifiedRunner(t, f, src, specWithMetrics(metrics, changed), path, t0.Add(time.Hour))
	passada(t, r2)
	if len(sink.events) != 1 || !sink.last().Restored || !sink.last().Since.Equal(t0) {
		t.Fatalf("trocar value: reiniciou o cronômetro: %+v", sink.events)
	}
	if got := sink.last().ValueColumn; got != "auth_5m_avg" {
		t.Errorf("valueColumn = %q, quero auth_5m_avg — a regra NOVA vale a partir daqui", got)
	}
}

// TestRestoredAlarmRefiresWithFreshFiredAt: o cronômetro (since) sobrevive à
// restauração, mas firedAt NÃO é lido do estado durável (alarms.state v2 não
// tem esse campo) — o alarme restaurado volta pending e passa de novo por
// Runner.decide ao completar o prazo, e É esse disparo, deste processo, que
// firedAt descreve.
func TestRestoredAlarmRefiresWithFreshFiredAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r1, sink1, clock1 := runnerDeTeste(t, f, path)
	passada(t, r1)
	clock1.advance(2 * time.Minute)
	passada(t, r1) // dispara no processo 1.
	if len(sink1.events) != 1 || sink1.last().State != StateFiring {
		t.Fatalf("o cenário não disparou no processo 1: %+v", sink1.events)
	}

	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink2 := &sinkOfTest{}
	restart := t0.Add(time.Hour)
	clock2 := &clockOfTest{now: restart}
	r2, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink2, StatePath: path, Now: clock2.Now})
	if err != nil {
		t.Fatal(err)
	}
	passada(t, r2)
	if len(sink2.events) != 1 || sink2.last().State != StateFiring {
		t.Fatalf("o processo 2 não redisparou: %+v", sink2.events)
	}

	got := r2.ActiveGroups()
	if len(got) != 1 || !got[0].Firing {
		t.Fatalf("retrato depois do redisparo = %+v, quero um disparo", got)
	}
	if !got[0].FiredAt.Equal(restart) {
		t.Errorf("firedAt = %v, quero %v — o disparo é DESTE processo, não herdado do anterior", got[0].FiredAt, restart)
	}
	if !got[0].Since.Equal(t0) {
		t.Errorf("since = %v, quero %v — o cronômetro É o restaurado", got[0].Since, t0)
	}
}

// TestFiringEventCarriesValueColumnAndFiringValue: o evento e a linha de log
// carregam o mesmo valor capturado no disparo; a resolução não carrega
// nenhum — ela não descreve um valor novo.
func TestFiringEventCarriesValueColumnAndFiringValue(t *testing.T) {
	metrics := defaultWindowMetrics()
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 50, live: true})
	src := newFakeMetricSource(metrics)
	src.setWindows(1, 0.5, 50)
	alarm := schema.AlarmSpec{Name: "olt_down", When: "total < 100", Value: "auth_5m_avg", SustainedFor: 2 * time.Minute}
	r, sink, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, alarm), "", t0)
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)

	e := sink.last()
	if e.State != StateFiring {
		t.Fatalf("estado = %q, quero disparo", e.State)
	}
	if e.ValueColumn != "auth_5m_avg" || !e.HasFiringValue || e.FiringValue != 0.5 {
		t.Fatalf("evento = valueColumn=%q firingValue=%v hasFiringValue=%v, quero auth_5m_avg/0.5/true",
			e.ValueColumn, e.FiringValue, e.HasFiringValue)
	}
	got := emitir(t, e)
	if got["valueColumn"] != "auth_5m_avg" {
		t.Errorf("linha de log: valueColumn = %v", got["valueColumn"])
	}
	if got["firingValue"] != 0.5 {
		t.Errorf("linha de log: firingValue = %v, quero 0.5", got["firingValue"])
	}

	f.setTotal("OLT_A", 1, 500)
	clock.advance(time.Minute)
	passada(t, r)
	resolved := sink.last()
	if resolved.State != StateResolved {
		t.Fatalf("estado = %q, quero resolução", resolved.State)
	}
	if resolved.ValueColumn != "" || resolved.HasFiringValue {
		t.Errorf("a resolução carregou valor: valueColumn=%q hasFiringValue=%v", resolved.ValueColumn, resolved.HasFiringValue)
	}
	gotResolved := emitir(t, resolved)
	if gotResolved["valueColumn"] != nil || gotResolved["firingValue"] != nil {
		t.Errorf("a resolução logou valor: %+v", gotResolved)
	}
}

// TestValueNamingGroupKeyIsRefused: groupKey identifica O GRUPO, não o valor
// que disparou — os dois espaços de nome não se confundem, e o evento já tem
// GroupKey e Columns para a identidade.
func TestValueNamingGroupKeyIsRefused(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	_, err := Compile("telecom", specWith(schema.AlarmSpec{
		Name: "olt_down", When: "total < 100", Value: "groupKey", SustainedFor: 2 * time.Minute,
	}), []counter.Definition{def})
	if err == nil {
		t.Fatal("aceitou value: groupKey")
	}
	if !strings.Contains(err.Error(), "groupKey") {
		t.Fatalf("erro %q não menciona groupKey", err)
	}
}

// TestValueNamingAGroupByColumnIsRefused: o groupBy aceita int64 e boolean, e
// citá-lo em `value:` confundiria "quem" (a OLT) com "o quê" (o número que
// disparou).
func TestValueNamingAGroupByColumnIsRefused(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	_, err := Compile("telecom", specWith(schema.AlarmSpec{
		Name: "olt_down", When: "total < 100", Value: "slot", SustainedFor: 2 * time.Minute,
	}), []counter.Definition{def})
	if err == nil {
		t.Fatal("aceitou value: slot, uma coluna do groupBy")
	}
	if !strings.Contains(err.Error(), "slot") {
		t.Fatalf("erro %q não menciona slot", err)
	}
}

// TestValueNamingAnUnknownColumnIsRefused: recusado na subida — o alarme é o
// pior lugar do produto para descobrir um erro de digitação de madrugada. Com
// métricas declaradas, a mensagem tem de LISTAR as colunas aceitas — total e
// as geradas pelas métricas —, não só recusar em silêncio: é o que evita uma
// segunda rodada só para descobrir o nome certo.
func TestValueNamingAnUnknownColumnIsRefused(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	_, err := Compile("telecom", specWithMetrics(defaultWindowMetrics(), schema.AlarmSpec{
		Name: "olt_down", When: "total < 100", Value: "nao_existe", SustainedFor: 2 * time.Minute,
	}), []counter.Definition{def})
	if err == nil {
		t.Fatal("aceitou value inexistente")
	}
	if !strings.Contains(err.Error(), "nao_existe") {
		t.Fatalf("erro %q não menciona nao_existe", err)
	}
	if !strings.Contains(err.Error(), "accepted:") {
		t.Errorf("erro %q não lista as colunas aceitas", err)
	}
	if !strings.Contains(err.Error(), schema.CounterTotalColumn) {
		t.Errorf("erro %q não menciona %q entre as aceitas", err, schema.CounterTotalColumn)
	}
	if !strings.Contains(err.Error(), "auth_5m_avg") {
		t.Errorf("erro %q não menciona auth_5m_avg — uma coluna gerada por métrica — entre as aceitas", err)
	}
}
