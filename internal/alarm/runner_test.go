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

// runnerDeTeste monta um runner sobre um contador falso com um alarme de
// `total < 100` sustentado por dois minutos.
func runnerDeTeste(t *testing.T, f *fakeCounter, statePath string) (*Runner, *sinkOfTest, *clockOfTest) {
	t.Helper()
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
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

func passada(t *testing.T, r *Runner) {
	t.Helper()
	if err := r.EvaluateOnce(); err != nil {
		t.Fatal(err)
	}
}

// TestPassadaSoDisparaQuandoOPrazoSeCumpre: a sustentação medida pela varredura
// de verdade, e não só pela máquina de estado.
func TestPassadaSoDisparaQuandoOPrazoSeCumpre(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	if len(sink.events) != 0 {
		t.Fatalf("emitiu na primeira passada: %+v", sink.events)
	}
	clock.advance(time.Minute)
	passada(t, r)
	if len(sink.events) != 0 {
		t.Fatalf("emitiu antes do prazo: %+v", sink.events)
	}
	clock.advance(time.Minute)
	passada(t, r)
	if len(sink.events) != 1 {
		t.Fatalf("%d eventos ao completar o prazo, quero 1", len(sink.events))
	}
	if got := sink.last().State; got != StateFiring {
		t.Errorf("estado = %q, quero %q", got, StateFiring)
	}
}

// TestEventoDeDisparoCarregaOGrupoEOTotalObservado: o evento tem de dizer QUAL
// equipamento e com que número, senão quem o recebe não sabe o que fazer.
func TestEventoDeDisparoCarregaOGrupoEOTotalObservado(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 42, live: true})
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	e := sink.last()
	if e.Dataset != "telecom" || e.Counter != "autenticados_por_pon" || e.Alarm != "olt_down" {
		t.Errorf("identificação do alarme = %+v", e)
	}
	if e.GroupKey != "OLT_A|3" {
		t.Errorf("groupKey = %q, quero OLT_A|3", e.GroupKey)
	}
	if e.Total != 42 {
		t.Errorf("total = %d, quero 42 — o valor observado na decisão", e.Total)
	}
	if e.Condition != "total < 100" || e.SustainedFor != 2*time.Minute {
		t.Errorf("condição e prazo = %q / %v", e.Condition, e.SustainedFor)
	}
	if !e.Since.Equal(t0) {
		t.Errorf("since = %v, quero %v — o instante em que a condição passou a valer", e.Since, t0)
	}
	if e.Restored {
		t.Error("sustentação observada por este processo não pode vir marcada como restaurada")
	}
	if len(e.Columns) != 2 || e.Columns[0].Name != "olt" || e.Columns[0].Value != "OLT_A" ||
		e.Columns[1].Name != "slot" || e.Columns[1].Value != "3" {
		t.Errorf("colunas do groupBy = %+v", e.Columns)
	}
}

// TestCondicaoQuePassaResolveComMotivoDeCondicao é o "voltou".
func TestCondicaoQuePassaResolveComMotivoDeCondicao(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	f.setTotal("OLT_A", 3, 500)
	clock.advance(time.Minute)
	passada(t, r)
	if len(sink.events) != 2 {
		t.Fatalf("%d eventos, quero disparo e resolução", len(sink.events))
	}
	e := sink.last()
	if e.State != StateResolved || e.Reason != ReasonConditionCleared {
		t.Errorf("resolução = %q / %q", e.State, e.Reason)
	}
	if e.Total != 500 {
		t.Errorf("total = %d, quero 500 — o valor que fez a condição passar", e.Total)
	}
}

// TestGrupoAposentadoResolveComMotivoProprio: o assunto desapareceu, e o motivo
// é distinto de "o problema passou" — um equipamento removido do inventário não
// é um equipamento que voltou.
func TestGrupoAposentadoResolveComMotivoProprio(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	f.retire("OLT_A", 3)
	clock.advance(time.Minute)
	passada(t, r)
	e := sink.last()
	if e.State != StateResolved || e.Reason != ReasonGroupRetired {
		t.Fatalf("resolução = %q / %q, quero %q", e.State, e.Reason, ReasonGroupRetired)
	}
}

// TestGrupoQueNaoSatisfazACondicaoNaoOcupaMemoria: o estado é por par
// (alarme, grupo) em observação, e um contador por PON tem milhares de grupos
// saudáveis. Guardar tracker para todos faria a memória crescer com o
// inventário, e não com o problema.
func TestGrupoQueNaoSatisfazACondicaoNaoOcupaMemoria(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"),
		grupo{olt: "OLT_A", slot: 1, total: 500, live: true},
		grupo{olt: "OLT_A", slot: 2, total: 500, live: true},
		grupo{olt: "OLT_B", slot: 1, total: 50, live: true},
	)
	r, _, _ := runnerDeTeste(t, f, "")
	passada(t, r)
	if got := r.trackedGroups(); got != 1 {
		t.Fatalf("%d grupos com estado, quero 1: só o que satisfaz a condição", got)
	}
	f.setTotal("OLT_B", 1, 500)
	passada(t, r)
	if got := r.trackedGroups(); got != 0 {
		t.Fatalf("%d grupos com estado depois de a condição passar, quero 0", got)
	}
}

// TestEstadoAtivoEhGravadoNaTransicao: é o que permite ao processo seguinte
// saber desde quando a condição valia.
func TestEstadoAtivoEhGravadoNaTransicao(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, _, _ := runnerDeTeste(t, f, path)
	passada(t, r)
	st, err := checkpoint.LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Alarms) != 1 || len(st.Alarms[0].Groups) != 1 {
		t.Fatalf("estado gravado = %+v", st)
	}
	a := st.Alarms[0]
	if a.Counter != "autenticados_por_pon" || a.Alarm != "olt_down" ||
		a.When != "total < 100" || a.SustainedFor != 2*time.Minute {
		t.Errorf("a entrada não carrega a assinatura da regra: %+v", a)
	}
	if a.Groups[0].SinceUnixMs != t0.UnixMilli() {
		t.Errorf("since = %d, quero %d", a.Groups[0].SinceUnixMs, t0.UnixMilli())
	}
}

// TestEstadoSalvoEhRestauradoEDisparaComALacuna: a decisão de persistir o
// "desde quando", vista de fora — o processo volta e o alarme sai de imediato,
// dizendo quanto tempo ninguém estava olhando.
func TestEstadoSalvoEhRestauradoEDisparaComALacuna(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})

	// Primeiro processo: observa a condição passar a valer e cai.
	r1, _, _ := runnerDeTeste(t, f, path)
	passada(t, r1)

	// Segundo processo, uma hora depois, com a condição ainda valendo.
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: t0.Add(time.Hour)}
	r2, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink, StatePath: path, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	passada(t, r2)
	if len(sink.events) != 1 {
		t.Fatalf("%d eventos na primeira passada do processo novo, quero 1", len(sink.events))
	}
	e := sink.last()
	if e.State != StateFiring {
		t.Fatalf("estado = %q", e.State)
	}
	if !e.Restored {
		t.Error("o disparo tem de vir marcado: a sustentação foi medida de um instante lido do disco")
	}
	if !e.Since.Equal(t0) {
		t.Errorf("since = %v, quero %v restaurado do arquivo", e.Since, t0)
	}
	if e.DowntimeMs != time.Hour.Milliseconds() {
		t.Errorf("lacuna = %dms, quero %dms — é o que diz a quem recebe que ninguém observou o período",
			e.DowntimeMs, time.Hour.Milliseconds())
	}
}

// TestEstadoSalvoComRegraDivergenteEhDescartado: prazo antigo sobre condição
// nova faria o alarme disparar por uma sustentação que ninguém mediu.
func TestEstadoSalvoComRegraDivergenteEhDescartado(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r1, _, _ := runnerDeTeste(t, f, path)
	passada(t, r1)

	// Mesmo alarme, prazo trocado no YAML.
	def := defByPON("autenticados_por_pon")
	outro := schema.AlarmSpec{Name: "olt_down", When: "total < 100", SustainedFor: 30 * time.Minute}
	defs, err := Compile("telecom", specWith(outro), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: t0.Add(time.Hour)}
	r2, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink, StatePath: path, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	passada(t, r2)
	if len(sink.events) != 0 {
		t.Fatalf("disparou com o cronômetro da regra antiga: %+v", sink.events)
	}
}

// TestEstadoAusenteNaoImpedeASubida: a primeira subida é o caso normal, e
// arquivo ausente não é anomalia nem recusa.
func TestEstadoAusenteNaoImpedeASubida(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 500, live: true})
	r, sink, _ := runnerDeTeste(t, f, path)
	passada(t, r)
	if len(sink.events) != 0 {
		t.Fatalf("emitiu sem condição nenhuma valendo: %+v", sink.events)
	}
}

// TestTrocarOKeyFormatNaoPerdeOEstadoDoAlarme é a razão de a identidade do
// grupo ser a chave CANÔNICA em bytes, e não o texto do groupKey.
//
// O molde do groupKey pode mudar sem reconstruir o contador — trocá-lo
// re-renderiza o texto na abertura e não mexe em um byte de disco. Se o estado
// de sustentação fosse indexado pelo texto, uma troca de molde descartaria em
// silêncio os cronômetros de todos os alarmes ativos, e cada um recomeçaria do
// zero sem ninguém saber por quê.
func TestTrocarOKeyFormatNaoPerdeOEstadoDoAlarme(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	g := grupo{olt: "OLT_A", slot: 3, total: 50, live: true}

	// Primeiro processo, molde default: o texto do grupo é "OLT_A|3", e a
	// condição passa a valer.
	f1 := newFakeCounter(t, defByPON("autenticados_por_pon"), g)
	r1, _, _ := runnerDeTeste(t, f1, path)
	passada(t, r1)
	if got := string(f1.texts[0]); got != "OLT_A|3" {
		t.Fatalf("texto do grupo no molde default = %q", got)
	}

	// Segundo processo: o MESMO contador reaberto com o molde trocado. Reabrir
	// re-renderiza o texto de todos os grupos, então o groupKey passa a ser
	// "OLT_A.03" — e é aí que uma identidade baseada no texto perderia o
	// cronômetro salvo.
	def := defByPON("autenticados_por_pon")
	def.Format = moldeDe(t, "{olt}.{slot:02}", def)
	f2 := newFakeCounter(t, def, g)
	if got := string(f2.texts[0]); got != "OLT_A.03" {
		t.Fatalf("texto do grupo no molde novo = %q; o teste não exercita a troca", got)
	}
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock := &clockOfTest{now: t0.Add(time.Hour)}
	r2, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f2},
		Options{Sink: sink, StatePath: path, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	passada(t, r2)
	if len(sink.events) != 1 {
		t.Fatalf("%d eventos, quero 1: o cronômetro salvo tinha de sobreviver à troca do molde", len(sink.events))
	}
	if e := sink.last(); !e.Restored || !e.Since.Equal(t0) {
		t.Errorf("o disparo não usou o instante salvo: restored=%v since=%v", e.Restored, e.Since)
	}
}
