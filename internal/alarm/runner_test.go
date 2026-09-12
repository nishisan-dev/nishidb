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

// TestResolucaoDizDesdeQuandoACondicaoValia foi escrito por causa de um defeito
// que só apareceu ao rodar o produto e LER a linha de log: a resolução saía com
// o instante zero (0001-01-01) no campo `since`.
//
// A causa é a ordem: clear() apaga o cronômetro, e o evento era montado depois,
// lendo o campo já zerado. O `since` descreve o evento, então tem de viajar com
// a decisão — quem recebe um "voltou" precisa saber quanto tempo durou o
// problema, e o instante zero não é só inútil, é uma afirmação falsa.
func TestResolucaoDizDesdeQuandoACondicaoValia(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	f.setTotal("OLT_A", 3, 500)
	clock.advance(5 * time.Minute)
	passada(t, r)

	e := sink.last()
	if e.State != StateResolved {
		t.Fatalf("estado = %q", e.State)
	}
	if e.Since.IsZero() {
		t.Fatal("a resolução saiu com o instante zero em `since`: o cronômetro foi apagado antes de o evento ser montado")
	}
	if !e.Since.Equal(t0) {
		t.Errorf("since = %v, quero %v — o instante em que a condição passou a valer", e.Since, t0)
	}
	if got := e.At.Sub(e.Since); got != 7*time.Minute {
		t.Errorf("duração do problema = %v, quero 7m (2 de sustentação + 5 disparado)", got)
	}
}

// TestResolucaoPorAposentadoriaTambemDizDesdeQuando: o grupo sai do contador, e
// a varredura não tem mais nem o texto dele — mas o cronômetro ainda é um fato.
func TestResolucaoPorAposentadoriaTambemDizDesdeQuando(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, sink, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	f.retire("OLT_A", 3)
	clock.advance(time.Minute)
	passada(t, r)

	e := sink.last()
	if e.Reason != ReasonGroupRetired {
		t.Fatalf("motivo = %q", e.Reason)
	}
	if e.Since.IsZero() || !e.Since.Equal(t0) {
		t.Errorf("since = %v, quero %v", e.Since, t0)
	}
	// O texto do grupo é re-renderizado da chave canônica, porque a varredura
	// não viu o grupo nesta passada e ninguém tem o texto em mão.
	if e.GroupKey != "OLT_A|3" {
		t.Errorf("groupKey = %q, quero OLT_A|3 renderizado da chave canônica", e.GroupKey)
	}
}

// TestOEncerramentoGravaOEstadoComOInstanteDaParada corrige um defeito que só
// apareceu no piloto, num reinício de verdade.
//
// O estado é gravado nas TRANSIÇÕES. Um processo que fica nove minutos sem
// nenhuma e então é reiniciado tinha o seu savedAt nove minutos no passado — e
// o downtimeMs do evento restaurado reportava nove minutos de lacuna para uma
// parada de segundos. O campo promete "quanto tempo ninguém observou" e
// entregava "quanto tempo desde a última mudança de estado", que é outra coisa,
// e enganosa para quem usa o número para julgar se pode confiar no alarme.
//
// O encerramento gracioso passa a gravar. Num crash sobra o último save, que é
// o melhor que se tem — mas aí a lacuna é de fato desconhecida, e um número
// grande é a resposta honesta.
func TestOEncerramentoGravaOEstadoComOInstanteDaParada(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, _, clock := runnerDeTeste(t, f, path)

	// Duas transições: entra em observação, e dispara ao cumprir o prazo. A
	// SEGUNDA é a última — o disparo também é transição, e as passadas quietas
	// têm de vir depois dele para o teste exercitar o defeito.
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	ultimaTransicao := t0.Add(2 * time.Minute)
	depoisDaTransicao, err := checkpoint.LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if depoisDaTransicao.SavedAtUnixMs != ultimaTransicao.UnixMilli() {
		t.Fatalf("savedAt da transição = %d, quero %d", depoisDaTransicao.SavedAtUnixMs, ultimaTransicao.UnixMilli())
	}

	// Nove minutos de passadas SEM transição nenhuma: o arquivo não é
	// reescrito, e é daí que vinha o número errado.
	for i := 0; i < 9; i++ {
		clock.advance(time.Minute)
		passada(t, r)
	}
	semTransicao, err := checkpoint.LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if semTransicao.SavedAtUnixMs != ultimaTransicao.UnixMilli() {
		t.Fatalf("o arquivo foi reescrito sem transição (savedAt %d, esperava %d): "+
			"o teste não exercita o defeito", semTransicao.SavedAtUnixMs, ultimaTransicao.UnixMilli())
	}

	// O encerramento grava com o instante da parada.
	r.Stop()
	naParada, err := checkpoint.LoadAlarmState(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, quero := naParada.SavedAtUnixMs, ultimaTransicao.Add(9*time.Minute).UnixMilli(); got != quero {
		t.Fatalf("savedAt da parada = %d, quero %d — o encerramento não gravou", got, quero)
	}
	// O cronômetro em si NÃO se move: o que muda é quando o estado foi salvo.
	if len(naParada.Alarms) != 1 || naParada.Alarms[0].Groups[0].SinceUnixMs != t0.UnixMilli() {
		t.Errorf("o `desde quando` foi alterado pela parada: %+v", naParada.Alarms)
	}
}

// TestALacunaMedeAParadaENaoOTempoSemTransicao é o mesmo defeito visto de fora,
// pelo número que vai no evento.
func TestALacunaMedeAParadaENaoOTempoSemTransicao(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})

	// Processo 1: observa a condição passar a valer, fica nove minutos sem
	// transição e encerra graciosamente.
	r1, _, clock1 := runnerDeTeste(t, f, path)
	passada(t, r1)
	clock1.advance(2 * time.Minute)
	passada(t, r1) // dispara: última transição
	for i := 0; i < 9; i++ {
		clock1.advance(time.Minute)
		passada(t, r1)
	}
	r1.Stop()

	// Processo 2: sobe DEZ SEGUNDOS depois. A lacuna é de dez segundos, não de
	// nove minutos e dez segundos.
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	sink := &sinkOfTest{}
	clock2 := &clockOfTest{now: t0.Add(11*time.Minute + 10*time.Second)}
	r2, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink, StatePath: path, Now: clock2.Now})
	if err != nil {
		t.Fatal(err)
	}
	passada(t, r2)
	if len(sink.events) != 1 {
		t.Fatalf("%d eventos, quero o disparo restaurado", len(sink.events))
	}
	e := sink.last()
	if !e.Restored {
		t.Fatal("o disparo não veio marcado como restaurado")
	}
	if got, quero := e.DowntimeMs, int64(10_000); got != quero {
		t.Errorf("lacuna = %dms, quero %dms — o número está medindo o tempo sem transição, não a parada", got, quero)
	}
	// E o cronômetro continua sendo o original: a sustentação vale desde t0.
	if !e.Since.Equal(t0) {
		t.Errorf("since = %v, quero %v", e.Since, t0)
	}
}
