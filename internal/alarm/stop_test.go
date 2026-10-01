package alarm

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// slowSink é o dublê de um destino de gravação lenta: com slow ligado, cada
// Emit custa delay, como a admissão que espera uma gravação durável numa
// rajada. A emissão de número trigger fica PRESA até release, para que o
// teste cancele com uma contagem conhecida. É lido pelo laço e pelo teste,
// por isso o mutex.
type slowSink struct {
	mu      sync.Mutex
	events  []Event
	slow    bool
	delay   time.Duration
	trigger int
	reached chan struct{}
	release chan struct{}
	slowN   int
}

func newSlowSink() *slowSink {
	return &slowSink{delay: stopEmitDelay, trigger: stopTrigger, reached: make(chan struct{}), release: make(chan struct{})}
}

func (s *slowSink) Emit(e Event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	slow := s.slow
	held := false
	if slow {
		s.slowN++
		held = s.slowN == s.trigger
	}
	s.mu.Unlock()
	if held {
		close(s.reached)
		<-s.release
	}
	if slow {
		time.Sleep(s.delay)
	}
}

func (s *slowSink) snapshot() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

func (s *slowSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func (s *slowSink) reset() {
	s.mu.Lock()
	s.events = nil
	s.mu.Unlock()
}

func (s *slowSink) goSlow() {
	s.mu.Lock()
	s.slow = true
	s.mu.Unlock()
}

const (
	stopEmitDelay = 10 * time.Millisecond
	stopTrigger   = 20
)

func stopTestRunner(t *testing.T, f *fakeCounter, sink Sink, clock *clockOfTest, statePath string) *Runner {
	t.Helper()
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: sink, StatePath: statePath, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// interruptSlowPass sobe o laço, cuja primeira passada é imediata e lenta, e
// cancela com a emissão de número stopTrigger presa no destino: o cancelamento
// acontece com uma contagem conhecida. Depois solta a emissão e chama o Stop,
// que precisa voltar bem antes de a rajada inteira (pending emissões) acabar.
// Devolve quantas emissões havia no instante do cancelamento.
func interruptSlowPass(t *testing.T, r *Runner, sink *slowSink, pending int) int {
	t.Helper()
	sink.reset()
	sink.goSlow()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	select {
	case <-sink.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("a rajada não começou dentro do prazo")
	}
	// O ctx do laço deriva deste: depois do cancel ele está cancelado, e a
	// emissão em curso já entrou na contagem.
	cancel()
	atCancel := sink.count()
	close(sink.release)
	stopped := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		r.Stop()
		stopped <- time.Since(start)
	}()
	full := time.Duration(pending) * stopEmitDelay
	// O prazo é só teto contra trava, e por isso é folgado: o tempo do Stop
	// inclui o fsync do alarms.state, que sob carga de disco passou de 1 s
	// com o código correto (5 falhas em 100 com 12 escritores em fsync na
	// .217). Quem prova que a passada parou no grupo corrente é a contagem
	// logo abaixo — a passada inteira emitiria todos os grupos pendentes.
	limit := full
	select {
	case took := <-stopped:
		t.Logf("Stop voltou em %s; a rajada inteira levaria %s", took, full)
	case <-time.After(limit):
		took := <-stopped
		t.Fatalf("o Stop esperou a passada em voo: voltou em %s, prazo %s (a rajada inteira leva %s)", took, limit, full)
	}
	// Um alarme por grupo: a decisão em curso no cancelamento é exatamente a
	// emissão presa, já contada. Qualquer emissão a mais é uma decisão nova
	// tomada depois do cancelamento.
	if got := sink.count(); got != atCancel {
		t.Fatalf("%d emissões depois do Stop, %d no instante do cancelamento: a passada decidiu %d grupo(s) a mais depois de cancelada",
			got, atCancel, got-atCancel)
	}
	return atCancel
}

func savedSince(t *testing.T, statePath string) (map[string]int64, int64) {
	t.Helper()
	st, err := checkpoint.LoadAlarmState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	saved := make(map[string]int64)
	for _, a := range st.Alarms {
		for _, g := range a.Groups {
			saved[string(g.Group)] = g.SinceUnixMs
		}
	}
	return saved, st.SavedAtUnixMs
}

// TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups: o Stop no
// meio de uma rajada lenta de disparos volta depois do grupo corrente, não
// depois da rajada inteira; a passada abandonada não resolve ninguém; a view
// e o estado gravado refletem o que foi emitido; e a subida seguinte reanuncia
// como restaurado o que venceu o prazo e mantém pendente o que não venceu.
//
// Coortes, na ordem das linhas do contador: BURST dispara na passada
// interrompida; FIRING já disparou antes dela; PENDING está em sustentação.
// FIRING e PENDING ficam depois do ponto de interrupção: são os não visitados
// que uma fase dois indevida resolveria (FIRING com `resolved`, PENDING em
// silêncio).
func TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups(t *testing.T) {
	const burst, firing, pending = 300, 50, 50
	var gs []grupo
	for i := range burst {
		gs = append(gs, grupo{olt: "BURST", slot: int64(i), total: 500, live: true})
	}
	for i := range firing {
		gs = append(gs, grupo{olt: "FIRING", slot: int64(i), total: 50, live: true})
	}
	for i := range pending {
		gs = append(gs, grupo{olt: "PENDING", slot: int64(i), total: 500, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	statePath := filepath.Join(t.TempDir(), "alarms.state")
	sink := newSlowSink()
	clock := &clockOfTest{now: t0}
	r := stopTestRunner(t, f, sink, clock, statePath)

	// t0: FIRING entra em sustentação.
	passada(t, r)
	// t0+1m: BURST entra em sustentação.
	clock.advance(time.Minute)
	for i := range burst {
		f.setTotal("BURST", int64(i), 50)
	}
	passada(t, r)
	// t0+2m: FIRING dispara; PENDING entra em sustentação.
	clock.advance(time.Minute)
	for i := range pending {
		f.setTotal("PENDING", int64(i), 50)
	}
	passada(t, r)
	if n := sink.count(); n != firing {
		t.Fatalf("preparação: %d disparos, esperado %d", n, firing)
	}

	// t0+3m: a passada do laço dispara BURST, devagar, e o Stop chega no meio.
	clock.advance(time.Minute)
	atCancel := interruptSlowPass(t, r, sink, burst)

	// A passada abandonada só disparou; ninguém foi resolvido.
	fired := make(map[string]bool)
	for _, e := range sink.snapshot() {
		if e.State != StateFiring || !strings.HasPrefix(e.GroupKey, "BURST|") {
			t.Fatalf("a passada interrompida emitiu %s para %s: só disparos de BURST eram possíveis", e.State, e.GroupKey)
		}
		fired[e.GroupKey] = true
	}
	if len(fired) != atCancel || atCancel != stopTrigger {
		t.Fatalf("%d disparos distintos, %d no cancelamento: esperado %d", len(fired), atCancel, stopTrigger)
	}

	// A view acompanha o que foi emitido antes da interrupção.
	view := make(map[string]bool)
	for _, g := range r.ActiveGroups() {
		view[g.GroupKey] = g.Firing
	}
	for i, g := range gs {
		text := string(f.texts[i])
		want := g.olt == "FIRING" || fired[text]
		if got, ok := view[text]; !ok || got != want {
			t.Fatalf("view depois do Stop: %s presente=%v disparado=%v, esperado disparado=%v", text, ok, got, want)
		}
	}

	// O estado gravado pelo Stop guarda TODOS os cronômetros, inclusive dos
	// não visitados, com o since de antes da passada.
	saved, savedAt := savedSince(t, statePath)
	if savedAt != clock.now.UnixMilli() {
		t.Errorf("o estado não foi gravado pelo Stop: savedAt %d, esperado %d", savedAt, clock.now.UnixMilli())
	}
	for i, g := range gs {
		want := t0
		switch g.olt {
		case "BURST":
			want = t0.Add(time.Minute)
		case "PENDING":
			want = t0.Add(2 * time.Minute)
		}
		since, ok := saved[string(f.keys[i])]
		if !ok || since != want.UnixMilli() {
			t.Fatalf("grupo %s|%d: gravado=%v since=%d, esperado since=%d", g.olt, g.slot, ok, since, want.UnixMilli())
		}
	}

	// Subida seguinte, 10 s depois da parada.
	clock.advance(10 * time.Second)
	after := &sinkOfTest{}
	next := stopTestRunner(t, f, after, clock, statePath)
	passada(t, next)
	byGroup := make(map[string]Event)
	for _, e := range after.events {
		byGroup[e.GroupKey] = e
	}
	for i, g := range gs {
		text := string(f.texts[i])
		e, ok := byGroup[text]
		switch g.olt {
		case "PENDING":
			if ok {
				t.Fatalf("%s ainda sem prazo cumprido emitiu %s na subida", text, e.State)
			}
		default:
			if !ok || e.State != StateFiring || !e.Restored {
				t.Fatalf("%s venceu o prazo e devia voltar disparado e restaurado: %+v (presente=%v)", text, e, ok)
			}
			if want := t0.Add(time.Minute); g.olt == "BURST" && !e.Since.Equal(want) {
				t.Fatalf("%s voltou com since %v, esperado %v", text, e.Since, want)
			}
		}
	}
	if got, want := len(after.events), burst+firing; got != want {
		t.Fatalf("subida emitiu %d eventos, esperado %d", got, want)
	}
	if got := next.trackedGroups(); got != len(gs) {
		t.Fatalf("subida acompanha %d grupos, esperado %d", got, len(gs))
	}
}

// TestStopDuringRetirementBatchAbortsPhaseTwo: grupos disparados que somem
// todos na mesma passada viram um lote de resoluções na fase dois, cada uma
// tão cara quanto um disparo. O Stop no meio volta depois da resolução em
// curso; as já emitidas saem do estado, as demais ficam no arquivo e, com os
// grupos ainda ausentes na subida seguinte, são descartadas pela ausência.
func TestStopDuringRetirementBatchAbortsPhaseTwo(t *testing.T) {
	const retired = 300
	var gs []grupo
	for i := range retired {
		gs = append(gs, grupo{olt: "GONE", slot: int64(i), total: 50, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	statePath := filepath.Join(t.TempDir(), "alarms.state")
	sink := newSlowSink()
	clock := &clockOfTest{now: t0}
	r := stopTestRunner(t, f, sink, clock, statePath)
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	if n := sink.count(); n != retired {
		t.Fatalf("preparação: %d disparos, esperado %d", n, retired)
	}
	for i := range retired {
		f.retire("GONE", int64(i))
	}

	clock.advance(time.Minute)
	atCancel := interruptSlowPass(t, r, sink, retired)

	resolved := make(map[string]bool)
	for _, e := range sink.snapshot() {
		if e.State != StateResolved || e.Reason != ReasonGroupRetired {
			t.Fatalf("a fase dois emitiu %s/%s para %s: só resoluções por aposentadoria eram possíveis", e.State, e.Reason, e.GroupKey)
		}
		resolved[e.GroupKey] = true
	}
	if len(resolved) != atCancel || atCancel != stopTrigger {
		t.Fatalf("%d resoluções distintas, %d no cancelamento: esperado %d", len(resolved), atCancel, stopTrigger)
	}

	// Estado e view: as resolvidas saíram, as demais continuam com o since
	// do disparo.
	saved, _ := savedSince(t, statePath)
	view := make(map[string]bool)
	for _, g := range r.ActiveGroups() {
		view[g.GroupKey] = g.Firing
	}
	for i := range gs {
		text := string(f.texts[i])
		since, inState := saved[string(f.keys[i])]
		firing, inView := view[text]
		if resolved[text] {
			if inState || inView {
				t.Fatalf("%s já resolvido continua no estado (%v) ou na view (%v)", text, inState, inView)
			}
			continue
		}
		if !inState || since != t0.UnixMilli() {
			t.Fatalf("%s sem decisão: gravado=%v since=%d, esperado since=%d", text, inState, since, t0.UnixMilli())
		}
		if !inView || !firing {
			t.Fatalf("%s sem decisão devia continuar disparado na view: presente=%v disparado=%v", text, inView, firing)
		}
	}

	// Subida seguinte com os grupos ainda ausentes: o estado só guarda o
	// cronômetro, os restaurados voltam pendentes e a ausência os descarta.
	clock.advance(10 * time.Second)
	after := &sinkOfTest{}
	next := stopTestRunner(t, f, after, clock, statePath)
	if got, want := next.trackedGroups(), retired-atCancel; got != want {
		t.Fatalf("subida restaurou %d grupos, esperado %d", got, want)
	}
	passada(t, next)
	if len(after.events) != 0 {
		t.Fatalf("subida emitiu %d evento(s) para grupos ausentes restaurados pendentes: %+v", len(after.events), after.events[0])
	}
	if got := next.trackedGroups(); got != 0 {
		t.Fatalf("subida ainda acompanha %d grupos ausentes", got)
	}
}
