package series

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// loopEvery é o every dos testes do laço. Divide o dia, e t0 (10:30) é
// fronteira dele; o alinhamento com every que NÃO divide o dia tem teste
// próprio (TestFloorBoundaryAlignsToTheUnixEpochNotToYearOne).
const loopEvery = 5 * time.Minute

// loopBase é uma fronteira de loopEvery: os testes descrevem os instantes como
// deslocamentos dela.
var loopBase = FloorBoundary(t0, loopEvery)

// loopEvent é o que o laço faz, na ordem em que faz: pede uma espera ao
// temporizador (fire != nil) ou publica a passada de at (fire == nil). Um canal
// único, e não um por tipo, porque um select sobre dois canais prontos escolhe
// ao acaso, e a ORDEM entre esperar e passar é o que os testes afirmam.
type loopEvent struct {
	wait time.Duration
	fire chan time.Time
	at   time.Time
}

// loopClock é o relógio E o temporizador do laço nos testes. Atômico porque o
// laço o lê na goroutine dele enquanto o teste o move (o clockOfTest dos
// testes do EmitOnce não é seguro entre goroutines).
type loopClock struct {
	ns     atomic.Int64
	events chan loopEvent
}

func (c *loopClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *loopClock) set(at time.Time)        { c.ns.Store(at.UnixNano()) }
func (c *loopClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// After registra a espera pedida e devolve o canal que o teste dispara. O canal
// de eventos tem folga para que o laço nunca bloqueie aqui: bloqueado no envio,
// ele não chegaria ao select que o Stop acorda.
func (c *loopClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.events <- loopEvent{wait: d, fire: ch}
	return ch
}

// loopOutput é o destino dos testes do laço: roda o gancho da passada (que
// simula a duração dela movendo o relógio) e anuncia o at publicado.
//
// Os ganchos são uma FILA, um por passada, enfileirados antes de a passada
// começar: trocar um gancho único depois de receber o evento disputaria com a
// passada seguinte, que no atraso sai imediatamente. Passada sem gancho dura
// zero.
type loopOutput struct {
	clock *loopClock
	mu    sync.Mutex
	hooks []func()
}

func (o *loopOutput) then(hooks ...func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.hooks = append(o.hooks, hooks...)
}

func (o *loopOutput) Publish(_ context.Context, _, _ []byte, at time.Time) error {
	o.mu.Lock()
	var hook func()
	if len(o.hooks) > 0 {
		hook, o.hooks = o.hooks[0], o.hooks[1:]
	}
	o.mu.Unlock()
	if hook != nil {
		hook()
	}
	o.clock.events <- loopEvent{at: at}
	return nil
}

// syncBuffer é o log dos testes: o laço escreve na goroutine dele.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// events devolve as linhas de log com o evento pedido, decodificadas.
func (b *syncBuffer) events(t *testing.T, name string) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("linha de log inválida %q: %v", line, err)
		}
		if m["event"] == name {
			out = append(out, m)
		}
	}
	return out
}

type loopHarness struct {
	e      *Emitter
	clock  *loopClock
	output *loopOutput
	log    *syncBuffer
}

// startLoop sobe o laço com o relógio em start. Só-Kafka, com um grupo: cada
// passada é exatamente um evento.
func startLoop(t *testing.T, start time.Time) *loopHarness {
	t.Helper()
	clock := &loopClock{events: make(chan loopEvent, 64)}
	clock.set(start)
	h := &loopHarness{clock: clock, output: &loopOutput{clock: clock}, log: &syncBuffer{}}
	e, err := NewEmitter(Options{
		Dataset: "services", Counter: "by_olt", Columns: []string{"olt", "slot"},
		Reader:      newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 10, live: true}),
		DisableFile: true, Outputs: []Output{h.output}, Every: loopEvery,
		Now: clock.Now, After: clock.After,
		Log: logging.New(h.log, logging.FormatJSON, slog.LevelInfo),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.e = e
	t.Cleanup(func() { _ = e.Close() })
	e.Start(context.Background())
	return h
}

// passesTake enfileira a duração das próximas passadas, uma por passada, no
// relógio do teste.
func (h *loopHarness) passesTake(ds ...time.Duration) {
	for _, d := range ds {
		h.output.then(func() { h.clock.advance(d) })
	}
}

// next devolve o próximo evento do laço. O prazo é guarda de travamento, e não
// medida: o laço verde nunca chega perto dele.
func (h *loopHarness) next(t *testing.T) loopEvent {
	t.Helper()
	select {
	case ev := <-h.clock.events:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("o laço não fez nada em 5 s: travado")
		return loopEvent{}
	}
}

func (h *loopHarness) expectWait(t *testing.T) loopEvent {
	t.Helper()
	ev := h.next(t)
	if ev.fire == nil {
		t.Fatalf("o laço emitiu a passada de %s quando devia esperar a fronteira", ev.at.Format(time.RFC3339Nano))
	}
	return ev
}

func (h *loopHarness) expectPass(t *testing.T) time.Time {
	t.Helper()
	ev := h.next(t)
	if ev.fire != nil {
		t.Fatalf("o laço pediu espera de %v quando a passada devia sair imediatamente", ev.wait)
	}
	return ev.at
}

// fireAt dispara a espera com o relógio em now: o temporizador pode acordar
// adiantado ou atrasado, e o laço tem de decidir pelo relógio, não pelo disparo.
func (h *loopHarness) fireAt(ev loopEvent, now time.Time) {
	h.clock.set(now)
	ev.fire <- now
}

func at(offset time.Duration) time.Time { return loopBase.Add(offset) }

func TestFloorBoundaryAlignsToTheUnixEpochNotToYearOne(t *testing.T) {
	// every que NÃO divide o dia: a época e o ano 1 dão fronteiras diferentes.
	every := 7 * time.Minute
	ts := time.Date(2026, 9, 12, 10, 31, 17, 5, time.UTC)
	got := FloorBoundary(ts, every)
	if got.UnixNano()%every.Nanoseconds() != 0 {
		t.Fatalf("fronteira %v não é múltipla de %v na época Unix", got, every)
	}
	if got.After(ts) || ts.Sub(got) >= every {
		t.Fatalf("fronteira %v não é a mais recente <= %v", got, ts)
	}
	// O teste só discrimina se as duas grafias divergem neste instante.
	if got.Equal(ts.Truncate(every)) {
		t.Fatalf("instante mal escolhido: Truncate e a época coincidem em %v", got)
	}
	// every que divide o dia: as duas coincidem, e a fronteira é a do relógio.
	if got := FloorBoundary(ts, loopEvery); !got.Equal(time.Date(2026, 9, 12, 10, 30, 0, 0, time.UTC)) {
		t.Fatalf("fronteira de 5m = %v", got)
	}
	// Antes de 1970 o resto é negativo, e a fronteira continua <= t.
	old := time.Date(1969, 12, 31, 23, 58, 30, 0, time.UTC)
	if got := FloorBoundary(old, time.Minute); !got.Equal(time.Date(1969, 12, 31, 23, 58, 0, 0, time.UTC)) {
		t.Fatalf("fronteira antes da época = %v", got)
	}
	// Sobre a fronteira, o teto é ela mesma; fora, a seguinte.
	if c := ceilBoundary(loopBase, loopEvery); !c.Equal(loopBase) {
		t.Fatalf("teto sobre a fronteira = %v, quero %v", c, loopBase)
	}
	if c := ceilBoundary(loopBase.Add(time.Nanosecond), loopEvery); !c.Equal(loopBase.Add(loopEvery)) {
		t.Fatalf("teto fora da fronteira = %v", c)
	}
}

// A subida FORA da fronteira e um temporizador que acorda ATRASADO: sem os
// dois, `at = now()` e a passada imediata passariam verdes.
func TestLoopFirstPassWaitsForTheFirstBoundaryAndCarriesIt(t *testing.T) {
	start := at(7*time.Second + 123*time.Millisecond)
	h := startLoop(t, start)
	w := h.expectWait(t)
	if want := at(loopEvery).Sub(start); w.wait != want {
		t.Fatalf("primeira espera = %v, quero %v (até a primeira fronteira >= agora)", w.wait, want)
	}
	h.fireAt(w, at(loopEvery+3*time.Millisecond))
	got := h.expectPass(t)
	if !got.Equal(at(loopEvery)) {
		t.Fatalf("at da primeira passada = %v, quero a fronteira %v", got, at(loopEvery))
	}
	if got.UnixNano()%loopEvery.Nanoseconds() != 0 {
		t.Fatalf("at %v não é múltiplo de %v na época", got, loopEvery)
	}
}

// Subida exatamente sobre uma fronteira: a passada é ela, sem espera.
func TestLoopStartingOnABoundaryEmitsThatBoundary(t *testing.T) {
	h := startLoop(t, at(0))
	if got := h.expectPass(t); !got.Equal(at(0)) {
		t.Fatalf("at = %v, quero %v", got, at(0))
	}
}

func TestLoopKeepsAFixedPeriodWhenThePassIsShorterThanEvery(t *testing.T) {
	h := startLoop(t, at(time.Second))
	h.passesTake(40 * time.Second)
	h.fireAt(h.expectWait(t), at(loopEvery))
	first := h.expectPass(t)
	w := h.expectWait(t)
	// A passada durou 40 s: a espera é até a fronteira, e não um every inteiro.
	if want := at(2 * loopEvery).Sub(at(loopEvery + 40*time.Second)); w.wait != want {
		t.Fatalf("espera depois da passada = %v, quero %v", w.wait, want)
	}
	h.fireAt(w, at(2*loopEvery))
	second := h.expectPass(t)
	if second.Sub(first) != loopEvery {
		t.Fatalf("at consecutivos %v e %v não diferem exatamente %v", first, second, loopEvery)
	}
	if s := h.e.Stats(); s.SkippedBoundaries != 0 {
		t.Fatalf("puladas = %d sem atraso nenhum", s.SkippedBoundaries)
	}
}

func TestLoopSkipsTheBoundariesALatePassOverran(t *testing.T) {
	h := startLoop(t, at(time.Second))
	h.passesTake(17 * time.Minute)
	h.fireAt(h.expectWait(t), at(loopEvery))
	if got := h.expectPass(t); !got.Equal(at(loopEvery)) {
		t.Fatalf("primeira passada em %v", got)
	}
	// Fim em +22m: a próxima sai JÁ, com a fronteira mais recente (+20m); +10m
	// e +15m foram puladas.
	got := h.expectPass(t)
	if !got.Equal(at(4 * loopEvery)) {
		t.Fatalf("passada atrasada com at = %v, quero a fronteira mais recente %v", got, at(4*loopEvery))
	}
	h.e.Stop()
	if s := h.e.Stats(); s.SkippedBoundaries != 2 {
		t.Fatalf("puladas = %d, quero 2 (+10m e +15m)", s.SkippedBoundaries)
	}
	warns := h.log.events(t, "series.boundaries_skipped")
	if len(warns) != 1 {
		t.Fatalf("%d WARN de fronteiras puladas, quero 1 por ocorrência: %v", len(warns), warns)
	}
	w := warns[0]
	if w["skipped"] != float64(2) || w["at"] != at(4*loopEvery).Format(timeLayout) ||
		w["dataset"] != "services" || w["counter"] != "by_olt" || w["previous_pass_ms"] != float64((17*time.Minute).Milliseconds()) {
		t.Fatalf("WARN sem os campos esperados: %v", w)
	}
}

// Uma fronteira pulada só, nas duas bordas: fim exatamente em next + every, e
// fim dentro de [next+every, next+2·every). Os dois saem já, com a fronteira
// mais recente, e contam UMA pulada cada.
func TestLoopSkipsExactlyOneBoundaryAtBothEdges(t *testing.T) {
	h := startLoop(t, at(time.Second))
	// +5m dura 10m: fim em +15m == next(+10m) + every. Depois, +15m dura 12m:
	// fim em +27m, dentro de [+25m, +30m), e next é +20m.
	h.passesTake(10*time.Minute, 12*time.Minute)
	h.fireAt(h.expectWait(t), at(loopEvery))
	h.expectPass(t)
	if got := h.expectPass(t); !got.Equal(at(3 * loopEvery)) {
		t.Fatalf("fim == next + every: at = %v, quero %v (uma pulada, +10m)", got, at(3*loopEvery))
	}
	if got := h.expectPass(t); !got.Equal(at(5 * loopEvery)) {
		t.Fatalf("fim dentro de [next+every, next+2·every): at = %v, quero %v (uma pulada, +20m)", got, at(5*loopEvery))
	}
	h.expectWait(t)
	h.e.Stop()
	if s := h.e.Stats(); s.SkippedBoundaries != 2 {
		t.Fatalf("puladas = %d, quero 2 (uma por ocorrência)", s.SkippedBoundaries)
	}
	warns := h.log.events(t, "series.boundaries_skipped")
	if len(warns) != 2 || warns[0]["skipped"] != float64(1) || warns[1]["skipped"] != float64(1) {
		t.Fatalf("WARN esperados: dois, com skipped=1 cada; veio %v", warns)
	}
}

// As bordas do atraso que NÃO pula: fim um nanossegundo antes da fronteira
// seguinte, fim exatamente nela (passada de duração igual a every) e fim entre
// ela e a outra.
func TestLoopLatePassThatSkipsNothingCountsNothing(t *testing.T) {
	h := startLoop(t, at(time.Second))
	// Fim um nanossegundo antes da fronteira seguinte; depois, uma passada de
	// duração exatamente every (fim == próxima fronteira); depois, fim entre
	// a próxima e a seguinte.
	h.passesTake(loopEvery-time.Nanosecond, loopEvery, 7*time.Minute)
	h.fireAt(h.expectWait(t), at(loopEvery))
	h.expectPass(t)

	w := h.expectWait(t)
	if w.wait != time.Nanosecond {
		t.Fatalf("espera = %v, quero 1ns até a fronteira", w.wait)
	}
	h.fireAt(w, at(2*loopEvery))
	if got := h.expectPass(t); !got.Equal(at(2 * loopEvery)) {
		t.Fatalf("at = %v", got)
	}
	if got := h.expectPass(t); !got.Equal(at(3 * loopEvery)) {
		t.Fatalf("fim == fronteira: a passada sai já com at = %v, quero %v", got, at(3*loopEvery))
	}
	if got := h.expectPass(t); !got.Equal(at(4 * loopEvery)) {
		t.Fatalf("fim entre fronteiras: a passada sai já com at = %v, quero %v", got, at(4*loopEvery))
	}
	h.e.Stop()
	if s := h.e.Stats(); s.SkippedBoundaries != 0 {
		t.Fatalf("puladas = %d, e nenhuma fronteira foi pulada", s.SkippedBoundaries)
	}
	if warns := h.log.events(t, "series.boundaries_skipped"); len(warns) != 0 {
		t.Fatalf("WARN sem fronteira pulada: %v", warns)
	}
}

// O sono que passa da conta (VM pausada, NTP para frente) reaplica a política
// ao acordar: sai a fronteira mais recente, nunca a que se esperava.
func TestLoopReappliesThePolicyWhenTheSleepOverruns(t *testing.T) {
	h := startLoop(t, at(time.Second))
	h.fireAt(h.expectWait(t), at(loopEvery))
	h.expectPass(t)
	h.fireAt(h.expectWait(t), at(4*loopEvery+time.Second))
	if got := h.expectPass(t); !got.Equal(at(4 * loopEvery)) {
		t.Fatalf("at = %v, quero a fronteira mais recente %v", got, at(4*loopEvery))
	}
	h.e.Stop()
	if s := h.e.Stats(); s.SkippedBoundaries != 2 {
		t.Fatalf("puladas = %d, quero 2", s.SkippedBoundaries)
	}
	if warns := h.log.events(t, "series.boundaries_skipped"); len(warns) != 1 {
		t.Fatalf("%d WARN, quero 1", len(warns))
	}
}

// Relógio voltando ABAIXO do último at (end < last): a espera vai até
// last + every pelo relógio de parede, e passa de every.
func TestLoopAtIsStrictlyIncreasingWhenTheClockGoesBack(t *testing.T) {
	h := startLoop(t, at(time.Second))
	h.output.then(func() { h.clock.set(at(-3 * time.Minute)) })
	h.fireAt(h.expectWait(t), at(loopEvery))
	first := h.expectPass(t)
	w := h.expectWait(t)
	if want := at(2 * loopEvery).Sub(at(-3 * time.Minute)); w.wait != want {
		t.Fatalf("espera com o relógio voltado = %v, quero %v (até last + every)", w.wait, want)
	}
	// Acorda ainda antes dela: volta a dormir.
	h.fireAt(w, at(-2*time.Minute))
	h.fireAt(h.expectWait(t), at(2*loopEvery))
	second := h.expectPass(t)
	if !second.After(first) || second.Sub(first) != loopEvery {
		t.Fatalf("at %v depois de %v: tem de ser estritamente crescente", second, first)
	}
}

// Temporizador que acorda adiantado: a passada nunca começa antes do at.
func TestLoopNeverStartsAPassBeforeItsBoundary(t *testing.T) {
	h := startLoop(t, at(time.Second))
	h.fireAt(h.expectWait(t), at(loopEvery-time.Millisecond))
	w := h.expectWait(t)
	if w.wait != time.Millisecond {
		t.Fatalf("despertar adiantado: nova espera = %v, quero 1ms", w.wait)
	}
	h.fireAt(w, at(loopEvery))
	if got := h.expectPass(t); !got.Equal(at(loopEvery)) {
		t.Fatalf("at = %v", got)
	}
}

func TestStopDuringTheBoundaryWaitReturnsWithoutTheTimer(t *testing.T) {
	h := startLoop(t, at(time.Second))
	w := h.expectWait(t) // nunca disparada no caminho verde
	stopped := make(chan struct{})
	go func() { h.e.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		// Dispara a espera para destravar o laço: sem isto, o Close da
		// limpeza do teste esperaria o mesmo temporizador para sempre.
		h.fireAt(w, at(time.Second))
		<-stopped
		t.Fatal("o Stop esperou o temporizador da fronteira")
	}
	if h.e.Active() {
		t.Fatal("o laço continua de pé depois do Stop")
	}
}

// blockingOutput aceita a primeira amostra de cada passada e, na segunda,
// espera vaga para sempre — até o ctx ser cancelado, como o seriesDelivery.
type blockingOutput struct {
	calls   atomic.Int64
	waiting chan struct{}
}

func (o *blockingOutput) Publish(ctx context.Context, _, _ []byte, _ time.Time) error {
	if o.calls.Add(1) == 1 {
		return nil
	}
	close(o.waiting)
	<-ctx.Done()
	return ctx.Err()
}

// Stop com o Publish em espera: a passada aborta, e o abortamento não é falha
// — nada de `series.stopped`, de erro contado, de sucesso nem de duração. A
// linha já no buffer do arquivo sai no Close: a passada parcial daquele at.
func TestStopWithAPublishWaitingAbortsThePassSilently(t *testing.T) {
	clock := &loopClock{events: make(chan loopEvent, 64)}
	clock.set(at(0))
	log := &syncBuffer{}
	out := &blockingOutput{waiting: make(chan struct{})}
	dir := t.TempDir()
	e, err := NewEmitter(Options{
		Dataset: "services", Counter: "by_olt", Columns: []string{"olt", "slot"},
		Reader: newFakeCounter(t,
			group{olt: "OLT_A", slot: 1, total: 10, live: true},
			group{olt: "OLT_B", slot: 1, total: 20, live: true}),
		Outputs: []Output{out}, Every: loopEvery, Dir: dir, File: "s.ndjson", MaxBytes: 1 << 20, Keep: 1,
		Now: clock.Now, After: clock.After,
		Log: logging.New(log, logging.FormatJSON, slog.LevelInfo),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.Start(context.Background())
	select {
	case <-out.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("a passada não chegou ao segundo grupo")
	}
	e.Stop()
	if got := log.events(t, "series.stopped"); len(got) != 0 {
		t.Fatalf("a passada abortada pelo Stop logou series.stopped: %v", got)
	}
	s := e.Stats()
	if s.Errors != 0 || s.LastSuccessUnixMs != 0 || s.LastPassDuration != 0 {
		t.Fatalf("a passada abortada contou como falha ou sucesso: %+v", s)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "s.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	lines := splitLines(string(b))
	if len(lines) != 1 || !strings.Contains(lines[0], `"groupKey":"OLT_A|1"`) || !strings.Contains(lines[0], at(0).Format(timeLayout)) {
		t.Fatalf("o arquivo devia terminar com a passada parcial (só o primeiro grupo): %q", lines)
	}
}

// LastPassDuration é o intervalo do relógio da passada, e só da concluída.
func TestLastPassDurationIsTheClockSpanOfTheLastCompletedPass(t *testing.T) {
	clock := &loopClock{events: make(chan loopEvent, 64)}
	clock.set(t0)
	out := &loopOutput{clock: clock}
	out.then(func() { clock.advance(1234 * time.Millisecond) })
	e, err := NewEmitter(Options{
		Dataset: "services", Counter: "by_olt", Columns: []string{"olt", "slot"},
		Reader:      newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 10, live: true}),
		DisableFile: true, Outputs: []Output{out}, Every: loopEvery, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if got := e.Stats().LastPassDuration; got != 1234*time.Millisecond {
		t.Fatalf("duração da última passada = %v, quero 1.234s", got)
	}
}
