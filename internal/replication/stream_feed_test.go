package replication

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

// newFeedForTest abre um feed com intervalo longo, para que só o Flush e o
// Close explícitos do teste fechem lotes.
func newFeedForTest(t *testing.T) *Stream {
	t.Helper()
	s, err := New(Options{Interval: time.Minute, MaxMessages: 100, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// O Close faz o flush do intervalo ativo para que ele seja ENTREGUE: recusar
// o Acquire antes de olhar os lotes retidos perdia justamente a cauda que o
// flush do Close acabou de produzir.
func TestAcquireServesRetainedBatchesAfterClose(t *testing.T) {
	s := newFeedForTest(t)
	now := time.Unix(10, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	s.Flush(now, "test")
	s.Publish("events", Insert, rec(2, "b"), now)
	epoch, _ := s.Cursor()
	s.Close()

	b, release, err := s.Acquire(context.Background(), epoch, 0)
	if err != nil {
		t.Fatalf("primeiro lote retido depois do Close: err=%v", err)
	}
	if b.From != 1 || b.To != 1 {
		t.Fatalf("primeiro lote From=%d To=%d, quer 1..1", b.From, b.To)
	}
	release()
	b, release, err = s.Acquire(context.Background(), epoch, 1)
	if err != nil {
		t.Fatalf("lote do flush do Close: err=%v", err)
	}
	if b.Reason != "close" || b.To != 2 || len(b.Entries) != 1 || string(b.Entries[0].Record.Payload) != "b" {
		t.Fatalf("lote do Close inesperado: reason=%q to=%d entries=%#v", b.Reason, b.To, b.Entries)
	}
	release()
	// A regra do buraco continua valendo depois do Close.
	if _, _, err := s.Acquire(context.Background(), epoch, 3); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("cursor além do produzido depois do Close: err=%v, quer resync", err)
	}
}

// Esgotada a cauda, o Acquire tem de devolver ErrClosed NA HORA. O canal que
// o Close deixa em s.changed nunca é fechado; esperar nele só termina pelo
// contexto — e aí o erro seria o do contexto, não ErrClosed.
func TestAcquireReturnsClosedWithoutWaitingAfterDrain(t *testing.T) {
	s := newFeedForTest(t)
	now := time.Unix(10, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	epoch, _ := s.Cursor()
	s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, _, err := s.Acquire(ctx, epoch, 1)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("cauda esgotada depois do Close: err=%v, quer ErrClosed", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("Acquire esperou %v antes de devolver ErrClosed", waited)
	}
}

// acquireResult é o que a goroutine leitora devolve ao teste.
type acquireResult struct {
	batch *Batch
	err   error
}

// startBlockedAcquire dispara um Acquire numa goroutine e só volta quando ela
// está de fato estacionada no select do Acquire. Sem essa espera, o Close
// poderia acontecer antes de o leitor chegar ao select, e o teste passaria
// pelo caminho "já fechado" sem exercitar o despertar.
func startBlockedAcquire(t *testing.T, s *Stream, epoch string, after uint64) <-chan acquireResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	out := make(chan acquireResult, 1)
	// Conta a partir do que já estava estacionado: um leitor largado por
	// outro teste não pode passar pelo deste.
	baseline := parkedAcquires()
	go func() {
		b, release, err := s.Acquire(ctx, epoch, after)
		if err == nil {
			release()
		}
		out <- acquireResult{b, err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for parkedAcquires() <= baseline {
		if time.Now().After(deadline) {
			t.Fatal("o leitor não estacionou no Acquire")
		}
		time.Sleep(time.Millisecond)
	}
	return out
}

// parkedAcquires conta, nas pilhas de todas as goroutines, as estacionadas
// num select dentro de (*Stream).Acquire.
func parkedAcquires() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "[select") && strings.Contains(g, "(*Stream).Acquire") {
			n++
		}
	}
	return n
}

// awaitAcquire espera o resultado do leitor por no máximo um segundo.
func awaitAcquire(t *testing.T, out <-chan acquireResult) acquireResult {
	t.Helper()
	select {
	case r := <-out:
		return r
	case <-time.After(time.Second):
		t.Fatal("o Close não acordou o leitor estacionado em 1s")
		return acquireResult{}
	}
}

// O leitor que já esperava com a cauda esgotada tem de ser acordado pelo
// Close e receber ErrClosed, e não ficar preso até o contexto dele vencer.
func TestCloseWakesParkedReaderWithClosed(t *testing.T) {
	s := newFeedForTest(t)
	now := time.Unix(10, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	s.Flush(now, "test")
	epoch, seq := s.Cursor()
	out := startBlockedAcquire(t, s, epoch, seq)

	s.Close()
	if r := awaitAcquire(t, out); !errors.Is(r.err, ErrClosed) {
		t.Fatalf("leitor estacionado depois do Close: err=%v, quer ErrClosed", r.err)
	}
}

// Com uma entrada pendente no intervalo ativo, o leitor estacionado recebe o
// lote que o próprio Close fechou.
func TestCloseDeliversPendingIntervalToParkedReader(t *testing.T) {
	s := newFeedForTest(t)
	epoch, _ := s.Cursor()
	out := startBlockedAcquire(t, s, epoch, 0)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(10, 0))

	s.Close()
	r := awaitAcquire(t, out)
	if r.err != nil {
		t.Fatalf("leitor estacionado com intervalo pendente: err=%v", r.err)
	}
	if r.batch.Reason != "close" || r.batch.To != 1 || len(r.batch.Entries) != 1 {
		t.Fatalf("lote entregue: reason=%q to=%d entries=%d, quer close 1 1", r.batch.Reason, r.batch.To, len(r.batch.Entries))
	}
}

// As duas portas do invalidateLocked contam: o Invalidate explícito e o
// estouro do teto de memória no Publish.
func TestInvalidationIsCounted(t *testing.T) {
	s, err := New(Options{Interval: time.Minute, MaxMessages: 100, MaxBatchBytes: 200, MaxBufferedBytes: 300})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Status().Invalidations; got != 0 {
		t.Fatalf("feed novo com %d invalidações", got)
	}
	s.Invalidate()
	if got := s.Status().Invalidations; got != 1 {
		t.Fatalf("depois do Invalidate: %d invalidações, quer 1", got)
	}
	// Um record maior que o teto inteiro não cabe nem com o feed vazio.
	s.Publish("events", Insert, rec(1, string(make([]byte, 400))), time.Unix(1, 0))
	if got := s.Status().Invalidations; got != 2 {
		t.Fatalf("depois do estouro no Publish: %d invalidações, quer 2", got)
	}
}

// Cada entrada carrega o instante da sua publicação; na coalescência vale o
// da ÚLTIMA, que é quando o estado publicado passou a valer.
func TestPublishStampsEntryInstantAndCoalescingKeepsLast(t *testing.T) {
	s := newFeedForTest(t)
	t1, t2, t3 := time.Unix(10, 0), time.Unix(20, 0), time.Unix(30, 0)
	s.Publish("events", Insert, rec(1, "a"), t1)
	s.Publish("events", Update, rec(2, "x"), t1)
	s.Publish("events", Update, rec(2, "y"), t2)
	s.Publish("events", Update, rec(2, "z"), t3)
	s.Flush(t3, "test")
	epoch, _ := s.Cursor()
	b, release, err := s.Acquire(context.Background(), epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(b.Entries) != 2 {
		t.Fatalf("entries=%d, quer 2", len(b.Entries))
	}
	if !b.Entries[0].At.Equal(t1) {
		t.Fatalf("instante do insert=%v, quer %v", b.Entries[0].At, t1)
	}
	if got := b.Entries[1]; string(got.Record.Payload) != "z" || !got.At.Equal(t3) {
		t.Fatalf("update coalescido: payload=%q at=%v, quer z em %v", got.Record.Payload, got.At, t3)
	}
}
