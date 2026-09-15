package countermetrics

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Um Start no meio de um Stop — depois do cancelamento, antes de o laço
// anterior sair — não pode subir um segundo laço: ele ficaria sem
// cancelamento, o Wait do Stop esperaria por ele para sempre, e as duas
// goroutines fariam SampleOnce ao mesmo tempo sobre um arquivo que não é
// thread-safe. O seam stopWaitHook põe o Start exatamente nessa brecha, então
// o veredito não depende de escalonamento.
func TestStartDuringStopDoesNotSpawnASecondLoop(t *testing.T) {
	alive, peak := traceLoops(t)
	t.Cleanup(func() { stopWaitHook = nil })
	h := newHarness(t)
	h.counter.add("OLT_A", 1)
	s := h.open(t)
	ctx := context.Background()
	s.Start(ctx)

	startedDuringStop := make(chan bool, 1)
	blockedStartDone := make(chan struct{})
	stopWaitHook = func() {
		stopWaitHook = nil
		go func() {
			s.Start(ctx)
			close(blockedStartDone)
		}()
		select {
		case <-blockedStartDone:
			startedDuringStop <- true
		case <-time.After(200 * time.Millisecond):
			startedDuringStop <- false
		}
	}
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()

	if <-startedDuringStop {
		t.Fatal("Start subiu um laço enquanto o Stop aguardava o laço anterior sair")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop não terminou: um laço subiu no meio dele e ficou sem cancelamento")
	}
	select {
	case <-blockedStartDone:
	case <-time.After(5 * time.Second):
		t.Fatal("o Start que esperava o Stop não terminou depois dele")
	}
	closeSampler(t, s)
	if p := peak.Load(); p != 1 {
		t.Errorf("máximo de laços vivos ao mesmo tempo = %d, esperado 1", p)
	}
	if n := alive.Load(); n != 0 {
		t.Errorf("%d laço(s) vivo(s) depois do Close", n)
	}
}

// Estresse: goroutines alternando Start e Stop. Nunca há dois laços vivos, e
// tudo termina no prazo. Sob -race, dois laços ao mesmo tempo também
// apareceriam como corrida no estado do escritor.
func TestConcurrentStartAndStopNeverRunTwoLoops(t *testing.T) {
	alive, peak := traceLoops(t)
	h := newHarness(t)
	for _, key := range []string{"OLT_A", "OLT_B", "OLT_C"} {
		h.counter.add(key, 1)
	}
	s := h.open(t)
	ctx := context.Background()

	const (
		workers    = 8
		iterations = 100
	)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				s.Start(ctx)
				s.Stop()
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("Start/Stop concorrentes não terminaram no prazo")
	}
	closeSampler(t, s)

	t.Logf("máximo de laços vivos ao mesmo tempo: %d", peak.Load())
	if p := peak.Load(); p != 1 {
		t.Errorf("máximo de laços vivos ao mesmo tempo = %d, esperado 1", p)
	}
	if n := alive.Load(); n != 0 {
		t.Errorf("%d laço(s) vivo(s) depois do Close", n)
	}
}
