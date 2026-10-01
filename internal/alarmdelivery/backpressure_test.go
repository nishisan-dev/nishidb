package alarmdelivery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

// burstEvents é o tamanho da rajada da garantia 1: quatro vezes o buffer de
// memória real, o bastante para que o descarte por memória cheia apareça.
// A rajada grande (dez mil) fica com o gate de medida.
const burstEvents = 4 * maxMemoryEvents

// gatedWriter segura no passo real de gravação, a partir da gravação de
// número `from`, até open; as anteriores passam direto. Com failWith, a
// gravação segurada falha ao ser solta.
type gatedWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func gateWrites(t *testing.T, q *queue, from int64, failWith error) *gatedWriter {
	t.Helper()
	g := &gatedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var writes atomic.Int64
	var enteredOnce sync.Once
	q.write = func(path string, body []byte, perm os.FileMode) error {
		if writes.Add(1) >= from {
			enteredOnce.Do(func() { close(g.entered) })
			<-g.release
			if failWith != nil {
				return failWith
			}
		}
		return atomicfile.Write(path, body, perm)
	}
	// Registrado depois do close de testQueue, roda antes dele: um teste que
	// falha no meio não deixa o close esperando um gravador preso.
	t.Cleanup(g.open)
	return g
}

func (g *gatedWriter) open() { g.once.Do(func() { close(g.release) }) }

func (g *gatedWriter) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("o gravador não chegou ao passo de gravação dentro do prazo")
	}
}

// offerAsync roda a admissão fora do goroutine do teste, para que uma espera
// indevida vire falha com prazo, e não o timeout do go test.
func offerAsync(q *queue, body []byte) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		q.offer(body, time.Now())
		close(done)
	}()
	return done
}

func mustReturnWithin(t *testing.T, done <-chan struct{}, limit time.Duration, failure string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("%s (prazo de %s)", failure, limit)
	}
}

// mustStillWait é a metade positiva dos testes de despertar: prova que a
// admissão de fato esperou, senão o despertar testado depois seria vácuo.
func mustStillWait(t *testing.T, done <-chan struct{}, d time.Duration, failure string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(failure)
	case <-time.After(d):
	}
}

// offerAllWithin admite os corpos em ordem fora do goroutine do teste, com
// prazo: um despertar perdido vira falha com mensagem, e não um pacote travado.
// O close do Cleanup de testQueue acorda a admissão que tiver ficado presa.
func offerAllWithin(t *testing.T, q *queue, bodies [][]byte, limit time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, body := range bodies {
			q.offer(body, time.Now())
		}
	}()
	mustReturnWithin(t, done, limit, fmt.Sprintf("a rajada de %d admissões não terminou: despertar perdido na espera por vaga", len(bodies)))
}

func onlyDrop(t *testing.T, s Stats, reason string, want uint64) {
	t.Helper()
	for r, got := range s.Dropped {
		expected := uint64(0)
		if r == reason {
			expected = want
		}
		if got != expected {
			t.Fatalf("descartes por %q = %d, esperado %d: %+v", r, got, expected, s.Dropped)
		}
	}
}

// Garantia 1: a rajada maior que o buffer espera o gravador, não descarta.
func TestBurstLargerThanMemoryIsPersistedWithoutDrops(t *testing.T) {
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
	if q.memoryLimit != maxMemoryEvents || q.memoryByteLimit != maxMemoryBytes {
		t.Fatalf("a rajada precisa dos limites reais de memória: %d/%d", q.memoryLimit, q.memoryByteLimit)
	}
	bodies := make([][]byte, burstEvents)
	for i := range bodies {
		bodies[i] = eventBody(t, fmt.Sprintf("burst-%05d", i))
	}
	offerAllWithin(t, q, bodies, burstEvents*50*time.Millisecond)
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
	s := q.snapshot()
	onlyDrop(t, s, "", 0)
	if s.DiskEvents != burstEvents || s.MemoryEvents != 0 {
		t.Fatalf("a rajada não chegou inteira ao disco: %d em disco, %d em memória", s.DiskEvents, s.MemoryEvents)
	}
	if s.AdmissionWaitSeconds <= 0 {
		t.Fatal("a rajada não esperou vaga: o teste não exercitou a contrapressão")
	}
	recovered := testQueue(t, q.dir, nil)
	if len(recovered.pending) != burstEvents {
		t.Fatalf("a recuperação reencontrou %d de %d eventos", len(recovered.pending), burstEvents)
	}
	for i, e := range recovered.pending {
		if e.seq != uint64(i+1) {
			t.Fatalf("posição %d com sequência %d: a recuperação perdeu a ordem", i, e.seq)
		}
		_, identity, err := readRecord(recovered.path(e.seq))
		if want := fmt.Sprintf("burst-%05d", i); err != nil || identity.ID != want {
			t.Fatalf("posição %d: evento %q (%v), esperado %q", i, identity.ID, err, want)
		}
	}
}

// Os dois tetos de memória (eventos e bytes) fazem esperar o gravador, e o
// tempo esperado aparece em AdmissionWaitSeconds.
func TestMemoryLimitsMakeAdmissionWaitForTheWriter(t *testing.T) {
	for _, kind := range []string{"memory_count", "memory_bytes"} {
		t.Run(kind, func(t *testing.T) {
			q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
			body := eventBody(t, "same")
			if kind == "memory_count" {
				q.memoryLimit = 2
			} else {
				q.memoryByteLimit = int64(2 * len(body))
			}
			gate := gateWrites(t, q, 1, nil)
			q.offer(body, time.Now())
			gate.waitEntered(t)
			q.offer(body, time.Now())
			waiter := offerAsync(q, body)
			mustStillWait(t, waiter, 50*time.Millisecond, "admissão com a memória cheia não esperou o gravador")
			if q.snapshot().AdmissionWaitSeconds != 0 {
				t.Fatal("espera ainda em curso já foi contabilizada")
			}
			gate.open()
			mustReturnWithin(t, waiter, 5*time.Second, "a vaga liberada pelo gravador não acordou a admissão")
			if err := q.close(); err != nil {
				t.Fatal(err)
			}
			s := q.snapshot()
			onlyDrop(t, s, "", 0)
			if s.DiskEvents != 3 || s.AdmissionWaitSeconds <= 0 {
				t.Fatalf("esperado 3 em disco e espera contabilizada: %+v", s)
			}
			if recovered := testQueue(t, q.dir, nil); recovered.snapshot().DiskEvents != 3 {
				t.Fatal("reinício perdeu eventos admitidos depois da espera")
			}
		})
	}
}

// A espera conta do primeiro instante sem vaga até a admissão, atravessando
// despertares em que a vaga não veio. Os despertares aqui são forçados pelo
// teste (o Cond admite despertar sem vaga, e o concorrente que leva a vaga
// produz o mesmo efeito); só a liberação final do gravador admite o evento.
func TestAdmissionWaitIsCountedAcrossWakeupsWithoutSpace(t *testing.T) {
	const step = 100 * time.Millisecond
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
	q.memoryLimit = 1
	gate := gateWrites(t, q, 1, nil)
	body := eventBody(t, "wakeups")
	q.offer(body, time.Now())
	gate.waitEntered(t)
	waiter := offerAsync(q, body)
	for range 2 {
		mustStillWait(t, waiter, step, "admissão com a memória cheia não esperou o gravador")
		q.mu.Lock()
		q.space.Broadcast()
		q.mu.Unlock()
	}
	mustStillWait(t, waiter, step, "um despertar sem vaga admitiu o evento")
	gate.open()
	mustReturnWithin(t, waiter, 5*time.Second, "a vaga liberada pelo gravador não acordou a admissão")
	// Três passos de espera; a folga cobre só o escalonamento da largada.
	if got, floor := q.snapshot().AdmissionWaitSeconds, (3*step - step/2).Seconds(); got < floor {
		t.Fatalf("espera contabilizada %.3fs, esperado pelo menos %.3fs: a contagem recomeçou num despertar sem vaga", got, floor)
	}
	onlyDrop(t, q.snapshot(), "", 0)
}

// Garantia 2: o close acorda quem espera, sem depender do gravador — que fica
// preso no disco o tempo todo e só é solto depois da asserção.
func TestCloseWakesAdmissionWaitingForMemory(t *testing.T) {
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
	q.memoryLimit = 2
	gate := gateWrites(t, q, 1, nil)
	body := eventBody(t, "close")
	q.offer(body, time.Now())
	gate.waitEntered(t)
	q.offer(body, time.Now())
	waiter := offerAsync(q, body)
	mustStillWait(t, waiter, 50*time.Millisecond, "admissão com a memória cheia não esperou o gravador")
	closed := make(chan error, 1)
	go func() { closed <- q.close() }()
	mustReturnWithin(t, waiter, 2*time.Second, "close não acordou a admissão que esperava vaga em memória")
	select {
	case <-closed:
		t.Fatal("close terminou com o gravador preso: o despertar não foi provado independente do gravador")
	default:
	}
	onlyDrop(t, q.snapshot(), DropClosed, 1)
	if q.snapshot().AdmissionWaitSeconds <= 0 {
		t.Fatal("a espera que terminou em descarte por closed não foi contabilizada")
	}
	gate.open()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close não terminou depois de soltar o gravador")
	}
	if recovered := testQueue(t, q.dir, nil); recovered.snapshot().DiskEvents != 2 {
		t.Fatal("close perdeu os eventos admitidos antes dele")
	}
}

// Garantia 3: a falha da fila acorda quem espera, venha ela do gravador ou do
// remetente. Na falha do remetente o gravador continua preso, então só o aviso
// da própria falha pode acordar a admissão.
func TestQueueFailureWakesAdmissionWaitingForMemory(t *testing.T) {
	t.Run("writer", func(t *testing.T) {
		q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
		q.memoryLimit = 2
		gate := gateWrites(t, q, 1, errors.New("disk failure"))
		body := eventBody(t, "writer")
		q.offer(body, time.Now())
		gate.waitEntered(t)
		q.offer(body, time.Now())
		waiter := offerAsync(q, body)
		mustStillWait(t, waiter, 50*time.Millisecond, "admissão com a memória cheia não esperou o gravador")
		gate.open()
		mustReturnWithin(t, waiter, 2*time.Second, "a falha do gravador não acordou a admissão")
		// A espera e o evento que estava no channel; o que falhou na gravação
		// pode ter sido publicado e não é perda certa (decide a recuperação).
		eventually(t, func() bool { return q.snapshot().MemoryEvents == 0 })
		onlyDrop(t, q.snapshot(), DropFailed, 2)
		if err := q.close(); !errors.Is(err, ErrQueue) {
			t.Fatalf("falha do gravador não chegou ao close: %v", err)
		}
	})
	t.Run("sender", func(t *testing.T) {
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
		defer receiver.Close()
		q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
		q.memoryLimit = 2
		q.remove = func(string) error { return errors.New("remove failure") }
		gate := gateWrites(t, q, 2, nil)
		body := eventBody(t, "sender")
		q.offer(body, time.Now())
		eventually(t, func() bool { return q.snapshot().DiskEvents == 1 })
		q.offer(body, time.Now())
		gate.waitEntered(t)
		q.offer(body, time.Now())
		waiter := offerAsync(q, body)
		mustStillWait(t, waiter, 50*time.Millisecond, "admissão com a memória cheia não esperou o gravador")
		q.start(context.Background())
		mustReturnWithin(t, waiter, 2*time.Second, "a falha do remetente não acordou a admissão")
		select {
		case <-gate.release:
			t.Fatal("o gravador foi solto antes da asserção")
		default:
		}
		if s := q.snapshot(); s.State != "failed" || s.Dropped[DropFailed] != 1 || s.MemoryEvents != 2 {
			t.Fatalf("esperado destino falho, um descarte e o gravador ainda preso: %+v", s)
		}
	})
}

// Garantia 4: sem vaga em DISCO a admissão descarta na hora, com a memória
// livre ou cheia — a contrapressão é só de memória.
func TestDiskLimitsDropWithoutWaiting(t *testing.T) {
	for _, kind := range []string{"disk_count", "disk_bytes"} {
		for _, memory := range []string{"memory_free", "memory_full"} {
			t.Run(kind+"/"+memory, func(t *testing.T) {
				q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
				body := eventBody(t, "disk")
				if kind == "disk_count" {
					q.diskLimit = 2
				} else {
					q.maxBytes = int64(2 * (len(body) + recordHeaderBytes))
				}
				if memory == "memory_full" {
					// Os dois admitidos ocupam a memória inteira e o disco
					// inteiro: o gravador preso não vai liberar vaga nenhuma.
					q.memoryLimit = 2
					gate := gateWrites(t, q, 1, nil)
					q.offer(body, time.Now())
					gate.waitEntered(t)
					q.offer(body, time.Now())
				} else {
					q.offer(body, time.Now())
					q.offer(body, time.Now())
					eventually(t, func() bool { return q.snapshot().DiskEvents == 2 })
				}
				done := offerAsync(q, body)
				mustReturnWithin(t, done, 500*time.Millisecond, "admissão sem vaga em disco esperou em vez de descartar")
				onlyDrop(t, q.snapshot(), DropDiskFull, 1)
				if q.snapshot().AdmissionWaitSeconds != 0 {
					t.Fatal("descarte por disco contabilizou espera")
				}
			})
		}
	}
}

// Garantia 5: a vaga em memória volta com a publicação do arquivo, nunca com
// a confirmação HTTP. Um receptor que nunca responde não segura a rajada.
func TestBurstNeverWaitsForHTTP(t *testing.T) {
	const events = 4 * maxMemoryEvents
	var posts atomic.Int64
	release := make(chan struct{})
	receiver := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		<-release
	}))
	defer receiver.Close()
	defer close(release)
	target := fastEndpoint(receiver.URL)
	target.client.Timeout = time.Hour
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), target)
	q.start(context.Background())
	q.offer(eventBody(t, "stuck"), time.Now())
	eventually(t, func() bool { return posts.Load() == 1 })
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i < events; i++ {
			q.offer(eventBody(t, fmt.Sprintf("burst-%d", i)), time.Now())
		}
	}()
	mustReturnWithin(t, done, 60*time.Second, "a rajada esperou o receptor HTTP")
	eventually(t, func() bool { return q.snapshot().DiskEvents == events })
	s := q.snapshot()
	onlyDrop(t, s, "", 0)
	if s.Delivered != 0 || posts.Load() != 1 {
		t.Fatalf("o receptor preso confirmou algo: %+v, POSTs=%d", s, posts.Load())
	}
}
