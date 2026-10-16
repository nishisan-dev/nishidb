package alarmdelivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

func eventBody(t *testing.T, id string) []byte {
	t.Helper()
	body, err := json.Marshal(alarm.Event{ID: id, Dataset: "d", Counter: "c", Alarm: "a", State: alarm.StateFiring, GroupKey: "olt", At: time.Now().UTC(), Since: time.Now().Add(-time.Minute), SustainedFor: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("a condição não se confirmou dentro do prazo")
}

// testQueue abre a fila com o backoff encurtado; target nil é a fila pausada.
func testQueue(t *testing.T, dir string, target transport) *queue {
	t.Helper()
	q, err := openQueue(dir, "noc", 256<<20, target, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	q.initialDelay = time.Millisecond
	q.maxDelay = 5 * time.Millisecond
	t.Cleanup(func() { _ = q.close() })
	return q
}

// fastEndpoint é o webhook dos testes. O nome ficou de quando o backoff curto
// morava no endpoint; hoje ele vem de testQueue.
func fastEndpoint(url string) *httpTransport {
	return newHTTPTransport(url, "", time.Second)
}

// O bloqueio é no passo real de gravação: nenhum POST pode ultrapassá-lo.
//
// "Nunca espera o disco" vale aqui COM vaga em memória, que é o caso deste
// teste (um evento num buffer de 256). Com o buffer cheio a admissão espera o
// gravador, e isso é provado em backpressure_test.go. O nome fica porque um
// registro de aceite de 19/09 o cita.
func TestOfferNeverWaitsForDiskAndHTTPWaitsForDurablePublication(t *testing.T) {
	var calls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(204) }))
	defer receiver.Close()
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	q.write = func(path string, body []byte, perm os.FileMode) error {
		close(entered)
		<-release
		return atomicfile.Write(path, body, perm)
	}
	q.start(context.Background())
	offered := make(chan struct{})
	go func() { q.offer(eventBody(t, "first"), time.Now()); close(offered) }()
	select {
	case <-offered:
	case <-time.After(time.Second):
		t.Fatal("admissão esperou o disco")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("gravação não foi chamada")
	}
	if calls.Load() != 0 {
		t.Fatal("HTTP ultrapassou a persistência")
	}
	if q.snapshot().MemoryEvents != 1 || q.snapshot().DiskEvents != 0 {
		t.Fatal("evento anunciado durável antes de persistir")
	}
	unblock()
	eventually(t, func() bool { return q.snapshot().Delivered == 1 })
	if calls.Load() != 1 {
		t.Fatalf("POSTs = %d", calls.Load())
	}
}

// Só o teto de DISCO descarta: o de memória faz esperar, e tem teste próprio.
func TestQueueLimitsRejectOnlyNewEvents(t *testing.T) {
	for _, kind := range []string{"disk_count", "disk_bytes"} {
		t.Run(kind, func(t *testing.T) {
			q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
			body := eventBody(t, "same")
			want := DropDiskFull
			switch kind {
			case "disk_count":
				q.diskLimit = 2
			case "disk_bytes":
				q.maxBytes = int64(2 * (len(body) + recordHeaderBytes))
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var writes atomic.Int64
			q.write = func(path string, body []byte, perm os.FileMode) error {
				if writes.Add(1) == 1 {
					close(entered)
					<-release
				}
				return atomicfile.Write(path, body, perm)
			}
			q.offer(body, time.Now())
			<-entered
			q.offer(body, time.Now())
			q.offer(body, time.Now())
			s := q.snapshot()
			if s.MemoryEvents != 2 || s.Dropped[want] != 1 {
				t.Fatalf("limite não preservou os antigos: %+v", s)
			}
			once.Do(func() { close(release) })
			if err := q.close(); err != nil {
				t.Fatal(err)
			}
			recovered := testQueue(t, q.dir, nil)
			if recovered.snapshot().DiskEvents != 2 {
				t.Fatal("reinício perdeu eventos aceitos")
			}
		})
	}
}

func TestOversizedEventIsCountedWithoutCreatingFile(t *testing.T) {
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
	q.offer(make([]byte, maxEventBytes+1), time.Now())
	if q.snapshot().Dropped[DropTooLarge] != 1 {
		t.Fatal("evento grande não foi contado")
	}
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(q.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatal("evento grande chegou ao disco")
	}
}

func TestRecoveryAtEveryPublicationStep(t *testing.T) {
	for _, step := range []string{atomicfile.StepWrite, atomicfile.StepFsync, atomicfile.StepRename, atomicfile.StepDirsync} {
		t.Run(step, func(t *testing.T) {
			q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
			q.write = func(path string, body []byte, perm os.FileMode) error {
				return atomicfile.WriteWithHook(path, body, perm, func(actual string) error {
					if actual == step {
						return errors.New("crash")
					}
					return nil
				})
			}
			body := eventBody(t, "stable")
			q.offer(body, time.Now())
			eventually(t, func() bool { return q.snapshot().State == "failed" })
			if err := q.close(); !errors.Is(err, ErrQueue) {
				t.Fatalf("falha de disco não chegou ao Close: %v", err)
			}
			recovered := testQueue(t, q.dir, nil)
			expected := 0
			if step == atomicfile.StepRename || step == atomicfile.StepDirsync {
				expected = 1
			}
			if recovered.snapshot().DiskEvents != expected {
				t.Fatalf("etapa %s: %+v", step, recovered.snapshot())
			}
			if expected == 1 {
				got, identity, err := readRecord(recovered.path(1))
				if err != nil || string(got) != string(body) || identity.ID != "stable" {
					t.Fatalf("evento recuperado divergiu: %v", err)
				}
			}
			files, err := os.ReadDir(q.dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != expected {
				t.Fatalf("temporário não foi removido: %v", files)
			}
		})
	}
}

func TestHTTPAckWithoutDurableRemovalReplaysSameEventBeforeNext(t *testing.T) {
	var mu sync.Mutex
	var ids, bodies []string
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		ids = append(ids, r.Header.Get("Idempotency-Key"))
		bodies = append(bodies, string(body))
		mu.Unlock()
		w.WriteHeader(204)
	}))
	defer receiver.Close()
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
	q.remove = func(string) error { return errors.New("crash before ack") }
	q.offer(eventBody(t, "first"), time.Now())
	q.offer(eventBody(t, "second"), time.Now())
	eventually(t, func() bool { return q.snapshot().DiskEvents == 2 })
	q.start(context.Background())
	eventually(t, func() bool { return q.snapshot().State == "failed" })
	_ = q.close()
	recovered := testQueue(t, q.dir, fastEndpoint(receiver.URL))
	recovered.start(context.Background())
	eventually(t, func() bool { return recovered.snapshot().Delivered == 2 })
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(ids, []string{"first", "first", "second"}) || bodies[0] != bodies[1] {
		t.Fatalf("reentrega mudou ordem ou corpo: %v", ids)
	}
}

func TestSenderWaitsForDirectorySyncBeforeNextEvent(t *testing.T) {
	var calls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(204) }))
	defer receiver.Close()
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var syncs atomic.Int64
	q.syncDir = func(path string) error {
		if syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return atomicfile.SyncDir(path)
	}
	q.offer(eventBody(t, "one"), time.Now())
	q.offer(eventBody(t, "two"), time.Now())
	eventually(t, func() bool { return q.snapshot().DiskEvents == 2 })
	q.start(context.Background())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("remoção não sincronizou o diretório")
	}
	if calls.Load() != 1 || q.snapshot().Delivered != 0 {
		t.Fatal("avançou antes da remoção durável")
	}
	once.Do(func() { close(release) })
	eventually(t, func() bool { return q.snapshot().Delivered == 2 })
}

func TestPersistentHTTPErrorsKeepFIFOAndIdenticalBodies(t *testing.T) {
	for _, status := range []int{400, 401, 403, 429, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var attempts atomic.Int64
			var repaired atomic.Bool
			var mu sync.Mutex
			var ids, bodies []string
			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				ids = append(ids, r.Header.Get("Idempotency-Key"))
				bodies = append(bodies, string(body))
				mu.Unlock()
				attempts.Add(1)
				if !repaired.Load() {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(204)
			}))
			defer receiver.Close()
			q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
			q.offer(eventBody(t, "first"), time.Now())
			q.offer(eventBody(t, "second"), time.Now())
			eventually(t, func() bool { return q.snapshot().DiskEvents == 2 })
			q.start(context.Background())
			eventually(t, func() bool { return q.snapshot().FailedAttempts >= 2 })
			if q.snapshot().DiskEvents != 2 {
				t.Fatal("erro HTTP descartou pendência")
			}
			mu.Lock()
			for i, id := range ids {
				if id != "first" || bodies[i] != bodies[0] {
					t.Errorf("erro alterou ordem/corpo: %v", ids)
				}
			}
			mu.Unlock()
			repaired.Store(true)
			eventually(t, func() bool { return q.snapshot().Delivered == 2 })
		})
	}
}

func TestCloseCancelsHTTPAndPreservesUnsentEvents(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	receiver := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { close(entered); <-release }))
	defer receiver.Close()
	defer close(release)
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
	q.offer(eventBody(t, "first"), time.Now())
	q.start(context.Background())
	<-entered
	q.offer(eventBody(t, "second"), time.Now())
	done := make(chan error, 1)
	go func() { done <- q.close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close esperou o receptor")
	}
	recovered := testQueue(t, q.dir, nil)
	if recovered.snapshot().DiskEvents != 2 {
		t.Fatalf("Close não preservou pendências: %+v", recovered.snapshot())
	}
}

func TestCorruptPublishedRecordRefusesRecovery(t *testing.T) {
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
	q.offer(eventBody(t, "first"), time.Now())
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(q.path(1))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-2] ^= 1
	if err := os.WriteFile(q.path(1), raw, 0600); err != nil {
		t.Fatal(err)
	}
	_, err = openQueue(q.dir, "noc", 256<<20, nil, logging.Discard())
	if !errors.Is(err, ErrQueue) || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("corrupção não recusada pela guarda própria: %v", err)
	}
	if _, err := os.Stat(q.path(1)); err != nil {
		t.Fatal("arquivo corrompido foi apagado")
	}
}

func TestPostAuthenticationRedirectAndRetryAfter(t *testing.T) {
	var reached atomic.Bool
	redirect := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { reached.Store(true) }))
	defer redirect.Close()
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Idempotency-Key") != "id" {
			t.Error("cabeçalhos incorretos")
		}
		w.Header().Set("Location", redirect.URL)
		w.WriteHeader(307)
	}))
	defer receiver.Close()
	endpoint := fastEndpoint(receiver.URL)
	endpoint.token = "secret"
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), endpoint)
	result := q.transport.send(context.Background(), eventBody(t, "id"), eventIdentity{ID: "id"})
	if result.ok || len(result.diagnostic) != 2 || result.diagnostic[1] != 307 || reached.Load() {
		t.Fatal("redirect foi seguido ou aceito")
	}
	now := time.Now().UTC().Truncate(time.Second)
	for value, want := range map[string]time.Duration{"3": 3 * time.Second, "invalid": 0, "-2": 0, now.Add(9 * time.Second).Format(http.TimeFormat): 9 * time.Second} {
		if got := parseRetryAfter(value, now); got != want {
			t.Fatalf("Retry-After %q=%s, esperado %s", value, got, want)
		}
	}
}

func TestSuccessfulDeliveryReclaimsAdmissionCapacity(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer receiver.Close()
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
	body := eventBody(t, "repeat")
	q.diskLimit = 1
	q.maxBytes = int64(len(body) + recordHeaderBytes)
	q.start(context.Background())
	for i := uint64(1); i <= 4; i++ {
		q.offer(body, time.Now())
		eventually(t, func() bool { return q.snapshot().Delivered == i })
		s := q.snapshot()
		if s.DiskBytes != 0 || s.DiskEvents != 0 || s.MemoryBytes != 0 || s.Dropped[DropDiskFull] != 0 {
			t.Fatalf("capacidade não liberada: %+v", s)
		}
	}
}

func TestConcurrentAdmissionAndCloseNeverPanicOrLoseAcceptedFiles(t *testing.T) {
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), nil)
	body := eventBody(t, "concurrent")
	q.offer(body, time.Now())
	eventually(t, func() bool { return q.snapshot().DiskEvents == 1 })
	var wg sync.WaitGroup
	start := make(chan struct{})
	const events = 100
	for i := 0; i < events; i++ {
		wg.Go(func() { <-start; q.offer(body, time.Now()) })
	}
	closed := make(chan error, 1)
	go func() { <-start; closed <- q.close() }()
	close(start)
	wg.Wait()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	q.offer(body, time.Now()) // Também prova a recusa depois de Close retornar.
	s := q.snapshot()
	if s.DiskEvents+int(s.Dropped[DropClosed]) != events+2 || s.MemoryEvents != 0 || s.DiskEvents == 0 || s.Dropped[DropClosed] == 0 {
		t.Fatalf("admissão/Close perderam eventos: %+v", s)
	}
	recovered := testQueue(t, q.dir, nil)
	if recovered.snapshot().DiskEvents != s.DiskEvents {
		t.Fatal("arquivos aceitos não sobreviveram")
	}
}

func TestCancelledSenderIsNoLongerReportedActive(t *testing.T) {
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint("http://127.0.0.1:1"))
	ctx, cancel := context.WithCancel(context.Background())
	q.start(ctx)
	if q.snapshot().State != "running" {
		t.Fatal("remetente não publicou a subida")
	}
	cancel()
	select {
	case <-q.senderDone:
	case <-time.After(time.Second):
		t.Fatal("remetente não cancelou")
	}
	if q.snapshot().State != "ready" {
		t.Fatal("remetente parado continua anunciado ativo")
	}
}

// closeCounter é um transporte que só conta quantas vezes foi fechado.
type closeCounter struct{ closes atomic.Int64 }

func (*closeCounter) kind() string         { return kindWebhook }
func (*closeCounter) reject([]byte) string { return "" }
func (c *closeCounter) close()             { c.closes.Add(1) }
func (*closeCounter) send(context.Context, []byte, eventIdentity) attempt {
	return attempt{}
}

// openQueue assume o transporte: fecha-o uma vez no close da fila e, se a
// abertura falhar, ali mesmo — senão o cliente do destino vazaria.
func TestOpenQueueOwnsTheTransportEvenWhenOpeningFails(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0600); err != nil {
		t.Fatal(err)
	}
	failed := &closeCounter{}
	if _, err := openQueue(filepath.Join(blocker, "noc"), "noc", 256<<20, failed, logging.Discard()); !errors.Is(err, ErrQueue) {
		t.Fatalf("abertura sobre um arquivo não falhou: %v", err)
	}
	if n := failed.closes.Load(); n != 1 {
		t.Fatalf("abertura falha fechou o transporte %d vezes, esperado 1", n)
	}
	opened := &closeCounter{}
	q, err := openQueue(filepath.Join(t.TempDir(), "noc"), "noc", 256<<20, opened, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if n := opened.closes.Load(); n != 0 {
		t.Fatalf("abertura bem-sucedida fechou o transporte %d vezes", n)
	}
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
	if n := opened.closes.Load(); n != 1 {
		t.Fatalf("close da fila fechou o transporte %d vezes, esperado 1", n)
	}
}
