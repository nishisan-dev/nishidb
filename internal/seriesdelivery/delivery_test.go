package seriesdelivery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type heldRecord struct {
	record *kgo.Record
	done   func(*kgo.Record, error)
}

// heldProducer guarda os registros até o teste mandar concluí-los. Com lock
// porque o Publish que espera vaga chama TryProduce na goroutine dele.
type heldProducer struct {
	mu      sync.Mutex
	records []heldRecord
	// beforeClose, quando não nil, é aguardado pelo Close antes de falhar os
	// registros guardados.
	beforeClose chan struct{}
}

func (p *heldProducer) TryProduce(_ context.Context, r *kgo.Record, done func(*kgo.Record, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, heldRecord{r, done})
}

func (p *heldProducer) Close() {
	if p.beforeClose != nil {
		<-p.beforeClose
	}
	for _, r := range p.take(-1) {
		r.done(r.record, kgo.ErrClientClosed)
	}
}

// take retira os n primeiros registros guardados (n < 0: todos).
func (p *heldProducer) take(n int) []heldRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n < 0 || n > len(p.records) {
		n = len(p.records)
	}
	out := append([]heldRecord(nil), p.records[:n]...)
	p.records = p.records[n:]
	return out
}

// release conclui com sucesso o primeiro registro guardado cujo tamanho é size.
func (p *heldProducer) release(t *testing.T, size int) {
	t.Helper()
	p.mu.Lock()
	for i, r := range p.records {
		if len(r.record.Key)+len(r.record.Value) == size {
			p.records = append(p.records[:i], p.records[i+1:]...)
			p.mu.Unlock()
			r.done(r.record, nil)
			return
		}
	}
	p.mu.Unlock()
	t.Fatalf("nenhum registro guardado de %d bytes", size)
}

func (p *heldProducer) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

// testOutput monta uma saída sobre o produtor falso, com a espera ligada ao
// lock do destino (newDestination) — montar o destino à mão deixaria o Cond
// sem L, e a primeira espera entraria em pânico.
func testOutput(p producer) *output {
	return outputOn(newDestination(p, "", time.Minute))
}

func outputOn(d *destination) *output {
	return &output{d: d, stats: protocol.SeriesDeliveryStats{Dropped: make(map[string]uint64)}}
}

// waits conta as vezes em que uma admissão foi dormir e anuncia cada uma.
type waits struct {
	n  atomic.Int64
	ch chan int64
}

func watchWaits(d *destination) *waits {
	w := &waits{ch: make(chan int64, 64)}
	d.waitHook = func() { w.ch <- w.n.Add(1) }
	return w
}

// expect espera a n-ésima ida ao sono. O prazo é guarda de travamento.
func (w *waits) expect(t *testing.T, n int64) {
	t.Helper()
	for {
		select {
		case got := <-w.ch:
			if got >= n {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("a admissão não foi dormir pela %dª vez (contagem %d)", n, w.n.Load())
		}
	}
}

// publishAsync publica numa goroutine e devolve o canal do resultado.
func publishAsync(ctx context.Context, o *output, key, body []byte) chan error {
	done := make(chan error, 1)
	go func() { done <- o.Publish(ctx, key, body, time.Now()) }()
	return done
}

func returned(t *testing.T, done chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: o Publish não devolveu", what)
		return nil
	}
}

func mustPublish(t *testing.T, o *output, key, body []byte) {
	t.Helper()
	if err := o.Publish(context.Background(), key, body, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// Capacidade cheia, por registros ou por bytes, ESPERA em vez de descartar, e
// a espera sai pela vaga: nenhum `buffer_full`, e o registro que esperou é
// admitido. A chave e o corpo são copiados — o emissor reusa o scratch.
func TestFullDestinationWaitsInsteadOfDropping(t *testing.T) {
	for _, byteLimit := range []bool{false, true} {
		t.Run(fmt.Sprint(byteLimit), func(t *testing.T) {
			p := &heldProducer{}
			o := testOutput(p)
			defer o.d.cancel()
			w := watchWaits(o.d)
			key, body := []byte("k"), []byte("v")
			want := MaxRecords
			if byteLimit {
				body = []byte(strings.Repeat("x", 900_000))
				want = MaxBytes / (len(body) + len(key))
			}
			for i := 0; i < want; i++ {
				mustPublish(t, o, key, body)
			}
			body[0], key[0] = 'z', 'z'
			done := publishAsync(context.Background(), o, key, body)
			w.expect(t, 1)
			s := o.snapshot()
			if p.len() != want || s.PendingRecords != int64(want) || s.PendingBytes > MaxBytes || s.Dropped["buffer_full"] != 0 {
				t.Fatalf("admissão fora dos limites ou com descarte: %+v registros=%d", s, p.len())
			}
			first := p.take(1)[0]
			if first.record.Key[0] != 'k' || first.record.Value[0] == 'z' {
				t.Fatal("o registro reteve o scratch do emissor")
			}
			first.done(first.record, nil)
			if err := returned(t, done, "vaga liberada"); err != nil {
				t.Fatal(err)
			}
			s = o.snapshot()
			if p.len() != want || s.Delivered != 1 || s.Dropped["buffer_full"] != 0 || s.PendingRecords != int64(want) {
				t.Fatalf("o registro que esperou não foi admitido: %+v", s)
			}
			p.Close()
			s = o.snapshot()
			if s.PendingBytes != 0 || s.PendingRecords != 0 || s.Dropped["shutdown"] != uint64(want) {
				t.Fatalf("vazamento de capacidade: %+v", s)
			}
		})
	}
}

// G10: mais registros que a capacidade, publicados sem pausa, chegam TODOS. O
// produtor confirma em lotes, e só quando uma admissão vai dormir: a passada
// é obrigada a encher o destino e a esperar. Um produtor que confirmasse por
// conta própria podia acompanhar a passada sem deixar o buffer encher — e o
// `buffer_full` antigo passaria verde.
func TestPublishWaitsAndDeliversMoreThanTheCapacity(t *testing.T) {
	p := &heldProducer{}
	o := testOutput(p)
	defer o.d.cancel()
	var sleeps atomic.Int64
	o.d.waitHook = func() {
		sleeps.Add(1)
		// Sob o lock do destino: os callbacks o tomam, então confirmam fora.
		go func() {
			for _, r := range p.take(500) {
				r.done(r.record, nil)
			}
		}()
	}
	const n = 3*MaxRecords + 7
	for i := 0; i < n; i++ {
		mustPublish(t, o, []byte("k"), []byte("v"))
	}
	for p.len() > 0 {
		for _, r := range p.take(-1) {
			r.done(r.record, nil)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for o.snapshot().Delivered != n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s := o.snapshot(); s.Delivered != n || s.Dropped["buffer_full"] != 0 || s.PendingRecords != 0 {
		t.Fatalf("entregues %d de %d: %+v", s.Delivered, n, s)
	}
	if sleeps.Load() == 0 {
		t.Fatal("a passada nunca esperou: o teste não exerceu o destino cheio")
	}
}

// G11: a espera acorda pela liberação, e não por varredura do relógio. A
// admissão vai dormir UMA vez e não reavalia nada enquanto nada é liberado;
// liberado um registro, ela é admitida sem dormir de novo. A janela de
// observação só pode reprovar o mutante que reavalia sozinho: com a espera
// por Cond, a contagem é 1 qualquer que seja a janela.
func TestAdmissionWakesOnReleaseWithoutPolling(t *testing.T) {
	p := &heldProducer{}
	o := testOutput(p)
	defer o.d.cancel()
	w := watchWaits(o.d)
	for i := 0; i < MaxRecords; i++ {
		mustPublish(t, o, []byte("k"), []byte("v"))
	}
	done := publishAsync(context.Background(), o, []byte("k"), []byte("v"))
	w.expect(t, 1)
	time.Sleep(50 * time.Millisecond)
	if got := w.n.Load(); got != 1 {
		t.Fatalf("a admissão reavaliou %d vezes sem nenhuma liberação: varredura, e não espera", got)
	}
	r := p.take(1)[0]
	r.done(r.record, nil)
	if err := returned(t, done, "liberação"); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load(); got != 1 {
		t.Fatalf("a admissão dormiu %d vezes; liberado o registro, ela cabia", got)
	}
}

// G12 (lado do destino): ctx cancelado com o Publish em espera devolve o erro,
// conta `shutdown` e não produz nada.
func TestCancelledPublishWaitingReturnsTheErrorAndCountsShutdown(t *testing.T) {
	p := &heldProducer{}
	o := testOutput(p)
	defer o.d.cancel()
	w := watchWaits(o.d)
	for i := 0; i < MaxRecords; i++ {
		mustPublish(t, o, []byte("k"), []byte("v"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := publishAsync(ctx, o, []byte("k"), []byte("v"))
	w.expect(t, 1)
	cancel()
	if err := returned(t, done, "cancelamento"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish cancelado devolveu %v, quero context.Canceled", err)
	}
	s := o.snapshot()
	if s.Dropped["shutdown"] != 1 || p.len() != MaxRecords || s.PendingRecords != MaxRecords {
		t.Fatalf("cancelamento mal contado: %+v registros=%d", s, p.len())
	}
}

// E8: ctx já cancelado não admite nada, nem com vaga sobrando.
func TestCancelledContextAdmitsNothingEvenWithRoom(t *testing.T) {
	p := &heldProducer{}
	o := testOutput(p)
	defer o.d.cancel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.Publish(ctx, []byte("k"), []byte("v"), time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish com ctx cancelado devolveu %v", err)
	}
	if s := o.snapshot(); p.len() != 0 || s.Dropped["shutdown"] != 1 || s.PendingRecords != 0 {
		t.Fatalf("ctx cancelado admitiu: %+v registros=%d", s, p.len())
	}
}

// G13: o Close do gerente acorda o Publish em espera ANTES de fechar o
// produtor. O produtor falso só fecha (e só falha os registros guardados, cujo
// callback também acordaria a espera) depois de o Publish devolver: sem o
// despertar do fechamento, os dois esperariam um pelo outro.
func TestManagerCloseWakesAPublishWaitingForRoom(t *testing.T) {
	p := &heldProducer{beforeClose: make(chan struct{})}
	d := newDestination(p, "", time.Minute)
	o := outputOn(d)
	m := &Manager{destinations: []*destination{d}, outputs: []*output{o}, log: logging.Discard(), stop: make(chan struct{}), done: make(chan struct{})}
	close(m.done)
	w := watchWaits(d)
	for i := 0; i < MaxRecords; i++ {
		mustPublish(t, o, []byte("k"), []byte("v"))
	}
	done := publishAsync(context.Background(), o, []byte("k"), []byte("v"))
	w.expect(t, 1)
	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()
	err := returned(t, done, "fechamento")
	close(p.beforeClose)
	if err != nil {
		t.Fatalf("Publish acordado pelo fechamento devolveu %v; só o cancelamento de ctx é erro", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Manager.Close travou")
	}
	if s := o.snapshot(); s.Dropped["shutdown"] != MaxRecords+1 || s.PendingRecords != 0 || s.PendingBytes != 0 {
		t.Fatalf("fechamento mal contado: %+v", s)
	}
}

// G14: duas saídas (dois contadores) esperando o MESMO destino cheio progridem
// as duas. O limite que aperta é o de bytes, com tamanhos mistos: A, que
// dormiu primeiro, quer um registro grande; B quer um pequeno. Liberar um
// pequeno abre vaga só para B. Um Signal acordaria só A (o mais antigo), que
// não cabe e volta a dormir — e B nunca acordaria.
func TestWaitersOnTheSameFullDestinationBothProgress(t *testing.T) {
	p := &heldProducer{}
	d := newDestination(p, "", time.Minute)
	defer d.cancel()
	a, b := outputOn(d), outputOn(d)
	w := watchWaits(d)
	const big, small = 500_000, 1_000
	bigBody, smallBody := make([]byte, big), make([]byte, small)
	for i := 0; i < 33; i++ {
		mustPublish(t, a, nil, bigBody)
	}
	for d.bytes+small <= MaxBytes {
		mustPublish(t, b, nil, smallBody)
	}
	if free := MaxBytes - d.bytes; free >= small || d.records >= MaxRecords {
		t.Fatalf("montagem errada: %d bytes livres, %d registros", free, d.records)
	}
	doneA := publishAsync(context.Background(), a, nil, bigBody)
	w.expect(t, 1)
	doneB := publishAsync(context.Background(), b, nil, smallBody)
	w.expect(t, 2)
	p.release(t, small)
	if err := returned(t, doneB, "B, com vaga para o pequeno"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-doneA:
		t.Fatalf("A admitido sem vaga para o grande: %v", err)
	default:
	}
	p.release(t, big)
	if err := returned(t, doneA, "A, com vaga para o grande"); err != nil {
		t.Fatal(err)
	}
	if sa, sb := a.snapshot(), b.snapshot(); sa.Dropped["buffer_full"]+sb.Dropped["buffer_full"] != 0 {
		t.Fatalf("descarte por buffer cheio: %+v %+v", sa, sb)
	}
}

// A vaga liberada por um registro que FALHOU também acorda quem espera: o
// destino fora do ar libera o buffer por expiração e por erro, nunca por
// sucesso, e um despertar só no sucesso deixaria a passada parada para
// sempre exatamente quando o destino cai.
func TestFailedCompletionAlsoWakesAPublishWaitingForRoom(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason string
	}{{context.DeadlineExceeded, "timeout"}, {kerr.TopicAuthorizationFailed, "publish_error"}} {
		t.Run(tc.reason, func(t *testing.T) {
			p := &heldProducer{}
			o := testOutput(p)
			defer o.d.cancel()
			w := watchWaits(o.d)
			for i := 0; i < MaxRecords; i++ {
				mustPublish(t, o, []byte("k"), []byte("v"))
			}
			done := publishAsync(context.Background(), o, []byte("k"), []byte("v"))
			w.expect(t, 1)
			r := p.take(1)[0]
			r.done(r.record, tc.err)
			if err := returned(t, done, "vaga liberada por falha"); err != nil {
				t.Fatal(err)
			}
			if s := o.snapshot(); s.Dropped[tc.reason] != 1 || s.Delivered != 0 || s.PendingRecords != MaxRecords {
				t.Fatalf("falha mal contada ou registro que esperou não admitido: %+v", s)
			}
		})
	}
}

// E5/G12: o cancelamento do ctx de UMA espera acorda essa espera, e não outra.
// B dorme primeiro, A depois: um Signal no lugar do Broadcast acordaria B (o
// mais antigo), que não cabe e volta a dormir, e A nunca devolveria. B
// continua esperando até haver vaga.
func TestCancellingOneWaiterReturnsItAndKeepsTheOtherWaiting(t *testing.T) {
	p := &heldProducer{}
	d := newDestination(p, "", time.Minute)
	defer d.cancel()
	a, b := outputOn(d), outputOn(d)
	w := watchWaits(d)
	for i := 0; i < MaxRecords; i++ {
		mustPublish(t, b, []byte("k"), []byte("v"))
	}
	doneB := publishAsync(context.Background(), b, []byte("k"), []byte("v"))
	w.expect(t, 1)
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := publishAsync(ctxA, a, []byte("k"), []byte("v"))
	w.expect(t, 2)
	cancelA()
	if err := returned(t, doneA, "A cancelado, sem vaga nenhuma"); !errors.Is(err, context.Canceled) {
		t.Fatalf("A devolveu %v, quero context.Canceled", err)
	}
	if s := a.snapshot(); s.Dropped["shutdown"] != 1 || s.PendingRecords != 0 {
		t.Fatalf("cancelamento de A mal contado: %+v", s)
	}
	select {
	case err := <-doneB:
		t.Fatalf("B devolveu (%v) sem vaga nenhuma", err)
	default:
	}
	if p.len() != MaxRecords {
		t.Fatalf("o cancelamento de A produziu registro: %d guardados", p.len())
	}
	r := p.take(1)[0]
	r.done(r.record, nil)
	if err := returned(t, doneB, "B, com vaga"); err != nil {
		t.Fatal(err)
	}
}

// G15: AdmissionWaitSeconds só cresce com espera.
func TestAdmissionWaitIsCountedOnlyWhenThePublishWaited(t *testing.T) {
	p := &heldProducer{}
	o := testOutput(p)
	defer o.d.cancel()
	w := watchWaits(o.d)
	for i := 0; i < MaxRecords; i++ {
		mustPublish(t, o, []byte("k"), []byte("v"))
	}
	if got := o.snapshot().AdmissionWaitSeconds; got != 0 {
		t.Fatalf("espera de admissão = %v sem nenhuma espera", got)
	}
	done := publishAsync(context.Background(), o, []byte("k"), []byte("v"))
	w.expect(t, 1)
	r := p.take(1)[0]
	r.done(r.record, nil)
	if err := returned(t, done, "liberação"); err != nil {
		t.Fatal(err)
	}
	if got := o.snapshot().AdmissionWaitSeconds; got <= 0 {
		t.Fatalf("espera de admissão = %v depois de uma espera", got)
	}
}

func TestOversizedAndCompletedRecordsHaveSingleOutcome(t *testing.T) {
	p := &heldProducer{}
	o := testOutput(p)
	defer o.d.cancel()
	mustPublish(t, o, nil, make([]byte, config.DefaultKafkaMessageBytes-recordFramingBytes+1))
	if p.len() != 0 || o.snapshot().Dropped["message_too_large"] != 1 {
		t.Fatal("oversized record admitted")
	}
	for _, err := range []error{nil, context.DeadlineExceeded, kerr.TopicAuthorizationFailed, kerr.MessageTooLarge} {
		mustPublish(t, o, []byte("key"), []byte("value"))
		r := p.take(1)[0]
		r.done(r.record, err)
	}
	s := o.snapshot()
	if s.Delivered != 1 || s.LastSuccessTimestamp == 0 || s.PendingRecords != 0 || s.PendingBytes != 0 || s.Dropped["timeout"] != 1 || s.Dropped["publish_error"] != 1 || s.Dropped["message_too_large"] != 2 {
		t.Fatalf("bad outcomes: %+v", s)
	}
}

// kgoLikeProducer reproduz a ordem do worker de promessas do kgo: chama o
// callback ANTES de decrementar a própria contagem, e recusa com
// ErrMaxBuffered o TryProduce que chega com a contagem no limite. complete
// encena a pior intercalação, que no kgo de verdade depende de o worker perder
// a CPU entre as duas coisas: o decremento só acontece depois de o próximo
// TryProduce chegar.
type kgoLikeProducer struct {
	mu             sync.Mutex
	limitRecords   int64
	limitBytes     int64
	records, bytes int64
	held           []heldRecord
	produced       chan struct{}
}

func (p *kgoLikeProducer) TryProduce(_ context.Context, r *kgo.Record, done func(*kgo.Record, error)) {
	size := int64(len(r.Key) + len(r.Value))
	p.mu.Lock()
	over := p.records >= p.limitRecords || p.bytes+size > p.limitBytes
	if !over {
		p.records++
		p.bytes += size
		p.held = append(p.held, heldRecord{r, done})
	}
	p.mu.Unlock()
	if p.produced != nil {
		p.produced <- struct{}{}
	}
	if over {
		done(r, kgo.ErrMaxBuffered)
	}
}

func (p *kgoLikeProducer) Close() {}

// complete confirma o registro mais antigo na ordem do kgo: promessa, e só
// depois o decremento — aqui, depois do TryProduce seguinte.
func (p *kgoLikeProducer) complete(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	r := p.held[0]
	p.held = p.held[1:]
	p.mu.Unlock()
	r.done(r.record, nil)
	select {
	case <-p.produced:
	case <-time.After(5 * time.Second):
		t.Fatal("o Publish acordado pela vaga não chegou ao TryProduce")
	}
	p.mu.Lock()
	p.records--
	p.bytes -= int64(len(r.record.Key) + len(r.record.Value))
	p.mu.Unlock()
}

// configuredKgoLimits lê os limites de buffer do cliente kgo que o Open monta
// de verdade — e não de uma constante do teste, para que o teste reprove a
// configuração, e não uma cópia dela.
func configuredKgoLimits(t *testing.T) (records, bytes int64) {
	t.Helper()
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	client, ok := m.destinations[0].client.(*kgo.Client)
	if !ok {
		t.Fatalf("o destino não usa um *kgo.Client: %T", m.destinations[0].client)
	}
	r, okR := client.OptValue(kgo.MaxBufferedRecords).(int64)
	b, okB := client.OptValue(kgo.MaxBufferedBytes).(int64)
	if !okR || !okB {
		t.Fatalf("limites do kgo ilegíveis: %v %v", client.OptValue(kgo.MaxBufferedRecords), client.OptValue(kgo.MaxBufferedBytes))
	}
	return r, b
}

// G10b (E1): o Publish que o callback acorda chega ao TryProduce com o kgo
// ainda contando o registro que acabou de sair. Com os limites do kgo que o
// Open configura, essa intercalação NÃO pode virar ErrMaxBuffered — o
// `buffer_full` voltaria escondido atrás da contrapressão. A prova é
// determinística porque a corrida, contra o kgo de verdade, quase nunca se
// manifesta: o worker de promessas libera mais rápido do que a passada repõe,
// e a passada só chega ao limite com o worker parado.
func TestKgoLimitsLeaveRoomForThePromiseBeforeItsDecrement(t *testing.T) {
	limitRecords, limitBytes := configuredKgoLimits(t)
	for _, byteLimit := range []bool{false, true} {
		t.Run(fmt.Sprint(byteLimit), func(t *testing.T) {
			p := &kgoLikeProducer{limitRecords: limitRecords, limitBytes: limitBytes}
			o := testOutput(p)
			defer o.d.cancel()
			w := watchWaits(o.d)
			body := []byte("v")
			want := MaxRecords
			if byteLimit {
				body = make([]byte, 900_000)
				want = MaxBytes / len(body)
			}
			for i := 0; i < want; i++ {
				mustPublish(t, o, nil, body)
			}
			p.produced = make(chan struct{}, 1)
			for i := 0; i < 3; i++ {
				done := publishAsync(context.Background(), o, nil, body)
				w.expect(t, int64(i+1))
				p.complete(t)
				if err := returned(t, done, "vaga liberada"); err != nil {
					t.Fatal(err)
				}
				if s := o.snapshot(); s.Dropped["buffer_full"] != 0 || s.Delivered != uint64(i+1) || s.PendingRecords != int64(want) {
					t.Fatalf("o kgo recusou o registro acordado pela vaga local: %+v", s)
				}
			}
		})
	}
}

// Ponta a ponta contra o kgo e o kfake: três capacidades publicadas sem pausa
// chegam todas, sem `buffer_full`, e a admissão ESPEROU. O broker segura as
// requisições até o destino encher — sem isto, a vazão relativa entre o
// kfake e a passada decidiria se o buffer enche, e o teste passaria por
// acaso. Não substitui o teste acima: a corrida do E1 não é alcançável sob
// demanda aqui.
func TestKafkaDestinationDeliversThreeCapacitiesWithoutBufferFull(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	release := make(chan struct{})
	var released atomic.Bool
	var once sync.Once
	open := func() { once.Do(func() { released.Store(true); close(release) }) }
	defer open()
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !released.Load() {
			c.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	cfg, specs := deliveryConfig(c.ListenAddrs(), "samples")
	cfg.KafkaTargets[0].Timeout = "30s"
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	out := m.Outputs("d", "c")[0]
	const n = 3 * MaxRecords
	done := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := out.Publish(context.Background(), []byte(fmt.Sprintf("k%d", i%97)), []byte("v"), time.Now()); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	deadline := time.Now().Add(10 * time.Second)
	for m.Stats("d")[0].PendingRecords != MaxRecords {
		if time.Now().After(deadline) {
			t.Fatalf("o destino não encheu com o broker parado: %+v", m.Stats("d")[0])
		}
		time.Sleep(time.Millisecond)
	}
	open()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("a passada não terminou com o broker liberado")
	}
	var s protocol.SeriesDeliveryStats
	for deadline = time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if s = m.Stats("d")[0]; s.PendingRecords == 0 {
			break
		}
	}
	if s.Delivered != n || s.Dropped["buffer_full"] != 0 || s.AdmissionWaitSeconds <= 0 {
		t.Fatalf("entregues %d de %d, buffer_full %d, espera %v: %+v", s.Delivered, n, s.Dropped["buffer_full"], s.AdmissionWaitSeconds, s)
	}
}

// O Timestamp do registro é o at da passada: a fronteira, que fica no passado
// durante a passada inteira e mais ainda numa passada atrasada. O prazo de
// entrega conta da admissão, e não dele — um kgo.RecordDeliveryTimeout, que o
// kgo mede a partir do Timestamp, expiraria o registro antes de ele sair.
func TestRecordStampedWithAPastBoundaryIsStillDelivered(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cfg, specs := deliveryConfig(c.ListenAddrs(), "samples")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	boundary := time.Now().Add(-time.Hour).Truncate(time.Minute)
	if err := m.Outputs("d", "c")[0].Publish(context.Background(), []byte("k"), []byte("v"), boundary); err != nil {
		t.Fatal(err)
	}
	s := waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool { return s[0].PendingRecords == 0 })
	if s[0].Delivered != 1 {
		t.Fatalf("registro com o at de uma hora atrás não foi entregue: %+v", s[0])
	}
}

func deliveryConfig(brokers []string, targets ...string) (*config.Config, map[string]*schema.DatasetSpec) {
	cfg := &config.Config{}
	for _, target := range targets {
		cfg.KafkaTargets = append(cfg.KafkaTargets, config.KafkaTargetSpec{Name: target, BootstrapServers: brokers, Topic: target, Timeout: "500ms"})
	}
	specs := map[string]*schema.DatasetSpec{"d": {Name: "d", Counters: []schema.CounterSpec{{Name: "c", Emit: &schema.EmitSpec{Targets: targets}}}}}
	return cfg, specs
}

func waitStats(t *testing.T, m *Manager, match func([]protocol.SeriesDeliveryStats) bool) []protocol.SeriesDeliveryStats {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		s := m.Stats("d")
		if match(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("delivery did not settle: %+v", m.Stats("d"))
	return nil
}

func TestKafkaDestinationsConfirmIndependentlyAndPublishOneRecordPerSample(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.AllowAutoTopicCreation(), kfake.SeedTopics(3, "first", "second"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cfg, specs := deliveryConfig(c.ListenAddrs(), "first", "second", "absent")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, out := range m.Outputs("d", "c") {
		for i := 0; i < 3; i++ {
			if err := out.Publish(context.Background(), []byte("same-key"), []byte(fmt.Sprintf(`{"total":%d}`, i)), time.Now()); err != nil {
				t.Error(err)
			}
		}
	}
	waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool {
		return s[0].Delivered == 3 && s[1].Delivered == 3 && s[2].PendingRecords == 0 && s[2].Dropped["publish_error"]+s[2].Dropped["timeout"] == 3
	})
	reader, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics("first", "second"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	counts := map[string]int{}
	partitions := map[string]int32{}
	for counts["first"]+counts["second"] < 6 {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Value) != fmt.Sprintf(`{"total":%d}`, counts[r.Topic]) || string(r.Key) != "same-key" {
				t.Errorf("bad order or body: %+v", r)
			}
			if counts[r.Topic] > 0 && partitions[r.Topic] != r.Partition {
				t.Error("stable key changed partition")
			}
			counts[r.Topic]++
			partitions[r.Topic] = r.Partition
		})
	}
}

func TestPublishFailuresAreNotRetriedAndNextSampleRecovers(t *testing.T) {
	for _, code := range []int16{kerr.NotEnoughReplicas.Code, kerr.RequestTimedOut.Code, kerr.NotEnoughReplicasAfterAppend.Code, -1} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var calls atomic.Int64
			var healthy atomic.Bool
			c.ControlKey(0, func(req kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				calls.Add(1)
				if req.(*kmsg.ProduceRequest).Acks != -1 {
					t.Error("producer did not request acknowledgements from all in-sync replicas")
				}
				if healthy.Load() {
					return nil, nil, false
				}
				if code == -1 {
					return nil, errors.New("connection lost"), true
				}
				resp := kmsg.NewPtrProduceResponse()
				resp.SetVersion(req.GetVersion())
				for _, topic := range req.(*kmsg.ProduceRequest).Topics {
					tr := kmsg.NewProduceResponseTopic()
					tr.Topic = topic.Topic
					for _, partition := range topic.Partitions {
						pr := kmsg.NewProduceResponseTopicPartition()
						pr.Partition = partition.Partition
						pr.ErrorCode = code
						tr.Partitions = append(tr.Partitions, pr)
					}
					resp.Topics = append(resp.Topics, tr)
				}
				return resp, nil, true
			})
			cfg, specs := deliveryConfig(c.ListenAddrs(), "samples")
			m, err := Open(cfg, specs, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			out := m.Outputs("d", "c")[0]
			if err := out.Publish(context.Background(), []byte("k"), []byte("old"), time.Now()); err != nil {
				t.Error(err)
			}
			waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool {
				return s[0].PendingRecords == 0 && s[0].Delivered == 0 && s[0].Dropped["publish_error"]+s[0].Dropped["timeout"] == 1
			})
			if calls.Load() != 1 {
				t.Fatalf("retried failed sample %d times", calls.Load())
			}
			healthy.Store(true)
			if err := out.Publish(context.Background(), []byte("k"), []byte("new"), time.Now()); err != nil {
				t.Error(err)
			}
			waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool { return s[0].Delivered == 1 })
			if calls.Load() != 2 {
				t.Fatalf("old sample replayed: %d requests", calls.Load())
			}
		})
	}
}

func TestUnavailableBrokerDoesNotPreventStartupOrBoundedShutdown(t *testing.T) {
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := m.Outputs("d", "c")[0]
	var offers sync.WaitGroup
	for i := 0; i < 4; i++ {
		offers.Add(1)
		go func() {
			defer offers.Done()
			for j := 0; j < 100; j++ {
				if err := out.Publish(context.Background(), []byte("k"), []byte("v"), time.Now()); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown waiting for Kafka")
	}
	offers.Wait()
	m.Close()
	s := m.Stats("d")[0]
	var dropped uint64
	for _, n := range s.Dropped {
		dropped += n
	}
	if dropped != 400 || s.PendingRecords != 0 || s.PendingBytes != 0 {
		t.Fatalf("shutdown lost accounting: %+v", s)
	}
}

func TestSlowBrokerExpiresInflightSampleWithoutResending(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	release := make(chan struct{})
	defer close(release)
	var requests atomic.Int64
	c.ControlKey(0, func(_ kmsg.Request) (kmsg.Response, error, bool) {
		requests.Add(1)
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	cfg, specs := deliveryConfig(c.ListenAddrs(), "samples")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	started := time.Now()
	if err := m.Outputs("d", "c")[0].Publish(context.Background(), []byte("key"), []byte("sample"), started); err != nil {
		t.Error(err)
	}
	waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool {
		return s[0].PendingRecords == 0 && s[0].Dropped["timeout"] == 1
	})
	if time.Since(started) > 3*time.Second || requests.Load() != 1 {
		t.Fatalf("timeout/retry contract broken: duration=%s requests=%d", time.Since(started), requests.Load())
	}
}

func TestStartupRejectsUnknownReferencesAndInvalidLocalCredentials(t *testing.T) {
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	specs["d"].Counters[0].Emit.Targets = []string{"unknown"}
	if m, err := Open(cfg, specs, nil); err == nil {
		m.Close()
		t.Fatal("unknown reference accepted")
	}
	specs["d"].Counters[0].Emit.Targets = []string{"samples"}
	t.Setenv("SERIES_TEST_USER", "user")
	t.Setenv("SERIES_TEST_PASS", "")
	cfg.KafkaTargets[0].Security = schema.SecuritySpec{Protocol: schema.ProtocolSASLSSL, SASLMechanism: schema.SASLPlain, UsernameEnv: "SERIES_TEST_USER", PasswordEnv: "SERIES_TEST_PASS"}
	if m, err := Open(cfg, specs, nil); err == nil {
		m.Close()
		t.Fatal("missing credential accepted")
	}
	cfg.KafkaTargets[0].Security = schema.SecuritySpec{Protocol: schema.ProtocolSSL, CAFile: "/nonexistent/ca.pem"}
	if m, err := Open(cfg, specs, nil); err == nil {
		m.Close()
		t.Fatal("invalid CA accepted")
	}
}

// TestConfiguredMaxMessageBytesDropsRecordAboveTheTargetLimit prova que o teto
// de admissão sai de `kafkaTargets[].maxMessageBytes`, e não do número fixo de
// antes: uma amostra entre o teto configurado e o default é descartada como
// message_too_large na admissão, e uma abaixo do teto é admitida.
func TestConfiguredMaxMessageBytesDropsRecordAboveTheTargetLimit(t *testing.T) {
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	cfg.KafkaTargets[0].MaxMessageBytes = 4 << 10
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	out := m.Outputs("d", "c")[0]
	if err := out.Publish(context.Background(), nil, make([]byte, 8<<10), time.Now()); err != nil {
		t.Fatal(err)
	}
	s := m.Stats("d")[0]
	if s.Dropped["message_too_large"] != 1 || s.PendingRecords != 0 {
		t.Fatalf("amostra de 8KiB num destino de 4KiB deveria cair como message_too_large na admissão: %+v", s)
	}
	// A admissão só descarta por tamanho, buffer ou shutdown; o que acontece
	// depois (broker inalcançável) não interessa aqui.
	if err := out.Publish(context.Background(), nil, make([]byte, 1<<10), time.Now()); err != nil {
		t.Fatal(err)
	}
	s = m.Stats("d")[0]
	if s.Dropped["message_too_large"] != 1 || s.Dropped["buffer_full"] != 0 || s.Dropped["shutdown"] != 0 {
		t.Fatalf("amostra de 1KiB deveria ser admitida: %+v", s)
	}
}

// TestDefaultMaxMessageBytesMatchesFranzGoBatchDefault trava o default de
// maxMessageBytes contra a FONTE dele, o default de ProducerBatchMaxBytes do
// franz-go: é essa igualdade que faz o destino sem a chave se comportar como
// antes de ela existir.
func TestDefaultMaxMessageBytesMatchesFranzGoBatchDefault(t *testing.T) {
	cl, err := kgo.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	got, ok := cl.OptValue(kgo.ProducerBatchMaxBytes).(int32)
	if !ok || int64(got) != config.DefaultKafkaMessageBytes || config.DefaultKafkaMessageBytes != 1_000_012 {
		t.Fatalf("default do franz-go = %v, config.DefaultKafkaMessageBytes = %d: os dois têm de ser 1.000.012", got, config.DefaultKafkaMessageBytes)
	}
}

// TestAdmissionBoundaryReservesFramingBytes fixa a fronteira da admissão com
// números literais: com maxMessageBytes=4096, chave+corpo de 3968 bytes
// (4096−128) entra e 3969 cai como message_too_large.
func TestAdmissionBoundaryReservesFramingBytes(t *testing.T) {
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	cfg.KafkaTargets[0].MaxMessageBytes = 4096
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	out := m.Outputs("d", "c")[0]
	if err := out.Publish(context.Background(), []byte("k"), make([]byte, 3967), time.Now()); err != nil {
		t.Fatal(err)
	}
	if s := m.Stats("d")[0]; s.Dropped["message_too_large"] != 0 {
		t.Fatalf("3968 bytes deveriam ser admitidos com maxMessageBytes=4096: %+v", s)
	}
	if err := out.Publish(context.Background(), []byte("k"), make([]byte, 3968), time.Now()); err != nil {
		t.Fatal(err)
	}
	if s := m.Stats("d")[0]; s.Dropped["message_too_large"] != 1 {
		t.Fatalf("3969 bytes deveriam cair como message_too_large com maxMessageBytes=4096: %+v", s)
	}
}

// TestSampleLargerThanTransportBufferIsMessageTooLarge prova que a admissão
// não passa de MaxBytes: com maxMessageBytes=64MiB, a amostra de 16MiB+1 nunca
// caberia no buffer do destino, e o motivo certo é message_too_large, não
// buffer_full (que diria ao operador que o buffer está cheio, e ele está vazio).
func TestSampleLargerThanTransportBufferIsMessageTooLarge(t *testing.T) {
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	cfg.KafkaTargets[0].MaxMessageBytes = 64 << 20
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Outputs("d", "c")[0].Publish(context.Background(), nil, make([]byte, MaxBytes+1), time.Now()); err != nil {
		t.Fatal(err)
	}
	if s := m.Stats("d")[0]; s.Dropped["message_too_large"] != 1 || s.Dropped["buffer_full"] != 0 {
		t.Fatalf("amostra maior que o buffer de transporte deveria cair como message_too_large: %+v", s)
	}
}

// TestConfiguredMaxMessageBytesRaisesProducerBatchLimit prova a outra metade
// do motor: o mesmo maxMessageBytes vira o ProducerBatchMaxBytes do cliente.
// Com 2MiB configurados e um broker que aceita 4MiB, a amostra de 1,5MiB é
// entregue; sem a opção, o franz-go a recusaria pelo teto default (1.000.012)
// e ela cairia como message_too_large.
func TestConfiguredMaxMessageBytesRaisesProducerBatchLimit(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"), kfake.BrokerConfigs(map[string]string{"message.max.bytes": "4194304"}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cfg, specs := deliveryConfig(c.ListenAddrs(), "samples")
	cfg.KafkaTargets[0].MaxMessageBytes = 2 << 20
	cfg.KafkaTargets[0].Timeout = "5s"
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Outputs("d", "c")[0].Publish(context.Background(), []byte("k"), make([]byte, 1536<<10), time.Now()); err != nil {
		t.Fatal(err)
	}
	s := waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool {
		return s[0].Delivered == 1 || s[0].PendingRecords == 0
	})
	if s[0].Delivered != 1 {
		t.Fatalf("amostra de 1,5MiB não entregue num destino de 2MiB: %+v", s[0])
	}
}
