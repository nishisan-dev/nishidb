// Package alarmdelivery entrega eventos de alarme sem fazer I/O na varredura.
// Cada fila pertence ao flock do dataset e tem dois donos de I/O: o gravador
// e o remetente. O mutex protege somente memória, nunca disco nem a entrega.
//
// A fila é a mesma para os dois tipos de destino; o que muda é o transporte
// do remetente (sender.go): um POST HTTP por tentativa no webhook, uma
// produção síncrona no Kafka. Disco, FIFO, retentativa e identidade são da
// fila e valem igual para os dois.
//
// A admissão aplica contrapressão por memória: com o buffer cheio, a varredura
// espera o GRAVADOR liberar vaga, em vez de descartar o evento. Ela nunca
// espera a entrega — a memória é liberada quando o arquivo é publicado, não
// quando o destino confirma —, e nunca espera vaga em disco: o teto de disco
// continua descartando com `disk_full`, que é o que impede um destino fora
// do ar por dias de travar a varredura.
package alarmdelivery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

const (
	maxMemoryEvents   = 256
	maxMemoryBytes    = 8 << 20
	maxDiskEvents     = 50_000
	maxEventBytes     = 1 << 20
	recordHeaderBytes = 16
)

// Um corpo admitido sempre cabe no buffer vazio; sem isso, a espera por vaga
// em memória não teria fim para um corpo entre os dois limites. A conversão
// para uint não compila se a diferença ficar negativa.
const _ = uint(maxMemoryBytes - maxEventBytes)

// ErrQueue identifica falha de abertura ou persistência da fila do dataset.
var ErrQueue = errors.New("alarm delivery queue failure")

// Motivos fechados: nenhum label depende de URL, grupo ou conteúdo recebido.
const (
	// DropMemoryFull não é mais emitido: o buffer cheio faz a admissão esperar
	// o gravador. Continua no conjunto fechado porque o label saiu na v0.10.0 e
	// painéis e alertas o referenciam; a série segue publicada com zero.
	DropMemoryFull = "memory_full"
	DropDiskFull   = "disk_full"
	DropTooLarge   = "event_too_large"
	DropEncoding   = "encoding_error"
	DropFailed     = "destination_failed"
	DropClosed     = "closed"
)

// DropReasons devolve uma cópia para quem publica as séries com zero.
func DropReasons() []string {
	return []string{DropMemoryFull, DropDiskFull, DropTooLarge, DropEncoding, DropFailed, DropClosed}
}

// Stats é o retrato compartilhado entre /v1/stats e o coletor Prometheus.
type Stats = protocol.AlarmDeliveryStats

type queuedEvent struct {
	body []byte
	at   time.Time
}
type entry struct {
	seq  uint64
	size int64
	at   time.Time
}

type queue struct {
	dir, name              string
	log                    *logging.Logger
	maxBytes               int64
	memoryLimit, diskLimit int
	memoryByteLimit        int64
	mu                     sync.Mutex
	pending                []entry
	head                   int
	sequence               uint64
	reservedBytes          int64
	stats                  Stats
	failed                 error
	closed                 bool
	started                bool
	input                  chan queuedEvent
	wake                   chan struct{}
	writerDone             chan struct{}
	senderDone             chan struct{}
	cancel                 context.CancelFunc
	closeOnce              sync.Once
	// space acorda a admissão que espera vaga em memória. Usa mu e é
	// sinalizado em toda liberação de memória pelo gravador, no close e na
	// falha da fila.
	space *sync.Cond
	// Substituíveis pelos testes para bloquear ou falhar no passo real de I/O.
	write   func(string, []byte, os.FileMode) error
	remove  func(string) error
	syncDir func(string) error
	// transport é nil na fila pausada (destino removido do config).
	transport transport
	// initialDelay e maxDelay são o backoff do remetente, iguais para os dois
	// transportes; os testes os encurtam.
	initialDelay, maxDelay time.Duration
}

// openQueue assume o transporte: ele é fechado no close da fila, ou aqui
// mesmo se a abertura falhar.
func openQueue(dir, name string, maxBytes int64, target transport, log *logging.Logger) (*queue, error) {
	q, err := openQueueDir(dir, name, maxBytes, target, log)
	if err != nil && target != nil {
		target.close()
	}
	return q, err
}

func openQueueDir(dir, name string, maxBytes int64, target transport, log *logging.Logger) (*queue, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("%w: create directory: %v", ErrQueue, err)
	}
	// Garante também a criação durável do diretório de destino e da raiz.
	for _, parent := range []string{filepath.Dir(dir), filepath.Dir(filepath.Dir(dir))} {
		if err := atomicfile.SyncDir(parent); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrQueue, err)
		}
	}
	q := &queue{dir: dir, name: name, maxBytes: maxBytes, transport: target, log: log,
		initialDelay: time.Second, maxDelay: 5 * time.Minute,
		memoryLimit: maxMemoryEvents, diskLimit: maxDiskEvents, memoryByteLimit: maxMemoryBytes,
		input: make(chan queuedEvent, maxMemoryEvents), wake: make(chan struct{}, 1),
		writerDone: make(chan struct{}), senderDone: make(chan struct{}),
		write: atomicfile.Write, remove: os.Remove, syncDir: atomicfile.SyncDir,
		stats: Stats{Destination: name, State: "ready", Dropped: make(map[string]uint64)},
	}
	q.space = sync.NewCond(&q.mu)
	for _, reason := range DropReasons() {
		q.stats.Dropped[reason] = 0
	}
	if target == nil {
		q.stats.State = "paused"
	} else {
		q.stats.Kind = target.kind()
	}
	if err := q.recover(); err != nil {
		return nil, fmt.Errorf("%w: destination %q: %v", ErrQueue, name, err)
	}
	go q.persistLoop()
	return q, nil
}

func (q *queue) path(seq uint64) string { return filepath.Join(q.dir, fmt.Sprintf("%020d.evt", seq)) }

// offer reserva memória e capacidade de disco para um evento. Nunca espera a
// entrega e nunca espera vaga em disco: fila fechada ou falha, corpo grande
// demais (para a fila ou para o transporte, ver transport.reject), corpo que o
// transporte não sabe enviar e teto de disco (quantidade ou bytes, contando as
// reservas em memória) descartam na hora. Só a memória cheia (eventos OU bytes) faz esperar, e a
// espera termina pela vaga liberada pelo gravador, pelo close (`closed`) ou
// pela falha da fila (`destination_failed`); a cada despertar todas as
// condições são reavaliadas, porque o disco pode ter enchido no intervalo.
// O tempo esperado acumula em AdmissionWaitSeconds.
func (q *queue) offer(body []byte, at time.Time) {
	// O transporte é imutável depois de openQueue, e a recusa dele não faz
	// I/O: roda antes do lock.
	refused := ""
	if len(body) > maxEventBytes {
		refused = DropTooLarge
	} else if q.transport != nil {
		refused = q.transport.reject(body)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	size := int64(len(body) + recordHeaderBytes)
	var waitStart time.Time
	for {
		reason := ""
		switch {
		case q.closed:
			reason = DropClosed
		case q.failed != nil:
			reason = DropFailed
		case refused != "":
			reason = refused
		case len(q.pending)-q.head+q.stats.MemoryEvents >= q.diskLimit || size > q.maxBytes-q.reservedBytes:
			reason = DropDiskFull
		}
		if reason != "" {
			q.endWaitLocked(waitStart)
			q.dropLocked(reason)
			return
		}
		if q.stats.MemoryEvents < q.memoryLimit && int64(len(body)) <= q.memoryByteLimit-q.stats.MemoryBytes {
			break
		}
		if waitStart.IsZero() {
			waitStart = time.Now()
		}
		q.space.Wait()
	}
	q.endWaitLocked(waitStart)
	// Não bloqueia: a capacidade do channel é maxMemoryEvents, e MemoryEvents
	// (que conta o channel mais a gravação em curso) está abaixo de
	// memoryLimit, que nunca passa dela. close(q.input) também exige mu.
	q.input <- queuedEvent{body: body, at: at}
	q.stats.MemoryEvents++
	q.stats.MemoryBytes += int64(len(body))
	q.reservedBytes += size
}

// endWaitLocked acumula a espera por vaga; waitStart zero é admissão sem espera.
func (q *queue) endWaitLocked(waitStart time.Time) {
	if !waitStart.IsZero() {
		q.stats.AdmissionWaitSeconds += time.Since(waitStart).Seconds()
	}
}

// O log de descarte é emitido pelo chamador fora da varredura: o worker
// publica os totais ao observar mudanças, evitando uma linha por evento.
func (q *queue) dropLocked(reason string) { q.stats.Dropped[reason]++ }
func (q *queue) drop(reason string)       { q.mu.Lock(); q.dropLocked(reason); q.mu.Unlock() }

func (q *queue) persistLoop() {
	defer close(q.writerDone)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var reported uint64
	defer func() { q.reportDrops(&reported) }()
	for {
		var item queuedEvent
		select {
		case <-ticker.C:
			q.reportDrops(&reported)
			continue
		case next, ok := <-q.input:
			if !ok {
				return
			}
			item = next
		}
		size := int64(len(item.body) + recordHeaderBytes)
		q.mu.Lock()
		failed := q.failed != nil
		q.sequence++
		seq := q.sequence
		q.mu.Unlock()
		var err error
		if !failed {
			err = q.write(q.path(seq), encodeRecord(item.body), 0600)
		}
		if err != nil {
			q.fail(err)
		}
		q.mu.Lock()
		q.stats.MemoryEvents--
		q.stats.MemoryBytes -= int64(len(item.body))
		if failed || err != nil {
			q.reservedBytes -= size
			// Uma escrita falha pode ter feito rename antes do erro. Não afirmar
			// perda certa desse evento: recovery decidirá se o arquivo existe.
			if failed {
				q.dropLocked(DropFailed)
			}
		} else {
			q.pending = append(q.pending, entry{seq: seq, size: size, at: item.at})
			q.stats.DiskEvents++
			q.stats.DiskBytes += size
		}
		// A vaga em memória volta na publicação do arquivo (ou na falha dela),
		// nunca na confirmação do destino: é o que impede o destino de travar
		// a varredura.
		q.space.Broadcast()
		q.mu.Unlock()
		q.signal()
	}
}

func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *queue) fail(err error) {
	q.mu.Lock()
	q.stats.PersistenceErrors++
	first := q.failed == nil
	if first {
		q.failed = fmt.Errorf("%w: destination %q: %v", ErrQueue, q.name, err)
		q.stats.State = "failed"
	}
	// A falha pode vir do remetente com o gravador parado no disco: só este
	// aviso acorda quem espera vaga para que desista com destination_failed.
	q.space.Broadcast()
	cancel := q.cancel
	q.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if first {
		q.log.Error("alarm.delivery.failed", "alarm delivery queue stopped; restart after repairing storage", "destination", q.name, "error", err.Error())
	}
}

func (q *queue) start(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started || q.closed || q.failed != nil || q.transport == nil {
		return
	}
	q.started = true
	q.stats.State = "running"
	ctx, q.cancel = context.WithCancel(ctx)
	go func() {
		defer func() {
			q.mu.Lock()
			if q.stats.State == "running" {
				q.stats.State = "ready"
			}
			q.mu.Unlock()
			close(q.senderDone)
		}()
		q.sendLoop(ctx)
	}()
}

func (q *queue) close() error {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		close(q.input)
		// Quem espera vaga desiste com `closed` sem esperar o gravador, que
		// pode estar parado no disco enquanto o close o aguarda abaixo.
		q.space.Broadcast()
		cancel, started := q.cancel, q.started
		q.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		<-q.writerDone
		if started {
			<-q.senderDone
		}
		if q.transport != nil {
			q.transport.close()
		}
		q.mu.Lock()
		if q.failed == nil {
			q.stats.State = "closed"
		}
		q.mu.Unlock()
	})
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.failed
}

func (q *queue) snapshot() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.stats
	s.Dropped = make(map[string]uint64, len(q.stats.Dropped))
	for k, v := range q.stats.Dropped {
		s.Dropped[k] = v
	}
	if q.head < len(q.pending) {
		s.OldestPendingSeconds = max(0, time.Since(q.pending[q.head].at).Seconds())
	}
	return s
}

// reportDrops roda no gravador, nunca sob o lock de admissão nem na varredura.
func (q *queue) reportDrops(reported *uint64) {
	s := q.snapshot()
	var total uint64
	for _, count := range s.Dropped {
		total += count
	}
	if total != *reported {
		q.log.Warn("alarm.delivery.dropped", "alarm events were dropped for this destination", "destination", q.name, "dropped", total)
		*reported = total
	}
}
