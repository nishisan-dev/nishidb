// Package seriesdelivery publica as amostras dos contadores, sem garantia de
// entrega, em destinos Kafka nomeados. A capacidade de cada destino conta os
// registros em voo até o callback deles, e o Publish ESPERA vaga quando ela
// acaba (contrapressão): a memória continua limitada por MaxRecords e
// MaxBytes, e a varredura anda no ritmo do destino em vez de descartar.
package seriesdelivery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/series"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Os limites de transporte valem por destino global, somando todos os
// contadores que o usam. São a contabilidade LOCAL, que é o limite que vale:
// o do kgo é configurado com folga acima deles (ver kgoBufferSlack).
const (
	MaxRecords = 10_000
	MaxBytes   = 16 << 20
	// Folga reservada para o enquadramento do lote Kafka.
	recordFramingBytes = 128
)

// kgoBufferSlack multiplica os limites locais para formar os do kgo.
//
// O kgo chama o callback do usuário ANTES de decrementar o próprio contador de
// registros e bytes (franz-go v1.21.6, producer.go:854 chama a promessa e só
// em :865-866 decrementa bufferedRecords/bufferedBytes). Com limites
// idênticos, o Publish que o callback acorda pela vaga local chegaria ao
// TryProduce com o kgo ainda contando o registro que acabou de sair, e o kgo
// recusaria com ErrMaxBuffered — o `buffer_full` voltaria escondido atrás da
// contrapressão. As promessas correm num worker ÚNICO por cliente
// (producer.go:745-761, `first` da promiseBatch), registro a registro, então
// o kgo conta no máximo UM registro além da contabilidade local; dobrar o
// limite é folga de sobra, e a contabilidade local continua sendo o teto de
// memória.
const kgoBufferSlack = 2

var dropReasons = [...]string{"buffer_full", "message_too_large", "timeout", "publish_error", "shutdown"}

// DropReasons returns the bounded label vocabulary.
func DropReasons() []string { return append([]string(nil), dropReasons[:]...) }

type route struct{ dataset, counter string }

type producer interface {
	TryProduce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Close()
}

type destination struct {
	maxRecordBytes int64
	client         producer
	topic          string
	timeout        time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	// space acorda quem espera vaga. É um sync.Cond sobre mu, e não um canal
	// de geração, porque a condição já mora sob mu (records, bytes, closed) e
	// a espera precisa reavaliá-la inteira a cada despertar. Todo despertar é
	// Broadcast, nunca Signal: com o limite de bytes e tamanhos mistos, o
	// único acordado por um Signal pode ser quem ainda não cabe, e quem
	// caberia dormiria até a próxima liberação — ou para sempre, se ela não
	// vier. Justiça não é exigida; progresso é.
	space          sync.Cond
	closed         bool
	records, bytes int64
	offers         sync.WaitGroup
	completions    sync.WaitGroup
	// waitHook é chamado, sob mu, a cada vez que uma admissão vai dormir.
	// Exclusivo de teste: conta despertares sem depender de prazo.
	waitHook func()
}

// newDestination monta o destino com a espera ligada ao seu lock.
func newDestination(client producer, topic string, timeout time.Duration) *destination {
	ctx, cancel := context.WithCancel(context.Background())
	d := &destination{maxRecordBytes: config.DefaultKafkaMessageBytes - recordFramingBytes, client: client, topic: topic, timeout: timeout, ctx: ctx, cancel: cancel}
	d.space.L = &d.mu
	return d
}

type output struct {
	d        *destination
	dataset  string
	stats    protocol.SeriesDeliveryStats // protected by d.mu
	reported uint64                       // owned by the manager's logging loop
}

// Manager owns one producer per referenced global destination. Routes are
// immutable once constructed; only counters and capacity are mutable.
type Manager struct {
	destinations []*destination
	routes       map[route][]series.Output
	outputs      []*output
	log          *logging.Logger
	done         chan struct{}
	stop         chan struct{}
	once         sync.Once
}

// Open validates references and local credentials without requiring a broker.
func Open(cfg *config.Config, specs map[string]*schema.DatasetSpec, log *logging.Logger) (*Manager, error) {
	if err := cfg.ValidateKafkaTargets(); err != nil {
		return nil, err
	}
	known := make(map[string]bool)
	for _, k := range cfg.KafkaTargets {
		known[k.Name] = true
	}
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	used := make(map[string]bool)
	for _, name := range names {
		for _, c := range specs[name].Counters {
			if c.Emit == nil {
				continue
			}
			seen := make(map[string]bool)
			for _, target := range c.Emit.Targets {
				if seen[target] || target != "file" && !known[target] {
					return nil, fmt.Errorf("%w: dataset %q counter %q unknown or duplicate target %q", config.ErrKafkaTarget, name, c.Name, target)
				}
				seen[target] = true
				if target != "file" {
					used[target] = true
				}
			}
		}
	}
	if log == nil {
		log = logging.Discard()
	}
	m := &Manager{routes: make(map[route][]series.Output), log: log.Component("series"), stop: make(chan struct{}), done: make(chan struct{})}
	byName := make(map[string]*destination)
	// No logging loop starts until all producers and routes have been built.
	fail := func(err error) (*Manager, error) {
		for _, d := range m.destinations {
			d.cancel()
			d.client.Close()
		}
		return nil, err
	}
	for _, k := range cfg.KafkaTargets {
		tlsConfig, mechanism, err := kafka.ResolveSecurity(k.Security)
		if err != nil {
			return fail(fmt.Errorf("%w: destination %q: %v", config.ErrKafkaTarget, k.Name, err))
		}
		if !used[k.Name] {
			continue
		}
		timeout, err := k.DeliveryTimeout()
		if err != nil {
			return fail(err)
		}
		messageBytes, err := k.MessageBytes()
		if err != nil {
			return fail(err)
		}
		// Sem kgo.RecordDeliveryTimeout, de propósito: o kgo mede esse prazo
		// a partir do Timestamp do PRIMEIRO registro do lote
		// (franz-go v1.21.6, sink.go:2013-2017), conferido na montagem de cada
		// requisição (tryAddBatch, sink.go:2143) e na retentativa de um lote
		// (handleRetryBatches, sink.go:1368). O Timestamp é o at da passada —
		// a fronteira, que fica no passado durante toda a passada e mais ainda
		// na passada atrasada —, então todo registro publicado mais de
		// `timeout` depois do at expiraria antes de sair. O prazo vale pelo
		// contexto por registro do Publish, criado na ADMISSÃO, que o kgo
		// confere antes (maybeFailErr, sink.go:1921-1930) e também na espera de
		// metadado de tópico desconhecido.
		opts := []kgo.Opt{
			kgo.SeedBrokers(k.BootstrapServers...), kgo.DefaultProduceTopic(k.Topic),
			kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(),
			kgo.RecordRetries(0), kgo.UnknownTopicRetries(0),
			kgo.ProducerBatchMaxBytes(int32(messageBytes)),
			kgo.MaxBufferedRecords(kgoBufferSlack * MaxRecords), kgo.MaxBufferedBytes(kgoBufferSlack * MaxBytes),
			kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
			kgo.ProduceRequestTimeout(max(timeout, 100*time.Millisecond)),
			kgo.RequestTimeoutOverhead(100 * time.Millisecond), kgo.DialTimeout(timeout),
		}
		if tlsConfig != nil {
			opts = append(opts, kgo.DialTLSConfig(tlsConfig))
		}
		if mechanism != nil {
			opts = append(opts, kgo.SASL(mechanism))
		}
		client, err := kgo.NewClient(opts...)
		if err != nil {
			return fail(fmt.Errorf("%w: destination %q: %v", config.ErrKafkaTarget, k.Name, err))
		}
		d := newDestination(client, k.Topic, timeout)
		d.maxRecordBytes = min(messageBytes-recordFramingBytes, MaxBytes)
		m.destinations = append(m.destinations, d)
		byName[k.Name] = d
	}
	for _, name := range names {
		for _, c := range specs[name].Counters {
			if c.Emit == nil {
				continue
			}
			for _, target := range c.Emit.Targets {
				if target == "file" {
					continue
				}
				o := &output{d: byName[target], dataset: name, stats: protocol.SeriesDeliveryStats{Counter: c.Name, Destination: target, Dropped: make(map[string]uint64)}}
				for _, reason := range dropReasons {
					o.stats.Dropped[reason] = 0
				}
				m.outputs = append(m.outputs, o)
				r := route{name, c.Name}
				m.routes[r] = append(m.routes[r], o)
			}
		}
	}
	if len(m.outputs) == 0 {
		close(m.done)
	} else {
		go m.reportLoop()
	}
	return m, nil
}

// Outputs returns immutable bindings for one counter.
func (m *Manager) Outputs(dataset, counter string) []series.Output {
	if m == nil {
		return nil
	}
	return m.routes[route{dataset, counter}]
}

// Publish admite uma amostra no destino.
//
// Destino fechado (`shutdown`) e registro maior que o lote do Kafka
// (`message_too_large`) descartam na hora. Capacidade cheia — registros OU
// bytes, contando os em voo até o callback — ESPERA: a espera termina pela
// vaga liberada por um callback, pelo fechamento do destino (`shutdown`) ou
// pelo cancelamento de ctx (`shutdown`, e o erro de ctx é devolvido para o
// chamador abortar a passada). A cada despertar todas as condições são
// reavaliadas. O ctx é conferido também na ENTRADA: cancelado não admite mais
// nada, nem com vaga.
//
// Devolve erro SÓ no cancelamento de ctx. Falha do Kafka não é erro do
// Publish: ela aparece no callback, como descarte contado.
//
// O tempo esperado acumula em AdmissionWaitSeconds da saída. O
// context.AfterFunc que acorda a espera no cancelamento só é registrado no
// caminho de espera: no caminho sem espera ele seria uma alocação por
// registro (TestPublishWithoutWaitKeepsItsMeasuredAllocations).
func (o *output) Publish(ctx context.Context, key, body []byte, at time.Time) error {
	d := o.d
	n := int64(len(key)) + int64(len(body))
	d.mu.Lock()
	var waitStart time.Time
	var stopWake func() bool
	for {
		reason := ""
		switch {
		case d.closed, ctx.Err() != nil:
			reason = "shutdown"
		case n > d.maxRecordBytes:
			reason = "message_too_large"
		}
		if reason != "" {
			o.endWaitLocked(waitStart)
			o.stats.Dropped[reason]++
			d.mu.Unlock()
			if stopWake != nil {
				stopWake()
			}
			return ctx.Err()
		}
		if d.records < MaxRecords && n <= MaxBytes-d.bytes {
			break
		}
		if waitStart.IsZero() {
			waitStart = time.Now()
			stopWake = context.AfterFunc(ctx, func() {
				d.mu.Lock()
				d.space.Broadcast()
				d.mu.Unlock()
			})
		}
		if d.waitHook != nil {
			d.waitHook()
		}
		d.space.Wait()
	}
	o.endWaitLocked(waitStart)
	d.records++
	d.bytes += n
	o.stats.PendingRecords++
	o.stats.PendingBytes += n
	d.offers.Add(1)
	d.completions.Add(1)
	d.mu.Unlock()
	if stopWake != nil {
		stopWake()
	}
	defer d.offers.Done()
	// A admissão vem antes da alocação. Nenhum callback retém o scratch do
	// emissor: chave e corpo são copiados aqui.
	rctx, cancel := context.WithTimeout(d.ctx, d.timeout)
	r := &kgo.Record{Topic: d.topic, Key: append([]byte(nil), key...), Value: append([]byte(nil), body...), Timestamp: at}
	d.client.TryProduce(rctx, r, func(_ *kgo.Record, err error) {
		defer d.completions.Done()
		cancel()
		d.mu.Lock()
		defer d.mu.Unlock()
		d.records--
		d.bytes -= n
		o.stats.PendingRecords--
		o.stats.PendingBytes -= n
		d.space.Broadcast()
		if err == nil {
			o.stats.Delivered++
			o.stats.LastSuccessTimestamp = float64(time.Now().UnixMilli()) / 1000
			return
		}
		o.stats.Dropped[dropReason(err)]++
	})
	return nil
}

// endWaitLocked acumula a espera por vaga; waitStart zero é admissão sem
// espera, e não soma nada.
func (o *output) endWaitLocked(waitStart time.Time) {
	if !waitStart.IsZero() {
		o.stats.AdmissionWaitSeconds += time.Since(waitStart).Seconds()
	}
}

// dropReason traduz a falha de um registro no motivo do descarte.
//
// `buffer_full` não sai mais da admissão, que espera em vez de descartar; só
// o ErrMaxBuffered do kgo ainda o produz, e com a folga de kgoBufferSlack ele
// não deveria acontecer. Continua no vocabulário porque é contrato de rótulo.
func dropReason(err error) string {
	var networkError net.Error
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, kgo.ErrClientClosed), errors.Is(err, kgo.ErrAborting):
		return "shutdown"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrRecordTimeout), errors.Is(err, kerr.RequestTimedOut), errors.As(err, &networkError) && networkError.Timeout():
		return "timeout"
	case errors.Is(err, kerr.MessageTooLarge):
		return "message_too_large"
	case errors.Is(err, kgo.ErrMaxBuffered):
		return "buffer_full"
	default:
		return "publish_error"
	}
}

func (o *output) snapshot() protocol.SeriesDeliveryStats {
	o.d.mu.Lock()
	defer o.d.mu.Unlock()
	s := o.stats
	s.Dropped = make(map[string]uint64, len(dropReasons))
	for reason, n := range o.stats.Dropped {
		s.Dropped[reason] = n
	}
	return s
}

// Stats is the common source for /v1/stats and Prometheus.
func (m *Manager) Stats(dataset string) []protocol.SeriesDeliveryStats {
	if m == nil {
		return nil
	}
	var out []protocol.SeriesDeliveryStats
	for _, o := range m.outputs {
		if o.dataset == dataset {
			out = append(out, o.snapshot())
		}
	}
	return out
}

func (m *Manager) report() {
	for _, o := range m.outputs {
		s := o.snapshot()
		var total uint64
		for _, n := range s.Dropped {
			total += n
		}
		if total == o.reported {
			continue
		}
		m.log.Warn("series.delivery.dropped", "counter samples not confirmed or discarded", "dataset", o.dataset, "counter", s.Counter, "destination", s.Destination, "dropped", s.Dropped)
		o.reported = total
	}
}

func (m *Manager) reportLoop() {
	defer close(m.done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.report()
		case <-m.stop:
			return
		}
	}
}

// Close cancels pending records without flushing or waiting for broker recovery.
// The caller stops emitters first. Concurrent offers remain safe and bounded.
func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		close(m.stop)
		<-m.done
		for _, d := range m.destinations {
			d.mu.Lock()
			d.closed = true
			// O fechamento acorda quem espera vaga: sem isto, um Publish em
			// espera só sairia pelo callback de um registro em voo, e o
			// destino sem nada em voo o deixaria dormindo para sempre.
			d.space.Broadcast()
			d.mu.Unlock()
			d.cancel()
		}
		for _, d := range m.destinations {
			d.offers.Wait()
			d.client.Close()
			// Client.Close fails buffered records but may return before the
			// promise worker finishes. Wait only for local callback accounting.
			d.completions.Wait()
		}
		m.report()
	})
}
