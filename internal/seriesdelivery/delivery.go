// Package seriesdelivery publishes best-effort counter samples to named Kafka
// destinations. Capacity includes records in flight and their callbacks.
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

// Transport limits apply per global destination, across all its counters.
const (
	MaxRecords = 10_000
	MaxBytes   = 16 << 20
	// Match franz-go's batch limit, including space for Kafka framing.
	maxRecordBytes = 1_000_012 - 128
)

var dropReasons = [...]string{"buffer_full", "message_too_large", "timeout", "publish_error", "shutdown"}

// DropReasons returns the bounded label vocabulary.
func DropReasons() []string { return append([]string(nil), dropReasons[:]...) }

type route struct{ dataset, counter string }

type producer interface {
	TryProduce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Close()
}

type destination struct {
	client         producer
	topic          string
	timeout        time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	closed         bool
	records, bytes int64
	offers         sync.WaitGroup
	completions    sync.WaitGroup
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
	if err := cfg.ValidateSeriesDelivery(); err != nil {
		return nil, err
	}
	known := make(map[string]bool)
	for _, k := range cfg.SeriesDelivery.Kafka {
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
					return nil, fmt.Errorf("%w: dataset %q counter %q unknown or duplicate target %q", config.ErrSeriesDelivery, name, c.Name, target)
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
	for _, k := range cfg.SeriesDelivery.Kafka {
		tlsConfig, mechanism, err := kafka.ResolveSecurity(k.Security)
		if err != nil {
			return fail(fmt.Errorf("%w: destination %q: %v", config.ErrSeriesDelivery, k.Name, err))
		}
		if !used[k.Name] {
			continue
		}
		timeout, err := k.DeliveryTimeout()
		if err != nil {
			return fail(err)
		}
		opts := []kgo.Opt{
			kgo.SeedBrokers(k.BootstrapServers...), kgo.DefaultProduceTopic(k.Topic),
			kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(),
			kgo.RecordRetries(0), kgo.UnknownTopicRetries(0),
			kgo.MaxBufferedRecords(MaxRecords), kgo.MaxBufferedBytes(MaxBytes),
			kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
			kgo.RecordDeliveryTimeout(max(timeout, time.Second)),
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
			return fail(fmt.Errorf("%w: destination %q: %v", config.ErrSeriesDelivery, k.Name, err))
		}
		ctx, cancel := context.WithCancel(context.Background())
		d := &destination{client: client, topic: k.Topic, timeout: timeout, ctx: ctx, cancel: cancel}
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

func (o *output) Publish(key, body []byte, at time.Time) {
	d := o.d
	n := int64(len(key)) + int64(len(body))
	d.mu.Lock()
	reason := ""
	switch {
	case d.closed:
		reason = "shutdown"
	case n > maxRecordBytes:
		reason = "message_too_large"
	case d.records >= MaxRecords || n > MaxBytes-d.bytes:
		reason = "buffer_full"
	}
	if reason != "" {
		o.stats.Dropped[reason]++
		d.mu.Unlock()
		return
	}
	d.records++
	d.bytes += n
	o.stats.PendingRecords++
	o.stats.PendingBytes += n
	d.offers.Add(1)
	d.completions.Add(1)
	d.mu.Unlock()
	defer d.offers.Done()
	// Admission precedes allocation. No callback retains the emitter's scratch.
	ctx, cancel := context.WithTimeout(d.ctx, d.timeout)
	r := &kgo.Record{Topic: d.topic, Key: append([]byte(nil), key...), Value: append([]byte(nil), body...), Timestamp: at}
	d.client.TryProduce(ctx, r, func(_ *kgo.Record, err error) {
		defer d.completions.Done()
		cancel()
		d.mu.Lock()
		defer d.mu.Unlock()
		d.records--
		d.bytes -= n
		o.stats.PendingRecords--
		o.stats.PendingBytes -= n
		if err == nil {
			o.stats.Delivered++
			o.stats.LastSuccessTimestamp = float64(time.Now().UnixMilli()) / 1000
			return
		}
		o.stats.Dropped[dropReason(err)]++
	})
}

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
