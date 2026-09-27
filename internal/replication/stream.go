// Package replication implements the bounded, volatile change feed shared by
// followers. It never performs network or disk I/O on the publishing goroutine.
package replication

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrResyncRequired means the requested cursor is no longer available.
var ErrResyncRequired = errors.New("replication: RESYNC_REQUIRED")

// ErrClosed means the change feed has been shut down.
var ErrClosed = errors.New("replication: closed")

// Operation identifies the materialized change represented by an entry.
type Operation uint8

const (
	// Insert adds the first materialized version of a key.
	Insert Operation = iota + 1
	// Update replaces the materialized version of an existing key.
	Update
	// Delete writes a tombstone for a key.
	Delete
	// Schema precedes records using newly discovered fields.
	Schema
	// Reset replaces a query projection, never a base or history table.
	Reset
	// Retain advances the history retention floor (Record.RowID).
	Retain
)

// Entry is one ordered materialized change in the feed.
type Entry struct {
	Sequence  uint64       `json:"sequence"`
	Table     string       `json:"table"`
	Operation Operation    `json:"operation"`
	Record    types.Record `json:"record"`
	// At é o instante em que a mudança foi publicada no feed. Quando a
	// coalescência substitui uma entrada pendente, vale o instante da ÚLTIMA
	// publicação, junto com o último estado.
	At time.Time `json:"at"`
}

// Options govern one dataset's feed. The memory ceiling includes batches that
// have been evicted but are still held by a network reader.
type Options struct {
	Interval         time.Duration
	MaxMessages      int
	MaxBatchBytes    int64
	MaxBufferedBytes int64
}

// DefaultOptions returns the bounded feed defaults used by configuration.
func DefaultOptions() Options {
	return Options{5 * time.Second, 10000, 8 << 20, 64 << 20}
}

// Validate checks interval and memory limits before a feed is opened.
func (o Options) Validate() error {
	if o.Interval < 0 || o.Interval > time.Minute || o.Interval%time.Second != 0 {
		return errors.New("replication: flushInterval must be an integer from 0s to 60s")
	}
	if o.MaxMessages < 1 || o.MaxBatchBytes < 1 || o.MaxBufferedBytes < o.MaxBatchBytes {
		return errors.New("replication: positive limits required; maxBufferedBytes must cover maxBatchBytes")
	}
	return nil
}

// Batch is an immutable, leased group of feed entries.
type Batch struct {
	Epoch    string    `json:"epoch"`
	From     uint64    `json:"from"`
	To       uint64    `json:"to"`
	At       time.Time `json:"at"`
	Reason   string    `json:"reason"`
	Messages int       `json:"messages"`
	Entries  []Entry   `json:"entries"`
	bytes    int64
	refs     int
	retained bool
}

type identity struct {
	table string
	row   types.RowID
}
type pending struct {
	entry  Entry
	memory []byte
}

// Status describes the feed cursor, retention window and flush counters.
type Status struct {
	Epoch           string `json:"epoch"`
	Produced        uint64 `json:"produced"`
	AvailableAfter  uint64 `json:"availableAfter"`
	BufferedBytes   int64  `json:"bufferedBytes"`
	InputMessages   uint64 `json:"inputMessages"`
	OutputMessages  uint64 `json:"outputMessages"`
	EvictedBatches  uint64 `json:"evictedBatches"`
	Flushes         uint64 `json:"flushes"`
	LastFlushReason string `json:"lastFlushReason"`
	// Invalidations conta as vezes em que o feed descartou o intervalo
	// retido e forçou os seguidores a ressincronizar — por Invalidate ou por
	// estouro do teto de memória no Publish.
	Invalidations uint64 `json:"invalidations"`
}

// Stream is a bounded in-memory feed shared by all followers of one dataset.
type Stream struct {
	mu                 sync.Mutex
	opts               Options
	epoch              string
	sequence, floor    uint64
	active             list.List
	last               map[identity]*list.Element
	first              uint64
	started            time.Time
	count              int
	activeBytes, bytes int64
	batches            []*Batch
	changed            chan struct{}
	closed             bool
	stats              Status
}

// New creates a feed with a fresh generation identifier.
func New(o Options) (*Stream, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	s := &Stream{opts: o, epoch: hex.EncodeToString(id[:]), last: make(map[identity]*list.Element), changed: make(chan struct{})}
	return s, nil
}

// Cursor returns the current generation and produced sequence.
func (s *Stream) Cursor() (string, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.sequence
}

func entryBytes(e Entry) int64 {
	return int64(192 + len(e.Table) + len(e.Record.Key) + len(e.Record.Payload))
}

// Publish copies the borrowed record. Replacing a pending operation moves it to
// its last occurrence, without crossing another operation of that entity.
// History rows have distinct RowIDs and consequently never coalesce.
func (s *Stream) Publish(table string, op Operation, r types.Record, at time.Time) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.sequence
	}
	s.sequence++
	s.stats.InputMessages++
	e := Entry{Sequence: s.sequence, Table: table, Operation: op, Record: r, At: at}
	id := identity{table, r.RowID}
	n := s.last[id]
	if op >= Schema || (n != nil && n.Value.(*pending).entry.Operation != op) {
		n = nil
		// The previous operation is a hard boundary. Do not leave its list
		// element indexed, otherwise a later operation of the same kind could
		// incorrectly deduplicate across the boundary.
		delete(s.last, id)
	}
	oldBytes := int64(0)
	if n != nil {
		oldBytes = entryBytes(n.Value.(*pending).entry)
	}
	size := entryBytes(e)
	if s.count > 0 && s.activeBytes-oldBytes+size > s.opts.MaxBatchBytes {
		s.flushLocked(at, "bytes")
		n = nil
		oldBytes = 0
	}
	if !s.makeRoomLocked(size - oldBytes) {
		// All remaining space belongs to in-flight readers, or this single
		// record exceeds the configured ceiling. Lose the interval explicitly;
		// never stall the database or publish a feed with an invisible hole.
		s.invalidateLocked()
		return s.sequence
	}
	if s.count == 0 {
		s.first = e.Sequence
		s.started = at
	}
	var p *pending
	if n == nil {
		p = &pending{}
		n = s.active.PushBack(p)
	} else {
		p = n.Value.(*pending)
		s.active.MoveToBack(n)
	}
	// Capacity is also bounded: shrinking an outlier releases its allocation.
	length := len(r.Key) + len(r.Payload)
	if cap(p.memory) < length || cap(p.memory) > length*2 {
		p.memory = make([]byte, length)
	} else {
		p.memory = p.memory[:length]
	}
	copy(p.memory, r.Key)
	copy(p.memory[len(r.Key):], r.Payload)
	e.Record.Key = p.memory[:len(r.Key):len(r.Key)]
	e.Record.Payload = p.memory[len(r.Key):]
	p.entry = e
	if op < Schema {
		s.last[id] = n
	}
	s.activeBytes += size - oldBytes
	s.bytes += size - oldBytes
	s.count++
	switch {
	case s.opts.Interval == 0:
		s.flushLocked(at, "immediate")
	case s.count >= s.opts.MaxMessages:
		s.flushLocked(at, "messages")
	case s.activeBytes >= s.opts.MaxBatchBytes:
		s.flushLocked(at, "bytes")
	}
	return e.Sequence
}

func (s *Stream) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }

func (s *Stream) makeRoomLocked(need int64) bool {
	for s.bytes+need > s.opts.MaxBufferedBytes && len(s.batches) > 0 {
		b := s.batches[0]
		s.batches[0] = nil
		s.batches = s.batches[1:]
		b.retained = false
		s.floor = b.To
		s.stats.EvictedBatches++
		if b.refs == 0 {
			s.bytes -= b.bytes
		}
	}
	return s.bytes+need <= s.opts.MaxBufferedBytes
}

func (s *Stream) invalidateLocked() {
	s.bytes -= s.activeBytes
	s.active.Init()
	clear(s.last)
	s.count = 0
	s.activeBytes = 0
	for _, b := range s.batches {
		b.retained = false
		if b.refs == 0 {
			s.bytes -= b.bytes
		}
	}
	s.batches = nil
	s.floor = s.sequence
	s.stats.Invalidations++
	s.notifyLocked()
}

// Invalidate is used if a producer can no longer guarantee complete capture.
func (s *Stream) Invalidate() { s.mu.Lock(); defer s.mu.Unlock(); s.invalidateLocked() }

func (s *Stream) flushLocked(at time.Time, reason string) {
	if s.count == 0 {
		return
	}
	b := &Batch{Epoch: s.epoch, From: s.first, To: s.sequence, At: at, Reason: reason, Messages: s.count, bytes: s.activeBytes, retained: true, Entries: make([]Entry, 0, s.active.Len())}
	for n := s.active.Front(); n != nil; n = n.Next() {
		b.Entries = append(b.Entries, n.Value.(*pending).entry)
	}
	s.batches = append(s.batches, b)
	s.stats.OutputMessages += uint64(len(b.Entries))
	s.stats.Flushes++
	s.stats.LastFlushReason = reason
	s.active.Init()
	clear(s.last)
	s.count = 0
	s.activeBytes = 0
	s.notifyLocked()
}

// Flush closes the active interval and returns the produced sequence.
func (s *Stream) Flush(at time.Time, reason string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushLocked(at, reason)
	return s.sequence
}

// Tick closes an interval whose configured delay has elapsed.
func (s *Stream) Tick(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 && at.Sub(s.started) >= s.opts.Interval {
		s.flushLocked(at, "interval")
	}
}

// Start runs the interval timer. A zero interval is event-driven and needs no
// goroutine; message and byte thresholds still flush synchronously in Publish.
func (s *Stream) Start(ctx context.Context) {
	if s.opts.Interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(s.opts.Interval)
		defer t.Stop()
		for {
			select {
			case now := <-t.C:
				s.Tick(now)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Acquire returns a leased immutable batch. Release must be called even when
// the network disconnects. No per-follower copy of the batch is allocated.
//
// Depois do Close, a cauda retida continua sendo servida: o Close faz o flush
// do intervalo ativo justamente para que ele seja entregue. Esgotada a cauda,
// devolve ErrClosed na hora — nunca espera em s.changed, porque o canal criado
// pelo notifyLocked do Close não será fechado por ninguém.
func (s *Stream) Acquire(ctx context.Context, epoch string, after uint64) (*Batch, func(), error) {
	for {
		s.mu.Lock()
		if epoch != s.epoch || after < s.floor || after > s.sequence {
			s.mu.Unlock()
			return nil, nil, ErrResyncRequired
		}
		for _, b := range s.batches {
			if b.To <= after {
				continue
			}
			if b.From > after+1 {
				s.mu.Unlock()
				return nil, nil, ErrResyncRequired
			}
			b.refs++
			s.mu.Unlock()
			var once sync.Once
			return b, func() {
				once.Do(func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					b.refs--
					if b.refs == 0 && !b.retained {
						s.bytes -= b.bytes
					}
				})
			}, nil
		}
		if s.closed {
			s.mu.Unlock()
			return nil, nil, ErrClosed
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-ch:
		}
	}
}

// Check validates that a cursor is still covered by the retained feed.
func (s *Stream) Check(epoch string, after uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch != s.epoch || after < s.floor || after > s.sequence {
		return fmt.Errorf("%w: cursor %d, available (%d,%d]", ErrResyncRequired, after, s.floor, s.sequence)
	}
	return nil
}

// Status returns a point-in-time feed snapshot.
func (s *Stream) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.stats
	out.Epoch = s.epoch
	out.Produced = s.sequence
	out.AvailableAfter = s.floor
	out.BufferedBytes = s.bytes
	return out
}

// Close flushes pending entries and wakes all waiting readers.
func (s *Stream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.flushLocked(time.Now(), "close")
		s.closed = true
		s.notifyLocked()
	}
}
