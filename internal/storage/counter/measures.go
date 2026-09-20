package counter

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// MeasureDefinition é a definição persistida de uma taxa; Unit é a unidade em nanossegundos.
type MeasureDefinition struct {
	Name         string
	Window, Unit time.Duration
}

// ErrMeasureSnapshotCorrupt identifica estado durável ausente ou inválido.
var ErrMeasureSnapshotCorrupt = errors.New("counter: corrupt measure snapshot")

const measureBuckets = 60
const measureEntrySize = measureBuckets * 16 // época i64 e contagem i64 por balde

type measureState struct {
	mu        sync.RWMutex
	def       MeasureDefinition
	region    *mmap.Region
	positions map[uint32]uint32
	free      []uint32
	next      uint32
	epoch     int64
	active    bool // novas definições só recebem eventos depois da recovery
}

type measureHeader struct {
	Definition MeasureDefinition
	Epoch      int64
	Groups     uint32
}

func validateMeasures(def Definition) error {
	if len(def.Measures) > 8 || (def.DisableTotal && len(def.Measures) == 0) {
		return errors.New("counter: expected 1 to 8 measures when total is disabled")
	}
	seen := make(map[string]bool)
	for _, m := range def.Measures {
		if m.Name == "" || strings.ContainsAny(m.Name, "/\\.") || seen[m.Name] {
			return fmt.Errorf("counter: invalid or repeated measure name %q", m.Name)
		}
		seen[m.Name] = true
		if m.Window < time.Minute || m.Window > 30*24*time.Hour || m.Window%time.Minute != 0 {
			return fmt.Errorf("counter: invalid measure window %s", m.Window)
		}
		if m.Unit != time.Second && m.Unit != time.Minute && m.Unit != time.Hour && m.Unit != 24*time.Hour {
			return fmt.Errorf("counter: invalid measure unit %s", m.Unit)
		}
	}
	return nil
}

// MeasureResetNames lista definições novas ou alteradas, antes de ActivateMeasures.
func (c *Counter) MeasureResetNames() []string {
	var names []string
	for _, m := range c.measures {
		if !m.active {
			names = append(names, m.def.Name)
		}
	}
	return names
}

// OpenMeasures descarta o mmap de trabalho e restaura apenas a geração referenciada pelo manifest.
// Configuração diferente inicia janela nova; arquivo ausente/corrompido não é reset silencioso.
func (c *Counter) OpenMeasures(epoch uint64, persisted bool, reset bool) error {
	for _, def := range c.def.Measures {
		path := filepath.Join(c.dir, "measure_"+def.Name+".work")
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		region, err := mmap.Open(path, measureEntrySize*16)
		if err != nil {
			return err
		}
		c.measures = append(c.measures, &measureState{def: def, region: region, positions: make(map[uint32]uint32)})
	}
	if !persisted {
		return nil
	}
	f, err := os.Open(c.measurePath(epoch))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: counter %s: required measure snapshot: %w", ErrMeasureSnapshotCorrupt, c.def.Name, err)
		}
		return measureReadError(err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return measureReadError(err)
	}
	if st.Size() < 20 {
		return fmt.Errorf("%w: counter %s: truncated measure snapshot", ErrMeasureSnapshotCorrupt, c.def.Name)
	}
	hash := crc32.NewIEEE()
	r := io.TeeReader(io.LimitReader(f, st.Size()-4), hash)
	var pre [16]byte
	if _, err := io.ReadFull(r, pre[:]); err != nil {
		return measureReadError(err)
	}
	if string(pre[:4]) != "NDR1" {
		return fmt.Errorf("%w: counter %s: invalid measure snapshot version", ErrMeasureSnapshotCorrupt, c.def.Name)
	}
	if binary.LittleEndian.Uint64(pre[4:12]) != epoch {
		return fmt.Errorf("%w: counter %s: measure snapshot generation mismatch", ErrMeasureSnapshotCorrupt, c.def.Name)
	}
	count := binary.LittleEndian.Uint32(pre[12:])
	if count == 0 || count > 8 {
		return fmt.Errorf("%w: counter %s: invalid measure count", ErrMeasureSnapshotCorrupt, c.def.Name)
	}
	var buf [measureEntrySize + 4]byte
	seen := make(map[string]bool)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return measureReadError(err)
		}
		n := binary.LittleEndian.Uint32(buf[:4])
		if n == 0 || n > 4096 {
			return fmt.Errorf("%w: counter %s: invalid measure header length", ErrMeasureSnapshotCorrupt, c.def.Name)
		}
		hb := make([]byte, n)
		if _, err := io.ReadFull(r, hb); err != nil {
			return measureReadError(err)
		}
		var header measureHeader
		if err := json.Unmarshal(hb, &header); err != nil {
			return fmt.Errorf("%w: %w", ErrMeasureSnapshotCorrupt, err)
		}
		if seen[header.Definition.Name] || header.Groups > c.def.MaxGroups {
			return fmt.Errorf("%w: counter %s: invalid measure snapshot groups", ErrMeasureSnapshotCorrupt, c.def.Name)
		}
		seen[header.Definition.Name] = true
		var target *measureState
		for _, m := range c.measures {
			if !reset && m.def == header.Definition {
				target = m
				m.epoch = header.Epoch
				m.active = true
				break
			}
		}
		for j := uint32(0); j < header.Groups; j++ {
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return measureReadError(err)
			}
			id := binary.LittleEndian.Uint32(buf[:4])
			if target == nil {
				continue
			}
			if id == 0 || id >= c.Limit() {
				return fmt.Errorf("%w: counter %s: unknown measure group %d", ErrMeasureSnapshotCorrupt, c.def.Name, id)
			}
			if _, exists := target.positions[id]; exists {
				return fmt.Errorf("%w: counter %s: repeated measure group %d", ErrMeasureSnapshotCorrupt, c.def.Name, id)
			}
			if _, live := c.dict.Key(id); !live {
				continue
			}
			pos, err := target.position(id)
			if err != nil {
				return measureReadError(err)
			}
			target.region.WithBytes(int(pos)*measureEntrySize, measureEntrySize, func(b []byte) { copy(b, buf[4:]) })
		}
	}
	var tail [1]byte
	if n, err := r.Read(tail[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("%w: counter %s: trailing measure snapshot bytes", ErrMeasureSnapshotCorrupt, c.def.Name)
	}
	if _, err := io.ReadFull(f, buf[:4]); err != nil {
		return measureReadError(err)
	}
	if binary.LittleEndian.Uint32(buf[:4]) != hash.Sum32() {
		return fmt.Errorf("%w: counter %s: measure snapshot checksum mismatch", ErrMeasureSnapshotCorrupt, c.def.Name)
	}
	c.measureSnapshotBytes.Store(st.Size())
	return nil
}

func (m *measureState) position(id uint32) (uint32, error) {
	if p, ok := m.positions[id]; ok {
		return p, nil
	}
	p := m.next
	if len(m.free) > 0 {
		p = m.free[len(m.free)-1]
		m.free = m.free[:len(m.free)-1]
	} else {
		needed := int64(p+1) * measureEntrySize
		if needed > m.region.Size() {
			if err := m.region.Grow(max(needed, m.region.Size()*2)); err != nil {
				return 0, err
			}
		}
		m.next++
	}
	m.region.WithBytes(int(p)*measureEntrySize, measureEntrySize, func(b []byte) { clear(b) })
	m.positions[id] = p
	return p, nil
}

func (m *measureState) observe(id uint32, at int64) error {
	if !m.active {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	width := m.def.Window.Milliseconds() / measureBuckets
	epoch := max(m.epoch, at/width)
	if epoch < 0 {
		return errors.New("counter: measure timestamp precedes Unix epoch")
	}
	m.epoch = epoch
	p, err := m.position(id)
	if err != nil {
		return err
	}
	var overflow bool
	m.region.WithBytes(int(p)*measureEntrySize+int(epoch%measureBuckets)*16, 16, func(b []byte) {
		if int64(binary.LittleEndian.Uint64(b)) != epoch {
			clear(b)
			binary.LittleEndian.PutUint64(b, uint64(epoch))
		}
		n := binary.LittleEndian.Uint64(b[8:])
		if n >= math.MaxInt64 {
			overflow = true
			return
		}
		binary.LittleEndian.PutUint64(b[8:], n+1)
	})
	if overflow {
		return errors.New("counter: measure bucket overflow")
	}
	return nil
}

func (m *measureState) count(id uint32, at int64) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.positions[id]
	if !ok {
		return 0
	}
	epoch := max(m.epoch, at/(m.def.Window.Milliseconds()/measureBuckets))
	var n int64
	m.region.WithBytes(int(p)*measureEntrySize, measureEntrySize, func(b []byte) {
		for i := 0; i < measureBuckets; i++ {
			slot := b[i*16:]
			e := int64(binary.LittleEndian.Uint64(slot))
			if e > epoch-measureBuckets && e <= epoch {
				value := int64(binary.LittleEndian.Uint64(slot[8:]))
				// Saturar protege a projeção e a amostragem de wraparound em
				// uma janela cuja soma exceda o intervalo inteiro.
				n += min(value, math.MaxInt64-n)
			}
		}
	})
	return n
}

// MeasureCount lê o numerador sem manter um total acumulado. O instante é único por passada.
func (c *Counter) MeasureCount(id uint32, index int, at int64) int64 {
	if index < 0 || index >= len(c.measures) {
		return 0
	}
	return c.measures[index].count(id, at)
}

// ActivateMeasures habilita definições novas depois da recovery, sem backfill.
func (c *Counter) ActivateMeasures() {
	for _, m := range c.measures {
		m.active = true
	}
}

func (c *Counter) observeMeasures(entry uint32, payload []byte) error {
	if entry&conditionBit == 0 || len(c.measures) == 0 {
		return nil
	}
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			break
		}
		if id == c.def.WriteInstantField && v.Type == types.TypeTimestamp {
			for _, m := range c.measures {
				if err := m.observe(entry&^conditionBit, v.I64); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return fmt.Errorf("counter %s: accepted event has no write timestamp", c.def.Name)
}

func (c *Counter) measurePath(epoch uint64) string {
	return filepath.Join(c.dir, fmt.Sprintf("measures.%d.snapshot", epoch))
}

// SaveMeasures grava uma geração imutável antes do manifest, sob o writer do Store.
func (c *Counter) SaveMeasures(epoch uint64) error {
	if len(c.measures) == 0 {
		return nil
	}
	var size int64
	err := atomicfile.WriteStream(c.measurePath(epoch), 0o644, func(w io.Writer) error {
		hash := crc32.NewIEEE()
		out := io.MultiWriter(w, hash, byteCounter{size: &size})
		var buf [measureEntrySize + 4]byte
		copy(buf[:4], "NDR1")
		binary.LittleEndian.PutUint64(buf[4:12], epoch)
		binary.LittleEndian.PutUint32(buf[12:16], uint32(len(c.measures)))
		if _, err := out.Write(buf[:16]); err != nil {
			return err
		}
		for _, m := range c.measures {
			if err := m.save(out, buf[:]); err != nil {
				return err
			}
		}
		binary.LittleEndian.PutUint32(buf[:4], hash.Sum32())
		_, err := w.Write(buf[:4])
		return err
	})
	if err == nil {
		c.measureSnapshotBytes.Store(size + 4)
	}
	return err
}

type byteCounter struct{ size *int64 }

func (c byteCounter) Write(b []byte) (int, error) { *c.size += int64(len(b)); return len(b), nil }

func (m *measureState) save(w io.Writer, buf []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, err := json.Marshal(measureHeader{m.def, m.epoch, uint32(len(m.positions))})
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(buf, uint32(len(h)))
	if _, err = w.Write(buf[:4]); err != nil {
		return err
	}
	if _, err = w.Write(h); err != nil {
		return err
	}
	ids := make([]uint32, 0, len(m.positions))
	for id := range m.positions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		binary.LittleEndian.PutUint32(buf, id)
		m.region.WithBytes(int(m.positions[id])*measureEntrySize, measureEntrySize, func(b []byte) { copy(buf[4:], b) })
		if _, err = w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

// CleanupMeasures só remove gerações antigas depois do commit do manifest.
func (c *Counter) CleanupMeasures(epoch uint64) error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	keep := filepath.Base(c.measurePath(epoch))
	for _, e := range entries {
		n := e.Name()
		obsoleteWork := strings.HasPrefix(n, "measure_") && strings.HasSuffix(n, ".work")
		for _, m := range c.measures {
			if n == "measure_"+m.def.Name+".work" {
				obsoleteWork = false
				break
			}
		}
		if obsoleteWork || (strings.HasPrefix(n, "measures.") && (strings.HasSuffix(n, ".snapshot") || strings.HasSuffix(n, ".snapshot.tmp")) && (n != keep || len(c.measures) == 0)) {
			if err := os.Remove(filepath.Join(c.dir, n)); err != nil {
				return err
			}
		}
	}
	if len(c.measures) == 0 {
		c.measureSnapshotBytes.Store(0)
	}
	return nil
}

func (c *Counter) measureBytes() int64 {
	n := c.measureSnapshotBytes.Load()
	for _, m := range c.measures {
		n += m.region.Size()
	}
	return n
}

func (c *Counter) closeMeasures() error {
	var errs []error
	for _, m := range c.measures {
		errs = append(errs, m.region.Close())
	}
	return errors.Join(errs...)
}

func (c *Counter) retireMeasures(id uint32, at int64) bool {
	for _, m := range c.measures {
		if m.count(id, at) > 0 {
			return false
		}
	}
	for _, m := range c.measures {
		m.mu.Lock()
		if p, ok := m.positions[id]; ok {
			delete(m.positions, id)
			m.free = append(m.free, p)
		}
		m.mu.Unlock()
	}
	return true
}

func measureReadError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %w", ErrMeasureSnapshotCorrupt, err)
	}
	return err
}
