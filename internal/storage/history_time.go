package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A .tidx is derived from a complete, immutable .dat. It is independent of
// field indexes and their retention window. Missing/untrusted bounds never
// exclude records. No data reads or metadata writes occur in query planning.
type historyTimeBounds struct {
	segment          uint32
	field            types.FieldID
	size             int64
	minimum, maximum int64
	known            bool
}

type historyTimeState struct {
	mu       sync.RWMutex // leaf; never takes writer or retirement locks
	bounds   map[uint32]historyTimeBounds
	workerMu sync.Mutex
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	passMu   sync.Mutex
	closing  atomic.Bool
	active   atomic.Bool
	pending  atomic.Int64
	skipped  atomic.Int64
	rebuilt  atomic.Int64
}

func historyTimePath(dir string, sec uint32) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%08d.tidx", sec))
}

func isHistoryTimeBoundsFile(name string) bool {
	if !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".tidx") {
		return false
	}
	id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".tidx"), 10, 32)
	return err == nil && name == filepath.Base(historyTimePath("", uint32(id)))
}

const historyTimeBoundsSize = 48

var historyTimeCRC = crc32.MakeTable(crc32.Castagnoli)

func (b historyTimeBounds) marshal() []byte {
	out := make([]byte, historyTimeBoundsSize)
	copy(out, "NDBT")
	out[4] = 1
	if b.known {
		out[5] = 1
	}
	binary.LittleEndian.PutUint32(out[8:12], b.segment)
	binary.LittleEndian.PutUint32(out[12:16], uint32(b.field))
	binary.LittleEndian.PutUint64(out[16:24], uint64(b.size))
	binary.LittleEndian.PutUint64(out[24:32], uint64(b.minimum))
	binary.LittleEndian.PutUint64(out[32:40], uint64(b.maximum))
	binary.LittleEndian.PutUint32(out[44:48], crc32.Checksum(out[:44], historyTimeCRC))
	return out
}

func readHistoryTimeBounds(path string, sec uint32, field types.FieldID, size int64) (historyTimeBounds, bool) {
	// Bound reads even if a corrupt derived file is unexpectedly enormous.
	f, err := os.Open(path)
	if err != nil {
		return historyTimeBounds{}, false
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || info.Size() != historyTimeBoundsSize {
		return historyTimeBounds{}, false
	}
	var buf [historyTimeBoundsSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return historyTimeBounds{}, false
	}
	if string(buf[:4]) != "NDBT" || buf[4] != 1 || buf[5] > 1 ||
		binary.LittleEndian.Uint32(buf[12:16]) != uint32(field) ||
		binary.LittleEndian.Uint32(buf[44:]) != crc32.Checksum(buf[:44], historyTimeCRC) {
		return historyTimeBounds{}, false
	}
	b := historyTimeBounds{segment: binary.LittleEndian.Uint32(buf[8:]), field: types.FieldID(binary.LittleEndian.Uint32(buf[12:])),
		size: int64(binary.LittleEndian.Uint64(buf[16:])), minimum: int64(binary.LittleEndian.Uint64(buf[24:])),
		maximum: int64(binary.LittleEndian.Uint64(buf[32:])), known: buf[5] == 1}
	return b, b.segment == sec && b.field == field && b.size == size && (!b.known || b.minimum <= b.maximum)
}

// CanPruneHistoryTime reports whether field is this history's write timestamp.
func (s *Store) CanPruneHistoryTime(field types.FieldID) bool {
	return s.EhHistory() && field != 0 && field == s.writeFieldID
}

// loadHistoryTimeBounds reads only small derived files, never historical data.
func (s *Store) loadHistoryTimeBounds() error {
	if !s.CanPruneHistoryTime(s.writeFieldID) {
		return nil
	}
	refs, err := s.listaSegmentos()
	if err != nil {
		return err
	}
	s.historyTime.bounds = make(map[uint32]historyTimeBounds)
	live := make(map[string]bool, len(refs))
	var pending int64
	for _, ref := range refs {
		live[filepath.Base(historyTimePath(s.dirOfData(), ref.ID))] = true
		if ref.ID >= s.curSecID.Load() {
			continue
		}
		b, ok := readHistoryTimeBounds(historyTimePath(s.dirOfData(), ref.ID), ref.ID, s.writeFieldID, ref.SizeBytes)
		if ok {
			s.historyTime.bounds[ref.ID] = b
		} else {
			pending++
		}
	}
	s.historyTime.pending.Store(pending)
	// A crash may leave a sidecar or temporary file after retiring its data.
	entries, err := os.ReadDir(s.dirOfData())
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if (isHistoryTimeBoundsFile(name) && !live[name]) || strings.HasPrefix(name, ".tidx-tmp-") {
			if err := os.Remove(filepath.Join(s.dirOfData(), name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.opts.Log.Warn("historico.limites_temporais_orfaos", "metadados temporais órfãos não puderam ser removidos; a consulta continua disponível", "arquivo", name, "erro", err.Error())
			}
		}
	}
	return nil
}

// excludedHistorySegments only excludes CLOSED segments with complete bounds.
// The selected set is fixed for this query. Rotation can only make it more
// conservative: a previously active segment remains included after sealing.
func (s *Store) excludedHistorySegments(ctx context.Context, r types.HistoryTimeRange) (map[uint32]bool, error) {
	if !s.CanPruneHistoryTime(r.Field) {
		return nil, nil
	}
	refs, err := s.listaSegmentos()
	if err != nil {
		return nil, err
	}
	current := s.curSecID.Load()
	excluded := make(map[uint32]bool)
	s.historyTime.mu.RLock()
	defer s.historyTime.mu.RUnlock()
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, ok := s.historyTime.bounds[ref.ID]
		if ref.ID < current && ok && b.size == ref.SizeBytes && b.known && !r.Intersects(b.minimum, b.maximum) {
			excluded[ref.ID] = true
		}
	}
	s.historyTime.skipped.Add(int64(len(excluded)))
	return excluded, nil
}

// ScanHistoryTimeRange skips excluded segments before fetching any record.
func (s *Store) ScanHistoryTimeRange(ctx context.Context, snap types.Snapshot, r types.HistoryTimeRange) (types.RowIDIterator, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
	excluded, err := s.excludedHistorySegments(ctx, r)
	if err != nil {
		return nil, err
	}
	limit := snap.LimitRowID
	if r.Empty() {
		limit = snap.BaseRowID
	}
	return &liveIterator{s: s, snap: snap, next: snap.BaseRowID, limit: limit, ctx: ctx, excluded: excluded}, nil
}

// LookupHistoryTimeRange restricts index and fallback sources to the query period.
func (s *Store) LookupHistoryTimeRange(ctx context.Context, snap types.Snapshot, field types.FieldID, v types.Value, r types.HistoryTimeRange) (types.RowIDIterator, error) {
	if err := s.entra(); err != nil {
		return nil, err
	}
	defer s.sai()
	excluded, err := s.excludedHistorySegments(ctx, r)
	if err != nil {
		return nil, err
	}
	return s.lookupIndexedHistoryRange(ctx, snap, field, v, excluded, r.Empty())
}

// StartHistoryTimeBounds starts independently of secondary-index configuration,
// including on replicas. Rebuilding derived metadata never fails the writer.
func (s *Store) StartHistoryTimeBounds(ctx context.Context) {
	if !s.CanPruneHistoryTime(s.writeFieldID) {
		return
	}
	h := &s.historyTime
	h.workerMu.Lock()
	defer h.workerMu.Unlock()
	if h.cancel != nil || h.closing.Load() {
		return
	}
	if err := s.entra(); err != nil {
		return
	}
	s.sai()
	ctx, h.cancel = context.WithCancel(ctx)
	h.active.Store(true)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer h.active.Store(false)
		for {
			if err := s.rebuildHistoryTimeBounds(ctx); err != nil && ctx.Err() == nil {
				s.opts.Log.Warn("historico.limites_temporais_falharam", "a reconstrução temporal será tentada novamente; fragmentos sem limites continuam sendo consultados", "erro", err.Error())
			}
			timer := time.NewTimer(time.Minute)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

// ForHistoryTimeBounds cancels and joins the temporal metadata worker.
func (s *Store) ForHistoryTimeBounds() {
	h := &s.historyTime
	h.workerMu.Lock()
	defer h.workerMu.Unlock()
	if h.cancel != nil {
		h.cancel()
		h.wg.Wait()
		h.cancel = nil
	}
}

func (s *Store) rebuildHistoryTimeBounds(ctx context.Context) error {
	h := &s.historyTime
	h.passMu.Lock()
	defer h.passMu.Unlock()
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
	refs, err := s.listaSegmentos()
	if err != nil {
		return err
	}
	var pending []segment.Ref
	h.mu.RLock()
	for _, ref := range refs {
		b, ok := h.bounds[ref.ID]
		if ref.ID < s.curSecID.Load() && (!ok || b.size != ref.SizeBytes) {
			pending = append(pending, ref)
		}
	}
	h.mu.RUnlock()
	h.pending.Store(int64(len(pending)))
	var failures []error
	for _, ref := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.buildHistoryTimeBounds(ctx, ref); err != nil {
			if segmentLeftDataset(err) {
				h.pending.Add(-1)
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			failures = append(failures, err)
			continue
		}
		h.pending.Add(-1)
	}
	return errors.Join(failures...)
}

func (s *Store) buildHistoryTimeBounds(ctx context.Context, ref segment.Ref) error {
	// Never scan a newly created pre-swap segment: curSecID publication follows
	// file creation. Strictly lower IDs are already immutable in HistoryMode.
	if ref.ID >= s.curSecID.Load() {
		return nil
	}
	b := historyTimeBounds{segment: ref.ID, field: s.writeFieldID, size: ref.SizeBytes, known: true}
	first := true
	end, err := s.secR.ScanFrom(ref.ID, int64(segment.HeaderSize), func(_ uint32, rec types.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		ms, ok := writeInstant(rec.Payload, s.writeFieldID)
		if !ok {
			b.known = false
			return nil
		}
		if first {
			b.minimum, b.maximum, first = ms, ms, false
		} else {
			b.minimum = min(b.minimum, ms)
			b.maximum = max(b.maximum, ms)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if end != ref.SizeBytes {
		return fmt.Errorf("%w: limites temporais do segmento %d cobrem %d de %d bytes", ErrDatasetCorrupt, ref.ID, end, ref.SizeBytes)
	}
	if first {
		b.known = false
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Only publication holds the retirement barrier, never the long data scan.
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	info, err := os.Stat(segment.Path(s.dirOfData(), ref.ID))
	if err != nil {
		return err
	}
	if info.Size() != ref.SizeBytes || ref.ID >= s.curSecID.Load() {
		return nil
	}
	f, err := os.CreateTemp(s.dirOfData(), ".tidx-tmp-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.opts.Log.Warn("historico.limites_temporais_orfaos", "arquivo temporário será removido na próxima abertura", "arquivo", f.Name(), "erro", err.Error())
		}
	}()
	if _, err := f.Write(b.marshal()); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), historyTimePath(s.dirOfData(), ref.ID)); err != nil {
		return err
	}
	dir, err := os.Open(s.dirOfData())
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return err
	}
	s.historyTime.mu.Lock()
	s.historyTime.bounds[ref.ID] = b
	s.historyTime.mu.Unlock()
	s.historyTime.rebuilt.Add(1)
	return nil
}

func (s *Store) removeHistoryTimeBounds(sec uint32) {
	if !s.EhHistory() {
		return
	}
	s.historyTime.mu.Lock()
	delete(s.historyTime.bounds, sec)
	s.historyTime.mu.Unlock()
	if err := os.Remove(historyTimePath(s.dirOfData(), sec)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.opts.Log.Warn("historico.limites_temporais_orfaos", "metadados de fragmento aposentado serão removidos na próxima abertura", "segmento", sec, "erro", err.Error())
	}
}

// HistoryTimeStats reports pruning and temporal metadata reconstruction activity.
type HistoryTimeStats struct {
	Skipped, Pending, Rebuilt int64
	Active                    bool
}

// HistoryTimeStats returns the current temporal metadata counters.
func (s *Store) HistoryTimeStats() HistoryTimeStats {
	h := &s.historyTime
	return HistoryTimeStats{Skipped: h.skipped.Load(), Pending: h.pending.Load(), Rebuilt: h.rebuilt.Load(), Active: h.active.Load()}
}

// Derived temporal metadata occupies disk independently of secondary indexes.
func (s *Store) bytesOfHistoryTimeBounds() int64 {
	return blocosOfDiretorioSe(s.dirOfData(), func(name string) bool { return isHistoryTimeBoundsFile(name) || strings.HasPrefix(name, ".tidx-tmp-") })
}
