package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func seedTemporalHistory(t testing.TB, indexed bool, n int) (*Store, *schema.Compiled, int64) {
	t.Helper()
	indexBlock := ""
	if indexed {
		indexBlock = "  fields:\n    node:\n      index: true\n"
	}
	spec, err := schema.Parse([]byte(`version: 1
name: events
schemaName: test_history
materialization:
  key:
    field: id
history:
  maxBytes: 4GiB
`+indexBlock+`schema:
  include: ["*"]
  fields:
    id:
      type: string
    node:
      type: string
`), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	base := openStore(t, Options{})
	if _, err := spec.Compile(base.FieldDict()); err != nil {
		t.Fatal(err)
	}
	c, err := spec.DeriveHistory().Compile(base.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	hist := openStore(t, Options{Dataset: c.Name, Mode: HistoryMode, SharedDictionary: base.FieldDict(), SegmentSize: 512, WriteInstantFieldName: "_writtenAt", IndexedFieldsByName: schema.IndexedFieldNames(spec.DeriveHistory())})
	start := time.Date(2026, 10, 7, 0, 0, 0, 0, time.FixedZone("BR", -3*3600)).UnixMilli()
	for i := 0; i < n; i++ {
		appendTemporalRow(t, hist, c, i, start+int64(i)*10000, types.TypeTimestamp, true)
	}
	return hist, c, start
}

func appendTemporalRow(t testing.TB, s *Store, c *schema.Compiled, i int, ms int64, typ types.FieldType, includeTime bool) types.RowID {
	t.Helper()
	node := "BR_PIARG_OLT01"
	if i%4 == 0 {
		node = "OTHER"
	}
	values := map[string]types.Value{"id": {Type: types.TypeString, Bytes: []byte(fmt.Sprintf("row-%06d", i))}, "node": {Type: types.TypeString, Bytes: []byte(node)}}
	if includeTime {
		values["_writtenAt"] = types.Value{Type: typ, I64: ms}
	}
	var ids []types.FieldID
	byID := map[types.FieldID]types.Value{}
	for name, v := range values {
		f, ok := c.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("missing field %s", name)
		}
		ids = append(ids, f.ID)
		byID[f.ID] = v
	}
	slices.Sort(ids)
	var payload []byte
	for _, id := range ids {
		payload = mustAppendField(t, payload, id, byID[id])
	}
	row, err := s.Apenda(payload)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

// Fetch instrumentation verifies excluded segments never reach GetByRowID,
// rather than merely checking that WHERE eventually rejects their records.
type temporalReadSpy struct {
	*Store
	disabled  bool
	excluded  map[uint32]bool
	reads     int
	forbidden int
}

func (s *temporalReadSpy) CanPruneHistoryTime(f types.FieldID) bool {
	return !s.disabled && s.Store.CanPruneHistoryTime(f)
}
func (s *temporalReadSpy) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	s.reads++
	if p, ok := s.locs.Get(id); ok {
		sec, _, _ := rowindex.Unpack(p)
		if s.excluded[sec] {
			s.forbidden++
		}
	}
	return s.Store.GetByRowID(snap, id, buf)
}

func temporalQuery(t testing.TB, c *schema.Compiled, st query.Store, sql string, params []types.Value) []string {
	t.Helper()
	e := query.NewEngine()
	if err := e.Register(c.Name, c, st); err != nil {
		t.Fatal(err)
	}
	it, err := e.Execute(context.Background(), c.Name, sql, params)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out []string
	for it.Next() {
		row := it.Row()
		v := ""
		for _, col := range row.Cols() {
			v += fmt.Sprintf("%s:%t:%d:%d:%s;", col.Name, col.Null, col.Val.Type, col.Val.I64, col.Val.Bytes)
		}
		out = append(out, v)
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestHistoryTimeQueriesMatchFullExecution(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			s, c, start := seedTemporalHistory(t, indexed, 300)
			if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
				t.Fatal(err)
			}
			if indexed {
				// Simulate segments outside history.index.window. Time bounds survive.
				refs, _ := s.listaSegmentos()
				for i, ref := range refs {
					if i%2 == 0 && ref.ID < s.curSecID.Load() {
						if _, err := s.removeSidxOfDisk(ref.ID); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			queries := []struct {
				sql    string
				params []types.Value
			}{
				{`SELECT * FROM events_history WHERE node = 'BR_PIARG_OLT01' AND _writtenAt >= '2026-10-07T00:21:00-03:00' AND _writtenAt < '2026-10-07T00:22:00-03:00'`, nil},
				{`SELECT * FROM events_history WHERE node = 'BR_PIARG_OLT01' AND _writtenAt >= '2026-10-07T03:21:00Z' AND _writtenAt < '2026-10-07T03:22:00Z'`, nil},
				{`SELECT * FROM events_history WHERE _writtenAt >= ? AND _writtenAt < ?`, []types.Value{{Type: types.TypeString, Bytes: []byte("2026-10-07T00:21:00-03:00")}, {Type: types.TypeString, Bytes: []byte("2026-10-07T00:22:00-03:00")}}},
				{`SELECT * FROM events_history WHERE _writtenAt = '2026-10-07T00:21:00-03:00'`, nil},
				{`SELECT * FROM events_history WHERE _writtenAt > '2026-10-07T00:21:00-03:00' AND _writtenAt <= '2026-10-07T00:22:00-03:00'`, nil},
				{`SELECT * FROM events_history WHERE _writtenAt < '2026-10-07T00:01:00-03:00'`, nil},
				{`SELECT * FROM events_history WHERE _writtenAt >= '2026-10-07T00:49:00-03:00'`, nil},
				{`SELECT * FROM events_history WHERE _writtenAt >= '2026-10-07T00:22:00-03:00' AND _writtenAt < '2026-10-07T00:21:00-03:00'`, nil},
				{`SELECT * FROM events_history WHERE _writtenAt < '2026-10-07T00:01:00-03:00' OR node = 'BR_PIARG_OLT01'`, nil},
				{`SELECT * FROM events_history WHERE _writtenAt >= '2026-10-07T00:21:00-03:00' AND (node = 'BR_PIARG_OLT01' OR node = 'OTHER')`, nil},
			}
			for i, tc := range queries {
				baseline := &temporalReadSpy{Store: s, disabled: true}
				optimized := &temporalReadSpy{Store: s}
				if i == 0 {
					optimized.excluded, _ = s.excludedHistorySegments(context.Background(), types.HistoryTimeRange{Field: s.writeFieldID, HasLower: true, HasUpper: true, Lower: start + 1260000, Upper: start + 1320000, LowerInclusive: true})
				}
				want := temporalQuery(t, c, baseline, tc.sql, tc.params)
				got := temporalQuery(t, c, optimized, tc.sql, tc.params)
				if !slices.Equal(got, want) {
					t.Fatalf("query %d: got %d rows want %d", i, len(got), len(want))
				}
				if i == 0 {
					if len(got) != 5 {
						t.Fatalf("one-minute node query returned %d rows, want 5", len(got))
					}
					if optimized.forbidden != 0 || len(optimized.excluded) == 0 || optimized.reads >= baseline.reads/2 {
						t.Fatalf("reads optimized=%d baseline=%d forbidden=%d excluded=%d", optimized.reads, baseline.reads, optimized.forbidden, len(optimized.excluded))
					}
					t.Logf("one-minute query: %d → %d record fetches; %d closed segments excluded", baseline.reads, optimized.reads, len(optimized.excluded))
				}
			}
		})
	}
}

func TestHistoryTimeBoundsConservativeAndRestart(t *testing.T) {
	s, c, start := seedTemporalHistory(t, false, 0)
	// Every segment contains repeated and non-monotonic times. Endpoints alone
	// must not exclude the time in the middle of a fragment.
	for i := 0; i < 36; i++ {
		ms := start
		if i%3 == 1 {
			ms += 3600000
		}
		appendTemporalRow(t, s, c, i, ms, types.TypeTimestamp, true)
	}
	if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := types.HistoryTimeRange{Field: s.writeFieldID, HasLower: true, HasUpper: true, Lower: start + 3600000, Upper: start + 3600000, LowerInclusive: true, UpperInclusive: true}
	it, err := s.ScanHistoryTimeRange(context.Background(), s.presente(), r)
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 36 {
		t.Fatalf("out-of-order candidates lost: %d", len(rows))
	}
	refs, _ := s.listaSegmentos()
	ref := refs[0]
	path := historyTimePath(s.dirOfData(), ref.ID)
	if _, ok := readHistoryTimeBounds(path, ref.ID, s.writeFieldID, ref.SizeBytes); !ok {
		t.Fatal("missing valid bounds")
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, s.opts)
	if reopened.HistoryTimeStats().Pending != 0 || len(reopened.historyTime.bounds) != len(refs)-1 {
		t.Fatalf("bounds not loaded: %+v", reopened.HistoryTimeStats())
	}
	// Corrupt only derived metadata, then reopen: query remains available.
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened = openStore(t, s.opts)
	if reopened.HistoryTimeStats().Pending != 1 {
		t.Fatalf("pending=%d", reopened.HistoryTimeStats().Pending)
	}
	r.Lower, r.Upper = start+7200000, start+7200000
	excluded, err := reopened.excludedHistorySegments(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if excluded[ref.ID] {
		t.Fatal("untrusted sidecar excluded its data")
	}
	if err := reopened.rebuildHistoryTimeBounds(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := readHistoryTimeBounds(path, ref.ID, reopened.writeFieldID, ref.SizeBytes); !ok {
		t.Fatal("sidecar not repaired")
	}
	b := reopened.historyTime.bounds[ref.ID]
	for _, bad := range []historyTimeBounds{
		{segment: ref.ID + 1, field: b.field, size: b.size, minimum: b.minimum, maximum: b.maximum, known: true},
		{segment: ref.ID, field: b.field + 1, size: b.size, minimum: b.minimum, maximum: b.maximum, known: true},
		{segment: ref.ID, field: b.field, size: b.size - 1, minimum: b.minimum, maximum: b.maximum, known: true},
	} {
		if err := os.WriteFile(path, bad.marshal(), 0600); err != nil {
			t.Fatal(err)
		}
		if _, ok := readHistoryTimeBounds(path, ref.ID, b.field, b.size); ok {
			t.Fatal("incompatible bounds accepted")
		}
	}
}

func TestHistoryTimeUnknownAndIncompleteRecords(t *testing.T) {
	for _, typ := range []types.FieldType{types.TypeNull, types.TypeInt64} {
		t.Run(fmt.Sprintf("type=%d", typ), func(t *testing.T) {
			s, c, start := seedTemporalHistory(t, false, 0)
			for i := 0; i < 30; i++ {
				appendTemporalRow(t, s, c, i, start, typ, typ != types.TypeNull)
			}
			if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(s.historyTime.bounds) == 0 {
				t.Fatal("no closed segments")
			}
			for _, b := range s.historyTime.bounds {
				if b.known {
					t.Fatal("unknown timestamps trusted")
				}
			}
			r := types.HistoryTimeRange{Field: s.writeFieldID, HasLower: true, Lower: start + 100000, LowerInclusive: true}
			excluded, _ := s.excludedHistorySegments(context.Background(), r)
			if len(excluded) != 0 {
				t.Fatal("unknown segment excluded")
			}
			if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
				t.Fatal(err)
			}
			if s.HistoryTimeStats().Pending != 0 {
				t.Fatal("unknown timestamps caused perpetual rebuild")
			}
		})
	}
	s, _, _ := seedTemporalHistory(t, false, 30)
	refs, _ := s.listaSegmentos()
	ref := refs[0]
	// A partial trailing record must not publish bounds for a valid prefix.
	f, err := os.OpenFile(segment.Path(s.dirOfData(), ref.ID), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("bad tail")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	refs, _ = s.listaSegmentos()
	if err := s.buildHistoryTimeBounds(context.Background(), refs[0]); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("partial scan err=%v", err)
	}
	if _, err := os.Stat(historyTimePath(s.dirOfData(), ref.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial sidecar published")
	}
}

func TestHistoryTimeWorkerAndCancellation(t *testing.T) {
	s, c, start := seedTemporalHistory(t, false, 160)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.rebuildHistoryTimeBounds(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
	if len(s.historyTime.bounds) != 0 {
		t.Fatal("canceled scan published bounds")
	}
	s.StartHistoryTimeBounds(context.Background())
	s.StartHistoryTimeBounds(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for s.HistoryTimeStats().Rebuilt == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !s.HistoryTimeStats().Active || s.HistoryTimeStats().Rebuilt == 0 {
		t.Fatalf("worker did not run without field indexes: %+v", s.HistoryTimeStats())
	}
	s.ForHistoryTimeBounds()
	if s.HistoryTimeStats().Active {
		t.Fatal("worker not joined")
	}
	if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
		t.Fatal(err)
	}
	refs, _ := s.listaSegmentos()
	ref := refs[0]
	if err := s.aposentaOfHistory(ref.ID, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(historyTimePath(s.dirOfData(), ref.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("retirement left bounds")
	}
	if _, ok := s.historyTime.bounds[ref.ID]; ok {
		t.Fatal("retirement left cached bounds")
	}
	// Queries, writes and metadata rebuilding share no writer lock during scans.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 160; i < 260; i++ {
			appendTemporalRow(t, s, c, i, start+int64(i)*10000, types.TypeTimestamp, true)
		}
	}()
	go func() {
		defer wg.Done()
		for range 8 {
			if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
				t.Error(err)
			}
		}
	}()
	for range 8 {
		r := types.HistoryTimeRange{Field: s.writeFieldID, HasLower: true, Lower: start, LowerInclusive: true}
		snap := s.OpenSnapshot()
		it, err := s.ScanHistoryTimeRange(context.Background(), snap, r)
		if err != nil {
			t.Fatal(err)
		}
		collectRows(t, it)
		s.CloseSnapshot(snap)
	}
	wg.Wait()
	s.StartHistoryTimeBounds(context.Background())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.StartHistoryTimeBounds(context.Background())
	if s.HistoryTimeStats().Active {
		t.Fatal("closed store restarted worker")
	}
}

func BenchmarkHistoryTimePruning(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
			s, c, _ := seedTemporalHistory(b, indexed, 4000)
			b.Run("rebuild", func(b *testing.B) {
				for b.Loop() {
					s.historyTime.mu.Lock()
					clear(s.historyTime.bounds)
					s.historyTime.mu.Unlock()
					if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
			})
			for _, disabled := range []bool{true, false} {
				b.Run(fmt.Sprintf("disabled=%t", disabled), func(b *testing.B) {
					spy := &temporalReadSpy{Store: s, disabled: disabled}
					sql := `SELECT * FROM events_history WHERE node = 'BR_PIARG_OLT01' AND _writtenAt >= '2026-10-07T00:21:00-03:00' AND _writtenAt < '2026-10-07T00:22:00-03:00'`
					b.ResetTimer()
					for b.Loop() {
						temporalQuery(b, c, spy, sql, nil)
					}
					b.ReportMetric(float64(spy.reads)/float64(b.N), "fetches/op")
				})
			}
		})
	}
}

func TestHistoryTimeActiveSegmentAndSkippedWalkCancel(t *testing.T) {
	s, _, start := seedTemporalHistory(t, false, 600)
	if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := types.HistoryTimeRange{Field: s.writeFieldID, HasUpper: true, Upper: start - 1, UpperInclusive: true}
	excluded, err := s.excludedHistorySegments(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if excluded[s.curSecID.Load()] {
		t.Fatal("active fragment excluded")
	}
	// Force a long memory walk that returns no candidates; the execution
	// deadline must be checked inside Next, not only when it emits a row.
	ctx, cancel := context.WithCancel(context.Background())
	it, err := s.ScanHistoryTimeRange(ctx, s.presente(), r)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if it.Next() || !errors.Is(it.Err(), context.Canceled) {
		t.Fatalf("canceled walk err=%v", it.Err())
	}
	it.Close()
}

func TestHistoryTimeRebuildConcurrentWithRetention(t *testing.T) {
	s, c, start := seedTemporalHistory(t, false, 100)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 100; i < 180; i++ {
			appendTemporalRow(t, s, c, i, start+int64(i)*10000, types.TypeTimestamp, true)
		}
	}()
	go func() {
		defer wg.Done()
		for range 10 {
			if _, err := s.RetentionOnePass(context.Background(), LimitesOfRetention{MaxBytes: 4096}); err != nil {
				t.Error(err)
			}
		}
	}()
	for range 10 {
		if err := s.rebuildHistoryTimeBounds(context.Background()); err != nil {
			t.Error(err)
		}
		snap := s.OpenSnapshot()
		it, err := s.ScanHistoryTimeRange(context.Background(), snap, types.HistoryTimeRange{Field: s.writeFieldID, HasLower: true, Lower: start, LowerInclusive: true})
		if err != nil {
			t.Error(err)
			s.CloseSnapshot(snap)
			continue
		}
		var buf []byte
		for it.Next() {
			_, out, _, err := s.GetByRowID(snap, it.RowID(), buf)
			buf = out
			if err != nil {
				t.Error(err)
			}
		}
		if err := it.Err(); err != nil {
			t.Error(err)
		}
		it.Close()
		s.CloseSnapshot(snap)
	}
	wg.Wait()
}

func TestHistoryTimeOrphanCleanupIsConservative(t *testing.T) {
	s, _, _ := seedTemporalHistory(t, false, 0)
	dir := s.dirOfData()
	orphan := historyTimePath(dir, 1234)
	if err := os.WriteFile(orphan, []byte("orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	// Only canonical derived names belong to this subsystem.
	unrelated := dir + "/notes.tidx"
	if err := os.WriteFile(unrelated, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, s.opts)
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("canonical orphan was not cleaned")
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatal("unrelated file removed")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	// Unremovable orphan metadata must not make the data unavailable.
	if err := os.Mkdir(orphan, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan+"/unexpected", []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened = openStore(t, s.opts)
	if reopened == nil {
		t.Fatal("derived cleanup failure prevented opening")
	}
}
