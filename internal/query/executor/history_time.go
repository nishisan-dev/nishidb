package executor

import (
	"context"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// HistoryTimeStore is optional: virtual tables and older stores need not change.
type HistoryTimeStore interface {
	CanPruneHistoryTime(types.FieldID) bool
	ScanHistoryTimeRange(context.Context, types.Snapshot, types.HistoryTimeRange) (types.RowIDIterator, error)
	LookupHistoryTimeRange(context.Context, types.Snapshot, types.FieldID, types.Value, types.HistoryTimeRange) (types.RowIDIterator, error)
}

type historyRangeStore struct {
	Store
	ranged HistoryTimeStore
	ctx    context.Context
	snap   types.Snapshot
	bounds types.HistoryTimeRange
}

func (s *historyRangeStore) Lookup(f types.FieldID, v types.Value) (types.RowIDIterator, error) {
	return s.ranged.LookupHistoryTimeRange(s.ctx, s.snap, f, v, s.bounds)
}

type emptyHistoryIterator struct{}

func (emptyHistoryIterator) Next() bool         { return false }
func (emptyHistoryIterator) RowID() types.RowID { return 0 }
func (emptyHistoryIterator) Err() error         { return nil }
func (emptyHistoryIterator) Close() error       { return nil }

func sourceOfHistoryTimeRange(ctx context.Context, p *planner.HistoryTimeRangePlan, st Store, m *Metrics, snap types.Snapshot) (rowSource, error) {
	ranged, ok := st.(HistoryTimeStore)
	if !ok || !ranged.CanPruneHistoryTime(p.Range.Field) {
		return sourceOfPlanoContext(ctx, p.Source, st, m, snap)
	}
	countSource(m, func(m *Metrics) *atomic.Int64 { return &m.SourceHistoryTimeRange })
	if p.Range.Empty() {
		return &candidateSource{it: emptyHistoryIterator{}, st: st, snap: snap}, nil
	}
	if _, full := p.Source.(*planner.FullScanPlan); full {
		it, err := ranged.ScanHistoryTimeRange(ctx, snap, p.Range)
		if err != nil {
			return nil, err
		}
		return &candidateSource{it: it, st: st, snap: snap}, nil
	}
	return sourceOfPlanoContext(ctx, p.Source, &historyRangeStore{Store: st, ranged: ranged, ctx: ctx, snap: snap, bounds: p.Range}, m, snap)
}
