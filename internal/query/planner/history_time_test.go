package planner_test

import (
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

type temporalCapability struct {
	field     types.FieldID
	supported bool
}

func (s temporalCapability) Indexed(types.FieldID) bool { return true }
func (s temporalCapability) CanPruneHistoryTime(f types.FieldID) bool {
	return s.supported && f == s.field
}

func TestHistoryTimePlanMandatoryBounds(t *testing.T) {
	written := &schema.CompiledField{ID: 100, Name: "_writtenAt", Type: types.TypeTimestamp}
	node := &schema.CompiledField{ID: 101, Name: "node", Type: types.TypeString, Indexed: true}
	timeCmp := func(op parser.CmpOp, ms int64) binder.BoundExpr {
		return cmp(written, op, types.Value{Type: types.TypeTimestamp, I64: ms})
	}
	cases := []struct {
		name                       string
		where                      binder.BoundExpr
		wrapped                    bool
		empty                      bool
		lo, hi                     int64
		hasLo, hasHi, incLo, incHi bool
		base                       string
	}{
		{"minute with index", and(eq(node, str("OLT")), and(timeCmp(parser.CmpGte, 10), timeCmp(parser.CmpLt, 20))), true, false, 10, 20, true, true, true, false, "IndexScan"},
		{"strictest bounds", and(timeCmp(parser.CmpGte, 10), and(timeCmp(parser.CmpGt, 10), and(timeCmp(parser.CmpLte, 20), timeCmp(parser.CmpLt, 20)))), true, false, 10, 20, true, true, false, false, "FullScan"},
		{"equality", timeCmp(parser.CmpEq, 10), true, false, 10, 10, true, true, true, true, "FullScan"},
		{"upper only", timeCmp(parser.CmpLte, 20), true, false, 0, 20, false, true, false, true, "FullScan"},
		{"contradiction", and(timeCmp(parser.CmpGt, 20), timeCmp(parser.CmpLte, 10)), true, true, 20, 10, true, true, false, true, "FullScan"},
		{"equal exclusive", and(timeCmp(parser.CmpGt, 10), timeCmp(parser.CmpLte, 10)), true, true, 10, 10, true, true, false, true, "FullScan"},
		{"root or", or(timeCmp(parser.CmpGte, 10), eq(node, str("OLT"))), false, false, 0, 0, false, false, false, false, ""},
		{"outer mandatory limit", and(timeCmp(parser.CmpGte, 10), or(timeCmp(parser.CmpLt, 20), eq(node, str("OLT")))), true, false, 10, 0, true, false, true, false, "FullScan"},
		{"not equal", timeCmp(parser.CmpNe, 10), false, false, 0, 0, false, false, false, false, ""},
		{"no where", nil, false, false, 0, 0, false, false, false, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &planner.Metrics{}
			p := planner.New(temporalCapability{written.ID, true}, m).Plan(query(tc.where))
			r, ok := p.(*planner.HistoryTimeRangePlan)
			if ok != tc.wrapped {
				t.Fatalf("plan=%s", p.String())
			}
			if !ok {
				return
			}
			want := types.HistoryTimeRange{Field: written.ID, Lower: tc.lo, Upper: tc.hi, HasLower: tc.hasLo, HasUpper: tc.hasHi, LowerInclusive: tc.incLo, UpperInclusive: tc.incHi}
			if r.Range != want || r.Range.Empty() != tc.empty {
				t.Fatalf("bounds=%+v want=%+v", r.Range, want)
			}
			if !strings.HasPrefix(r.Source.String(), tc.base) {
				t.Fatalf("source=%s", r.Source.String())
			}
			if m.FullScan.Load() != 0 || m.HistoryTimeRange.Load() != 1 {
				t.Fatalf("full=%d temporal=%d", m.FullScan.Load(), m.HistoryTimeRange.Load())
			}
		})
	}
	for _, st := range []planner.Store{temporalCapability{written.ID, false}, nil} {
		p := planner.New(st, nil).Plan(query(timeCmp(parser.CmpGte, 10)))
		if p != planner.FullScan() {
			t.Fatalf("unsupported store plan=%s", p.String())
		}
	}
	// Promoted or malformed values must never become time bounds.
	if p := planner.New(temporalCapability{written.ID, true}, nil).Plan(query(cmp(written, parser.CmpGte, i64(10)))); p != planner.FullScan() {
		t.Fatalf("wrong-type plan=%s", p.String())
	}
}

func TestHistoryTimeRangeIntegerExtremes(t *testing.T) {
	for _, r := range []types.HistoryTimeRange{
		{HasLower: true, Lower: math.MaxInt64},
		{HasUpper: true, Upper: math.MinInt64},
		{HasLower: true, HasUpper: true, Lower: 10, Upper: 11},
	} {
		if !r.Empty() || r.Intersects(math.MinInt64, math.MaxInt64) {
			t.Fatalf("range not empty: %+v", r)
		}
	}
	r := types.HistoryTimeRange{HasLower: true, Lower: math.MaxInt64, LowerInclusive: true}
	if r.Empty() || !r.Intersects(math.MaxInt64, math.MaxInt64) {
		t.Fatal("inclusive maximum lost")
	}
}
