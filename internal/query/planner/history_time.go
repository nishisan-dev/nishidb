package planner

import (
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// HistoryTimeCapable only advertises capability; planning never reads data.
type HistoryTimeCapable interface{ CanPruneHistoryTime(types.FieldID) bool }

// HistoryTimeRangePlan preserves the underlying access path and the full WHERE.
type HistoryTimeRangePlan struct {
	Source Plan
	Range  types.HistoryTimeRange
}

func (*HistoryTimeRangePlan) isPlan() {}
func (p *HistoryTimeRangePlan) String() string {
	r := p.Range
	lo, hi := "-inf", "+inf"
	left, right := "(", ")"
	if r.HasLower {
		lo = strconv.FormatInt(r.Lower, 10)
		if r.LowerInclusive {
			left = "["
		}
	}
	if r.HasUpper {
		hi = strconv.FormatInt(r.Upper, 10)
		if r.UpperInclusive {
			right = "]"
		}
	}
	return "HistoryTimeRange(_writtenAt=" + left + lo + "," + hi + right + ", " + p.Source.String() + ")"
}

// Plan chooses the access path and applies mandatory history time restrictions.
func (p *Planner) Plan(bq *binder.BoundQuery) Plan {
	// Choose once without counting intermediate plans.
	base := (&Planner{store: p.store}).basePlan(bq)
	capable, ok := p.store.(HistoryTimeCapable)
	if !ok || bq == nil {
		return p.count(base)
	}
	var r types.HistoryTimeRange
	for _, e := range flattenAnd(bq.Where, nil, 0) {
		c, ok := e.(*binder.BoundComparison)
		if !ok || c.Field == nil || c.Field.Name != schema.WriteInstantFieldName ||
			c.Field.Type != types.TypeTimestamp || c.Val.Type != types.TypeTimestamp ||
			!capable.CanPruneHistoryTime(c.Field.ID) {
			continue
		}
		if c.Op != parser.CmpEq && c.Op != parser.CmpGt && c.Op != parser.CmpGte && c.Op != parser.CmpLt && c.Op != parser.CmpLte {
			continue
		}
		r.Field = c.Field.ID
		v := c.Val.I64
		if c.Op == parser.CmpEq || c.Op == parser.CmpGt || c.Op == parser.CmpGte {
			inclusive := c.Op != parser.CmpGt
			if !r.HasLower || v > r.Lower {
				r.Lower, r.LowerInclusive = v, inclusive
			} else if v == r.Lower {
				r.LowerInclusive = r.LowerInclusive && inclusive
			}
			r.HasLower = true
		}
		if c.Op == parser.CmpEq || c.Op == parser.CmpLt || c.Op == parser.CmpLte {
			inclusive := c.Op != parser.CmpLt
			if !r.HasUpper || v < r.Upper {
				r.Upper, r.UpperInclusive = v, inclusive
			} else if v == r.Upper {
				r.UpperInclusive = r.UpperInclusive && inclusive
			}
			r.HasUpper = true
		}
	}
	if !r.HasLower && !r.HasUpper {
		return p.count(base)
	}
	return p.count(&HistoryTimeRangePlan{Source: base, Range: r})
}
