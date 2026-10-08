package types

// HistoryTimeRange is a timestamp interval in epoch milliseconds. Bounds are
// kept inclusive/exclusive rather than shifted, so int64 extremes cannot wrap.
type HistoryTimeRange struct {
	Field                          FieldID
	HasLower, HasUpper             bool
	Lower, Upper                   int64
	LowerInclusive, UpperInclusive bool
}

func (r HistoryTimeRange) Empty() bool {
	if r.HasLower && !r.LowerInclusive && r.Lower == int64(1<<63-1) {
		return true
	}
	if r.HasUpper && !r.UpperInclusive && r.Upper == int64(-1<<63) {
		return true
	}
	if !r.HasLower || !r.HasUpper {
		return false
	}
	if r.Lower > r.Upper {
		return true
	}
	if r.Lower == r.Upper {
		return !r.LowerInclusive || !r.UpperInclusive
	}
	return !r.LowerInclusive && !r.UpperInclusive && r.Lower+1 == r.Upper
}

// Intersects compares the query with a closed interval covering every record.
func (r HistoryTimeRange) Intersects(minimum, maximum int64) bool {
	if r.Empty() {
		return false
	}
	if r.HasLower && (maximum < r.Lower || maximum == r.Lower && !r.LowerInclusive) {
		return false
	}
	if r.HasUpper && (minimum > r.Upper || minimum == r.Upper && !r.UpperInclusive) {
		return false
	}
	return true
}
