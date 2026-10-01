package counters_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestMetaCountersShowsTheScopeBesideTheCondition: `_meta.counters` ganha a
// coluna `scope`, o texto normalizado ao lado de `condition`, NULL quando o
// contador não declara scope; e o `rows` do contador conta só os membros.
func TestMetaCountersShowsTheScopeBesideTheCondition(t *testing.T) {
	scoped := defByPON("com_scope")
	scoped.Scope = "slot > 2"
	scoped.NewScopeEvaluator = func() counter.Evaluator { return &slotAbove{limit: 2} }
	withScope := openCounter(t, scoped)
	plain := openCounter(t, defByPON("sem_scope"))
	populaPON(t, withScope)
	populaPON(t, plain)
	rows := consulta(t, engineWithMeta(t, withScope, plain), "SELECT name, condition, scope, rows, total FROM _meta.counters ORDER BY name")
	if got := column(rows, "scope"); got != "slot > 2,<null>" {
		t.Fatalf("scope = %s, quero o texto e NULL", got)
	}
	if got := column(rows, "condition"); got != "authenticated = true,authenticated = true" {
		t.Fatalf("condition = %s — o where continua lá, intocado", got)
	}
	// populaPON: 3+47 linhas no slot 1, 58 no slot 3, 5 no slot 2. Só o slot 3
	// passa `slot > 2`.
	if got := column(rows, "rows") + "/" + column(rows, "total"); got != "58,113/58,110" {
		t.Fatalf("rows/total = %s, quero 58,113/58,110", got)
	}
}

// slotAbove é `scope: slot > limit` sem binder.
type slotAbove struct {
	fr    encoding.FieldReader
	limit int64
}

func (a *slotAbove) Evaluate(payload []byte) (bool, error) {
	a.fr.Reset(payload)
	for {
		id, v, ok := a.fr.Next()
		if !ok {
			return false, a.fr.Err()
		}
		if id == fSlot {
			return v.Type == types.TypeInt64 && v.I64 > a.limit, nil
		}
	}
}
