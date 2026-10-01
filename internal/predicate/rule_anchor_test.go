package predicate_test

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/predicate"
)

const anchorRulesYAML = `
version: 1
name: s
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - when: "sev > 3"
      anchor: lastAuthAt
      expireAfter: 48h
    - anchor: lastAuthAt
      expireAfter: 24h
    - expireAfterWrite: 1h
schema:
  fields:
    id:
      type: string
    sev:
      type: int64
    lastAuthAt:
      type: timestamp
`

// A regra só com âncora não tem programa e NÃO é fundo: o predicado de fundo
// é o do schema (sem `when` e sem `anchor`), e não "sem programa".
func TestCompileRulesResolvesAnchorAndFallback(t *testing.T) {
	c := compile(t, anchorRulesYAML)
	rules, err := predicate.CompileRules(c.TTLRules, c)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := c.FieldByName([]byte("lastAuthAt"))
	if !ok {
		t.Fatal("lastAuthAt não compilou")
	}
	want := []struct {
		prog     bool
		anchor   bool
		fallback bool
		d        time.Duration
	}{
		{true, true, false, 48 * time.Hour},
		{false, true, false, 24 * time.Hour},
		{false, false, true, time.Hour},
	}
	for i, w := range want {
		r := rules[i]
		if (r.Prog != nil) != w.prog || (r.Anchor == f.ID) != w.anchor || r.Fallback != w.fallback || r.Duration != w.d {
			t.Errorf("regra %d = {prog:%v anchor:%d fallback:%v d:%v}, quero %+v", i, r.Prog != nil, r.Anchor, r.Fallback, r.Duration, w)
		}
	}
}
