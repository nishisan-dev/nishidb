package counters_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// BenchmarkCompiledScopeTransition mede o custo por evento do `scope` com o
// avaliador REAL (internal/predicate, compilado do YAML), e não com um dublê:
// o mesmo contador do pedido do service-view, com e sem a chave `scope`, sobre
// os mesmos payloads (todos membros, então os dois lados seguem até o grupo).
func BenchmarkCompiledScopeTransition(b *testing.B) {
	const base = `version: 1
name: service_state
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    idPrimary:
      type: string
    eligible:
      type: boolean
    authenticated:
      type: boolean
counters:
  - name: spl1
    groupBy: [olt, idPrimary]
    where: "authenticated = true"
`
	for _, tc := range []struct{ name, extra string }{
		{"without_scope", ""},
		{"with_scope", `    scope: "eligible = true AND authenticated IS NOT NULL"` + "\n"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			spec, err := schema.Parse([]byte(base+tc.extra), schema.BuiltinDefaults)
			if err != nil {
				b.Fatal(err)
			}
			sc, err := spec.Compile(fakestorage.NewDict())
			if err != nil {
				b.Fatal(err)
			}
			defs, err := counters.Compile(spec, sc)
			if err != nil {
				b.Fatal(err)
			}
			c, err := counter.Open(b.TempDir(), defs[0], 1024, false, 0)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			payload := func(olt string, auth bool) []byte {
				fields := map[string]types.Value{
					"customerId":    {Type: types.TypeString, Bytes: []byte("c")},
					"olt":           {Type: types.TypeString, Bytes: []byte(olt)},
					"idPrimary":     {Type: types.TypeString, Bytes: []byte("P1")},
					"eligible":      {Type: types.TypeBool, I64: 1},
					"authenticated": {Type: types.TypeBool},
				}
				if auth {
					fields["authenticated"] = types.Value{Type: types.TypeBool, I64: 1}
				}
				return payloadByName(b, sc, fields)
			}
			ps := [][]byte{payload("A", true), payload("B", false), payload("A", false), payload("B", true)}
			const rows = 1 << 12
			for r := types.RowID(0); r < rows; r++ {
				if err := c.Transition(r, ps[int(r)%len(ps)], false); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			var r types.RowID
			i := 0
			for n := 0; n < b.N; n++ {
				r = (r + 1) & (rows - 1)
				i = (i + 1) % len(ps)
				if err := c.Transition(r, ps[i], false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
