package counters_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestCompileCarriesTheScopeAndEvaluatesItOnTheWholeRow: o scope declarado
// chega à Definition normalizado, com a fábrica do avaliador, e o avaliador lê
// o estado inteiro da linha pelo MESMO caminho do where.
func TestCompileCarriesTheScopeAndEvaluatesItOnTheWholeRow(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: spl
    groupBy: [olt]
    scope: "slot  >  0   AND port IS NOT NULL"
    where: authenticated = true
`)
	defs, err := counters.Compile(spec, sc)
	if err != nil {
		t.Fatal(err)
	}
	def := defs[0]
	if def.Scope != "slot > 0 AND port IS NOT NULL" || def.Where != "authenticated = true" {
		t.Fatalf("scope %q where %q", def.Scope, def.Where)
	}
	if def.NewScopeEvaluator == nil {
		t.Fatal("scope declarado sem fábrica de avaliador")
	}
	eval := def.NewScopeEvaluator()
	for _, tc := range []struct {
		slot int64
		port bool
		want bool
	}{{1, true, true}, {0, true, false}, {1, false, false}} {
		fields := map[string]types.Value{"olt": {Type: types.TypeString, Bytes: []byte("A")}, "slot": {Type: types.TypeInt64, I64: tc.slot}}
		if tc.port {
			fields["port"] = types.Value{Type: types.TypeInt64, I64: 1}
		}
		got, err := eval.Evaluate(payloadByName(t, sc, fields))
		if err != nil || got != tc.want {
			t.Errorf("scope com slot=%d port=%v = %v (%v), quero %v", tc.slot, tc.port, got, err, tc.want)
		}
	}
}

// payloadByName monta um payload em ordem crescente de FieldID a partir de
// nome → valor, resolvendo os ids pelo schema compilado.
func payloadByName(t testing.TB, sc *schema.Compiled, fields map[string]types.Value) []byte {
	t.Helper()
	ids := make([]types.FieldID, 0, len(fields))
	byID := make(map[types.FieldID]types.Value, len(fields))
	for name, v := range fields {
		f, ok := sc.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("campo %q não existe no schema", name)
		}
		ids = append(ids, f.ID)
		byID[f.ID] = v
	}
	slices.Sort(ids)
	var buf []byte
	var err error
	for _, id := range ids {
		if buf, err = encoding.AppendField(buf, id, byID[id]); err != nil {
			t.Fatal(err)
		}
	}
	return buf
}

// TestCompileRefusesScopeOverASummedField: o membro de um grupo não pode
// depender de `merge: sum` — a recusa é própria, com o rótulo `.scope`.
func TestCompileRefusesScopeOverASummedField(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    scope: octets > 0
`)
	_, err := counters.Compile(spec, sc)
	want := `counters[0].scope: condition "octets > 0" references "octets", which has merge: sum`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("scope sobre campo somado: %v, quero conter %q", err, want)
	}
}

// TestCompileRefusesColumnComparisonInTheScope: a comparação entre colunas é
// da condição de alarme; no scope (como no where) a recusa é a de sempre.
func TestCompileRefusesColumnComparisonInTheScope(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    scope: slot <= port
`)
	_, err := counters.Compile(spec, sc)
	if err == nil || !strings.Contains(err.Error(), "counters[0].scope") ||
		!strings.Contains(err.Error(), "comparação entre colunas não é suportada na v1") {
		t.Fatalf("coluna à direita no scope: %v", err)
	}
}

// TestSpecRefusesAnEmptyScopeAndRowsAsAColumnName: scope vazio é recusado
// apontando o remédio, e `rows` virou coluna fixa da tabela do contador — um
// groupBy com esse nome colide com ela.
func TestSpecRefusesAnEmptyScopeAndRowsAsAColumnName(t *testing.T) {
	for _, tc := range []struct{ block, want string }{
		{"  - name: x\n    groupBy: [olt]\n    scope: \"  \"\n",
			"counters[0].scope empty — remove the key to make every row with a complete groupBy a member"},
		{"  - name: x\n    groupBy: [rows]\n",
			`groupBy column "rows" collides with fixed column "rows" of table _counters.<name>`},
	} {
		yml := strings.Replace(yamlBase, "    authenticated:\n      type: boolean\n",
			"    authenticated:\n      type: boolean\n    rows:\n      type: int64\n", 1) + "counters:\n" + tc.block
		_, err := schema.Parse([]byte(yml), schema.BuiltinDefaults)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: %v, quero conter %q", tc.block, err, tc.want)
		}
	}
}
