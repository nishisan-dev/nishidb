package predicate_test

import (
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// counterRowYAML é a linha de um contador como o alarme a vê: total e rows
// int64, uma coluna int64 do groupBy que pode ser negativa, uma média double e
// um texto.
const counterRowYAML = `
version: 1
name: linha
schemaName: telecom
materialization:
  key:
    field: groupKey
schema:
  fields:
    groupKey:
      type: string
    total:
      type: int64
    rows:
      type: int64
    slot:
      type: int64
    auth_5m_avg:
      type: double
    olt:
      type: string
`

// evaluateRow compila a condição de alarme e a avalia sobre uma linha com as
// colunas int64 dadas; coluna fora do mapa fica ausente (NULL).
func evaluateRow(t *testing.T, c *schema.Compiled, condition string, cols map[string]int64) bool {
	t.Helper()
	prog, slots, err := predicate.CompileAlarmCondition("alarms[0].when", condition, c)
	if err != nil {
		t.Fatalf("%s: %v", condition, err)
	}
	fields := map[string]types.Value{"groupKey": {Type: types.TypeString, Bytes: []byte("G")}}
	for name, v := range cols {
		fields[name] = types.Value{Type: types.TypeInt64, I64: v}
	}
	ok, err := predicate.NewAvaliadorOfPayload(prog, slots.IDs).Evaluate(payloadOf(t, c, fields))
	if err != nil {
		t.Fatalf("%s: %v", condition, err)
	}
	return ok
}

// TestLinearColumnComparisonDecidesAtTheBoundary é a garantia da fronteira do
// alarme de proporção: `total * 10 <= rows` com rows=10 vale para total=1 e não
// para total=2, vale com o grupo vazio (0, 0), e coluna ausente é FALSE para
// todo operador.
func TestLinearColumnComparisonDecidesAtTheBoundary(t *testing.T) {
	c := compile(t, counterRowYAML)
	const when = "total * 10 <= rows"
	for _, tc := range []struct {
		total, rows int64
		want        bool
	}{
		{1, 10, true}, {2, 10, false}, {0, 0, true}, {0, 10, true}, {1, 9, false},
	} {
		if got := evaluateRow(t, c, when, map[string]int64{"total": tc.total, "rows": tc.rows}); got != tc.want {
			t.Errorf("%s com total=%d rows=%d = %v, quero %v", when, tc.total, tc.rows, got, tc.want)
		}
	}
	for _, op := range []string{"=", "!=", "<", "<=", ">", ">="} {
		cond := "total * 10 " + op + " rows"
		if evaluateRow(t, c, cond, map[string]int64{"rows": 10}) {
			t.Errorf("%s com total ausente valeu: coluna NULL compara FALSE para todo operador", cond)
		}
		if evaluateRow(t, c, cond, map[string]int64{"total": 1}) {
			t.Errorf("%s com rows ausente valeu: coluna NULL compara FALSE para todo operador", cond)
		}
	}
	// Combina com AND/OR/parênteses e com os termos `coluna <op> literal`.
	if !evaluateRow(t, c, "(total * 10 <= rows AND rows >= 6) OR slot = 99", map[string]int64{"total": 1, "rows": 10, "slot": 0}) {
		t.Error("a comparação entre colunas tem de compor com AND/OR e com termo de literal")
	}
	if evaluateRow(t, c, "total * 10 <= rows AND rows >= 6", map[string]int64{"total": 0, "rows": 5}) {
		t.Error("o qualificador rows >= 6 tem de barrar o grupo com 5 membros")
	}
}

// TestLinearColumnComparisonIsExactBeyondInt64 cobre o que int64 e float
// errariam: o produto que estoura 64 bits, os negativos dos dois lados, o
// multiplicador dos dois lados e o maior literal aceito.
func TestLinearColumnComparisonIsExactBeyondInt64(t *testing.T) {
	c := compile(t, counterRowYAML)
	for _, tc := range []struct {
		cond string
		cols map[string]int64
		want bool
	}{
		// 2 × 2^62 = 2^63 estoura int64 (vira MinInt64) e sairia "<= 5".
		{"total * 4611686018427387904 <= rows", map[string]int64{"total": 2, "rows": 5}, false},
		{"total * 4611686018427387904 > rows", map[string]int64{"total": 2, "rows": math.MaxInt64}, true},
		{"total * 4611686018427387904 <= rows", map[string]int64{"total": 1, "rows": math.MaxInt64}, true},
		// O maior multiplicador, com o maior e o menor int64.
		{"total * 9223372036854775807 > rows * 9223372036854775806", map[string]int64{"total": math.MaxInt64, "rows": math.MaxInt64}, true},
		{"slot * 9223372036854775807 < total", map[string]int64{"slot": math.MinInt64, "total": math.MinInt64}, true},
		{"slot * 9223372036854775807 = rows * 9223372036854775807", map[string]int64{"slot": math.MinInt64, "rows": math.MinInt64}, true},
		// Negativos dos dois lados, multiplicador dos dois lados.
		{"slot * 3 < total * 2", map[string]int64{"slot": -3, "total": 1}, true},
		{"slot * 3 > total * 2", map[string]int64{"slot": -3, "total": -5}, true},
		{"slot * 2 = total * 3", map[string]int64{"slot": -6, "total": -4}, true},
		{"slot * 2 != total * 3", map[string]int64{"slot": -6, "total": -4}, false},
		{"slot >= total", map[string]int64{"slot": -1, "total": -1}, true},
		{"slot < total", map[string]int64{"slot": -2, "total": 1}, true},
	} {
		if got := evaluateRow(t, c, tc.cond, tc.cols); got != tc.want {
			t.Errorf("%s com %v = %v, quero %v", tc.cond, tc.cols, got, tc.want)
		}
	}
}

// TestLinearColumnComparisonOperatorsAtExactEquality é a fronteira de cada
// operador com os dois produtos IGUAIS: os estritos (`<`, `>`) são falsos e os
// largos (`<=`, `>=`) verdadeiros. Sem estes casos, trocar `<` por `<=` ou `>`
// por `>=` no avaliador passaria despercebido — os outros testes nunca
// comparam produtos iguais com operador estrito. A igualdade é montada com
// multiplicador de um lado só, dos dois lados, com negativos e acima de 64 bits.
func TestLinearColumnComparisonOperatorsAtExactEquality(t *testing.T) {
	c := compile(t, counterRowYAML)
	for _, eq := range []struct {
		left, right string
		cols        map[string]int64
	}{
		{"total * 10", "rows", map[string]int64{"total": 1, "rows": 10}},
		{"slot * 2", "total * 3", map[string]int64{"slot": 6, "total": 4}},
		{"slot * 2", "total * 3", map[string]int64{"slot": -6, "total": -4}},
		// 2 × 2^62 = 2^62 × 2 = 2^63: os dois produtos estouram o int64.
		{"total * 4611686018427387904", "rows * 2", map[string]int64{"total": 2, "rows": 4611686018427387904}},
	} {
		for op, want := range map[string]bool{"<": false, ">": false, "<=": true, ">=": true, "=": true, "!=": false} {
			cond := eq.left + " " + op + " " + eq.right
			if got := evaluateRow(t, c, cond, eq.cols); got != want {
				t.Errorf("%s com %v (produtos iguais) = %v, quero %v", cond, eq.cols, got, want)
			}
		}
	}
}

// TestLinearColumnComparisonCitesBothColumnsInTheSlots: a coluna da direita
// entra nos Slots.IDs — sem ela o coletor não a leria do payload e a impressão
// digital das métricas do alarme não a enxergaria.
func TestLinearColumnComparisonCitesBothColumnsInTheSlots(t *testing.T) {
	c := compile(t, counterRowYAML)
	_, slots, err := predicate.CompileAlarmCondition("alarms[0].when", "total * 10 <= rows", c)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"total", "rows"} {
		f, _ := c.FieldByName([]byte(name))
		if _, ok := slots.Index(f.ID); !ok {
			t.Errorf("a coluna %q não está nos slots %v", name, slots.IDs)
		}
	}
}

// TestLinearColumnComparisonRefusesWithItsOwnMessage: cada recusa nova sai com
// a SUA mensagem — um teste que só procurasse o termo rejeitado passaria com a
// guarda removida, porque outra guarda ou o parser recusaria a mesma entrada
// por outro motivo.
func TestLinearColumnComparisonRefusesWithItsOwnMessage(t *testing.T) {
	c := compile(t, counterRowYAML)
	for _, tc := range []struct{ cond, want string }{
		{"auth_5m_avg <= rows", `column "auth_5m_avg" is double: a column comparison accepts only int64 columns`},
		{"total * 10 <= auth_5m_avg", `column "auth_5m_avg" is double: a column comparison accepts only int64 columns`},
		{"olt = total", `column "olt" is string: a column comparison accepts only int64 columns`},
		{"total * 0 <= rows", "multiplier 0 must be a positive integer (>= 1)"},
		{"total * -2 <= rows", "multiplier -2 must be a positive integer (>= 1)"},
		{"total <= rows * 1.5", "multiplier 1.5 is not an integer"},
		{"total * rows <= 10", "the multiplier must be an integer literal"},
		{"10 * total <= rows", "the multiplier goes to the right of the column: write `<column> * 10`"},
		{"total <= 10 * rows", "the multiplier goes to the right of the column: write `<column> * 10`"},
		{"total * 10 <= 100", "a multiplier is only accepted when both sides are columns"},
		{"total * 10 <= ?", "a multiplier is only accepted when both sides are columns"},
	} {
		_, _, err := predicate.CompileAlarmCondition("alarms[0].when", tc.cond, c)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: erro %v, quero conter %q", tc.cond, err, tc.want)
		}
	}
}

// TestColumnComparisonStaysRefusedOutsideTheAlarm: o WHERE do SQL, o `where`
// e o `scope` do contador e o `when` do TTL passam por CompileCondition, que
// continua recusando coluna à direita — com a mensagem de sempre — e nem
// reconhece o multiplicador.
func TestColumnComparisonStaysRefusedOutsideTheAlarm(t *testing.T) {
	c := compile(t, counterRowYAML)
	for _, tc := range []struct{ cond, want string }{
		{"total <= rows", "comparação entre colunas não é suportada na v1"},
		{"total * 10 <= rows", "esperado comparador, LIKE, IS NULL ou IS NOT NULL"},
	} {
		_, _, err := predicate.CompileCondition("counters[0].scope", tc.cond, c)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s fora do alarme: erro %v, quero conter %q", tc.cond, err, tc.want)
		}
	}
}
