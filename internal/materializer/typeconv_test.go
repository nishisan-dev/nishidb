package materializer

import (
	"errors"
	"math"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func str(s string) jsonp.Field {
	return jsonp.Field{Kind: jsonp.KindString, Raw: []byte(s)}
}

func num(s string) jsonp.Field {
	return jsonp.Field{Kind: jsonp.KindNumber, Raw: []byte(s)}
}

func boolean(v bool) jsonp.Field {
	if v {
		return jsonp.Field{Kind: jsonp.KindBool, Raw: []byte("true")}
	}
	return jsonp.Field{Kind: jsonp.KindBool, Raw: []byte("false")}
}

func null() jsonp.Field { return jsonp.Field{Kind: jsonp.KindNull} }

func obj(s string) jsonp.Field {
	return jsonp.Field{Kind: jsonp.KindObject, Raw: []byte(s)}
}

func arr(s string) jsonp.Field {
	return jsonp.Field{Kind: jsonp.KindArray, Raw: []byte(s)}
}

// convOK converte e falha o teste em erro.
func convOK(t *testing.T, f jsonp.Field, target types.FieldType) types.Value {
	t.Helper()
	v, _, err := Convert(f, target, nil)
	if err != nil {
		t.Fatalf("Convert(%v, %v) erro: %v", f, target, err)
	}
	if v.Type != target {
		t.Fatalf("Value.Type = %v, want %v", v.Type, target)
	}
	return v
}

// convErr espera TYPE_CONVERSION.
func convErr(t *testing.T, f jsonp.Field, target types.FieldType) {
	t.Helper()
	_, _, err := Convert(f, target, nil)
	if err == nil {
		t.Fatalf("Convert(%v, %v): esperava erro", f, target)
	}
	if !errors.Is(err, ErrTypeConversion) {
		t.Fatalf("errors.Is(err, ErrTypeConversion) = false: %v", err)
	}
}

func TestConvertString(t *testing.T) {
	v := convOK(t, str("Interim-Update"), types.TypeString)
	if string(v.Bytes) != "Interim-Update" {
		t.Errorf("Bytes = %q", v.Bytes)
	}
	convErr(t, num("1"), types.TypeString)
	convErr(t, boolean(true), types.TypeString)
	convErr(t, obj(`{"a":1}`), types.TypeString)
}

func TestConvertInt64(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"66952", 66952},
		{"-42", -42},
		{"7455409733", 7455409733},
		{"9223372036854775807", math.MaxInt64},
		{"-9223372036854775808", math.MinInt64},
	}
	for _, tc := range cases {
		v := convOK(t, num(tc.in), types.TypeInt64)
		if v.I64 != tc.want {
			t.Errorf("Convert(%q) = %d, want %d", tc.in, v.I64, tc.want)
		}
	}
	// int64 não aceita fração/expoente, bool, object, overflow.
	convErr(t, num("3.14"), types.TypeInt64)
	convErr(t, num("1e3"), types.TypeInt64)
	convErr(t, num("9223372036854775808"), types.TypeInt64)
	convErr(t, num("-9223372036854775809"), types.TypeInt64)
	convErr(t, boolean(true), types.TypeInt64)
	convErr(t, obj(`{"x":1}`), types.TypeInt64)
}

// Ajuste pós-amostra (vinculante): com o tipo DECLARADO no YAML, strings
// parseáveis são coeridas — a amostra real traz Connect-Info como string
// "1000000000" num campo int64. Em `auto`, string permanece string (o
// InferType não adivinha — coberto em TestInferType).
func TestCoerceDeclaredStringToNumeric(t *testing.T) {
	// int64 ← string de dígitos (-?[0-9]+).
	if v := convOK(t, str("1000000000"), types.TypeInt64); v.I64 != 1000000000 {
		t.Errorf("Connect-Info coerido = %d", v.I64)
	}
	if v := convOK(t, str("123"), types.TypeInt64); v.I64 != 123 {
		t.Errorf("I64 = %d", v.I64)
	}
	if v := convOK(t, str("-42"), types.TypeInt64); v.I64 != -42 {
		t.Errorf("I64 = %d", v.I64)
	}
	convErr(t, str(""), types.TypeInt64)
	convErr(t, str("abc"), types.TypeInt64)
	convErr(t, str("1.5"), types.TypeInt64)
	convErr(t, str(" 12 "), types.TypeInt64)
	convErr(t, str("9223372036854775808"), types.TypeInt64)

	// double ← string com a MESMA gramática de número JSON (nada de NaN,
	// Inf, hex ou underscore — coerência com o que entra por number).
	if v := convOK(t, str("3.14"), types.TypeDouble); math.Float64frombits(uint64(v.I64)) != 3.14 {
		t.Errorf("double = %v", math.Float64frombits(uint64(v.I64)))
	}
	if v := convOK(t, str("1e6"), types.TypeDouble); math.Float64frombits(uint64(v.I64)) != 1e6 {
		t.Errorf("double = %v", math.Float64frombits(uint64(v.I64)))
	}
	if v := convOK(t, str("-0.5"), types.TypeDouble); math.Float64frombits(uint64(v.I64)) != -0.5 {
		t.Errorf("double = %v", math.Float64frombits(uint64(v.I64)))
	}
	convErr(t, str(""), types.TypeDouble)
	convErr(t, str("abc"), types.TypeDouble)
	convErr(t, str("NaN"), types.TypeDouble)
	convErr(t, str("Inf"), types.TypeDouble)
	convErr(t, str("0x1p2"), types.TypeDouble)
	convErr(t, str("1_000"), types.TypeDouble)
	convErr(t, str(" 1 "), types.TypeDouble)
	convErr(t, str("01"), types.TypeDouble)

	// A coerção NÃO se estende a bool nem à direção contrária.
	convErr(t, str("true"), types.TypeBool)
	convErr(t, num("1"), types.TypeString)
}

func TestConvertExtNumberLong(t *testing.T) {
	f := num("66952")
	f.Ext = jsonp.ExtNumberLong
	v := convOK(t, f, types.TypeInt64)
	if v.I64 != 66952 {
		t.Errorf("I64 = %d", v.I64)
	}
}

func TestConvertDouble(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"0", 0},
		{"3.14", 3.14},
		{"-0.5", -0.5},
		{"2.5E-3", 0.0025},
		{"1e9", 1e9},
		{"42", 42},
	}
	for _, tc := range cases {
		v := convOK(t, num(tc.in), types.TypeDouble)
		if got := math.Float64frombits(uint64(v.I64)); got != tc.want {
			t.Errorf("Convert(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	convErr(t, boolean(false), types.TypeDouble)
	convErr(t, obj(`{"x":1}`), types.TypeDouble)
}

func TestConvertBool(t *testing.T) {
	if v := convOK(t, boolean(true), types.TypeBool); v.I64 != 1 {
		t.Errorf("true → I64 = %d, want 1", v.I64)
	}
	if v := convOK(t, boolean(false), types.TypeBool); v.I64 != 0 {
		t.Errorf("false → I64 = %d, want 0", v.I64)
	}
	convErr(t, str("true"), types.TypeBool)
	convErr(t, num("1"), types.TypeBool)
}

func TestConvertNullIsUniversal(t *testing.T) {
	targets := []types.FieldType{
		types.TypeString, types.TypeInt64, types.TypeDouble, types.TypeBool,
		types.TypeTimestamp, types.TypeIPv4, types.TypeIPv6,
		types.TypeIPv4Prefix, types.TypeIPv6Prefix, types.TypeBytes, types.TypeJSON,
	}
	for _, target := range targets {
		v, _, err := Convert(null(), target, nil)
		if err != nil {
			t.Fatalf("null → %v: erro %v", target, err)
		}
		if v.Type != types.TypeNull {
			t.Errorf("null → %v: Type = %v, want TypeNull", target, v.Type)
		}
	}
}

func TestConvertJSON(t *testing.T) {
	v := convOK(t, obj(`{"vsa":[1,2]}`), types.TypeJSON)
	if string(v.Bytes) != `{"vsa":[1,2]}` {
		t.Errorf("Bytes = %q", v.Bytes)
	}
	v = convOK(t, arr(`[1,2,3]`), types.TypeJSON)
	if string(v.Bytes) != `[1,2,3]` {
		t.Errorf("Bytes = %q", v.Bytes)
	}
	// Escalares não viram json na v1 (exigiria re-serialização com aspas).
	convErr(t, str("abc"), types.TypeJSON)
	convErr(t, num("1"), types.TypeJSON)
}

func TestConvertBytesGoToArena(t *testing.T) {
	// Tipos com Bytes copiam para o arena passado — nunca aliasam f.Raw
	// (que morre no próximo Next do parser).
	raw := []byte("mutavel")
	f := jsonp.Field{Kind: jsonp.KindString, Raw: raw}
	arena := make([]byte, 0, 64)
	v, arena2, err := Convert(f, types.TypeString, arena)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = 'X'
	if string(v.Bytes) != "mutavel" {
		t.Errorf("Value.Bytes aliasa f.Raw: %q", v.Bytes)
	}
	if len(arena2) < len("mutavel") {
		t.Errorf("arena não cresceu: len=%d", len(arena2))
	}
}

func TestInferType(t *testing.T) {
	cases := []struct {
		name string
		f    jsonp.Field
		want types.FieldType
	}{
		{"string", str("abc"), types.TypeString},
		{"int", num("42"), types.TypeInt64},
		{"negative int", num("-1"), types.TypeInt64},
		{"fraction", num("3.14"), types.TypeDouble},
		{"exponent", num("1e3"), types.TypeDouble},
		{"bool", boolean(true), types.TypeBool},
		{"object", obj(`{}`), types.TypeJSON},
		{"array", arr(`[]`), types.TypeJSON},
		{"null", null(), types.TypeNull},
		{"numberLong", jsonp.Field{Kind: jsonp.KindNumber, Ext: jsonp.ExtNumberLong, Raw: []byte("1")}, types.TypeInt64},
		{"date relaxed", jsonp.Field{Kind: jsonp.KindString, Ext: jsonp.ExtDate, Raw: []byte("2026-08-11T22:28:10Z")}, types.TypeTimestamp},
		{"date canonical", jsonp.Field{Kind: jsonp.KindNumber, Ext: jsonp.ExtDate, Raw: []byte("175")}, types.TypeTimestamp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := InferType(tc.f); got != tc.want {
				t.Errorf("InferType = %v, want %v", got, tc.want)
			}
		})
	}
}
