package materializer

import (
	"errors"
	"math"
	"strings"
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
	v, _, err := Convert(f, target, nil, nil)
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
	_, _, err := Convert(f, target, nil, nil)
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
	convErr(t, obj(`{"a":1}`), types.TypeString)
}

// Número JSON numa coluna de texto grava o TEXTO-FONTE, byte a byte.
//
// É a razão de a coerção ser aceitável aqui: para KindNumber o Raw do parser é a
// grafia da origem, não uma renderização do valor. Se fosse renderização, `1.50`
// voltaria como `1.5` e `007` como `7` — e a "conversão sem perda" seria falsa.
// O zero à direita e o zero à esquerda abaixo são exatamente os casos que
// distinguem uma coisa da outra.
//
// Antes, tudo isto era recusado: a coluna ficava NULA e o valor só sobrevivia
// dentro do documento, fora do alcance de qualquer WHERE.
func TestNumeroEmColunaDeTextoGravaOTextoFonte(t *testing.T) {
	casos := []string{"5", "1.50", "007", "-42", "1e3", "0.0"}
	for _, in := range casos {
		v := convOK(t, num(in), types.TypeString)
		if string(v.Bytes) != in {
			t.Errorf("Convert(num(%q), string) = %q, quer o texto-fonte %q", in, v.Bytes, in)
		}
	}
}

// Boolean numa coluna de texto grava o literal JSON, pela mesma razão do número
// acima: `true`/`false` são o que a origem escreveu.
func TestBooleanEmColunaDeTextoGravaOLiteral(t *testing.T) {
	if v := convOK(t, boolean(true), types.TypeString); string(v.Bytes) != "true" {
		t.Errorf("Convert(true, string) = %q, quer \"true\"", v.Bytes)
	}
	if v := convOK(t, boolean(false), types.TypeString); string(v.Bytes) != "false" {
		t.Errorf("Convert(false, string) = %q, quer \"false\"", v.Bytes)
	}
}

// Objeto e array continuam RECUSADOS numa coluna de texto, e a recusa é de outra
// classe que a do número: não há escalar a preservar.
//
// Enfiar um documento inteiro em `string` destruiria a razão de a coluna existir
// — `WHERE x = '...'` passaria a exigir casar a serialização byte a byte —, e a
// coluna misturaria escalar com estrutura sem nada distinguindo os dois na
// leitura. Quem quer o documento declara `type: json`.
func TestObjetoEArrayContinuamRecusadosEmColunaDeTexto(t *testing.T) {
	convErr(t, obj(`{"a":1}`), types.TypeString)
	convErr(t, obj(`{}`), types.TypeString)
	convErr(t, arr(`[1,2,3]`), types.TypeString)
	convErr(t, arr(`[]`), types.TypeString)
}

// null numa coluna de texto continua devolvendo TypeNull SEM erro — ausência, e
// não conversão. Está aqui para que a mudança acima não o arraste junto: o ramo
// do null é anterior ao switch de alvo, e um refactor que o movesse para dentro
// passaria a tratar null como "veio noutra forma".
func TestNullEmColunaDeTextoContinuaSendoAusencia(t *testing.T) {
	v, _, err := Convert(null(), types.TypeString, nil, nil)
	if err != nil {
		t.Fatalf("Convert(null, string) erro: %v", err)
	}
	if v.Type != types.TypeNull {
		t.Errorf("Type = %v, quer TypeNull", v.Type)
	}
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

	// A coerção NÃO se estende ao bool: `"true"` continua sendo texto, e um
	// campo declarado `boolean` que aceitasse a string passaria a depender de
	// convenção de grafia da origem ("True", "1", "yes") para decidir o valor.
	convErr(t, str("true"), types.TypeBool)

	// E NÃO se estende a int64 ← double. Truncar `5.7` em `5` grava um valor
	// que ninguém mandou, e a coluna passa a mentir em silêncio: a soma bate
	// diferente, o filtro por igualdade encontra a linha errada, e nada no
	// caminho acusa. Perder o valor é ruim; inventar outro é pior — e é por
	// isso que a simetria da coerção para de propósito aqui.
	convErr(t, num("5.7"), types.TypeInt64)
	convErr(t, num("5.0"), types.TypeInt64)
	convErr(t, str("5.7"), types.TypeInt64)
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
		v, _, err := Convert(null(), target, nil, nil)
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

// I3 do fix round: o valor interpolado em erros é limitado (~64 bytes) —
// um payload grande num campo errado não pode inflar logs/mensagens.
func TestConversionErrorTruncatesValue(t *testing.T) {
	big := `{"b":,` + strings.Repeat("x", 10_000) + `}`
	_, _, err := Convert(obj(big), types.TypeJSON, nil, nil)
	if err == nil {
		t.Fatal("esperava erro")
	}
	if n := len(err.Error()); n > 160 {
		t.Errorf("mensagem não truncada: %d bytes", n)
	}
	if !strings.Contains(err.Error(), "…") {
		t.Errorf("sem marcador de truncamento: %q", err.Error())
	}
	// Valor curto sai inteiro, sem marcador.
	_, _, err = Convert(str("abc"), types.TypeInt64, nil, nil)
	if err == nil || strings.Contains(err.Error(), "…") {
		t.Errorf("valor curto não deveria ser truncado: %v", err)
	}
}

// O jsonp entrega janelas de nested por skip ESTRUTURAL, sem validar o
// conteúdo (leniência documentada lá). A porta de armazenamento é aqui:
// json.Valid rejeita a janela antes de ela virar valor persistido — senão
// a primeira query que projetasse o campo emitiria NDJSON inválido.
func TestConvertJSONRejectsInvalidWindow(t *testing.T) {
	cases := []jsonp.Field{
		obj(`{]`),
		obj(`{ffff}`),
		obj(`{"b" "c"}`),
		obj(`{"b":,}`),
		obj(`{"b":01}`),
		obj("{\"b\":\"\x01\"}"),
		obj(`{"b":"\q"}`),
		arr(`[1,2,,]`),
	}
	for _, f := range cases {
		t.Run(string(f.Raw), func(t *testing.T) {
			convErr(t, f, types.TypeJSON)
		})
	}
}

func TestConvertBytesGoToArena(t *testing.T) {
	// Tipos com Bytes copiam para o arena passado — nunca aliasam f.Raw
	// (que morre no próximo Next do parser).
	raw := []byte("mutavel")
	f := jsonp.Field{Kind: jsonp.KindString, Raw: raw}
	arena := make([]byte, 0, 64)
	v, arena2, err := Convert(f, types.TypeString, nil, arena)
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
