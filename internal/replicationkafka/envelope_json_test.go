package replicationkafka

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

// validJSONCases tem a expectativa escrita À MÃO para cada caso; o oráculo
// do encoding/json é conferido em cima dela, não no lugar dela.
var validJSONCases = []struct {
	name string
	in   string
	want bool
}{
	// Objeto, array e aninhamento.
	{"empty object", `{}`, true},
	{"empty array", `[]`, true},
	{"simple object", `{"a":1}`, true},
	{"nested", `{"a":{"b":{"c":[{},[],{"d":[1,[2,[3]]]}]}}}`, true},
	{"array of every kind", `[1,2.5,-0.0e+10,true,false,null,"x",{},[]]`, true},
	{"whitespace everywhere", " \t\r\n{ \"a\" : [ 1 , 2 ] , \"b\" :\n{ } }\r\n ", true},
	{"trailing comma in array", `[1,]`, false},
	{"trailing comma in object", `{"a":1,}`, false},
	{"key without value", `{"a"}`, false},
	{"unquoted key", `{a:1}`, false},
	{"number key", `{1:1}`, false},
	{"missing comma in object", `{"a":1 "b":2}`, false},
	{"missing comma in array", `[1 2]`, false},
	{"unclosed object", `{"a":1`, false},
	{"unclosed array", `[1`, false},
	{"lone closer", `}`, false},
	{"mismatched closer", `[1}`, false},
	{"mismatched closer in object", `{"a":1]`, false},
	{"extra closer", `[1]]`, false},
	{"leading comma", `[,1]`, false},
	{"colon in array", `[1:2]`, false},
	{"empty array closed by brace", `[}`, false},
	{"empty object closed by bracket", `{]`, false},
	{"missing colon", `{"a" 1}`, false},
	{"empty key followed by number without colon", `{""00}`, false},
	// Números.
	{"zero", `0`, true},
	{"negative", `-12`, true},
	{"fraction and exponent", `1.5E-3`, true},
	{"exponent plus", `1e+9`, true},
	{"negative zero fraction", `-0.25`, true},
	{"leading zero", `01`, false},
	{"lone minus", `-`, false},
	{"trailing dot", `1.`, false},
	{"leading dot", `.5`, false},
	{"empty exponent", `1e`, false},
	{"signed empty exponent", `1e+`, false},
	{"leading plus", `+1`, false},
	{"hex", `0x1`, false},
	{"NaN", `NaN`, false},
	// Strings e escapes.
	{"plain string", `"s"`, true},
	{"every simple escape", `"\" \\ \/ \b \f \n \r \t"`, true},
	{"unicode escape", `"\u00e7"`, true},
	{"surrogate pair escape", `"\ud83d\ude00"`, true},
	{"lone surrogate escape (RFC grammar, encoding/json parity)", `"\ud800"`, true},
	{"raw multibyte UTF-8", `"ação 🙂"`, true},
	{"unterminated string", `"abc`, false},
	{"bad escape", `"\x"`, false},
	{"short unicode escape", `"\u12"`, false},
	{"non-hex unicode escape", `"\uZZZZ"`, false},
	{"escape at end", `"\`, false},
	{"raw control char", "\"a\x01b\"", false},
	{"raw newline in string", "\"a\nb\"", false},
	// UTF-8 inválido.
	{"invalid byte", "\"U\xff1\"", false},
	{"truncated sequence", "\"\xc3\"", false},
	{"overlong encoding", "\"\xc0\xaf\"", false},
	{"UTF-8 encoded surrogate", "\"\xed\xa0\x80\"", false},
	{"above U+10FFFF", "\"\xf4\x90\x80\x80\"", false},
	{"high byte outside string", "[1,\xff]", false},
	// Literais.
	{"true", `true`, true},
	{"false", `false`, true},
	{"null", `null`, true},
	{"truncated literal", `tru`, false},
	{"literal with tail", `nulll`, false},
	{"capitalized literal", `True`, false},
	{"null prefix then digits", `n000`, false},
	{"true prefix then garbage", `txxx`, false},
	// Documento inteiro: vazio e lixo no fim.
	{"empty", ``, false},
	{"only whitespace", " \n\t", false},
	{"garbage after value", `{"a":1}x`, false},
	{"two values", `{"a":1}{}`, false},
	{"NUL after value", "{\"a\":1}\x00", false},
	{"bad event", `{"id": "A", broken`, false},
}

func TestValidJSONRecognizesTheGrammar(t *testing.T) {
	for _, c := range validJSONCases {
		t.Run(c.name, func(t *testing.T) {
			if got := validJSON([]byte(c.in)); got != c.want {
				t.Errorf("validJSON(%q) = %v, quer %v", c.in, got, c.want)
			}
			if oracle := json.Valid([]byte(c.in)) && utf8.ValidString(c.in); oracle != c.want {
				t.Errorf("a expectativa escrita diverge do oráculo json.Valid && utf8.Valid (%v) para %q", oracle, c.in)
			}
		})
	}
}

// O teto de aninhamento é o do encoding/json: 10000 níveis passam, 10001 não.
func TestValidJSONDepthLimitMatchesEncodingJSON(t *testing.T) {
	for _, depth := range []int{maxJSONDepth, maxJSONDepth + 1} {
		arr := strings.Repeat("[", depth) + strings.Repeat("]", depth)
		obj := strings.Repeat(`{"a":`, depth-1) + "{}" + strings.Repeat("}", depth-1)
		for _, in := range []string{arr, obj} {
			want := json.Valid([]byte(in))
			if got := validJSON([]byte(in)); got != want {
				t.Errorf("profundidade %d (%.8s…): validJSON = %v, json.Valid = %v", depth, in, got, want)
			}
		}
	}
	if !validJSON([]byte(strings.Repeat("[", maxJSONDepth) + strings.Repeat("]", maxJSONDepth))) {
		t.Error("o teto recusou exatamente maxJSONDepth níveis")
	}
}

// TestValidJSONMixedNestingAcrossWordBoundary cruza a fronteira de 64 níveis
// do bitset da pilha com objeto e array alternados, e troca UM fechador nos
// níveis vizinhos da fronteira (e nas pontas). Dois padrões:
//
//   - alternância pura: o nível d é objeto quando d é par;
//   - alternância com a fase invertida a partir do nível 64: o nível 64 tem o
//     MESMO tipo do 63. Sem essa inversão, um bitset lido da palavra errada
//     (stack[0] em vez de stack[top>>6]) acertaria por coincidência de
//     paridade, e o teste não o pegaria.
//
// A expectativa é escrita (válido sem troca, inválido com troca) e conferida
// também contra json.Valid.
func TestValidJSONMixedNestingAcrossWordBoundary(t *testing.T) {
	patterns := map[string]func(d int) bool{
		"alternating":               func(d int) bool { return d%2 == 0 },
		"alternating phase-shifted": func(d int) bool { return (d%2 == 0) != (d >= 64) },
	}
	const noFlip = -1
	for name, isObject := range patterns {
		for _, depth := range []int{63, 64, 65, 127, 128, 129, 130} {
			for _, flip := range []int{noFlip, 0, 62, 63, 64, 65, depth - 1} {
				if flip >= depth {
					continue
				}
				var sb strings.Builder
				for d := 0; d < depth; d++ {
					if isObject(d) {
						sb.WriteString(`{"k":`)
					} else {
						sb.WriteString(`[0,`)
					}
				}
				sb.WriteString(`{}`)
				for d := depth - 1; d >= 0; d-- {
					if isObject(d) != (d == flip) {
						sb.WriteString("}")
					} else {
						sb.WriteString("]")
					}
				}
				in := []byte(sb.String())
				want := flip == noFlip
				if got := validJSON(in); got != want {
					t.Errorf("%s, profundidade %d, fechador trocado no nível %d: validJSON = %v, quer %v",
						name, depth, flip, got, want)
				}
				if oracle := json.Valid(in); oracle != want {
					t.Errorf("%s, profundidade %d, troca %d: a expectativa diverge do json.Valid (%v)",
						name, depth, flip, oracle)
				}
			}
		}
	}
}

// FuzzValidJSONMatchesStdlib compara o validador com o oráculo em entrada
// arbitrária. Sob `go test` roda as sementes; `-fuzz` explora.
func FuzzValidJSONMatchesStdlib(f *testing.F) {
	for _, c := range validJSONCases {
		f.Add([]byte(c.in))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		want := json.Valid(in) && utf8.Valid(in)
		if got := validJSON(in); got != want {
			t.Errorf("validJSON(%q) = %v, oráculo = %v", in, got, want)
		}
	})
}
