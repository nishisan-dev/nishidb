package executor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func col(nome string, v types.Value) ColView { return ColView{Name: nome, Val: v} }

func str(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
func i64(v int64) types.Value  { return types.Value{Type: types.TypeInt64, I64: v} }
func f64(f float64) types.Value {
	return types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(f))}
}
func ts(ms int64) types.Value { return types.Value{Type: types.TypeTimestamp, I64: ms} }

func linhaDe(cols ...ColView) []byte { return AppendRow(nil, Row{cols: cols}) }

// umaLinhaSo é a invariante UNIVERSAL do encoder: o framing do protocolo
// inteiro depende dela. Um '\n' a mais dentro de um valor faz o cliente ler
// duas linhas inválidas no lugar de uma — e o stream inteiro dessincroniza a
// partir dali.
func umaLinhaSo(t *testing.T, out []byte) {
	t.Helper()
	if n := bytes.Count(out, []byte{'\n'}); n != 1 {
		t.Errorf("%d quebras de linha em %q, want exatamente 1", n, out)
	}
	if out[len(out)-1] != '\n' {
		t.Errorf("linha não termina em LF: %q", out)
	}
}

// O golden: uma row com todos os tipos, byte a byte.
func TestGoldenLinha(t *testing.T) {
	out := linhaDe(
		col("id", str("ABC")),
		col("status", str("Stop")),
		col("ip", types.Value{Type: types.TypeIPv4, Bytes: []byte{177, 1, 2, 3}}),
		col("bytes_in", i64(4096)),
		col("lat", f64(-3.5)),
		col("ok", types.Value{Type: types.TypeBool, I64: 1}),
		col("started", ts(1700000000123)),
		col("attrs", types.Value{Type: types.TypeJSON, Bytes: []byte(`{"a":1}`)}),
		col("raw", types.Value{Type: types.TypeBytes, Bytes: []byte{0x00, 0x01}}),
		col("pfx", types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{10, 0, 0, 0, 8}}),
		col("v6", types.Value{Type: types.TypeIPv6,
			Bytes: netip.MustParseAddr("2001:db8::1").AsSlice()}),
		ColView{Name: "missing", Null: true},
	)

	want := `{"id":"ABC","status":"Stop","ip":"177.1.2.3","bytes_in":4096,` +
		`"lat":-3.5,"ok":true,"started":"2023-11-14T22:13:20.123Z",` +
		`"attrs":{"a":1},"raw":"AAE=","pfx":"10.0.0.0/8",` +
		`"v6":"2001:db8::1","missing":null}` + "\n"

	if string(out) != want {
		t.Errorf("linha:\n got %s\nwant %s", out, want)
	}
	umaLinhaSo(t, out)
	if !json.Valid(out[:len(out)-1]) {
		t.Error("a linha não é JSON válido")
	}
}

func TestFormatacaoPorTipo(t *testing.T) {
	v6 := netip.MustParseAddr("2001:db8::1").AsSlice()
	casos := []struct {
		nome string
		v    types.Value
		want string
	}{
		{"int64 zero", i64(0), "0"},
		{"int64 negativo", i64(-1), "-1"},
		{"int64 mínimo", i64(math.MinInt64), "-9223372036854775808"},
		{"int64 máximo", i64(math.MaxInt64), "9223372036854775807"},

		{"double zero", f64(0), "0"},
		{"double zero negativo", f64(math.Copysign(0, -1)), "-0"},
		{"double comum", f64(3.14), "3.14"},
		{"double expoente grande", f64(1e21), "1e+21"},
		{"double expoente pequeno", f64(1e-7), "1e-07"},
		{"double máximo", f64(math.MaxFloat64), "1.7976931348623157e+308"},
		// JSON não tem NaN nem Inf: emitir o texto do 'g' produziria linha
		// inválida.
		{"NaN vira null", f64(math.NaN()), "null"},
		{"+Inf vira null", f64(math.Inf(1)), "null"},
		{"-Inf vira null", f64(math.Inf(-1)), "null"},

		{"bool falso", types.Value{Type: types.TypeBool}, "false"},
		{"bool verdadeiro", types.Value{Type: types.TypeBool, I64: 1}, "true"},
		{"bool fora de 0/1", types.Value{Type: types.TypeBool, I64: 7}, "true"},

		{"timestamp epoch", ts(0), `"1970-01-01T00:00:00.000Z"`},
		// Millis negativo: o resto truncado do Go daria fração negativa.
		{"timestamp negativo", ts(-1), `"1969-12-31T23:59:59.999Z"`},
		{"timestamp comum", ts(1700000000123), `"2023-11-14T22:13:20.123Z"`},
		{"timestamp ano > 9999", ts(300000000000000), `"11476-08-15T05:20:00.000Z"`},
		{"timestamp ano negativo", ts(-70000000000000), `"-0249-10-15T19:33:20.000Z"`},
		// Anos de 1, 2 e 3 dígitos exercitam o padding do formatador manual, que
		// replica a convenção do time.AppendFormat (mínimo 4 dígitos).
		{"timestamp ano de 1 dígito", ts(-62135596800000), `"0001-01-01T00:00:00.000Z"`},
		{"timestamp ano de 2 dígitos", ts(-60000000000000), `"0068-09-03T13:20:00.000Z"`},
		{"timestamp ano de 3 dígitos", ts(-50000000000000), `"0385-07-25T07:06:40.000Z"`},

		{"ipv4 zero", types.Value{Type: types.TypeIPv4, Bytes: []byte{0, 0, 0, 0}}, `"0.0.0.0"`},
		{"ipv4 broadcast", types.Value{Type: types.TypeIPv4, Bytes: []byte{255, 255, 255, 255}}, `"255.255.255.255"`},
		{"ipv4 largura errada", types.Value{Type: types.TypeIPv4, Bytes: []byte{1, 2, 3}}, "null"},

		{"ipv6", types.Value{Type: types.TypeIPv6, Bytes: v6}, `"2001:db8::1"`},
		{"ipv6 zero", types.Value{Type: types.TypeIPv6, Bytes: make([]byte, 16)}, `"::"`},
		{"ipv6 4-in-6", types.Value{Type: types.TypeIPv6,
			Bytes: netip.MustParseAddr("::ffff:1.2.3.4").AsSlice()}, `"::ffff:1.2.3.4"`},
		{"ipv6 largura errada", types.Value{Type: types.TypeIPv6, Bytes: make([]byte, 15)}, "null"},

		{"prefixo v4", types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{10, 0, 0, 0, 8}}, `"10.0.0.0/8"`},
		// Host bits PRESERVADOS: se alguém chamar Masked(), este caso cai.
		{"prefixo v4 com host bits", types.Value{Type: types.TypeIPv4Prefix,
			Bytes: []byte{10, 0, 0, 1, 24}}, `"10.0.0.1/24"`},
		{"prefixo v4 com bits inválidos", types.Value{Type: types.TypeIPv4Prefix,
			Bytes: []byte{10, 0, 0, 0, 33}}, "null"},
		{"prefixo v4 truncado", types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{10, 0, 0, 0}}, "null"},
		{"prefixo v6", types.Value{Type: types.TypeIPv6Prefix,
			Bytes: append(netip.MustParseAddr("2001:db8::").AsSlice(), 32)}, `"2001:db8::/32"`},
		{"prefixo v6 com bits inválidos", types.Value{Type: types.TypeIPv6Prefix,
			Bytes: append(make([]byte, 16), 129)}, "null"},

		{"bytes vazio", types.Value{Type: types.TypeBytes, Bytes: nil}, `""`},
		{"bytes com padding", types.Value{Type: types.TypeBytes, Bytes: []byte{0, 1}}, `"AAE="`},
		{"bytes sem padding", types.Value{Type: types.TypeBytes, Bytes: []byte{0, 1, 2}}, `"AAEC"`},
		{"bytes quatro", types.Value{Type: types.TypeBytes, Bytes: []byte{0, 1, 2, 3}}, `"AAECAw=="`},

		{"json objeto", types.Value{Type: types.TypeJSON, Bytes: []byte(`{"a":1}`)}, `{"a":1}`},
		{"json array", types.Value{Type: types.TypeJSON, Bytes: []byte(`[1,2]`)}, `[1,2]`},
		// Vazio inlinado produziria `"attrs":,`.
		{"json vazio vira null", types.Value{Type: types.TypeJSON, Bytes: nil}, "null"},

		{"tag desconhecida", types.Value{Type: types.FieldType(0x7F)}, "null"},
		{"tipo nulo", types.Value{}, "null"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			if got := string(AppendValue(nil, tc.v)); got != tc.want {
				t.Errorf("AppendValue = %s, want %s", got, tc.want)
			}
		})
	}
}

// O json pretty-printed é o caso que quebra em PRODUÇÃO e não em teste: o
// tokenizer guarda a janela crua do evento, então o '\n' do documento
// original chega até aqui.
func TestJSONPrettyNaoQuebraOFraming(t *testing.T) {
	pretty := []byte("{\"a\": 1,\n\t\"b\": [2,\r\n3]}")
	out := linhaDe(col("attrs", types.Value{Type: types.TypeJSON, Bytes: pretty}))

	umaLinhaSo(t, out)
	if !json.Valid(out[:len(out)-1]) {
		t.Errorf("a sanitização quebrou a validade: %s", out)
	}
	if bytes.ContainsAny(out[:len(out)-1], "\n\r\t") {
		t.Errorf("sobrou control char: %q", out)
	}
	// O comprimento não muda: um byte por um byte.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(out[:len(out)-1], &m); err != nil {
		t.Fatal(err)
	}
	if len(m["attrs"]) != len(pretty) {
		t.Errorf("comprimento mudou: %d → %d", len(pretty), len(m["attrs"]))
	}
}

// Mesmo com json INVÁLIDO (corrupção, ou um caminho futuro que esqueça de
// validar), o framing sobrevive: a falha degrada para "uma linha estranha",
// nunca para "o stream dessincronizou".
func TestJSONInvalidoNaoQuebraOFraming(t *testing.T) {
	out := linhaDe(col("attrs", types.Value{Type: types.TypeJSON, Bytes: []byte("{lixo\n\n{{")}))
	umaLinhaSo(t, out)
}

func TestEscapingDeString(t *testing.T) {
	casos := []struct {
		nome, in, want string
	}{
		{"vazia", "", `""`},
		{"ascii", "abc", `"abc"`},
		{"aspas", `a"b`, `"a\"b"`},
		{"barra invertida", `a\b`, `"a\\b"`},
		{"controles nomeados", "\b\t\n\f\r", `"\b\t\n\f\r"`},
		{"controle sem nome", "\x00\x1f", "\"\\u0000\\u001f\""},
		// NÃO escapados, deliberadamente: o consumidor é um driver JDBC.
		{"barra", "a/b", `"a/b"`},
		{"html", "<&>", `"<&>"`},
		{"del", "\x7f", "\"\x7f\""},
		{"acentos", "ção", `"ção"`},
		{"emoji", "🙂", `"🙂"`},
		{"separador de linha unicode", "\u2028", "\"\u2028\""},
		// UTF-8 inválido vira o ESCAPE de 6 caracteres, não o rune cru.
		{"byte solto", "a\xffb", `"a\ufffdb"`},
		{"sequência truncada", "a\xc3", `"a\ufffd"`},
		{"surrogate encodado", "\xed\xa0\x80", `"\ufffd\ufffd\ufffd"`},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			got := string(AppendJSONString(nil, []byte(tc.in)))
			if got != tc.want {
				t.Errorf("AppendJSONString(%q) = %s, want %s", tc.in, got, tc.want)
			}
			// O nome da coluna passa pelo MESMO escaping.
			if s := string(appendJSONStringS(nil, tc.in)); s != tc.want {
				t.Errorf("appendJSONStringS(%q) = %s, want %s", tc.in, s, tc.want)
			}
			if !json.Valid([]byte(got)) {
				t.Errorf("saída não é JSON válido: %s", got)
			}
		})
	}
}

// A string materializada CONTÉM o control char cru (o tokenizer decodifica
// \n para o byte literal), então o escaping é o que segura o framing.
func TestStringComQuebraDeLinhaNaoQuebraOFraming(t *testing.T) {
	out := linhaDe(col("txt", str("linha1\nlinha2")))
	umaLinhaSo(t, out)
}

// O dispatch é por Val.Type (tipo FÍSICO), nunca por ColView.Type (tipo
// DECLARADO). Num campo auto sem tipo fixado o declarado é TypeNull e o físico
// varia por row — formatar pelo declarado emitiria null para todo valor.
func TestFormatacaoIgnoraOTipoDeclarado(t *testing.T) {
	out := linhaDe(ColView{Name: "a", Type: types.TypeNull, Val: str("abc")})
	if !bytes.Contains(out, []byte(`"a":"abc"`)) {
		t.Errorf("declarado TypeNull com valor string: %s", out)
	}
	out = linhaDe(ColView{Name: "a", Type: types.TypeString, Val: i64(7)})
	if !bytes.Contains(out, []byte(`"a":7`)) {
		t.Errorf("declarado string com valor int64: %s", out)
	}
}

// NULL é emitido EXPLICITAMENTE, nunca omitido: o driver JDBC mapeia posição →
// valor e decodifica cada linha num único passe, sem bookkeeping de chave
// faltando. As duas representações do estado nulo são aceitas.
func TestNullExplicito(t *testing.T) {
	out := linhaDe(
		ColView{Name: "a", Null: true, Val: str("lixo da row anterior")},
		ColView{Name: "b", Val: types.Value{}},
		col("c", str("x")),
	)
	if string(out) != `{"a":null,"b":null,"c":"x"}`+"\n" {
		t.Errorf("linha = %s", out)
	}
}

func TestNomeDeColunaEscapado(t *testing.T) {
	// Alias, campo quoted com hífen, nome com aspas e nome com UTF-8 inválido.
	out := linhaDe(
		col("Acct-Status-Type", str("Stop")),
		col(`a"b`, str("x")),
		col("a\xffb", str("y")),
	)
	want := `{"Acct-Status-Type":"Stop","a\"b":"x","a\ufffdb":"y"}` + "\n"
	if string(out) != want {
		t.Errorf("linha = %s, want %s", out, want)
	}
}

func TestProjecaoVaziaEhObjetoVazio(t *testing.T) {
	if got := string(AppendRow(nil, Row{})); got != "{}\n" {
		t.Errorf("row vazia = %q", got)
	}
}

// ---------------------------------------------------------------- round-trip

// O timestamp emitido volta pelo parser da INGESTÃO: é o que garante que um
// valor exportado possa ser reingerido.
func TestTimestampRoundTrip(t *testing.T) {
	casos := []int64{0, -1, 1, 1700000000123, 253402300799999, -62135596800000}
	for _, ms := range casos {
		out := AppendValue(nil, ts(ms))
		semAspas := out[1 : len(out)-1]
		got, err := typeparse.RFC3339Millis(semAspas)
		if err != nil {
			t.Errorf("millis %d: saída %s não volta pelo parser: %v", ms, out, err)
			continue
		}
		if got != ms {
			t.Errorf("millis %d → %s → %d", ms, out, got)
		}
	}
}

// O time.AppendFormat é o ORÁCULO do formatador manual (que existe porque
// custa ~5x menos). Se os dois divergirem, o manual está errado.
func TestTimestampBateComOOraculo(t *testing.T) {
	casos := []int64{
		0, -1, 1, 999, -999, 1700000000123, 300000000000000, -70000000000000,
		253402300799999, -62135596800000, 946684800000,
	}
	for _, ms := range casos {
		got := AppendValue(nil, ts(ms))
		want := append([]byte{'"'},
			append(time.UnixMilli(ms).UTC().AppendFormat(nil, "2006-01-02T15:04:05.000Z"), '"')...)
		if !bytes.Equal(got, want) {
			t.Errorf("millis %d: got %s, want %s", ms, got, want)
		}
	}
}

func TestRedeRoundTrip(t *testing.T) {
	v4 := types.Value{Type: types.TypeIPv4, Bytes: []byte{177, 1, 2, 3}}
	out := AppendValue(nil, v4)
	a, b, c, d, ok := typeparse.IPv4(out[1 : len(out)-1])
	if !ok || a != 177 || b != 1 || c != 2 || d != 3 {
		t.Errorf("ipv4 não volta: %s", out)
	}

	pfx := types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{10, 0, 0, 1, 24}}
	out = AppendValue(nil, pfx)
	addr, bits, ok := typeparse.IPv4Prefix(out[1 : len(out)-1])
	if !ok || addr != [4]byte{10, 0, 0, 1} || bits != 24 {
		t.Errorf("prefixo não volta com host bits: %s (%v/%d)", out, addr, bits)
	}

	v6 := netip.MustParseAddr("2001:db8::1").AsSlice()
	out = AppendValue(nil, types.Value{Type: types.TypeIPv6, Bytes: v6})
	addr16, ok := typeparse.IPv6(out[1 : len(out)-1])
	if !ok || !bytes.Equal(addr16[:], v6) {
		t.Errorf("ipv6 não volta: %s", out)
	}
}

func TestBytesRoundTrip(t *testing.T) {
	orig := []byte{0, 1, 2, 3, 255, 128}
	out := AppendValue(nil, types.Value{Type: types.TypeBytes, Bytes: orig})
	got, err := base64.StdEncoding.DecodeString(string(out[1 : len(out)-1]))
	if err != nil || !bytes.Equal(got, orig) {
		t.Errorf("bytes não voltam: %s (%v)", out, err)
	}
}

// ---------------------------------------------------------------- guardas

// O gate de alocação depende de não haver reflexão nem formatação genérica no
// caminho quente. O teste torna a regra executável em vez de convencional.
func TestNdjsonSemFmtNemEncodingJSON(t *testing.T) {
	for _, arquivo := range []string{"ndjson.go", "ndjson_writer.go"} {
		b, err := os.ReadFile(arquivo)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		fim := strings.Index(src, "\n)")
		if fim < 0 {
			t.Fatalf("%s: bloco de imports não encontrado", arquivo)
		}
		imports := src[:fim]
		for _, proibido := range []string{`"fmt"`, `"encoding/json"`, `"reflect"`} {
			if strings.Contains(imports, proibido) {
				t.Errorf("%s importa %s no caminho quente", arquivo, proibido)
			}
		}
	}
}

// Paridade com o encoding/json: onde os dois DEVEM concordar, a comparação é
// byte a byte contra o encoder padrão (com o escaping de HTML desligado, que é
// a única diferença deliberada). É o oráculo mais forte disponível — mais forte
// que qualquer tabela escrita à mão.
func TestParidadeDeEscapingComEncodingJSON(t *testing.T) {
	casos := []string{
		"", "abc", `a"b`, `a\b`, "\b\t\n\f\r", "\x00\x1f",
		"a/b", "ção", "🙂", "a\xffb", "a\xc3", "\xed\xa0\x80",
		strings.Repeat("x", 200), "tab\there",
		// HTML entra na PARIDADE, e não nas divergências: com
		// SetEscapeHTML(false) o encoding/json também não escapa < > &. A
		// divergência real seria contra o encoder padrão, com HTML ligado.
		"<&>", "<script>", "a&b",
	}
	for _, in := range casos {
		t.Run(in, func(t *testing.T) {
			var sb strings.Builder
			enc := json.NewEncoder(&sb)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(in); err != nil {
				t.Fatal(err)
			}
			want := strings.TrimSuffix(sb.String(), "\n")
			if got := string(AppendJSONString(nil, []byte(in))); got != want {
				t.Errorf("AppendJSONString(%q):\n got %s\nwant %s", in, got, want)
			}
		})
	}
}

// Onde os dois DIVERGEM, a divergência é deliberada e está registrada: HTML e
// separadores de linha Unicode não são escapados, porque o consumidor é um
// driver JDBC e não uma página.
func TestDivergenciasDeliberadasComEncodingJSON(t *testing.T) {
	casos := map[string]string{
		"<&>":    `"<&>"`,
		"\u2028": "\"\u2028\"",
		"\u2029": "\"\u2029\"",
	}
	for in, want := range casos {
		if got := string(AppendJSONString(nil, []byte(in))); got != want {
			t.Errorf("AppendJSONString(%q) = %s, want %s", in, got, want)
		}
	}
}
