package executor

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// O fuzz afere as invariantes de PROTOCOLO, que são as que quebram o cliente:
// uma linha por row, JSON válido, e o valor de volta igual ao que entrou.
// Nenhuma delas depende de o conteúdo ser razoável — é justamente com conteúdo
// não razoável que elas falham.
func FuzzAppendRow(f *testing.F) {
	f.Add(uint8(types.TypeString), []byte("abc"), "col")
	f.Add(uint8(types.TypeString), []byte("com \"aspas\" e \n quebra"), "coluna")
	f.Add(uint8(types.TypeJSON), []byte(`{"a": 1,`+"\n"+`"b": 2}`), "attrs")
	f.Add(uint8(types.TypeBytes), []byte{0, 1, 2}, "raw")
	f.Add(uint8(types.TypeIPv4), []byte{10, 0, 0, 1}, "ip")
	f.Add(uint8(types.TypeIPv4), []byte{10, 0}, "ip_truncado")
	f.Add(uint8(types.TypeIPv6Prefix), []byte{1, 2, 3}, "pfx")
	f.Add(uint8(0x7F), []byte("x"), "tag_desconhecida")
	f.Add(uint8(types.TypeString), []byte("\xff\xfe"), "utf8_invalido")
	f.Add(uint8(types.TypeString), []byte("x"), "nome\xffcom\nlixo")

	f.Fuzz(func(t *testing.T, tag uint8, dados []byte, nome string) {
		v := types.Value{Type: types.FieldType(tag), Bytes: dados}
		// Os tipos de largura fixa leem I64; alimentá-lo com os bytes exercita
		// os dois caminhos com o mesmo corpus.
		for _, b := range dados {
			v.I64 = v.I64<<8 | int64(b)
		}
		out := AppendRow(nil, Row{cols: []ColView{{Name: nome, Val: v}}})

		// 1. Framing: exatamente uma quebra de linha, no fim.
		if n := bytes.Count(out, []byte{'\n'}); n != 1 {
			t.Fatalf("%d quebras de linha: %q", n, out)
		}
		if out[len(out)-1] != '\n' {
			t.Fatalf("não termina em LF: %q", out)
		}

		// 2. A linha é JSON válido.
		linha := out[:len(out)-1]
		if !json.Valid(linha) {
			t.Fatalf("JSON inválido: %q", linha)
		}

		// 3. A chave é o nome da coluna, normalizado para UTF-8 válido.
		var m map[string]json.RawMessage
		if err := json.Unmarshal(linha, &m); err != nil {
			t.Fatalf("unmarshal: %v (%q)", err, linha)
		}
		if len(m) != 1 {
			t.Fatalf("%d chaves, want 1: %q", len(m), linha)
		}
		if _, ok := m[normalizaUTF8([]byte(nome))]; !ok {
			t.Fatalf("chave não bate: %q em %q", nome, linha)
		}

		// 4. Para string, o valor volta igual ao original UTF-8-normalizado.
		if v.Type == types.TypeString {
			var s string
			if err := json.Unmarshal(m[normalizaUTF8([]byte(nome))], &s); err != nil {
				t.Fatalf("valor não é string: %v", err)
			}
			if want := normalizaUTF8(dados); s != want {
				t.Fatalf("valor voltou %q, want %q", s, want)
			}
		}
	})
}

func FuzzAppendJSONString(f *testing.F) {
	for _, s := range []string{
		"", "abc", `"`, `\`, "\n\t\x00", "ção", "🙂", "\xff", "\xc3", "\xed\xa0\x80",
		"  ", strings.Repeat("x", 300),
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, s []byte) {
		out := AppendJSONString(nil, s)

		if !json.Valid(out) {
			t.Fatalf("não é string JSON válida: %q", out)
		}
		if bytes.ContainsAny(out, "\n\r") {
			t.Fatalf("control char cru na saída: %q", out)
		}
		var got string
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("unmarshal: %v (%q)", err, out)
		}
		// A normalização é a MESMA do encoding/json: UM U+FFFD por BYTE
		// inválido.
		if want := normalizaUTF8(s); got != want {
			t.Fatalf("voltou %q, want %q", got, want)
		}
	})
}

// normalizaUTF8 é o oráculo de normalização: UM U+FFFD por BYTE inválido, que
// é a regra do encoding/json e a do nosso encoder.
//
// strings.ToValidUTF8 NÃO serve como oráculo aqui: ele COALESCE uma sequência
// de bytes inválidos num único replacement ("\xff\xfe" vira um U+FFFD, não
// dois). As duas normalizações são defensáveis; a nossa é a que dá paridade
// com o encoder padrão, e é ela que o teste tem de afirmar.
func normalizaUTF8(b []byte) string {
	var sb strings.Builder
	for i := 0; i < len(b); {
		r, tam := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && tam == 1 {
			sb.WriteRune(utf8.RuneError)
			i++
			continue
		}
		sb.Write(b[i : i+tam])
		i += tam
	}
	return sb.String()
}
