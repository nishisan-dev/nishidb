package lexer_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

func TestLiteraisString(t *testing.T) {
	casos := []struct {
		nome string
		sql  string
		text string
	}{
		{"simples", `'Stop'`, "Stop"},
		{"escape", `'it''s'`, "it's"},
		{"so aspa", `''''`, "'"},
		{"vazia", `''`, ""},
		{"newline cru", "'linha1\nlinha2'", "linha1\nlinha2"},
		{"utf8", "'café'", "café"},
		{"utf8 invalido passa verbatim", "'\xff\xfe'", "\xff\xfe"},
		{"nul", "'a\x00b'", "a\x00b"},
		{"aspas duplas dentro", `'a"b'`, `a"b`},
		{"varios escapes", `'a''b''c'`, "a'b'c"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			wantStream(t, tc.sql, []tok{{lexer.TokenString, tc.text, 0}})
		})
	}
}

func TestIdentQuoted(t *testing.T) {
	casos := []struct {
		nome string
		sql  string
		text string
	}{
		{"campo real", `"Acct-Status-Type"`, "Acct-Status-Type"},
		{"escape", `"a""b"`, `a"b`},
		{"utf8", `"café"`, "café"},
		{"aspa simples dentro", `"it's"`, "it's"},
		// Quoted NUNCA é keyword: "select" é uma coluna chamada select.
		{"keyword entre aspas", `"select"`, "select"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			wantStream(t, tc.sql, []tok{{lexer.TokenQuotedIdent, tc.text, 0}})
		})
	}
}

// A regressão clássica do pacote: devolver Text aliasando o buffer de
// unescape faz o texto de um token JÁ ENTREGUE mudar quando o próximo
// literal escapado é lido. Por isso os valores são guardados durante a
// iteração e conferidos só no fim.
func TestTextSobreviveAosProximosTokens(t *testing.T) {
	const sql = `'a''b' , 'c''d' , 'e''f'`
	var l lexer.Lexer
	l.Init(sql)

	var textos []string
	for {
		tk, err := l.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tk.Type == lexer.TokenEOF {
			break
		}
		if tk.Type == lexer.TokenString {
			textos = append(textos, tk.Text)
		}
	}

	want := []string{"a'b", "c'd", "e'f"}
	if len(textos) != len(want) {
		t.Fatalf("%d strings, want %d", len(textos), len(want))
	}
	for i := range want {
		if textos[i] != want[i] {
			t.Errorf("texto %d = %q, want %q (Text aliasando o scratch?)", i, textos[i], want[i])
		}
	}
}

// Mesma armadilha entre queries: o scratch é reutilizado no Init.
func TestTextSobreviveAoInit(t *testing.T) {
	var l lexer.Lexer
	l.Init(`'primeiro''valor'`)
	tk, err := l.Next()
	if err != nil {
		t.Fatal(err)
	}
	guardado := tk.Text

	l.Init(`'outro''texto'`)
	if _, err := l.Next(); err != nil {
		t.Fatal(err)
	}
	if guardado != "primeiro'valor" {
		t.Errorf("texto da query anterior = %q, want %q", guardado, "primeiro'valor")
	}
}
