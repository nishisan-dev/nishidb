package lexer_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

func TestLiteraisString(t *testing.T) {
	cases := []struct {
		name string
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
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantStream(t, tc.sql, []tok{{lexer.TokenString, tc.text, 0}})
		})
	}
}

func TestIdentQuoted(t *testing.T) {
	cases := []struct {
		name string
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
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

	var texts []string
	for {
		tk, err := l.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tk.Type == lexer.TokenEOF {
			break
		}
		if tk.Type == lexer.TokenString {
			texts = append(texts, tk.Text)
		}
	}

	want := []string{"a'b", "c'd", "e'f"}
	if len(texts) != len(want) {
		t.Fatalf("%d strings, want %d", len(texts), len(want))
	}
	for i := range want {
		if texts[i] != want[i] {
			t.Errorf("texto %d = %q, want %q (Text aliasando o scratch?)", i, texts[i], want[i])
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
	saved := tk.Text

	l.Init(`'outro''texto'`)
	if _, err := l.Next(); err != nil {
		t.Fatal(err)
	}
	if saved != "primeiro'valor" {
		t.Errorf("texto da query anterior = %q, want %q", saved, "primeiro'valor")
	}
}
