package lexer_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

// lexUntilErr tokeniza até o primeiro erro e o devolve.
func lexUntilErr(t *testing.T, sql string) error {
	t.Helper()
	var l lexer.Lexer
	l.Init(sql)
	for i := 0; i <= len(sql)+2; i++ {
		tk, err := l.Next()
		if err != nil {
			return err
		}
		if tk.Type == lexer.TokenEOF {
			t.Fatalf("%q tokenizou sem erro", sql)
		}
	}
	t.Fatalf("lexer não terminou em %q", sql)
	return nil
}

// A tabela fechada de mensagens do §3.7. Cada caso afere o sentinela, a
// substring que identifica a guarda (convenção do projeto: mensagem é
// contrato de teste) E a posição — que é o que o usuário usa para achar o
// erro numa query de uma linha só.
func TestMensagensDeErro(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		msg  string
		pos  int
	}{
		{"caractere inesperado", "SELECT # FROM t", "caractere inesperado '#'", 7},
		{"arroba", "a @ b", "caractere inesperado '@'", 2},
		{"byte nao ascii", "café", "byte inesperado 0xc3 (identificador não-ASCII exige aspas duplas)", 3},
		{"byte de controle", "a \x01 b", "byte inesperado 0x01", 2},
		{"del", "a \x7f b", "byte inesperado 0x7f", 2},
		{"menos sozinho", "a - b", "caractere inesperado '-'", 2},
		{"menos com espaco antes do numero", "- 1", "caractere inesperado '-'", 0},
		{"comentario sql", "a --1", "caractere inesperado '-'", 2},
		{"bang sozinho", "a ! b", "caractere inesperado '!'", 2},
		{"operador antigo", "a <> b", "operador '<>' não é suportado, use '!='", 2},
		{"string sem fechamento", "'abc", "literal string sem fechamento", 0},
		{"string com escape no fim", "'abc''", "literal string sem fechamento", 0},
		{"ident sem fechamento", `"abc`, "identificador entre aspas sem fechamento", 0},
		{"ident vazio", `""`, "identificador entre aspas vazio", 0},
		{"numero invalido", "007", "literal numérico inválido", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := lexUntilErr(t, tc.sql)
			if !errors.Is(err, lexer.ErrSyntax) {
				t.Fatalf("erro = %v, want errors.Is ErrSyntax", err)
			}
			var se *lexer.SyntaxError
			if !errors.As(err, &se) {
				t.Fatalf("erro = %v, want *SyntaxError", err)
			}
			if se.Pos != tc.pos {
				t.Errorf("Pos = %d, want %d", se.Pos, tc.pos)
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("mensagem = %q, want conter %q", err, tc.msg)
			}
			// O prefixo é contrato: o parser reusa a mesma formatação.
			if want := fmt.Sprintf("erro de sintaxe na posição %d:", tc.pos); !strings.HasPrefix(err.Error(), want) {
				t.Errorf("mensagem = %q, want prefixo %q", err, want)
			}
			if se.Pos < 0 || se.Pos > len(tc.sql) {
				t.Errorf("Pos = %d fora de [0,%d]", se.Pos, len(tc.sql))
			}
		})
	}
}

// Sticky: depois do erro, toda chamada devolve O MESMO ponteiro e o cursor
// não anda. Sem isso, um parser que ignore um erro entra em loop ou monta
// uma AST parcial.
func TestErroSticky(t *testing.T) {
	var l lexer.Lexer
	l.Init("SELECT # FROM t")

	if _, err := l.Next(); err != nil {
		t.Fatalf("primeiro token: %v", err)
	}
	_, firstErr := l.Next()
	if firstErr == nil {
		t.Fatal("esperava erro no '#'")
	}
	for i := 0; i < 3; i++ {
		tk, err := l.Next()
		if err != firstErr {
			t.Fatalf("chamada %d devolveu erro diferente: %v (want o mesmo ponteiro)", i, err)
		}
		if tk != (lexer.Token{}) {
			t.Errorf("chamada %d devolveu token %v, want zero", i, tk)
		}
	}
}

func TestEOFIdempotente(t *testing.T) {
	cases := []string{"SELECT * FROM t", "", "   \n\t "}
	for _, sql := range cases {
		t.Run(fmt.Sprintf("%q", sql), func(t *testing.T) {
			var l lexer.Lexer
			l.Init(sql)
			for {
				tk, err := l.Next()
				if err != nil {
					t.Fatalf("erro inesperado: %v", err)
				}
				if tk.Type == lexer.TokenEOF {
					break
				}
			}
			// Três chamadas após o fim: o parser pede lookahead depois do EOF.
			for i := 0; i < 3; i++ {
				tk, err := l.Next()
				if err != nil {
					t.Fatalf("chamada %d após EOF: %v", i, err)
				}
				if tk.Type != lexer.TokenEOF {
					t.Fatalf("chamada %d após EOF = %v", i, tk)
				}
				if tk.Pos != len(sql) {
					t.Errorf("EOF.Pos = %d, want %d", tk.Pos, len(sql))
				}
				if tk.Text != "" {
					t.Errorf("EOF.Text = %q, want vazio", tk.Text)
				}
			}
		})
	}
}

// Reuso: o Init precisa limpar o erro sticky, senão a segunda query "falha"
// sem tocar em nenhum byte.
func TestReuseViaInit(t *testing.T) {
	var l lexer.Lexer

	l.Init("'it''s' # lixo")
	if tk, err := l.Next(); err != nil || tk.Text != "it's" {
		t.Fatalf("query A: %v / %v", tk, err)
	}
	if _, err := l.Next(); err == nil {
		t.Fatal("query A deveria falhar no '#'")
	}

	l.Init("SELECT id FROM t")
	var types []lexer.TokenType
	for {
		tk, err := l.Next()
		if err != nil {
			t.Fatalf("query B após reuso: %v", err)
		}
		types = append(types, tk.Type)
		if tk.Type == lexer.TokenEOF {
			break
		}
	}
	want := []lexer.TokenType{lexer.TokenSelect, lexer.TokenIdent, lexer.TokenFrom, lexer.TokenIdent, lexer.TokenEOF}
	if len(types) != len(want) {
		t.Fatalf("query B: %v", types)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Errorf("query B token %d = %v, want %v", i, types[i], want[i])
		}
	}
}
