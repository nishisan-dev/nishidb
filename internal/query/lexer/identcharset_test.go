package lexer_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

// O charset do identificador BARE é uma fronteira de correção, não estilo.
// Os nomes reais do dataset têm hífen (`Acct-Status-Type`, `NAS-IP-Address`),
// e é por isso que as aspas duplas são obrigatórias para eles. Se o hífen
// entrasse no identificador bare, `WHERE Acct-Status-Type = 'Stop'` deixaria
// de ser erro de sintaxe e passaria a resolver uma coluna — com o agravante de
// que o operador nunca saberia que escreveu algo ambíguo.
//
// A invariante existe no fuzz do IDENT; aqui ela fica determinística e com
// diagnóstico direto.
func TestIdentBareNaoAceitaHifen(t *testing.T) {
	casos := []struct {
		sql    string
		posErr int
	}{
		{"SELECT * FROM radius-sessions", len("SELECT * FROM radius")},
		{"SELECT a FROM x WHERE Acct-Status-Type = 'Stop'", len("SELECT a FROM x WHERE Acct")},
		{"SELECT nas-ip FROM x", len("SELECT nas")},
	}
	for _, tc := range casos {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := lexAllErr(t, tc.sql)
			if err == nil {
				t.Fatalf("%q foi aceito: hífen não pode entrar em identificador bare", tc.sql)
			}
			var se *lexer.SyntaxError
			if !errors.As(err, &se) {
				t.Fatalf("erro = %v, want *SyntaxError", err)
			}
			if se.Pos != tc.posErr {
				t.Errorf("Pos = %d, want %d", se.Pos, tc.posErr)
			}
			if !strings.Contains(err.Error(), "'-'") {
				t.Errorf("mensagem = %q, want apontar o caractere", err)
			}
		})
	}

	// O MESMO nome entre aspas é aceito, e é a forma que o usuário deve usar.
	// Sem esta metade, "recusar hífen" poderia ser cumprido recusando o nome
	// hifenizado em qualquer forma — o que tornaria o dataset inconsultável.
	tk := primeiroIdentQuoted(t, `SELECT "Acct-Status-Type" FROM x`)
	if tk.Text != "Acct-Status-Type" {
		t.Errorf("quoted = %q, want Acct-Status-Type", tk.Text)
	}
}

// lexAllErr consome o stream até EOF ou erro.
func lexAllErr(t *testing.T, sql string) ([]lexer.Token, error) {
	t.Helper()
	var l lexer.Lexer
	l.Init(sql)
	var out []lexer.Token
	for i := 0; i <= len(sql)+2; i++ {
		tk, err := l.Next()
		if err != nil {
			return out, err
		}
		if tk.Type == lexer.TokenEOF {
			return out, nil
		}
		out = append(out, tk)
	}
	t.Fatalf("lexer não terminou para %q", sql)
	return nil, nil
}

func primeiroIdentQuoted(t *testing.T, sql string) lexer.Token {
	t.Helper()
	toks, err := lexAllErr(t, sql)
	if err != nil {
		t.Fatal(err)
	}
	for _, tk := range toks {
		if tk.Type == lexer.TokenQuotedIdent {
			return tk
		}
	}
	t.Fatalf("nenhum ident quoted em %q", sql)
	return lexer.Token{}
}
