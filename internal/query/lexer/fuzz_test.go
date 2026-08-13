package lexer_test

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
)

// O fuzz não compara com oráculo: ele afere as PROPRIEDADES que o parser
// (Q12) assume do stream. Violar qualquer uma delas quebra o parser de um
// jeito que o teste de mesa não pegaria, porque o input seria inesperado.
func FuzzLexer(f *testing.F) {
	seeds := []string{
		"SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100",
		"SELECT * FROM t;",
		`''''`, `"a""b"`, "-", "<>", "007", "1e+", "'", `"`, "\xff",
		"a >", "café", "\x00", "   ", "",
		`SELECT "Acct-Status-Type" FROM s WHERE a IS NOT NULL`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		var l lexer.Lexer
		l.Init(sql)

		anterior := -1
		// Limite de segurança: cada token consome ao menos 1 byte, então
		// len+2 iterações bastam. Estourar significa que o lexer não avançou.
		for i := 0; i <= len(sql)+2; i++ {
			tk, err := l.Next()
			if err != nil {
				if !errors.Is(err, lexer.ErrSyntax) {
					t.Fatalf("erro sem ErrSyntax: %v", err)
				}
				var se *lexer.SyntaxError
				if !errors.As(err, &se) {
					t.Fatalf("erro não é *SyntaxError: %v", err)
				}
				if se.Pos < 0 || se.Pos > len(sql) {
					t.Fatalf("SyntaxError.Pos = %d fora de [0,%d]", se.Pos, len(sql))
				}
				return
			}
			if tk.Pos < 0 || tk.Pos > len(sql) {
				t.Fatalf("token %v com Pos fora de [0,%d]", tk, len(sql))
			}
			if tk.Pos < anterior {
				t.Fatalf("Pos retrocedeu: %d após %d (token %v)", tk.Pos, anterior, tk)
			}
			anterior = tk.Pos

			switch tk.Type {
			case lexer.TokenEOF:
				if tk.Pos != len(sql) {
					t.Fatalf("EOF.Pos = %d, want %d", tk.Pos, len(sql))
				}
				if tk.Text != "" {
					t.Fatalf("EOF.Text = %q", tk.Text)
				}
				return
			case lexer.TokenNumber:
				// Contrato com o Q12: todo número aceito é conversível pelo
				// typeparse, sem exceção.
				if !typeparse.JSONNumber([]byte(tk.Text)) {
					t.Fatalf("NUMBER(%q) não passa em typeparse.JSONNumber", tk.Text)
				}
			case lexer.TokenIdent:
				if tk.Text == "" {
					t.Fatal("IDENT vazio")
				}
				for j := 0; j < len(tk.Text); j++ {
					c := tk.Text[j]
					ok := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
						(j > 0 && c >= '0' && c <= '9')
					if !ok {
						t.Fatalf("IDENT(%q) com byte inválido %q na posição %d", tk.Text, c, j)
					}
				}
			}
		}
		t.Fatalf("lexer não terminou em %d iterações para %q", len(sql)+2, sql)
	})
}
