//go:build !race

package lexer_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

// Gate G1: lexar uma query SEM escapes não aloca. Todo Text é subslice do
// fonte, Token é valor e o Lexer é reutilizado entre queries.
//
// Fora do build com -race (a instrumentação altera a contagem).
func TestZeroAllocs(t *testing.T) {
	queries := []string{
		querySpec68,
		queryComplexa,
		"SELECT * FROM t;",
		`SELECT "Acct-Status-Type" FROM s WHERE a IS NOT NULL AND b >= -1.5e3`,
	}
	var l lexer.Lexer
	for _, sql := range queries {
		t.Run(sql[:min(len(sql), 30)], func(t *testing.T) {
			got := testing.AllocsPerRun(100, func() {
				l.Init(sql)
				for {
					tk, err := l.Next()
					if err != nil {
						t.Fatal(err)
					}
					sink = tk
					if tk.Type == lexer.TokenEOF {
						return
					}
				}
			})
			if got != 0 {
				t.Errorf("lexar %q alocou %.1f objetos, want 0", sql, got)
			}
		})
	}
}

// Gate G2: o custo de escape é O(literais escapados), NÃO O(tokens). Uma
// query com muitos tokens e um escape só não pode alocar mais que uma com
// poucos tokens e um escape só.
func TestAllocsCrescemComEscapesNaoComTokens(t *testing.T) {
	var l lexer.Lexer
	medir := func(sql string) float64 {
		return testing.AllocsPerRun(100, func() {
			l.Init(sql)
			for {
				tk, err := l.Next()
				if err != nil {
					t.Fatal(err)
				}
				sink = tk
				if tk.Type == lexer.TokenEOF {
					return
				}
			}
		})
	}

	umEscape := medir(`SELECT a FROM t WHERE x = 'it''s'`)
	if umEscape > 2 {
		t.Errorf("1 escape alocou %.1f, want <= 2", umEscape)
	}

	// Mesmo escape, MUITO mais tokens: a alocação não pode acompanhar.
	umEscapeMuitosTokens := medir(
		`SELECT a, b, c, d, e, f, g, h FROM t WHERE x = 'it''s' AND a > 1 AND b < 2 AND c >= 3 AND d <= 4 AND e != 5`)
	if umEscapeMuitosTokens > umEscape {
		t.Errorf("mesma quantidade de escapes com mais tokens alocou %.1f (antes %.1f): "+
			"o custo está acompanhando o número de tokens", umEscapeMuitosTokens, umEscape)
	}

	tresEscapes := medir(queryComEscapes)
	if tresEscapes > 4 {
		t.Errorf("3 escapes alocaram %.1f, want <= 4", tresEscapes)
	}
}
