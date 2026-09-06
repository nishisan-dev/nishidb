package parser_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// As posições da AST não são decoração: o binder as usa para apontar a coluna
// exata na mensagem que o usuário lê (`coluna "x" não existe (posição 41)`).
// Uma posição errada — ou zerada — manda o operador olhar para o byte 0 de
// uma query de 200 caracteres, e nada no resultado denuncia isso.
//
// Cada caso abaixo calcula a posição esperada com strings.Index sobre o
// PRÓPRIO SQL: um valor constante (0, ou o de outro token) não passa em mais
// de um caso.

// em devolve o offset da n-ésima ocorrência (1-based) de sub em sql.
func em(t *testing.T, sql, sub string, n int) int {
	t.Helper()
	pos, resto := 0, sql
	for i := 0; i < n; i++ {
		idx := strings.Index(resto, sub)
		if idx < 0 {
			t.Fatalf("%q não tem %d ocorrências de %q", sql, n, sub)
		}
		pos += idx
		resto = resto[idx+len(sub):]
		if i < n-1 {
			pos += len(sub)
		}
	}
	return pos
}

func TestNullCheckColumnPos(t *testing.T) {
	casos := []struct {
		sql    string
		coluna string
		vez    int
		not    bool
	}{
		{"SELECT a FROM x WHERE a IS NULL", "a", 2, false},
		{"SELECT a FROM x WHERE a IS NOT NULL", "a", 2, true},
		// Coluna quoted: a posição é a da ASPA de abertura, que é onde o
		// usuário vê o token começar.
		{`SELECT a FROM x WHERE "Acct-Status-Type" IS NULL`, `"Acct-Status-Type"`, 1, false},
		// Multi-linha: a quebra não pode deslocar a contagem.
		{"SELECT a\nFROM x\nWHERE status IS NOT NULL", "status", 1, true},
		// Dentro de parênteses e à direita de um AND: os dois deslocamentos
		// que mais quebram um cálculo de posição feito "de cabeça".
		{"SELECT a FROM x WHERE (b = 1 AND ip IS NULL)", "ip", 1, false},
	}
	for _, tc := range casos {
		t.Run(tc.sql, func(t *testing.T) {
			st := parseOK(t, tc.sql)
			nc := ultimoNullCheck(t, st.Where)
			if nc.Not != tc.not {
				t.Errorf("Not = %v, want %v", nc.Not, tc.not)
			}
			if want := em(t, tc.sql, tc.coluna, tc.vez); nc.ColumnPos != want {
				t.Errorf("ColumnPos = %d, want %d (%q)", nc.ColumnPos, want, tc.coluna)
			}
		})
	}
}

func TestComparisonColumnPos(t *testing.T) {
	casos := []struct {
		sql    string
		coluna string
		vez    int
	}{
		{"SELECT a FROM x WHERE a = 1", "a", 2},
		{`SELECT a FROM x WHERE "Framed-IP-Address" = '10.0.0.1'`, `"Framed-IP-Address"`, 1},
		{"SELECT a\nFROM x\nWHERE status = 'Stop'", "status", 1},
		{"SELECT a FROM x WHERE (nas_ip = '10.0.0.1')", "nas_ip", 1},
		{"SELECT a FROM x WHERE b = 1 AND username = 'ze'", "username", 1},
		{"SELECT a FROM x WHERE b = 1 OR b = 2 OR terminate = 'x'", "terminate", 1},
	}
	for _, tc := range casos {
		t.Run(tc.sql, func(t *testing.T) {
			st := parseOK(t, tc.sql)
			c := ultimaComparacao(t, st.Where)
			if want := em(t, tc.sql, tc.coluna, tc.vez); c.ColumnPos != want {
				t.Errorf("ColumnPos = %d, want %d (%q)", c.ColumnPos, want, tc.coluna)
			}
		})
	}
}

func TestLimitPos(t *testing.T) {
	casos := []struct {
		sql     string
		literal string
		limite  int64
	}{
		{"SELECT a FROM x LIMIT 10", "10", 10},
		{"SELECT a FROM x WHERE status = 'Stop' LIMIT 500", "500", 500},
		{"SELECT a\nFROM x\nLIMIT 7", "7", 7},
		{"SELECT a FROM x LIMIT 0;", "0", 0},
	}
	for _, tc := range casos {
		t.Run(tc.sql, func(t *testing.T) {
			st := parseOK(t, tc.sql)
			if !st.HasLimit || st.Limit != tc.limite {
				t.Fatalf("HasLimit=%v Limit=%d, want true/%d", st.HasLimit, st.Limit, tc.limite)
			}
			if want := em(t, tc.sql, tc.literal, 1); st.LimitPos != want {
				t.Errorf("LimitPos = %d, want %d", st.LimitPos, want)
			}
		})
	}
}

// ultimoNullCheck/ultimaComparacao descem pela direita da árvore: os casos
// acima põem o nó de interesse sempre no último termo.
func ultimoNullCheck(t *testing.T, e parser.Expr) *parser.NullCheck {
	t.Helper()
	for {
		switch n := e.(type) {
		case *parser.NullCheck:
			return n
		case *parser.LogicalExpr:
			e = n.Right
		default:
			t.Fatalf("nó = %T, want *NullCheck", e)
		}
	}
}

func ultimaComparacao(t *testing.T, e parser.Expr) *parser.Comparison {
	t.Helper()
	for {
		switch n := e.(type) {
		case *parser.Comparison:
			return n
		case *parser.LogicalExpr:
			e = n.Right
		default:
			t.Fatalf("nó = %T, want *Comparison", e)
		}
	}
}
