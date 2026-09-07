package parser_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// Estes testes verificam APENAS a forma da AST das queries da §106 — a
// rastreabilidade normativa dos 12 casos é do Q15, fim a fim, via
// query.Engine. Aqui o objetivo é garantir que a forma que o cliente envia
// vira a árvore que o binder espera.

func whereComparison(t *testing.T, sql string) *parser.Comparison {
	t.Helper()
	c, ok := parseOK(t, sql).Where.(*parser.Comparison)
	if !ok {
		t.Fatalf("Where de %q não é Comparison", sql)
	}
	return c
}

func TestAST106PrimaryLookup(t *testing.T) {
	c := whereComparison(t, "SELECT * FROM radius_sessions WHERE id = 'CMP-V0001PPCLP-013'")
	if c.Column != "id" || c.Op != parser.CmpEq || c.Operand.S != "CMP-V0001PPCLP-013" {
		t.Errorf("predicado = %#v", c)
	}
}

func TestAST106BitmapLookup(t *testing.T) {
	c := whereComparison(t, "SELECT * FROM radius_sessions WHERE status = 'Stop'")
	if c.Column != "status" || c.Operand.Kind != parser.OperandString {
		t.Errorf("predicado = %#v", c)
	}
}

func TestAST106HashLookup(t *testing.T) {
	c := whereComparison(t, "SELECT * FROM radius_sessions WHERE nas = 'i-br-sp-cmp-cmp-hl4-01'")
	if c.Column != "nas" || c.Operand.S != "i-br-sp-cmp-cmp-hl4-01" {
		t.Errorf("predicado = %#v", c)
	}
}

func TestAST106And(t *testing.T) {
	st := parseOK(t, "SELECT id FROM radius_sessions WHERE status = 'Stop' AND nas = 'x'")
	l, ok := st.Where.(*parser.LogicalExpr)
	if !ok || l.Op != parser.LogicalAnd {
		t.Fatalf("Where = %#v, want AND", st.Where)
	}
}

func TestAST106Or(t *testing.T) {
	st := parseOK(t, "SELECT id FROM radius_sessions WHERE status = 'Stop' OR status = 'Start'")
	l, ok := st.Where.(*parser.LogicalExpr)
	if !ok || l.Op != parser.LogicalOr {
		t.Fatalf("Where = %#v, want OR", st.Where)
	}
}

func TestAST106Comparison(t *testing.T) {
	cases := []struct {
		sql string
		op  parser.CmpOp
	}{
		{"SELECT * FROM s WHERE a = 1", parser.CmpEq},
		{"SELECT * FROM s WHERE a != 1", parser.CmpNe},
		{"SELECT * FROM s WHERE a > 1", parser.CmpGt},
		{"SELECT * FROM s WHERE a >= 1", parser.CmpGte},
		{"SELECT * FROM s WHERE a < 1", parser.CmpLt},
		{"SELECT * FROM s WHERE a <= 1", parser.CmpLte},
	}
	for _, tc := range cases {
		t.Run(tc.op.String(), func(t *testing.T) {
			if got := whereComparison(t, tc.sql).Op; got != tc.op {
				t.Errorf("Op = %v, want %v", got, tc.op)
			}
		})
	}
}

func TestAST106Null(t *testing.T) {
	st := parseOK(t, "SELECT * FROM s WHERE a IS NULL OR b IS NOT NULL")
	root, ok := st.Where.(*parser.LogicalExpr)
	if !ok || root.Op != parser.LogicalOr {
		t.Fatalf("Where = %#v", st.Where)
	}
	left, ok := root.Left.(*parser.NullCheck)
	if !ok || left.Not {
		t.Errorf("esquerda = %#v, want IS NULL", root.Left)
	}
	right, ok := root.Right.(*parser.NullCheck)
	if !ok || !right.Not {
		t.Errorf("direita = %#v, want IS NOT NULL", root.Right)
	}
}

func TestAST106Limit(t *testing.T) {
	st := parseOK(t, "SELECT * FROM s LIMIT 100")
	if !st.HasLimit || st.Limit != 100 {
		t.Errorf("HasLimit=%v Limit=%d", st.HasLimit, st.Limit)
	}
}

func TestAST106FullScan(t *testing.T) {
	st := parseOK(t, "SELECT * FROM radius_sessions")
	if st.Where != nil {
		t.Errorf("Where = %#v, want nil (full scan)", st.Where)
	}
	// Full scan com predicado não indexável: a decisão de plano é do Q14, a
	// forma aqui é a mesma.
	c := whereComparison(t, "SELECT * FROM radius_sessions WHERE username != 'x'")
	if c.Op != parser.CmpNe {
		t.Errorf("Op = %v", c.Op)
	}
}

func TestAST106Aliases(t *testing.T) {
	// Alias do YAML é identificador comum para o parser; quem resolve é o
	// binder.
	st := parseOK(t, "SELECT service_id, ip, nas FROM radius_sessions WHERE status = ?")
	if len(st.Projection) != 3 || st.Projection[0].Name != "service_id" {
		t.Errorf("projeção = %#v", st.Projection)
	}
	if st.ParamCount != 1 {
		t.Errorf("ParamCount = %d, want 1", st.ParamCount)
	}
}

func TestAST106QuotedFields(t *testing.T) {
	st := parseOK(t, `SELECT "Acct-Status-Type" FROM radius_sessions WHERE "Framed-IP-Address" = '10.0.0.1'`)
	if st.Projection[0].Name != "Acct-Status-Type" {
		t.Errorf("projeção = %#v", st.Projection)
	}
	if c := st.Where.(*parser.Comparison); c.Column != "Framed-IP-Address" {
		t.Errorf("coluna = %q", c.Column)
	}
}

func TestAST106Parameters(t *testing.T) {
	st := parseOK(t, "SELECT id FROM s WHERE status = ? AND nas = ? AND ip = ?")
	if st.ParamCount != 3 {
		t.Errorf("ParamCount = %d, want 3", st.ParamCount)
	}
}
