package parser_test

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// O fuzz afere as invariantes que o binder e o planner assumem da AST. A
// mais importante é implícita: se o parser entrar em loop (o erro nº 1 de
// descida recursiva — um caminho de erro que não consome token), o fuzz mata
// por timeout.
func FuzzParse(f *testing.F) {
	seeds := []string{
		"SELECT * FROM radius_sessions",
		"SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100",
		"SELECT a FROM x WHERE (a = 1 OR b = 2) AND c = 3",
		"SELECT a FROM x WHERE a IS NOT NULL",
		`SELECT "Acct-Status-Type" FROM s WHERE "Framed-IP-Address" = '10.0.0.1'`,
		"select * from x where a = TRUE limit 0;",
		"SELECT a FROM x WHERE a = -1.5e3",
		// Inválidas: o fuzz precisa exercitar os caminhos de erro também.
		"", "SELECT", "SELECT a", "SELECT a FROM", "SELECT a FROM x WHERE",
		"SELECT a FROM x WHERE a = 1 AND AND b = 2", "SELECT a FROM x LIMIT -1",
		"SELECT a FROM x; SELECT b FROM y", "((((", "SELECT # FROM x",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		st, err := parser.Parse(sql)

		// Invariante 1: erro e statement são mutuamente exclusivos.
		if err != nil {
			if st != nil {
				t.Fatalf("erro %v veio com stmt %#v", err, st)
			}
			var se *parser.SyntaxError
			if !errors.As(err, &se) {
				t.Fatalf("erro não é *SyntaxError: %v", err)
			}
			if !errors.Is(err, parser.ErrSyntax) {
				t.Fatalf("erro sem ErrSyntax: %v", err)
			}
			if se.Pos < 0 || se.Pos > len(sql) {
				t.Fatalf("Pos = %d fora de [0,%d]", se.Pos, len(sql))
			}
			return
		}
		if st == nil {
			t.Fatal("stmt nil sem erro")
		}

		// Invariante 2: Star e Projection são exclusivos.
		if st.Star && st.Projection != nil {
			t.Fatalf("Star com Projection = %#v", st.Projection)
		}
		if !st.Star && len(st.Projection) == 0 {
			t.Fatal("sem Star e sem colunas na projeção")
		}
		for _, c := range st.Projection {
			if c.Name == "" {
				t.Fatal("coluna com nome vazio")
			}
			if c.Pos < 0 || c.Pos >= len(sql) {
				t.Fatalf("ColumnRef.Pos = %d fora de [0,%d)", c.Pos, len(sql))
			}
		}
		if st.Dataset == "" {
			t.Fatal("dataset vazio sem erro")
		}
		if st.HasLimit && st.Limit < 0 {
			t.Fatalf("LIMIT negativo aceito: %d", st.Limit)
		}

		// Invariante 3: os placeholders são numerados 0..N-1 na ordem léxica,
		// que é a ordem do percurso in-order.
		var params []int
		var walk func(parser.Expr, int)
		walk = func(e parser.Expr, prof int) {
			// MaxExprDepth limita o aninhamento de PARÊNTESES, não a altura da
			// árvore: `a=1 AND a=1 AND …` é left-leaning e cresce um nível por
			// termo, sem recursão no parser. A guarda anterior (MaxExprDepth+64)
			// afirmava o contrário e reprovava query LEGAL a partir de ~130
			// termos.
			//
			// O que é verdade: todo nó consome ao menos um token, então a altura
			// não passa do tamanho da entrada. Isso ainda protege o walk de
			// recursão infinita numa AST cíclica, que é o risco real aqui.
			if prof > len(sql)+1 {
				t.Fatalf("árvore com altura %d para %d bytes de query: nó sem token", prof, len(sql))
			}
			switch n := e.(type) {
			case nil:
			case *parser.LogicalExpr:
				if n.Op == 0 {
					t.Fatal("LogicalOp zero")
				}
				walk(n.Left, prof+1)
				walk(n.Right, prof+1)
			case *parser.Comparison:
				if n.Op == 0 || n.Operand.Kind == 0 {
					t.Fatalf("nó com constante zero: %#v", n)
				}
				if n.ColumnPos < 0 || n.ColumnPos >= len(sql) {
					t.Fatalf("ColumnPos = %d fora de [0,%d)", n.ColumnPos, len(sql))
				}
				if n.Operand.Kind == parser.OperandParam {
					params = append(params, n.Operand.ParamIdx)
				}
			case *parser.NullCheck:
				if n.ColumnPos < 0 || n.ColumnPos >= len(sql) {
					t.Fatalf("ColumnPos = %d fora de [0,%d)", n.ColumnPos, len(sql))
				}
			default:
				t.Fatalf("nó de tipo desconhecido na AST: %T", e)
			}
		}
		walk(st.Where, 0)

		if len(params) != st.ParamCount {
			t.Fatalf("%d placeholders na árvore, ParamCount = %d", len(params), st.ParamCount)
		}
		for i, idx := range params {
			if idx != i {
				t.Fatalf("ParamIdx fora de ordem: %v (esperado 0,1,2,…)", params)
			}
		}
	})
}
