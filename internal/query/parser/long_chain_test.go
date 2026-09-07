package parser_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// Uma cadeia longa de AND é query LEGAL: `MaxExprDepth` limita o aninhamento
// de PARÊNTESES, e a espinha de AND/OR é construída iterativamente, sem
// recursão. Uma query gerada por máquina (um IN expandido, um filtro montado
// por UI) chega a centenas de termos sem intenção nenhuma.
//
// O teste existe porque a guarda antiga do fuzz afirmava o contrário e teria
// reprovado exatamente este caso.
func TestCadeiaLongaDeAndEhLegal(t *testing.T) {
	for _, n := range []int{130, 500} {
		terms := make([]string, n)
		for i := range terms {
			terms[i] = "a = 1"
		}
		sql := "SELECT a FROM x WHERE " + strings.Join(terms, " AND ")

		st, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("%d termos: %v", n, err)
		}
		// A árvore é left-leaning: height n-1, espinha à esquerda. É o formato
		// que o flatten do planner assume.
		height, e := 0, st.Where
		for {
			l, ok := e.(*parser.LogicalExpr)
			if !ok {
				break
			}
			if _, isLeaf := l.Right.(*parser.Comparison); !isLeaf {
				t.Fatalf("%d termos: filho direito = %T, want folha", n, l.Right)
			}
			height++
			e = l.Left
		}
		if height != n-1 {
			t.Errorf("%d termos: altura = %d, want %d", n, height, n-1)
		}
	}
}
