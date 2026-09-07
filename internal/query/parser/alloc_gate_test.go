//go:build !race

package parser_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// Tripwire, não meta: parse é caminho frio (uma vez por query) e alocar a AST
// é esperado. O teto existe para pegar regressão de ordem de grandeza —
// backtracking que reparseia, materialização do slice de tokens, ou alguém
// trocando o descent por regexp. Se este teste cair, a pergunta não é "como
// otimizar", é "o que passou a alocar por token".
func TestParseAllocsTripwire(t *testing.T) {
	const ceiling = 64.0
	got := testing.AllocsPerRun(100, func() {
		st, err := parser.Parse(querySpec72)
		if err != nil {
			t.Fatal(err)
		}
		sinkStmt = st
	})
	if got > ceiling {
		t.Errorf("Parse alocou %.0f objetos, teto %.0f", got, ceiling)
	}
	t.Logf("allocs por parse: %.0f (teto %.0f)", got, ceiling)
}
