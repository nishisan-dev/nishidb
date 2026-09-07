package parser_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// Os rótulos aparecem nas mensagens do binder ("tipo string incomparável
// com…") e no Plan.String() do planner: um mapeamento trocado manda o
// operador investigar a coisa errada.
func TestRotulosDasConstantes(t *testing.T) {
	t.Run("LogicalOp", func(t *testing.T) {
		cases := map[parser.LogicalOp]string{
			parser.LogicalAnd: "AND",
			parser.LogicalOr:  "OR",
		}
		for op, want := range cases {
			if got := op.String(); got != want {
				t.Errorf("LogicalOp(%d).String() = %q, want %q", op, got, want)
			}
		}
		// Zero value é inválido de propósito e precisa se anunciar.
		if got := parser.LogicalOp(0).String(); !strings.Contains(got, "?") {
			t.Errorf("LogicalOp(0).String() = %q, want marcador de inválido", got)
		}
	})

	t.Run("CmpOp", func(t *testing.T) {
		cases := map[parser.CmpOp]string{
			parser.CmpEq:  "=",
			parser.CmpNe:  "!=",
			parser.CmpGt:  ">",
			parser.CmpGte: ">=",
			parser.CmpLt:  "<",
			parser.CmpLte: "<=",
		}
		for op, want := range cases {
			if got := op.String(); got != want {
				t.Errorf("CmpOp(%d).String() = %q, want %q", op, got, want)
			}
		}
		if got := parser.CmpOp(0).String(); !strings.Contains(got, "?") {
			t.Errorf("CmpOp(0).String() = %q, want marcador de inválido", got)
		}
	})

	t.Run("OperandKind", func(t *testing.T) {
		// O vocabulário é o da tabela de coerção do binder: mudar aqui muda a
		// mensagem de erro de tipo que o usuário lê.
		cases := map[parser.OperandKind]string{
			parser.OperandString: "string",
			parser.OperandInt:    "int",
			parser.OperandFloat:  "float",
			parser.OperandBool:   "bool",
			parser.OperandParam:  "param",
		}
		for k, want := range cases {
			if got := k.String(); got != want {
				t.Errorf("OperandKind(%d).String() = %q, want %q", k, got, want)
			}
		}
		if got := parser.OperandKind(0).String(); !strings.Contains(got, "?") {
			t.Errorf("OperandKind(0).String() = %q, want marcador de inválido", got)
		}
	})
}

// Expr é selada: só os três nós deste pacote a implementam. É isso que
// permite ao binder usar type switch exaustivo com default de erro interno.
func TestExprEhSeladaComTresNos(t *testing.T) {
	var _ parser.Expr = (*parser.LogicalExpr)(nil)
	var _ parser.Expr = (*parser.Comparison)(nil)
	var _ parser.Expr = (*parser.NullCheck)(nil)

	// Um type switch exaustivo cobre toda árvore que o parser produz.
	st := parseOK(t, "SELECT a FROM x WHERE (a = 1 OR b IS NULL) AND c != 'z'")
	var count func(parser.Expr) int
	count = func(e parser.Expr) int {
		switch n := e.(type) {
		case *parser.LogicalExpr:
			return 1 + count(n.Left) + count(n.Right)
		case *parser.Comparison, *parser.NullCheck:
			return 1
		default:
			t.Fatalf("nó fora da interface selada: %T", e)
			return 0
		}
	}
	if got := count(st.Where); got != 5 {
		t.Errorf("%d nós na árvore, want 5", got)
	}
}

// O erro do lexer pode nascer em QUALQUER ponto da descida; cada caso abaixo
// pega um `advance()` diferente. Sem eles, um advance que engolisse o erro
// passaria despercebido no caminho feliz.
func TestErroDoLexerEmCadaProducao(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"apos select", "SELECT # FROM x"},
		{"apos coluna", "SELECT a # FROM x"},
		{"apos virgula", "SELECT a, # FROM x"},
		{"apos from", "SELECT a FROM #"},
		{"apos dataset", "SELECT a FROM x #"},
		{"apos where", "SELECT a FROM x WHERE #"},
		{"apos coluna do predicado", "SELECT a FROM x WHERE a #"},
		{"apos comparador", "SELECT a FROM x WHERE a = #"},
		{"apos operando", "SELECT a FROM x WHERE a = 1 #"},
		{"apos is", "SELECT a FROM x WHERE a IS #"},
		{"apos is not", "SELECT a FROM x WHERE a IS NOT #"},
		{"apos null", "SELECT a FROM x WHERE a IS NULL #"},
		{"apos and", "SELECT a FROM x WHERE a = 1 AND #"},
		{"apos or", "SELECT a FROM x WHERE a = 1 OR #"},
		{"apos abre parenteses", "SELECT a FROM x WHERE (#"},
		{"apos fecha parenteses", "SELECT a FROM x WHERE (a = 1) #"},
		{"apos limit", "SELECT a FROM x LIMIT #"},
		{"apos numero do limit", "SELECT a FROM x LIMIT 10 #"},
		{"apos ponto e virgula", "SELECT a FROM x; #"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := parser.Parse(tc.sql)
			if err == nil {
				t.Fatalf("Parse(%q) não falhou: %#v", tc.sql, st)
			}
			if st != nil {
				t.Errorf("stmt = %#v, want nil", st)
			}
			// Nem toda posição produz a mensagem do lexer (algumas param
			// antes, no parser); o que importa é que o erro sobe e é
			// sintático.
			if !strings.HasPrefix(err.Error(), "erro de sintaxe na posição") {
				t.Errorf("mensagem = %q", err)
			}
		})
	}
}

// Operando com número inválido no meio: o erro vem do typeparse via
// numberOperand, com a posição do literal.
func TestOperandoFloatInvalido(t *testing.T) {
	// O lexer aceita o lexema (gramática JSON), então quem recusa é o
	// conversor — este caso existe para cobrir esse ramo.
	sql := "SELECT a FROM x WHERE b = 1e400" // overflow de float64 → +Inf
	st, err := parser.Parse(sql)
	if err != nil {
		// Aceitável: se o typeparse recusar, precisa ser erro de sintaxe com
		// posição.
		if !strings.Contains(err.Error(), "erro de sintaxe na posição 26") {
			t.Errorf("erro = %q", err)
		}
		return
	}
	// Se aceitou, o valor precisa ser um float (a decisão de recusar Inf é do
	// typeparse, e o parser só repassa).
	if c := st.Where.(*parser.Comparison); c.Operand.Kind != parser.OperandFloat {
		t.Errorf("Operand = %#v, want float", c.Operand)
	}
}
