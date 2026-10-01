package parser

import (
	"strings"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
)

// ConditionOptions liga, no parse de uma condição solta, formas que o WHERE de
// uma consulta não aceita. O valor zero é a gramática do WHERE, byte a byte —
// é o que ParseCondition usa.
type ConditionOptions struct {
	// AllowsLinearColumnComparison aceita `<col> [* k] <op> <col> [* k]`, a
	// comparação linear inteira entre duas colunas da MESMA linha. Só a
	// condição de alarme (`when`/`qualifyWhen`) a liga: é lá que a proporção
	// entre `total` e `rows` do grupo é a pergunta. O WHERE do SQL, o `where`
	// e o `scope` do contador e o `when` do TTL continuam recusando coluna à
	// direita do comparador.
	AllowsLinearColumnComparison bool
}

// ColumnComparison é `<col> [* k] <op> <col> [* k]`: duas colunas da mesma
// linha, cada uma multiplicada por um inteiro k >= 1 (1 quando ausente).
//
// É um NÓ, e não um OperandKind novo dentro de Comparison, pela razão que o
// LikeExpr registra: nó desconhecido cai no `default` barulhento do binder e
// do predicado, enquanto um operando novo poderia escorregar pelo caminho de
// literal de quem não o conhece. Só ParseConditionWith com
// AllowsLinearColumnComparison o produz.
type ColumnComparison struct {
	Left  ColumnTerm
	Op    CmpOp
	Right ColumnTerm
}

// ColumnTerm é um lado de ColumnComparison.
type ColumnTerm struct {
	Column    string
	ColumnPos int
	// Multiplier é o inteiro que multiplica a coluna; sempre >= 1, e 1 quando
	// o `* k` não foi escrito.
	Multiplier int64
}

func (*ColumnComparison) isExpr() {}

// ParseConditionWith é ParseCondition com as formas que opts liga. Com opts
// zero, as duas são a mesma função.
func ParseConditionWith(sql string, opts ConditionOptions) (Expr, error) {
	p := &parser{allowsColumnComparison: opts.AllowsLinearColumnComparison}
	return p.parseLooseCondition(sql)
}

// refuseLeadingMultiplier recusa `k * col` com mensagem própria, no modo que
// aceita comparação entre colunas. Fora dele não faz nada: o literal à
// esquerda continua caindo no "esperado nome de coluna" de sempre.
func (p *parser) refuseLeadingMultiplier() error {
	if !p.allowsColumnComparison || p.cur.Type != lexer.TokenNumber {
		return nil
	}
	next, err := p.peek()
	if err != nil || next.Type != lexer.TokenStar {
		return nil
	}
	return p.errAt(p.cur.Pos, "the multiplier goes to the right of the column: write `<column> * "+p.cur.Text+
		"`, not `"+p.cur.Text+" * <column>`")
}

// parseMultiplier lê o `* k` depois de uma coluna e devolve k. Chamada com o
// parser NO `*`. Recusa, cada caso com a sua mensagem: k ausente ou coluna no
// lugar dele, k fracionário, k zero ou negativo, k fora do int64.
func (p *parser) parseMultiplier() (int64, error) {
	if err := p.advance(); err != nil {
		return 0, err
	}
	tok := p.cur
	if tok.Type != lexer.TokenNumber {
		return 0, p.errAt(tok.Pos, "the multiplier must be an integer literal: a column comparison multiplies a column "+
			"by a constant, never by another column")
	}
	if strings.ContainsAny(tok.Text, ".eE") {
		return 0, p.errAt(tok.Pos, "multiplier "+tok.Text+" is not an integer: a column comparison multiplies only by "+
			"a positive integer literal")
	}
	k, err := typeparse.Int64([]byte(tok.Text))
	if err != nil {
		return 0, p.errAt(tok.Pos, "multiplier "+tok.Text+" does not fit in int64")
	}
	if k < 1 {
		return 0, p.errAt(tok.Pos, "multiplier "+tok.Text+" must be a positive integer (>= 1)")
	}
	if err := p.advance(); err != nil {
		return 0, err
	}
	return k, nil
}

// parseColumnComparisonRest continua o termo depois do comparador, com a
// coluna da esquerda (e o seu multiplicador) já lida. Devolve (nil, nil)
// quando a direita não é coluna — o chamador segue pelo caminho do literal.
func (p *parser) parseColumnComparisonRest(left ColumnTerm, op CmpOp) (Expr, error) {
	if err := p.refuseLeadingMultiplier(); err != nil {
		return nil, err
	}
	name, pos, ok := p.identifier()
	if !ok {
		if left.Multiplier != 1 {
			return nil, p.errAt(p.cur.Pos, "a multiplier is only accepted when both sides are columns: compare the column "+
				"with the literal directly (`total <= 10`, not `total * 10 <= 100`)")
		}
		return nil, nil
	}
	if err := p.advance(); err != nil {
		return nil, err
	}
	if p.cur.Type == lexer.TokenDot {
		return nil, p.errAt(p.cur.Pos, "a column comparison accepts only plain columns, without a nested path")
	}
	right := ColumnTerm{Column: name, ColumnPos: pos, Multiplier: 1}
	if p.cur.Type == lexer.TokenStar {
		k, err := p.parseMultiplier()
		if err != nil {
			return nil, err
		}
		right.Multiplier = k
	}
	return &ColumnComparison{Left: left, Op: op, Right: right}, nil
}
