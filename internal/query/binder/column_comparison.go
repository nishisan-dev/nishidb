package binder

import (
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ConditionOptions liga, no bind de uma condição solta, formas que o WHERE de
// uma consulta não aceita — o par de parser.ConditionOptions. O valor zero é o
// bind de sempre, e é o que BindCondition usa.
type ConditionOptions struct {
	// AllowsLinearColumnComparison aceita parser.ColumnComparison. Sem ela o
	// nó é recusado aqui mesmo que um parser o tenha produzido: a recusa da
	// comparação entre colunas não pode depender de quem montou a AST.
	AllowsLinearColumnComparison bool
}

// BoundColumnComparison é `Left * LeftMultiplier <op> Right * RightMultiplier`
// sobre duas colunas int64 da mesma linha. Os multiplicadores são >= 1. A
// avaliação é exata, em 128 bits com sinal (internal/predicate): nem float, nem
// overflow silencioso.
type BoundColumnComparison struct {
	Left            *schema.CompiledField
	LeftSlot        int
	LeftMultiplier  int64
	Op              parser.CmpOp
	Right           *schema.CompiledField
	RightSlot       int
	RightMultiplier int64
}

func (*BoundColumnComparison) isBoundExpr() {}

// BindConditionWith é BindCondition com as formas que opts liga.
func BindConditionWith(e parser.Expr, sc *schema.Compiled, opts ConditionOptions) (*BoundCondition, error) {
	return bindCondition(e, sc, opts)
}

// columnComparison liga uma ColumnComparison. As DUAS colunas entram nos
// slots: o coletor precisa ler a da direita do payload tanto quanto a da
// esquerda, e quem deriva algo dos campos citados (a impressão digital das
// métricas do alarme) tem de enxergar as duas.
//
// Só int64 dos dois lados. Uma coluna double (a média de uma métrica de
// janela, ou qualquer coluna de taxa) é recusada em vez de convertida: a
// promessa da forma é a comparação inteira exata, e misturar float traria de
// volta o arredondamento que ela existe para evitar.
func (b *binder) columnComparison(n *parser.ColumnComparison) (BoundExpr, error) {
	if !b.allowsColumnComparison {
		return nil, errAt(n.Right.ColumnPos, "comparação entre colunas não é suportada na v1")
	}
	out := &BoundColumnComparison{Op: n.Op, LeftMultiplier: n.Left.Multiplier, RightMultiplier: n.Right.Multiplier}
	for _, side := range []struct {
		term  parser.ColumnTerm
		field **schema.CompiledField
	}{{n.Left, &out.Left}, {n.Right, &out.Right}} {
		if side.term.Multiplier < 1 {
			return nil, errAt(side.term.ColumnPos, "multiplier %d of column %q must be a positive integer (>= 1)",
				side.term.Multiplier, side.term.Column)
		}
		f, err := b.column(side.term.Column, side.term.ColumnPos)
		if err != nil {
			return nil, err
		}
		if t := f.EffectiveType(); t != types.TypeInt64 {
			return nil, errAt(side.term.ColumnPos, "column %q is %s: a column comparison accepts only int64 columns",
				f.Name, t)
		}
		b.markReferenced(f.ID)
		*side.field = f
	}
	return out, nil
}
