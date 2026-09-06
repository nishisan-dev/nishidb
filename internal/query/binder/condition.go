package binder

import (
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// BoundCondicao é uma condição resolvida contra o schema, com seus próprios
// slots. É o que o TTL condicional consome.
type BoundCondicao struct {
	Expr  BoundExpr
	Slots Slots

	// arena guarda os bytes dos literais; as janelas em Val.Bytes apontam para
	// cá, e é a retenção deste slice que as mantém válidas depois do retorno.
	arena []byte
}

// BindCondicao resolve uma condição solta contra o schema, pelo MESMO caminho
// do WHERE: mesma coerção, mesma tabela de comparabilidade, mesmo tratamento
// de coluna auto. É essa identidade que garante que `type = 2` numa regra e
// `type = 2` numa consulta signifiquem a mesma coisa.
//
// Parâmetro posicional é recusado: a condição vem do YAML e não há quem o
// forneça.
func BindCondicao(e parser.Expr, sc *schema.Compiled) (*BoundCondicao, error) {
	if e == nil || sc == nil {
		return nil, errNoPos("bind de condição sem expressão ou schema")
	}
	b := &binder{sc: sc, semParams: true, usados: map[types.FieldID]struct{}{}}
	expr, err := b.expr(e)
	if err != nil {
		return nil, err
	}
	bc := &BoundCondicao{Expr: expr, Slots: b.slots()}
	b.preencheSlotsDaExpr(bc.Expr, bc.Slots)
	// As janelas só ficam estáveis depois que o arena para de crescer, como no
	// Bind: emiti-las durante os appends deixaria Val.Bytes apontando para um
	// array que o append seguinte pode realocar.
	for _, j := range b.pendente {
		j.cmp.Val.Bytes = b.arena[j.start:j.end:j.end]
	}
	bc.arena = b.arena
	return bc, nil
}
