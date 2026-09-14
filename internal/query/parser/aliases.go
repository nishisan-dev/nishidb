package parser

import "github.com/nishisan-dev/nishidb/internal/query/lexer"

// parseProjectionAlias consome AS ou um identificador após a coluna. As
// palavras contextuais continuam válidas como nomes de campo; só o novo
// rótulo exige aspas para elas. FROM encerra a projeção sem alias.
func (p *parser) parseProjectionAlias(ref *ColumnRef) error {
	explicit := isWord(p.cur, "as")
	if !explicit && p.cur.Type != lexer.TokenIdent && p.cur.Type != lexer.TokenQuotedIdent {
		return nil
	}
	if ref.Star {
		return p.errAt(p.cur.Pos, "wildcard projection cannot have an alias")
	}
	if explicit {
		if err := p.advance(); err != nil {
			return err
		}
	}
	name, pos, ok := p.identifier()
	if !ok {
		return p.errAt(p.cur.Pos, "expected alias identifier; reserved words require double quotes")
	}
	if p.cur.Type == lexer.TokenIdent && reservedAlias(p.cur) {
		return p.errAt(pos, "reserved word cannot be an unquoted alias; use double quotes")
	}
	ref.Alias, ref.AliasPos = name, pos
	return p.advance()
}

// reservedAlias cobre o vocabulário contextual além das keywords que
// identifier já recusa. Não altera a classificação global do lexer.
func reservedAlias(tok lexer.Token) bool {
	for _, word := range [...]string{"as", "order", "by", "asc", "desc", "nulls", "first", "last", "offset", "like", "group", "join", "having", "distinct", "union"} {
		if isWord(tok, word) {
			return true
		}
	}
	return false
}
