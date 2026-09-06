package parser

import "github.com/nishisan-dev/nishidb/internal/query/lexer"

// ParseCondicao parseia uma expressão booleana SOLTA — sem SELECT, sem FROM —
// com a MESMA gramática do WHERE.
//
// Existe para que a condição de uma regra de TTL seja escrita e entendida
// exatamente como um WHERE. Envolver a condição num SELECT sintético daria o
// mesmo resultado ao custo de mensagens de erro apontando para colunas de uma
// query que o operador não escreveu.
//
// Parâmetro posicional é aceito pelo parser e recusado pelo binder: quem sabe
// que não há ninguém para fornecê-lo é o consumidor da condição, não a
// gramática.
func ParseCondicao(sql string) (Expr, error) {
	p := &parser{}
	p.lx.Init(sql)
	if err := p.advance(); err != nil {
		return nil, err
	}
	e, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.Type != lexer.TokenEOF {
		// Sobra é ERRO, e não texto ignorado: um `LIMIT 5` esquecido numa
		// regra viraria, em silêncio, uma regra que faz outra coisa.
		return nil, p.errAt(p.cur.Pos, "conteúdo inesperado após o fim da condição")
	}
	return e, nil
}
