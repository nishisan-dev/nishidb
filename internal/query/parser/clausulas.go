package parser

import "github.com/nishisan-dev/nishidb/internal/query/lexer"

// clausulasNaoSuportadas mapeia a palavra que abre uma cláusula ausente da v1
// para o nome que a mensagem publica.
//
// ORDER e GROUP publicam "ORDER BY" e "GROUP BY" mesmo sem o BY ter sido lido.
// Exigir o BY custaria um SEGUNDO lookahead num parser que se declara LL(1)
// ("a gramática é LL(1) e um passo basta"), e não compraria nada: ORDER em
// posição de cláusula não pode ser outra coisa.
//
// A lista é a do §9, não uma escolha nova: JOIN, GROUP BY, HAVING, UNION e
// DISTINCT estão nos não-objetivos da v1, e ORDER BY e OFFSET no README.
var clausulasNaoSuportadas = map[string]string{
	"order":    "ORDER BY",
	"group":    "GROUP BY",
	"offset":   "OFFSET",
	"join":     "JOIN",
	"having":   "HAVING",
	"distinct": "DISTINCT",
	"union":    "UNION",
}

// clausulaNaoSuportada devolve o nome da cláusula que tok abre, se abrir
// alguma.
//
// Duas condições, e as duas importam:
//
//   - SÓ TokenIdent, nunca TokenQuotedIdent. A doutrina do lexer é que quoted
//     NUNCA é keyword, e `SELECT "order" FROM t` seleciona uma coluna chamada
//     order — quem a quotou o fez de propósito.
//   - Comparação por lexer.ASCIIEqualFold, e não strings.EqualFold: o folding
//     Unicode do segundo dobra o Kelvin sign para 'k', e duas regras de
//     folding no mesmo front-end divergem no primeiro caso exótico.
//
// Note o que esta função NÃO faz: ela não decide sozinha que houve erro. Só é
// consultada nos sítios onde o parser JÁ ia recusar a query — a palavra
// continua sendo um identificador válido em toda posição em que a gramática a
// aceita.
func clausulaNaoSuportada(tok lexer.Token) (string, bool) {
	if tok.Type != lexer.TokenIdent {
		return "", false
	}
	for palavra, nome := range clausulasNaoSuportadas {
		if lexer.ASCIIEqualFold(tok.Text, palavra) {
			return nome, true
		}
	}
	return "", false
}
