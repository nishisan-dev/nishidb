package lexer

import (
	"errors"
	"fmt"
)

// ErrSyntax é a categoria de todo erro sintático do front-end SQL — lexer E
// parser. Use errors.Is(err, lexer.ErrSyntax).
//
// Mora aqui, e não num pacote de erros próprio, para que o cliente classifique
// erro de query com um único errors.Is: o parser (Q12) reporta os erros dele
// com este mesmo tipo e a mesma formatação.
var ErrSyntax = errors.New("erro de sintaxe em SQL")

// SyntaxError localiza o erro por offset de byte no SQL recebido.
//
// A posição é em BYTES, como no jsonp: com UTF-8 dentro de aspas, ela salta
// mais de 1 por caractere, e é assim que deve ser — o cliente que quiser
// coluna visual converte, mas a fonte da verdade é o offset.
type SyntaxError struct {
	Pos int    // offset de byte, 0-based, sempre em [0, len(sql)]
	Msg string // descrição sem a posição (ex.: `caractere inesperado '#'`)
	// Clause nomeia a cláusula SQL que a v1 não implementa ("ORDER BY",
	// "JOIN", ...) quando foi ela que causou o erro; vazio no erro sintático
	// comum. Quem lê o campo escolhe entre dois códigos de wire: SYNTAX
	// ("você digitou errado") e UNSUPPORTED_CLAUSE ("isto não existe aqui").
	//
	// É CAMPO, e não um tipo de erro próprio com Unwrap, de propósito. Com um
	// tipo, o servidor teria de testar errors.As(cláusula) ANTES de
	// errors.Is(ErrSyntax) na tradução do erro — e trocar a ordem dos dois
	// faria o código novo nunca disparar, degradando em silêncio para SYNTAX.
	// Com o campo, a condição é `se.Clause != ""` e não há ordem a errar; e
	// tudo que já classificava por ErrSyntax continua funcionando sem
	// mudança.
	Clause string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("erro de sintaxe na posição %d: %s", e.Pos, e.Msg)
}

func (e *SyntaxError) Unwrap() error { return ErrSyntax }

// NewSyntaxError constrói o erro. Exportado porque o parser (Q12) reporta os
// erros dele com o MESMO tipo e a MESMA formatação.
func NewSyntaxError(pos int, msg string) *SyntaxError {
	return &SyntaxError{Pos: pos, Msg: msg}
}

// NewUnsupportedClauseSyntaxError constrói a recusa de uma cláusula que a v1 não tem.
//
// A mensagem é montada aqui, e não pelo chamador, para que as sete cláusulas
// saiam com a MESMA frase: quem escreve SQL numa ferramenta lê a diferença
// entre "não suportado" e "erro de digitação" na forma da mensagem, não só no
// código de wire — que ele nem vê.
func NewUnsupportedClauseSyntaxError(pos int, clause string) *SyntaxError {
	return &SyntaxError{
		Pos:    pos,
		Msg:    clause + " não é suportado nesta versão",
		Clause: clause,
	}
}

// ASCIIEqualFold compara s com lower (que precisa estar em minúsculas) sem
// diferenciar caixa, em ASCII.
//
// Exportada porque o parser precisa da MESMA regra de folding que o lexer usa
// para reconhecer keyword. strings.EqualFold não serve: ele aplica folding
// UNICODE, e o Kelvin sign dobra para 'k' — duas regras de folding no mesmo
// front-end é a divergência que o comentário de keywordType já existia para
// impedir.
//
// Diferente do asciiEqualFold interno, esta CONFERE o comprimento: lá o
// switch len(s) já o garantiu, aqui o chamador é externo.
func ASCIIEqualFold(s, lower string) bool {
	if len(s) != len(lower) {
		return false
	}
	return asciiEqualFoldSameLen(s, lower)
}
