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
