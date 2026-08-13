package binder

import (
	"errors"
	"fmt"
)

// ErrBind é a categoria de todo erro de análise semântica. O cliente
// classifica com errors.Is(err, binder.ErrBind) e distingue de erro de
// SINTAXE (parser.ErrSyntax) sem inspecionar mensagem.
var ErrBind = errors.New("erro de bind")

// Error carrega a posição no SQL para o cliente destacar o trecho ofensor.
//
// Pos == -1 significa "não aplicável": erro de parâmetro não tem posição no
// texto da query, porque o parâmetro não está nela.
type Error struct {
	Msg string
	Pos int
}

func (e *Error) Error() string {
	if e.Pos < 0 {
		return e.Msg
	}
	return fmt.Sprintf("%s (posição %d)", e.Msg, e.Pos)
}

func (e *Error) Unwrap() error { return ErrBind }

func errAt(pos int, format string, args ...any) error {
	return &Error{Msg: fmt.Sprintf(format, args...), Pos: pos}
}

// errNoPos é para o que não tem lugar no SQL — hoje, só os parâmetros.
func errNoPos(format string, args ...any) error {
	return &Error{Msg: fmt.Sprintf(format, args...), Pos: -1}
}
