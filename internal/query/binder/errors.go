package binder

import (
	"errors"
	"fmt"
)

// ErrBind é a categoria de todo erro de análise semântica. O cliente
// classifica com errors.Is(err, binder.ErrBind) e distingue de erro de
// SINTAXE (parser.ErrSyntax) sem inspecionar mensagem.
var ErrBind = errors.New("erro de bind")

// MaxBindDepth limita a recursão do bind pelo lado DIREITO da árvore. É um
// teto de RECURSO, não de gramática: o lado esquerdo (a espinha de AND/OR, que
// é onde uma query grande cresce) é percorrido iterativamente e não tem teto
// nenhum — uma cadeia de 50 mil termos binda com pilha constante.
//
// O valor é folgado de propósito. Uma AST vinda do parser nunca chega perto:
// aprofundar à direita exige PARÊNTESES, e o parser já os limita a
// parser.MaxExprDepth (64). Quem alcança este teto é uma AST montada à mão —
// o Bind é público e recebe uma árvore que qualquer chamador pode construir.
const MaxBindDepth = 1024

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
