package parser

import "github.com/nishisan-dev/nishidb/internal/query/lexer"

// SyntaxError é o MESMO tipo produzido pelo lexer: o front-end SQL tem uma
// categoria só de erro sintático, e o cliente não precisa saber em que
// estágio o erro nasceu para classificá-lo.
type SyntaxError = lexer.SyntaxError

// ErrSyntax é o mesmo sentinela do lexer, reexportado para que o chamador
// escreva errors.Is(err, parser.ErrSyntax) sem importar os dois pacotes.
var ErrSyntax = lexer.ErrSyntax

// MaxExprDepth é o teto de aninhamento de parênteses no WHERE.
//
// Sem limite, `((((…` de um input adversarial estoura a pilha durante o fuzz
// (e em produção, num cliente malicioso). Cadeias AND/OR continuam
// ILIMITADAS, porque são iterativas — o teto vale só para a recursão.
const MaxExprDepth = 64
