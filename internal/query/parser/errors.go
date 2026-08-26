package parser

import "github.com/nishisan-dev/nishidb/internal/query/lexer"

// SyntaxError é o MESMO tipo produzido pelo lexer: o front-end SQL tem uma
// categoria só de erro sintático, e o cliente não precisa saber em que
// estágio o erro nasceu para classificá-lo.
type SyntaxError = lexer.SyntaxError

// ErrSyntax é o mesmo sentinela do lexer, reexportado para que o chamador
// escreva errors.Is(err, parser.ErrSyntax) sem importar os dois pacotes.
var ErrSyntax = lexer.ErrSyntax

// MaxExprDepth é o teto de aninhamento de PARÊNTESES no WHERE.
//
// Sem limite, `((((…` de um input adversarial estoura a pilha durante o fuzz
// (e em produção, num cliente malicioso). Ele NÃO limita o tamanho da cadeia
// de AND/OR — essa é iterativa e tem o seu próprio teto, o MaxWhereTerms
// abaixo. Confundir os dois foi a origem de dois defeitos neste bloco.
const MaxExprDepth = 64

// MaxWhereTerms é o teto de TERMOS (comparações e IS NULL) no WHERE.
//
// É um limite de RECURSO, não de gramática. Medido no binder: cada termo custa
// ~182 B de AST ligada, retidos enquanto a query viver. Sem teto, uma query
// gerada por máquina de 1 milhão de termos é aceita — 19 MB de SQL viram ~180
// MB de estruturas — num sistema cujo critério de aceite é caber em 1 GiB de
// RSS com o dataset inteiro. O teto atual limita isso a ~1,8 MB por query.
//
// 10.000 é folgado para o que uma máquina gera de verdade (um IN expandido, um
// filtro montado por UI) e restritivo para o que só um cliente descuidado ou
// malicioso produz. Nenhuma query escrita à mão chega perto.
const MaxWhereTerms = 10_000

// MaxLikePatternBytes limita o padrão de LIKE.
//
// É teto de RECURSO, no mesmo espírito de MaxWhereTerms e MaxExprDepth: o
// matcher de glob tem um ponto de backtrack, e um padrão patológico
// ('%a%a%a%...') sobre um valor longo é trabalho quadrático por linha. O
// timeout de query o cerca por cima; este teto o corta na entrada, onde o custo
// é zero e a mensagem é acionável.
const MaxLikePatternBytes = 1024
