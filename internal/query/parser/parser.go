package parser

import (
	"strings"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
)

// parser mantém UM token de lookahead. Nada de materializar o slice de
// tokens: a gramática é LL(1) e um passo basta.
type parser struct {
	lx     lexer.Lexer // por VALOR: é o que dá 0 alloc ao lexer
	cur    lexer.Token
	depth  int // profundidade de parênteses corrente
	params int // próximo ParamIdx a atribuir
}

// Parse converte SQL v1 em AST. É pura e segura para uso concorrente; a AST
// devolvida é imutável por convenção.
//
// Em erro devolve (nil, *SyntaxError) — sempre com posição de byte, inclusive
// para os erros que nascem no lexer, que são repassados COMO VIERAM (embrulhar
// com fmt.Errorf poria um prefixo na mensagem e quebraria o contrato de
// substring dos testes do pipeline).
func Parse(sql string) (*SelectStmt, error) {
	p := &parser{}
	p.lx.Init(sql)
	if err := p.advance(); err != nil {
		return nil, err
	}
	st, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if p.cur.Type == lexer.TokenSemicolon {
		if err := p.advance(); err != nil {
			return nil, err
		}
		if p.cur.Type != lexer.TokenEOF {
			return nil, p.errAt(p.cur.Pos, "apenas um comando por query")
		}
	} else if p.cur.Type != lexer.TokenEOF {
		return nil, p.errAt(p.cur.Pos, "conteúdo inesperado após o fim da query")
	}
	st.ParamCount = p.params
	return st, nil
}

func (p *parser) advance() error {
	tok, err := p.lx.Next()
	if err != nil {
		return err // já é *SyntaxError com a posição certa
	}
	p.cur = tok
	return nil
}

func (p *parser) errAt(pos int, msg string) error {
	return lexer.NewSyntaxError(pos, msg)
}

func (p *parser) parseSelect() (*SelectStmt, error) {
	if p.cur.Type != lexer.TokenSelect {
		return nil, p.errAt(p.cur.Pos, "esperado SELECT")
	}
	if err := p.advance(); err != nil {
		return nil, err
	}

	st := &SelectStmt{}
	if err := p.parseProjection(st); err != nil {
		return nil, err
	}
	if p.cur.Type != lexer.TokenFrom {
		return nil, p.errAt(p.cur.Pos, "esperado ',' ou FROM")
	}
	if err := p.advance(); err != nil {
		return nil, err
	}

	nome, pos, ok := p.identifier()
	if !ok {
		return nil, p.errAt(p.cur.Pos, "esperado nome do dataset")
	}
	st.Dataset, st.DatasetPos = nome, pos
	if err := p.advance(); err != nil {
		return nil, err
	}

	if p.cur.Type == lexer.TokenWhere {
		if err := p.advance(); err != nil {
			return nil, err
		}
		where, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		st.Where = where
	}
	// A ordem das cláusulas é fixa: LIMIT antes de WHERE cai no "conteúdo
	// inesperado" do Parse, e não é aceito em silêncio.
	if p.cur.Type == lexer.TokenLimit {
		if err := p.parseLimit(st); err != nil {
			return nil, err
		}
	}
	return st, nil
}

func (p *parser) parseProjection(st *SelectStmt) error {
	if p.cur.Type == lexer.TokenStar {
		st.Star = true
		if err := p.advance(); err != nil {
			return err
		}
		if p.cur.Type == lexer.TokenComma {
			return p.errAt(p.cur.Pos, "'*' não pode ser combinado com colunas")
		}
		return nil
	}

	primeira := true
	for {
		nome, pos, ok := p.identifier()
		if !ok {
			if primeira {
				return p.errAt(p.cur.Pos, "esperado nome de coluna ou '*'")
			}
			return p.errAt(p.cur.Pos, "esperado nome de coluna")
		}
		// Só aloca ao appendar: com Star, Projection precisa ficar nil.
		st.Projection = append(st.Projection, ColumnRef{Name: nome, Pos: pos})
		if err := p.advance(); err != nil {
			return err
		}
		if p.cur.Type != lexer.TokenComma {
			return nil
		}
		if err := p.advance(); err != nil {
			return err
		}
		primeira = false
	}
}

// identifier aceita nome bare ou entre aspas. NÃO normaliza caixa:
// identificadores são case-sensitive e o binder resolve nome exato.
func (p *parser) identifier() (string, int, bool) {
	switch p.cur.Type {
	case lexer.TokenIdent, lexer.TokenQuotedIdent:
		return p.cur.Text, p.cur.Pos, true
	default:
		return "", 0, false
	}
}

// parseOr e parseAnd são ITERATIVOS: recursão à direita produziria
// AND(a, AND(b,c)) e quebraria o flatten do planner, que percorre a espinha à
// esquerda.
func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == lexer.TokenOr {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &LogicalExpr{Op: LogicalOr, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.cur.Type == lexer.TokenAnd {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = &LogicalExpr{Op: LogicalAnd, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parsePrimary() (Expr, error) {
	switch p.cur.Type {
	case lexer.TokenLParen:
		if p.depth+1 > MaxExprDepth {
			return nil, p.errAt(p.cur.Pos, "expressão aninhada demais (máximo 64 níveis)")
		}
		p.depth++
		if err := p.advance(); err != nil {
			return nil, err
		}
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.cur.Type != lexer.TokenRParen {
			return nil, p.errAt(p.cur.Pos, "esperado ')'")
		}
		p.depth--
		if err := p.advance(); err != nil {
			return nil, err
		}
		return e, nil // sem nó de parênteses: ((a=1)) e a=1 são a mesma árvore
	case lexer.TokenNot:
		// Mensagem específica: "esperado predicado" mandaria o usuário
		// procurar a coisa errada.
		return nil, p.errAt(p.cur.Pos, "NOT só é suportado em IS NOT NULL")
	default:
		return p.parsePredicate()
	}
}

func (p *parser) parsePredicate() (Expr, error) {
	col, pos, ok := p.identifier()
	if !ok {
		switch p.cur.Type {
		case lexer.TokenString, lexer.TokenNumber, lexer.TokenPlaceholder,
			lexer.TokenTrue, lexer.TokenFalse:
			return nil, p.errAt(p.cur.Pos, "esperado nome de coluna à esquerda do comparador")
		default:
			return nil, p.errAt(p.cur.Pos, "esperado predicado")
		}
	}
	if err := p.advance(); err != nil {
		return nil, err
	}

	if p.cur.Type == lexer.TokenIs {
		if err := p.advance(); err != nil {
			return nil, err
		}
		not := false
		if p.cur.Type == lexer.TokenNot {
			not = true
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.cur.Type != lexer.TokenNull {
				return nil, p.errAt(p.cur.Pos, "esperado NULL após IS NOT")
			}
		} else if p.cur.Type != lexer.TokenNull {
			return nil, p.errAt(p.cur.Pos, "esperado NULL ou NOT após IS")
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &NullCheck{Column: col, ColumnPos: pos, Not: not}, nil
	}

	op, ok := cmpOp(p.cur.Type)
	if !ok {
		return nil, p.errAt(p.cur.Pos, "esperado comparador, IS NULL ou IS NOT NULL")
	}
	if err := p.advance(); err != nil {
		return nil, err
	}
	operand, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	return &Comparison{Column: col, ColumnPos: pos, Op: op, Operand: operand}, nil
}

func cmpOp(t lexer.TokenType) (CmpOp, bool) {
	switch t {
	case lexer.TokenEq:
		return CmpEq, true
	case lexer.TokenNeq:
		return CmpNe, true
	case lexer.TokenGt:
		return CmpGt, true
	case lexer.TokenGte:
		return CmpGte, true
	case lexer.TokenLt:
		return CmpLt, true
	case lexer.TokenLte:
		return CmpLte, true
	default:
		return 0, false
	}
}

func (p *parser) parseOperand() (Operand, error) {
	tok := p.cur
	switch tok.Type {
	case lexer.TokenString:
		if err := p.advance(); err != nil {
			return Operand{}, err
		}
		return Operand{Kind: OperandString, S: tok.Text, Pos: tok.Pos}, nil
	case lexer.TokenTrue, lexer.TokenFalse:
		if err := p.advance(); err != nil {
			return Operand{}, err
		}
		return Operand{Kind: OperandBool, B: tok.Type == lexer.TokenTrue, Pos: tok.Pos}, nil
	case lexer.TokenPlaceholder:
		// O índice é atribuído AQUI e em nenhum outro lugar: contar em dois
		// pontos produziria buracos na numeração.
		idx := p.params
		p.params++
		if err := p.advance(); err != nil {
			return Operand{}, err
		}
		return Operand{Kind: OperandParam, ParamIdx: idx, Pos: tok.Pos}, nil
	case lexer.TokenNumber:
		op, err := p.numberOperand(tok)
		if err != nil {
			return Operand{}, err
		}
		if err := p.advance(); err != nil {
			return Operand{}, err
		}
		return op, nil
	case lexer.TokenIdent, lexer.TokenQuotedIdent:
		return Operand{}, p.errAt(tok.Pos, "comparação entre colunas não é suportada na v1")
	default:
		return Operand{}, p.errAt(tok.Pos, "esperado literal, TRUE, FALSE ou '?'")
	}
}

// numberOperand converte o lexema pelos parsers da INGESTÃO (typeparse), não
// por strconv: é a mitigação registrada do risco de a coerção da query
// divergir da coerção que gravou o dado.
func (p *parser) numberOperand(tok lexer.Token) (Operand, error) {
	if strings.ContainsAny(tok.Text, ".eE") {
		f, err := typeparse.Float64([]byte(tok.Text))
		if err != nil {
			return Operand{}, p.errAt(tok.Pos, "número inválido")
		}
		return Operand{Kind: OperandFloat, F: f, Pos: tok.Pos}, nil
	}
	i, err := typeparse.Int64([]byte(tok.Text))
	if err != nil {
		// Com a gramática do lexer, a única falha alcançável aqui é overflow.
		// Promover a float perderia precisão numa comparação com coluna
		// int64, então é erro.
		return Operand{}, p.errAt(tok.Pos, "inteiro fora do intervalo de int64")
	}
	return Operand{Kind: OperandInt, I: i, Pos: tok.Pos}, nil
}

func (p *parser) parseLimit(st *SelectStmt) error {
	if err := p.advance(); err != nil {
		return err
	}
	if p.cur.Type == lexer.TokenPlaceholder {
		// LIMIT parametrizado exigiria replanejar por execução; a v1 recusa.
		return p.errAt(p.cur.Pos, "LIMIT não aceita parâmetro")
	}
	if p.cur.Type != lexer.TokenNumber {
		return p.errAt(p.cur.Pos, "esperado literal inteiro após LIMIT")
	}
	tok := p.cur
	if strings.ContainsAny(tok.Text, ".eE") {
		return p.errAt(tok.Pos, "LIMIT exige literal inteiro")
	}
	if strings.HasPrefix(tok.Text, "-") {
		return p.errAt(tok.Pos, "LIMIT não aceita valor negativo")
	}
	n, err := typeparse.Int64([]byte(tok.Text))
	if err != nil {
		return p.errAt(tok.Pos, "inteiro fora do intervalo de int64")
	}
	st.HasLimit = true
	st.Limit = n
	st.LimitPos = tok.Pos
	return p.advance()
}
