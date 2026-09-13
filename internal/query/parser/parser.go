package parser

import (
	"strconv"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
)

// parser mantém UM token de lookahead. Nada de materializar o slice de
// tokens: a gramática é LL(1) e um passo basta.
type parser struct {
	lx  lexer.Lexer // por VALOR: é o que dá 0 alloc ao lexer
	cur lexer.Token
	// lookahead/hasLookahead são UM token de lookahead, e existem por um motivo
	// só: distinguir `NOT LIKE` de um NOT em posição de comparador. Sem ele,
	// `x NOT LIKE 'a%'` obrigaria a recusar o NOT antes de saber o que ele abre.
	//
	// A gramática continua LL(1) em toda decisão de CLÁUSULA — o comentário do
	// `.` de nome qualificado, mais abaixo, contrasta justamente com o BY de
	// GROUP BY, que exigiria olhar adiante para decidir se uma cláusula começa.
	// Aqui não se decide cláusula: o parser já está no ponto em que só cabia
	// erro, e o lookahead só escolhe QUAL erro deixar de ser erro.
	lookahead    lexer.Token
	hasLookahead bool
	depth        int // profundidade de parênteses corrente
	terms        int // termos do WHERE já emitidos (teto de recurso, MaxWhereTerms)
	params       int // próximo ParamIdx a atribuir
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
		// Sítio principal das cláusulas ausentes: ORDER, GROUP, OFFSET, JOIN,
		// HAVING e UNION aparecem todas DEPOIS de um SELECT ... FROM [WHERE]
		// [LIMIT] sintaticamente completo, e caem exatamente aqui. Sem isto a
		// mensagem é "conteúdo inesperado após o fim da query", que manda o
		// usuário procurar um erro de digitação que não existe.
		if name, ok := unsupportedClause(p.cur); ok {
			return nil, lexer.NewUnsupportedClauseSyntaxError(p.cur.Pos, name)
		}
		return nil, p.errAt(p.cur.Pos, "conteúdo inesperado após o fim da query")
	}
	st.ParamCount = p.params
	return st, nil
}

func (p *parser) advance() error {
	if p.hasLookahead {
		p.cur, p.hasLookahead = p.lookahead, false
		return nil
	}
	tok, err := p.lx.Next()
	if err != nil {
		return err // já é *SyntaxError com a posição certa
	}
	p.cur = tok
	return nil
}

// peek devolve o token seguinte ao corrente sem consumi-lo.
//
// Erro do lexer NÃO é propagado aqui: ele fica para o advance seguinte, que é
// quem já sabe reportá-lo com a posição certa. Espiar não pode ser um segundo
// sítio de erro de sintaxe, senão a mesma query passa a falhar com mensagens
// diferentes conforme quem chegou primeiro.
func (p *parser) peek() (lexer.Token, error) {
	if p.hasLookahead {
		return p.lookahead, nil
	}
	tok, err := p.lx.Next()
	if err != nil {
		return lexer.Token{}, err
	}
	p.lookahead, p.hasLookahead = tok, true
	return tok, nil
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

	name, pos, ok := p.identifier()
	if !ok {
		return nil, p.errAt(p.cur.Pos, "esperado nome do dataset")
	}
	if err := p.advance(); err != nil {
		return nil, err
	}
	// Nome qualificado: `_meta.fields`. Continua LL(1) — o '.' É o token de
	// lookahead, e a decisão se toma olhando só para ele. O contraste é com o
	// BY de GROUP BY, que exigiria um SEGUNDO lookahead e por isso foi
	// recusado.
	if p.cur.Type == lexer.TokenDot {
		if err := p.advance(); err != nil {
			return nil, err
		}
		table, tablePos, ok := p.identifier()
		if !ok {
			return nil, p.errAt(p.cur.Pos, "esperado nome de tabela depois de \""+name+".\"")
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		if p.cur.Type == lexer.TokenDot {
			return nil, p.errAt(p.cur.Pos, "nome no FROM tem no máximo dois níveis (namespace.tabela); o NishiDB não tem catálogo")
		}
		st.Namespace, st.NamespacePos, st.Dataset, st.DatasetPos = name, pos, table, tablePos
	} else {
		st.Dataset, st.DatasetPos = name, pos
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
	// ORDER BY entra entre WHERE e LIMIT, que é a ordem do SQL padrão.
	if isWord(p.cur, "order") {
		if err := p.parseOrderBy(st); err != nil {
			return nil, err
		}
	}
	// A ordem das cláusulas é fixa: LIMIT antes de WHERE cai no "conteúdo
	// inesperado" do Parse, e não é aceito em silêncio.
	if p.cur.Type == lexer.TokenLimit {
		if err := p.parseLimit(st); err != nil {
			return nil, err
		}
	}
	// OFFSET depois de LIMIT, e a ordem é fixa pela mesma razão de todas as
	// outras: `OFFSET 10 LIMIT 5` cai no "conteúdo inesperado" do Parse em vez
	// de ser aceito numa ordem que só este produto entenderia.
	if isWord(p.cur, "offset") {
		if err := p.parseOffset(st); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// MaxOrderKeys limita quantas chaves um ORDER BY pode ter.
//
// Teto de RECURSO, no espírito de MaxWhereTerms: cada chave é um slot a mais a
// coletar por linha e uma comparação a mais por par no sort. Dezesseis é muito
// além de qualquer uso real e muito aquém de qualquer patologia.
const MaxOrderKeys = 16

// parseOrderBy lê `ORDER BY <chave> [, <chave>]*`.
//
// A palavra `ORDER` é reconhecida POR POSIÇÃO — ela só chega aqui num ponto em
// que a gramática não aceitaria um identificador —, e o `BY` é EXIGIDO logo
// depois. Isso é consumo, não lookahead: o parser se compromete ao ver ORDER e
// então cobra o resto. `SELECT order FROM t` e `WHERE order = 1` continuam
// válidos, porque ali `order` está em posição de identificador.
func (p *parser) parseOrderBy(st *SelectStmt) error {
	if err := p.advance(); err != nil {
		return err
	}
	if !isWord(p.cur, "by") {
		return p.errAt(p.cur.Pos, "esperado BY depois de ORDER")
	}
	if err := p.advance(); err != nil {
		return err
	}
	for {
		if len(st.OrderBy) >= MaxOrderKeys {
			return p.errAt(p.cur.Pos, "ORDER BY tem no máximo "+itoa(MaxOrderKeys)+" chaves")
		}
		k, err := p.parseOrderKey()
		if err != nil {
			return err
		}
		st.OrderBy = append(st.OrderBy, k)
		if p.cur.Type != lexer.TokenComma {
			return nil
		}
		if err := p.advance(); err != nil {
			return err
		}
	}
}

func (p *parser) parseOrderKey() (OrderKey, error) {
	// Ordenar por POSIÇÃO (`ORDER BY 1`) fica de fora, e a mensagem nomeia a
	// regra: com `SELECT *` a posição endereçaria um conjunto de colunas que
	// muda entre execuções, porque o universo de campos cresce com os eventos.
	if p.cur.Type == lexer.TokenNumber {
		return OrderKey{}, p.errAt(p.cur.Pos, "ORDER BY exige nome de coluna, não posição")
	}
	name, pos, ok := p.identifier()
	if !ok {
		return OrderKey{}, p.errAt(p.cur.Pos, "esperado nome de coluna no ORDER BY")
	}
	k := OrderKey{Column: name, ColumnPos: pos}
	if err := p.advance(); err != nil {
		return OrderKey{}, err
	}
	// Caminho qualificado (`doc.chave`), como no predicado e na projeção.
	if p.cur.Type == lexer.TokenDot {
		if err := p.advance(); err != nil {
			return OrderKey{}, err
		}
		path, cpos, ok := p.identifier()
		if !ok {
			return OrderKey{}, p.errAt(p.cur.Pos, "esperado caminho depois de \""+name+".\"")
		}
		k.Qualifier, k.Column, k.ColumnPos = name, path, cpos
		if err := p.advance(); err != nil {
			return OrderKey{}, err
		}
	}
	switch {
	case isWord(p.cur, "asc"):
		if err := p.advance(); err != nil {
			return OrderKey{}, err
		}
	case isWord(p.cur, "desc"):
		k.Desc = true
		if err := p.advance(); err != nil {
			return OrderKey{}, err
		}
	}
	if isWord(p.cur, "nulls") {
		if err := p.advance(); err != nil {
			return OrderKey{}, err
		}
		switch {
		case isWord(p.cur, "first"):
			k.NullsFirst = true
		case isWord(p.cur, "last"):
		default:
			return OrderKey{}, p.errAt(p.cur.Pos, "esperado FIRST ou LAST depois de NULLS")
		}
		if err := p.advance(); err != nil {
			return OrderKey{}, err
		}
	}
	return k, nil
}

// parseOffset lê `OFFSET <n>`, com as MESMAS recusas do LIMIT: sem parâmetro,
// sem fracionário e sem sinal.
//
// Reusa as regras em vez de afrouxá-las porque as duas cláusulas respondem à
// mesma pergunta ("quantas linhas"), e divergir aqui seria uma inconsistência
// que só apareceria para quem escrevesse a query errada — tarde demais.
func (p *parser) parseOffset(st *SelectStmt) error {
	if err := p.advance(); err != nil {
		return err
	}
	if p.cur.Type == lexer.TokenPlaceholder {
		return p.errAt(p.cur.Pos, "OFFSET não aceita parâmetro")
	}
	if p.cur.Type != lexer.TokenNumber {
		return p.errAt(p.cur.Pos, "esperado literal inteiro após OFFSET")
	}
	tok := p.cur
	if strings.ContainsAny(tok.Text, ".eE") {
		return p.errAt(tok.Pos, "OFFSET exige literal inteiro")
	}
	if strings.HasPrefix(tok.Text, "-") {
		return p.errAt(tok.Pos, "OFFSET exige literal inteiro sem sinal")
	}
	n, err := typeparse.Int64([]byte(tok.Text))
	if err != nil {
		return p.errAt(tok.Pos, "inteiro fora do intervalo de int64")
	}
	st.HasOffset = true
	st.Offset = n
	st.OffsetPos = tok.Pos
	return p.advance()
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

	first := true
	for {
		// O token é guardado ANTES de identifier(): a ColumnRef não registra se
		// o nome veio entre aspas, e o guard de DISTINCT abaixo precisa saber.
		columnTok := p.cur
		name, pos, ok := p.identifier()
		if !ok {
			if first {
				return p.errAt(p.cur.Pos, "esperado nome de coluna ou '*'")
			}
			if p.cur.Type == lexer.TokenStar {
				// `SELECT doc.*, *`: o curinga de topo não se combina com
				// nada, e a mistura colapsaria colunas em silêncio.
				return p.errAt(p.cur.Pos, "'*' não pode ser combinado com colunas")
			}
			return p.errAt(p.cur.Pos, "esperado nome de coluna")
		}
		// Só aloca ao appendar: com Star, Projection precisa ficar nil.
		if err := p.advance(); err != nil {
			return err
		}
		ref := ColumnRef{Name: name, Pos: pos}
		if p.cur.Type == lexer.TokenDot {
			if err := p.advance(); err != nil {
				return err
			}
			switch p.cur.Type {
			case lexer.TokenStar:
				// `pai.*`: as colunas extraídas do pai. O curinga de TOPO
				// continua não podendo ser combinado, e este herda a regra —
				// a checagem de mistura está no parseProjection acima.
				ref.Qualifier, ref.Name, ref.Star = name, "", true
			default:
				path, cpos, ok := p.identifier()
				if !ok {
					return p.errAt(p.cur.Pos, "esperado nome de coluna ou '*' depois de \""+name+".\"")
				}
				ref.Qualifier, ref.Name, ref.Pos = name, path, cpos
			}
			if err := p.advance(); err != nil {
				return err
			}
			if p.cur.Type == lexer.TokenDot {
				return p.errAt(p.cur.Pos, "nome na projeção tem no máximo dois níveis (field.caminho)")
			}
		}
		st.Projection = append(st.Projection, ref)
		if p.cur.Type != lexer.TokenComma {
			// DISTINCT é a única das sete que NÃO chega ao sítio principal:
			// em `SELECT DISTINCT a FROM t` o identifier() a aceita como nome
			// de coluna, e o erro nasce apontando para o `a`, que não tem
			// culpa nenhuma.
			//
			// As quatro condições, todas necessárias: é a PRIMEIRA coluna
			// (`SELECT a, distinct` é outra coisa); o token era BARE (quem
			// escreveu "distinct" entre aspas quer a coluna); a palavra casa;
			// e o que vem depois NÃO é FROM (`SELECT distinct FROM t`
			// seleciona uma coluna chamada distinct e continua válido).
			if first && p.cur.Type != lexer.TokenFrom {
				if name, ok := unsupportedClause(columnTok); ok && name == "DISTINCT" {
					return lexer.NewUnsupportedClauseSyntaxError(columnTok.Pos, name)
				}
			}
			return nil
		}
		if err := p.advance(); err != nil {
			return err
		}
		first = false
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
		return nil, p.errAt(p.cur.Pos, "NOT só é suportado em IS NOT NULL e NOT LIKE")
	default:
		return p.parsePredicate()
	}
}

func (p *parser) parsePredicate() (Expr, error) {
	// O teto é contado AQUI, na folha, e não na profundidade: a cadeia de
	// AND/OR é iterativa e cresce sem aninhar, então nenhum limite de
	// parênteses a alcança. Cada termo custa ~182 B de AST ligada, retidos
	// enquanto a query viver.
	p.terms++
	if p.terms > MaxWhereTerms {
		return nil, p.errAt(p.cur.Pos, "WHERE com mais de "+itoa(MaxWhereTerms)+" termos")
	}
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
	// Caminho qualificado no filtro, pela mesma razão de existir na projeção:
	// sem ele a gramática ficaria torta — `SELECT doc.x` valeria e
	// `WHERE doc.x = 'y'` não, para a mesma coluna.
	qualifier := ""
	if p.cur.Type == lexer.TokenDot {
		if err := p.advance(); err != nil {
			return nil, err
		}
		path, cpos, ok := p.identifier()
		if !ok {
			return nil, p.errAt(p.cur.Pos, "esperado nome de coluna depois de \""+col+".\"")
		}
		qualifier, col, pos = col, path, cpos
		if err := p.advance(); err != nil {
			return nil, err
		}
		if p.cur.Type == lexer.TokenDot {
			return nil, p.errAt(p.cur.Pos, "nome no filtro tem no máximo dois níveis (field.caminho)")
		}
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
		return &NullCheck{Qualifier: qualifier, Column: col, ColumnPos: pos, Not: not}, nil
	}

	// LIKE e NOT LIKE, reconhecidos POR POSIÇÃO e não como keyword do lexer.
	//
	// A palavra `like` continua sendo TokenIdent em toda posição em que a
	// gramática aceita um identificador — `SELECT like FROM t WHERE like LIKE
	// 'a%'` é query válida —, e é isso que preserva o contrato publicado de que
	// as palavras de cláusula não viraram keywords. Aqui o parser está numa
	// posição em que só cabia ERRO ("esperado comparador"), então reconhecer a
	// palavra e EXIGIR o resto é consumo, não lookahead: a gramática segue
	// LL(1).
	if not, ok := p.opensLike(); ok {
		// Um advance consome o LIKE; o NOT LIKE consome DOIS tokens, e o
		// primeiro é o NOT. Avançar uma vez só deixava o parser no LIKE e o
		// parseOperand seguinte o lia como coluna — recusando `x NOT LIKE 'a%'`
		// com "comparação entre colunas não é suportada".
		if not {
			if err := p.advance(); err != nil {
				return nil, err
			}
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		pattern, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if pattern.Kind == OperandString && len(pattern.S) > MaxLikePatternBytes {
			return nil, p.errAt(pattern.Pos, "padrão de LIKE acima de "+itoa(MaxLikePatternBytes)+" bytes")
		}
		if pattern.Kind != OperandString && pattern.Kind != OperandParam {
			return nil, p.errAt(pattern.Pos, "o padrão de LIKE é texto ou parâmetro")
		}
		return &LikeExpr{
			Qualifier: qualifier,
			Column:    col, ColumnPos: pos, Not: not, Pattern: pattern}, nil
	}

	op, ok := cmpOp(p.cur.Type)
	if !ok {
		return nil, p.errAt(p.cur.Pos, "esperado comparador, LIKE, IS NULL ou IS NOT NULL")
	}
	if err := p.advance(); err != nil {
		return nil, err
	}
	operand, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	return &Comparison{
		Qualifier: qualifier,
		Column:    col, ColumnPos: pos, Op: op, Operand: operand}, nil
}

// opensLike reconhece `LIKE` ou `NOT LIKE` na posição do comparador, e deixa o
// parser posicionado NO token do LIKE (o chamador avança).
//
// Devolve (not, ok). Não consome nada quando devolve false, para que o caminho
// do comparador siga intacto — é a mesma disciplina do unsupportedClause.
func (p *parser) opensLike() (bool, bool) {
	if isWord(p.cur, "like") {
		return false, true
	}
	if p.cur.Type != lexer.TokenNot {
		return false, false
	}
	// `NOT` aqui só pode abrir NOT LIKE: não há NOT genérico na v1, e o
	// parsePrimary já o recusa em posição de expressão. Um lookahead é
	// necessário, e é o ÚNICO do parser — sem ele, `x NOT LIKE 'a%'` teria de
	// escolher entre recusar o NOT antes de saber o que ele abre ou aceitar
	// `x NOT 'a%'` como se fosse alguma coisa.
	lookahead, err := p.peek()
	if err != nil || !isWord(lookahead, "like") {
		return false, false
	}
	return true, true
}

// isWord compara um TokenIdent BARE com uma palavra de cláusula.
//
// Duas condições, e as duas importam — são as mesmas do unsupportedClause:
// só TokenIdent e nunca TokenQuotedIdent, porque quem escreveu `"like"` quis
// uma coluna com esse nome; e ASCIIEqualFold em vez de strings.EqualFold,
// porque o folding Unicode do segundo dobra o Kelvin sign para 'k' e duas
// regras de folding no mesmo front-end divergem no primeiro caso exótico.
func isWord(tok lexer.Token, word string) bool {
	return tok.Type == lexer.TokenIdent && lexer.ASCIIEqualFold(tok.Text, word)
}

// itoa evita arrastar o fmt para o pacote por causa de uma mensagem de erro.
func itoa(v int) string { return strconv.Itoa(v) }

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
		// A gramática recusa o SINAL, não só o valor negativo: `LIMIT -0` cai
		// aqui embora -0 seja zero. Estreitar assim é deliberado (um LIMIT com
		// sinal é quase sempre erro de quem escreveu), mas a mensagem tem de
		// dizer a regra REAL — "não aceita valor negativo" mandaria o usuário
		// procurar um problema de valor que não existe em `-0`.
		return p.errAt(tok.Pos, "LIMIT exige literal inteiro sem sinal")
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
