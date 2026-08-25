package lexer

// Lexer tokeniza uma query. Use por VALOR (o parser embute um Lexer como
// campo, sem alocação): declare, chame Init e itere com Next. Não é seguro
// para uso concorrente; reutilizável entre queries via Init.
type Lexer struct {
	src     string
	pos     int
	err     *SyntaxError // sticky: uma vez setado, todo Next devolve o mesmo
	scratch []byte       // buffer de unescape, reutilizado entre queries
}

// Init prepara o lexer para um novo SQL, preservando o scratch já crescido.
// Limpa o erro sticky — sem isso a segunda query herdaria a falha da primeira
// e "falharia" sem tocar em nenhum byte.
func (l *Lexer) Init(sql string) {
	l.src = sql
	l.pos = 0
	l.err = nil
}

// Next devolve o próximo token.
//
// No fim da entrada devolve (Token{Type: TokenEOF, Pos: len(sql)}, nil), e o
// mesmo EOF em toda chamada subsequente — o parser costuma pedir lookahead
// depois do fim. Em erro sintático devolve (Token{}, *SyntaxError) e passa a
// devolver O MESMO erro em toda chamada seguinte, com o cursor congelado:
// sticky elimina a classe de bug em que um parser que ignore um erro entra em
// loop ou monta uma AST parcial.
func (l *Lexer) Next() (Token, error) {
	if l.err != nil {
		return Token{}, l.err
	}
	l.skipSpace()
	if l.pos >= len(l.src) {
		return Token{Type: TokenEOF, Pos: len(l.src)}, nil
	}

	start := l.pos
	b := l.src[start]
	switch {
	case isIdentStart(b):
		return l.scanIdent()
	case b >= '0' && b <= '9':
		return l.scanNumber()
	case b == '-':
		// Não existe menos binário nem unário fora de número na v1: emitir um
		// TokenMinus daria ao parser um token que a AST não modela.
		if l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1]) {
			return l.scanNumber()
		}
		return l.fail(start, "caractere inesperado '-'")
	case b == '\'':
		text, end, ok := l.scanQuoted('\'')
		if !ok {
			return l.fail(start, "literal string sem fechamento")
		}
		l.pos = end
		return Token{Type: TokenString, Text: text, Pos: start}, nil
	case b == '"':
		text, end, ok := l.scanQuoted('"')
		if !ok {
			return l.fail(start, "identificador entre aspas sem fechamento")
		}
		if text == "" {
			return l.fail(start, "identificador entre aspas vazio")
		}
		l.pos = end
		return Token{Type: TokenQuotedIdent, Text: text, Pos: start}, nil
	}

	// Operadores e pontuação. O lookahead é de 1 byte e SEMPRE com bounds
	// check: `WHERE a >` no fim da entrada termina em TokenGt + EOF.
	switch b {
	case '?':
		return l.punct(TokenPlaceholder, 1), nil
	case ',':
		return l.punct(TokenComma, 1), nil
	case '*':
		return l.punct(TokenStar, 1), nil
	case '(':
		return l.punct(TokenLParen, 1), nil
	case ')':
		return l.punct(TokenRParen, 1), nil
	case ';':
		return l.punct(TokenSemicolon, 1), nil
	case '.':
		// O ponto DENTRO de um número nunca chega aqui: o dispatch de cima
		// manda todo lexema iniciado por dígito (ou por '-' seguido de dígito)
		// para o scanNumber, que consome o ponto decimal junto.
		//
		// O ponto SEGUIDO de dígito continua sendo erro, com a mensagem que
		// sempre teve. `.5` não é literal válido na v1, e deixá-lo virar
		// TokenDot+NUMBER trocaria uma recusa que nomeia o caractere por um
		// erro de sintaxe distante, no parser, dizendo o que ele esperava
		// encontrar ali — que é o que o usuário NÃO está procurando. O ponto
		// é separador entre nomes, e é só isso.
		if l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1]) {
			return l.fail(start, "caractere inesperado '.'")
		}
		return l.punct(TokenDot, 1), nil
	case '=':
		return l.punct(TokenEq, 1), nil
	case '!':
		if l.peekIs('=') {
			return l.punct(TokenNeq, 2), nil
		}
		return l.fail(start, "caractere inesperado '!'")
	case '>':
		if l.peekIs('=') {
			return l.punct(TokenGte, 2), nil
		}
		return l.punct(TokenGt, 1), nil
	case '<':
		if l.peekIs('=') {
			return l.punct(TokenLte, 2), nil
		}
		if l.peekIs('>') {
			// Recusar aqui, e não deixar virar TokenLt+TokenGt, é o que
			// transforma um erro de parser distante ("caractere inesperado
			// '>'") numa mensagem que diz o que fazer.
			return l.fail(start, "operador '<>' não é suportado, use '!='")
		}
		return l.punct(TokenLt, 1), nil
	}

	switch {
	case b >= 0x80:
		return l.fail(start, "byte inesperado 0x"+hex2(b)+" (identificador não-ASCII exige aspas duplas)")
	case b >= 0x21 && b <= 0x7E:
		return l.fail(start, "caractere inesperado '"+string(b)+"'")
	default:
		return l.fail(start, "byte inesperado 0x"+hex2(b))
	}
}

// punct emite um token de pontuação/operador de n bytes.
func (l *Lexer) punct(t TokenType, n int) Token {
	start := l.pos
	l.pos += n
	return Token{Type: t, Text: l.src[start:l.pos], Pos: start}
}

// peekIs reporta se o byte seguinte ao corrente é c (com bounds check).
func (l *Lexer) peekIs(c byte) bool {
	return l.pos+1 < len(l.src) && l.src[l.pos+1] == c
}

// fail seta o erro sticky e devolve o par de erro. O cursor NÃO avança.
func (l *Lexer) fail(pos int, msg string) (Token, error) {
	l.err = NewSyntaxError(pos, msg)
	return Token{}, l.err
}

func (l *Lexer) skipSpace() {
	for l.pos < len(l.src) {
		switch l.src[l.pos] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			l.pos++
		default:
			return
		}
	}
}

// scanIdent lê um identificador bare e resolve keyword. Text é subslice do
// fonte: zero alocação, case preservado.
func (l *Lexer) scanIdent() (Token, error) {
	start := l.pos
	l.pos++ // o primeiro byte já foi validado pelo dispatch
	for l.pos < len(l.src) && isIdentPart(l.src[l.pos]) {
		l.pos++
	}
	text := l.src[start:l.pos]
	if t, ok := keywordType(text); ok {
		return Token{Type: t, Text: text, Pos: start}, nil
	}
	return Token{Type: TokenIdent, Text: text, Pos: start}, nil
}

// scanNumber impõe a gramática de número do JSON (RFC 8259) com '-' opcional,
// idêntica à de typeparse.JSONNumber.
//
// A validação vive AQUI, e não no conversor do parser, por duas razões: o
// typeparse.Int64 e o Float64 divergem em zeros à esquerda (Int64("007") = 7,
// Float64 recusa), o que faria a aceitação depender de o literal ser inteiro
// ou fracionário; e o erro do conversor não tem posição de byte para mostrar
// ao usuário. Com a gramática imposta aqui, todo TokenNumber aceito é
// conversível e todo lexema recusado carrega posição.
func (l *Lexer) scanNumber() (Token, error) {
	start := l.pos
	if l.pos < len(l.src) && l.src[l.pos] == '-' {
		l.pos++
	}
	// Parte inteira: '0' sozinho, ou [1-9][0-9]*. Leading zero é recusado
	// (ambiguidade octal).
	switch {
	case l.pos < len(l.src) && l.src[l.pos] == '0':
		l.pos++
	case l.pos < len(l.src) && l.src[l.pos] >= '1' && l.src[l.pos] <= '9':
		for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.pos++
		}
	default:
		return l.failNumber(start)
	}
	// Fração opcional: '.' seguido de ao menos um dígito.
	if l.pos < len(l.src) && l.src[l.pos] == '.' {
		l.pos++
		if l.pos >= len(l.src) || !isDigit(l.src[l.pos]) {
			return l.failNumber(start)
		}
		for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.pos++
		}
	}
	// Expoente opcional.
	if l.pos < len(l.src) && (l.src[l.pos] == 'e' || l.src[l.pos] == 'E') {
		l.pos++
		if l.pos < len(l.src) && (l.src[l.pos] == '+' || l.src[l.pos] == '-') {
			l.pos++
		}
		if l.pos >= len(l.src) || !isDigit(l.src[l.pos]) {
			return l.failNumber(start)
		}
		for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.pos++
		}
	}
	// Lixo aderente: sem esta guarda, "123abc" viraria NUMBER(123)+IDENT(abc)
	// e o erro apareceria — confuso — só no parser.
	if l.pos < len(l.src) {
		if c := l.src[l.pos]; isIdentPart(c) || c == '.' {
			return l.failNumber(start)
		}
	}
	return Token{Type: TokenNumber, Text: l.src[start:l.pos], Pos: start}, nil
}

// failNumber reporta na posição do INÍCIO do lexema: é o que o usuário
// precisa corrigir, não o byte ofensor no meio dele.
func (l *Lexer) failNumber(start int) (Token, error) {
	l.pos = start
	return l.fail(start, "literal numérico inválido")
}

// scanQuoted lê um literal entre aspas, resolvendo o escape de aspa
// duplicada. Devolve o texto sem aspas, o offset APÓS a aspa de fechamento e
// ok=false quando não há fechamento.
//
// Cuidado com 'abc”: o ” final é escape, então a string CONTINUA e termina
// sem terminador — tratar ” como terminador seguido de aspa solta aceitaria
// um input inválido.
func (l *Lexer) scanQuoted(quote byte) (text string, endPos int, ok bool) {
	start := l.pos
	i := start + 1
	escapes := 0
	for i < len(l.src) {
		if l.src[i] != quote {
			i++
			continue
		}
		if i+1 < len(l.src) && l.src[i+1] == quote {
			escapes++
			i += 2
			continue
		}
		break
	}
	if i >= len(l.src) {
		return "", 0, false
	}
	body := l.src[start+1 : i]
	if escapes == 0 {
		return body, i + 1, true // subslice: zero alocação
	}
	// string(l.scratch) COPIA — é essa cópia que dá o contrato "Token válido
	// para sempre". Devolver uma janela sobre o scratch faria o Text de um
	// token já entregue mudar quando o próximo literal escapado fosse lido.
	l.scratch = l.scratch[:0]
	for j := 0; j < len(body); j++ {
		l.scratch = append(l.scratch, body[j])
		if body[j] == quote {
			j++ // pula a segunda aspa do par
		}
	}
	return string(l.scratch), i + 1, true
}

// keywordType resolve keyword sem alocar: switch no comprimento e comparação
// ASCII case-insensitive.
//
// strings.EqualFold aplicaria folding UNICODE (o Kelvin sign dobra para 'k'),
// e strings.ToUpper alocaria. Aqui a regra "keyword é ASCII" continua valendo.
func keywordType(s string) (TokenType, bool) {
	switch len(s) {
	case 2:
		switch {
		case asciiEqualFold(s, "or"):
			return TokenOr, true
		case asciiEqualFold(s, "is"):
			return TokenIs, true
		}
	case 3:
		switch {
		case asciiEqualFold(s, "and"):
			return TokenAnd, true
		case asciiEqualFold(s, "not"):
			return TokenNot, true
		}
	case 4:
		switch {
		case asciiEqualFold(s, "from"):
			return TokenFrom, true
		case asciiEqualFold(s, "null"):
			return TokenNull, true
		case asciiEqualFold(s, "true"):
			return TokenTrue, true
		}
	case 5:
		switch {
		case asciiEqualFold(s, "where"):
			return TokenWhere, true
		case asciiEqualFold(s, "limit"):
			return TokenLimit, true
		case asciiEqualFold(s, "false"):
			return TokenFalse, true
		}
	case 6:
		if asciiEqualFold(s, "select") {
			return TokenSelect, true
		}
	}
	return TokenEOF, false
}

// asciiEqualFold compara s com um literal ASCII MINÚSCULO de mesmo tamanho.
//
// Correto porque lower só contém a-z: o único conjunto de bytes b com
// b|0x20 em [0x61,0x7A] é [A-Za-z], então dígito e '_' nunca casam por
// acidente. Não use este truque para normalizar texto arbitrário — comparar
// s[i]|0x20 == t[i]|0x20 faria '_' (0x5F) casar com 0x7F.
func asciiEqualFold(s, lower string) bool {
	return asciiEqualFoldMesmoTamanho(s, lower)
}

// asciiEqualFoldMesmoTamanho assume len(s) == len(lower) — o switch de
// keywordType já o garante no caminho quente, e ASCIIEqualFold confere antes
// de chamar. Uma implementação só, para que o parser e o lexer nunca discordem
// sobre o que é a mesma palavra.
func asciiEqualFoldMesmoTamanho(s, lower string) bool {
	for i := 0; i < len(s); i++ {
		if s[i]|0x20 != lower[i] {
			return false
		}
	}
	return true
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// isIdentStart/isIdentPart: identificador bare é ASCII puro (D-Q11-1). Nome
// não-ASCII é plenamente suportado entre aspas duplas — que já são
// obrigatórias para os nomes reais do dataset ("Acct-Status-Type" tem '-').
func isIdentStart(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isIdentPart(b byte) bool { return isIdentStart(b) || isDigit(b) }

const hexDigits = "0123456789abcdef"

// hex2 formata um byte em hexadecimal de 2 dígitos sem passar por fmt (o
// caminho de erro do lexer não é quente, mas fmt.Sprintf aqui obrigaria o
// pacote a importar fmt no arquivo do hot path).
func hex2(b byte) string {
	return string([]byte{hexDigits[b>>4], hexDigits[b&0x0F]})
}
