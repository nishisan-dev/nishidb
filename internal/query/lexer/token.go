// Package lexer tokeniza o subset SQL v1 do NishiDB (spec §59-66).
//
// É um pacote FOLHA: só depende de errors e fmt, e fmt apenas no caminho frio
// das mensagens. Não conhece schema, tipos nem storage — a decisão de que
// `SELECT` só vale no começo, de que `*` só vale na projeção e de qual coluna
// existe é do parser (Q12) e do binder (Q13).
//
// Contrato para o parser:
//
//   - a decisão de keyword é do TokenType, NUNCA do Text. Comparar
//     tok.Text == "SELECT" reintroduz case-sensitivity de keyword e passa
//     despercebido nos testes felizes;
//   - Text de TokenString e TokenQuotedIdent já vem sem aspas e com os
//     escapes ” e "" resolvidos, e é válido para sempre (é cópia, não janela
//     sobre o buffer de unescape);
//   - Text de TokenNumber satisfaz typeparse.JSONNumber — converta com
//     typeparse.Int64/Float64, nunca com strconv direto (risco R3: a coerção
//     da query divergindo da coerção da ingestão);
//   - Pos é offset de BYTE, 0-based, não decrescente, sempre em [0, len(sql)].
package lexer

import "fmt"

// TokenType identifica a categoria léxica de um token.
type TokenType uint8

const (
	// TokenEOF é o zero value de propósito: um Token{} não inicializado lê
	// como fim de entrada, que é a falha segura para um parser recursivo.
	TokenEOF TokenType = iota

	// Keywords (case-insensitive no fonte; Text preserva a grafia original).
	TokenSelect
	TokenFrom
	TokenWhere
	TokenLimit
	TokenAnd
	TokenOr
	TokenIs
	TokenNot
	TokenNull
	TokenTrue
	TokenFalse

	// Identificadores e literais.
	TokenIdent       // bare: [A-Za-z_][A-Za-z0-9_]*
	TokenQuotedIdent // "Acct-Status-Type" — Text sem aspas, "" já desescapado
	TokenString      // 'Stop' — Text sem aspas, '' já desescapado
	TokenNumber      // Text = lexema verbatim (inclui o '-' e o expoente)
	TokenPlaceholder // ?

	// Pontuação.
	TokenComma     // ,
	TokenStar      // *
	TokenLParen    // (
	TokenRParen    // )
	TokenSemicolon // ;
	// TokenDot separa o namespace do nome (`_meta.fields`) e o pai da
	// projeção aninhada (`persistent.*`). Não existe aritmética nem menos
	// unário na v1, então o '.' fora de número nunca é ambíguo: é SEMPRE
	// separador.
	TokenDot // .

	// Comparadores (spec §61).
	TokenEq  // =
	TokenNeq // !=
	TokenGt  // >
	TokenGte // >=
	TokenLt  // <
	TokenLte // <=
)

var tokenNames = [...]string{
	TokenEOF:         "EOF",
	TokenSelect:      "SELECT",
	TokenFrom:        "FROM",
	TokenWhere:       "WHERE",
	TokenLimit:       "LIMIT",
	TokenAnd:         "AND",
	TokenOr:          "OR",
	TokenIs:          "IS",
	TokenNot:         "NOT",
	TokenNull:        "NULL",
	TokenTrue:        "TRUE",
	TokenFalse:       "FALSE",
	TokenIdent:       "IDENT",
	TokenQuotedIdent: "QUOTED_IDENT",
	TokenString:      "STRING",
	TokenNumber:      "NUMBER",
	TokenPlaceholder: "?",
	TokenComma:       ",",
	TokenStar:        "*",
	TokenLParen:      "(",
	TokenRParen:      ")",
	TokenSemicolon:   ";",
	TokenDot:         ".",
	TokenEq:          "=",
	TokenNeq:         "!=",
	TokenGt:          ">",
	TokenGte:         ">=",
	TokenLt:          "<",
	TokenLte:         "<=",
}

// String devolve o nome do tipo para mensagens de erro e diffs de teste.
func (t TokenType) String() string {
	if int(t) < len(tokenNames) && tokenNames[t] != "" {
		return tokenNames[t]
	}
	return fmt.Sprintf("TokenType(%d)", uint8(t))
}

// Token é uma unidade léxica.
//
// Text depende do tipo:
//   - keywords, comparadores e pontuação: o lexema exato do fonte ("SeLeCt",
//     ">=", ",");
//   - TokenIdent: o identificador verbatim (case preservado);
//   - TokenQuotedIdent / TokenString: o conteúdo SEM as aspas e com os
//     escapes "" / ” já resolvidos;
//   - TokenNumber: o lexema verbatim, incluindo sinal e expoente;
//   - TokenEOF: "".
//
// Pos é o offset de BYTE (0-based) do PRIMEIRO byte do token no fonte — para
// tokens com aspas, a posição é a da aspa de ABERTURA. Em TokenEOF,
// Pos == len(fonte).
type Token struct {
	Type TokenType
	Text string
	Pos  int
}

// String formata o token para saída de teste: `SELECT@0`, `IDENT("id")@7`,
// `STRING("it's")@21`, `EOF@42`.
func (t Token) String() string {
	switch t.Type {
	case TokenIdent, TokenQuotedIdent, TokenString, TokenNumber:
		return fmt.Sprintf("%s(%q)@%d", t.Type, t.Text, t.Pos)
	default:
		return fmt.Sprintf("%s@%d", t.Type, t.Pos)
	}
}
