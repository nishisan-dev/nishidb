package lexer_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

// tok é a forma compacta de escrever a expectativa de um token.
type tok struct {
	t    lexer.TokenType
	text string
	pos  int
}

// lexAll tokeniza até EOF (inclusive) e devolve o stream completo.
func lexAll(t *testing.T, sql string) []lexer.Token {
	t.Helper()
	var l lexer.Lexer
	l.Init(sql)
	var out []lexer.Token
	for i := 0; i <= len(sql)+2; i++ {
		tk, err := l.Next()
		if err != nil {
			t.Fatalf("Next(%q): %v", sql, err)
		}
		out = append(out, tk)
		if tk.Type == lexer.TokenEOF {
			return out
		}
	}
	t.Fatalf("lexer não terminou em %q", sql)
	return nil
}

// wantStream compara o stream inteiro item a item (tipo, texto e posição).
func wantStream(t *testing.T, sql string, want []tok) {
	t.Helper()
	got := lexAll(t, sql)
	// O EOF é implícito na expectativa: sempre no fim, sempre em len(sql).
	want = append(want, tok{lexer.TokenEOF, "", len(sql)})
	if len(got) != len(want) {
		t.Fatalf("%q: %d tokens, want %d\n got: %v", sql, len(got), len(want), got)
	}
	for i := range want {
		if got[i].Type != want[i].t || got[i].Text != want[i].text || got[i].Pos != want[i].pos {
			t.Errorf("%q token %d = %v, want %s(%q)@%d",
				sql, i, got[i], want[i].t, want[i].text, want[i].pos)
		}
	}
}

// Os 28 tipos precisam aparecer com Pos conferido: é o contrato que o parser
// consome.
func TestTodosOsTokens(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []tok
	}{
		{
			name: "keywords",
			sql:  "SELECT FROM WHERE LIMIT AND OR IS NOT NULL TRUE FALSE",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenFrom, "FROM", 7},
				{lexer.TokenWhere, "WHERE", 12},
				{lexer.TokenLimit, "LIMIT", 18},
				{lexer.TokenAnd, "AND", 24},
				{lexer.TokenOr, "OR", 28},
				{lexer.TokenIs, "IS", 31},
				{lexer.TokenNot, "NOT", 34},
				{lexer.TokenNull, "NULL", 38},
				{lexer.TokenTrue, "TRUE", 43},
				{lexer.TokenFalse, "FALSE", 48},
			},
		},
		{
			name: "identificadores e literais",
			sql:  `id "Acct-Status-Type" 'Stop' -1.5e3 ?`,
			want: []tok{
				{lexer.TokenIdent, "id", 0},
				{lexer.TokenQuotedIdent, "Acct-Status-Type", 3},
				{lexer.TokenString, "Stop", 22},
				{lexer.TokenNumber, "-1.5e3", 29},
				{lexer.TokenPlaceholder, "?", 36},
			},
		},
		{
			name: "pontuacao e comparadores",
			sql:  ", * ( ) ; = != > >= < <=",
			want: []tok{
				{lexer.TokenComma, ",", 0},
				{lexer.TokenStar, "*", 2},
				{lexer.TokenLParen, "(", 4},
				{lexer.TokenRParen, ")", 6},
				{lexer.TokenSemicolon, ";", 8},
				{lexer.TokenEq, "=", 10},
				{lexer.TokenNeq, "!=", 12},
				{lexer.TokenGt, ">", 15},
				{lexer.TokenGte, ">=", 17},
				{lexer.TokenLt, "<", 20},
				{lexer.TokenLte, "<=", 22},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { wantStream(t, tc.sql, tc.want) })
	}
}

// TokenType.String alimenta mensagens de erro do parser: um tipo sem nome
// vira "TokenType(7)" no rosto do usuário.
func TestTokenTypeString(t *testing.T) {
	all := []lexer.TokenType{
		lexer.TokenEOF, lexer.TokenSelect, lexer.TokenFrom, lexer.TokenWhere, lexer.TokenLimit,
		lexer.TokenAnd, lexer.TokenOr, lexer.TokenIs, lexer.TokenNot, lexer.TokenNull,
		lexer.TokenTrue, lexer.TokenFalse, lexer.TokenIdent, lexer.TokenQuotedIdent,
		lexer.TokenString, lexer.TokenNumber, lexer.TokenPlaceholder, lexer.TokenComma,
		lexer.TokenStar, lexer.TokenLParen, lexer.TokenRParen, lexer.TokenSemicolon,
		lexer.TokenEq, lexer.TokenNeq, lexer.TokenGt, lexer.TokenGte, lexer.TokenLt, lexer.TokenLte,
		lexer.TokenDot,
	}
	if len(all) != 29 {
		t.Fatalf("a lista tem %d tipos, o contrato do parser fixa 29", len(all))
	}
	seen := map[string]lexer.TokenType{}
	for _, tt := range all {
		s := tt.String()
		if s == "" || strings.HasPrefix(s, "TokenType(") {
			t.Errorf("TokenType(%d).String() = %q: constante válida sem nome", tt, s)
		}
		if prev, dup := seen[s]; dup {
			t.Errorf("nome %q usado por TokenType(%d) e TokenType(%d)", s, prev, tt)
		}
		seen[s] = tt
	}
	if got := lexer.TokenType(200).String(); !strings.HasPrefix(got, "TokenType(") {
		t.Errorf("tipo fora de faixa = %q, want fallback TokenType(N)", got)
	}
}

func TestTokenString(t *testing.T) {
	cases := []struct {
		tk   lexer.Token
		want string
	}{
		{lexer.Token{Type: lexer.TokenSelect, Text: "SELECT", Pos: 0}, "SELECT@0"},
		{lexer.Token{Type: lexer.TokenIdent, Text: "id", Pos: 7}, `IDENT("id")@7`},
		{lexer.Token{Type: lexer.TokenString, Text: "it's", Pos: 21}, `STRING("it's")@21`},
		{lexer.Token{Type: lexer.TokenEOF, Pos: 42}, "EOF@42"},
	}
	for _, tc := range cases {
		if got := tc.tk.String(); got != tc.want {
			t.Errorf("Token.String() = %q, want %q", got, tc.want)
		}
	}
}

func TestOperadoresEPontuacao(t *testing.T) {
	// Maximal munch com bounds check: '>' no ÚLTIMO byte não pode dar panic.
	wantStream(t, "a >", []tok{
		{lexer.TokenIdent, "a", 0},
		{lexer.TokenGt, ">", 2},
	})
	// '> =' com espaço são dois tokens; recusar isso é do parser, não do lexer.
	wantStream(t, "> =", []tok{
		{lexer.TokenGt, ">", 0},
		{lexer.TokenEq, "=", 2},
	})
	wantStream(t, "<=>", []tok{
		{lexer.TokenLte, "<=", 0},
		{lexer.TokenGt, ">", 2},
	})
}
