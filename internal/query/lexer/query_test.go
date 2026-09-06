package lexer_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

// As queries reais do M1 precisam tokenizar. A rastreabilidade normativa dos
// 12 casos da §106 é do Q15 (fim a fim, via query.Engine) — aqui o objetivo é
// só garantir que o front-end aceita a forma que o cliente vai enviar.
func TestLex106Queries(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []tok
	}{
		{
			name: "spec68_parametro",
			sql:  "SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenIdent, "id", 7},
				{lexer.TokenComma, ",", 9},
				{lexer.TokenIdent, "ip", 11},
				{lexer.TokenFrom, "FROM", 14},
				{lexer.TokenIdent, "radius_sessions", 19},
				{lexer.TokenWhere, "WHERE", 35},
				{lexer.TokenIdent, "status", 41},
				{lexer.TokenEq, "=", 48},
				{lexer.TokenPlaceholder, "?", 50},
				{lexer.TokenLimit, "LIMIT", 52},
				{lexer.TokenNumber, "100", 58},
			},
		},
		{
			name: "projecao_estrela",
			sql:  "SELECT * FROM radius_sessions;",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenStar, "*", 7},
				{lexer.TokenFrom, "FROM", 9},
				{lexer.TokenIdent, "radius_sessions", 14},
				{lexer.TokenSemicolon, ";", 29},
			},
		},
		{
			name: "and",
			sql:  "SELECT id FROM s WHERE status = 'Stop' AND nas = 'x'",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenIdent, "id", 7},
				{lexer.TokenFrom, "FROM", 10},
				{lexer.TokenIdent, "s", 15},
				{lexer.TokenWhere, "WHERE", 17},
				{lexer.TokenIdent, "status", 23},
				{lexer.TokenEq, "=", 30},
				{lexer.TokenString, "Stop", 32},
				{lexer.TokenAnd, "AND", 39},
				{lexer.TokenIdent, "nas", 43},
				{lexer.TokenEq, "=", 47},
				{lexer.TokenString, "x", 49},
			},
		},
		{
			name: "or_parenteses",
			sql:  "SELECT id FROM s WHERE (a = 1 OR b = 2) AND c = 3",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenIdent, "id", 7},
				{lexer.TokenFrom, "FROM", 10},
				{lexer.TokenIdent, "s", 15},
				{lexer.TokenWhere, "WHERE", 17},
				{lexer.TokenLParen, "(", 23},
				{lexer.TokenIdent, "a", 24},
				{lexer.TokenEq, "=", 26},
				{lexer.TokenNumber, "1", 28},
				{lexer.TokenOr, "OR", 30},
				{lexer.TokenIdent, "b", 33},
				{lexer.TokenEq, "=", 35},
				{lexer.TokenNumber, "2", 37},
				{lexer.TokenRParen, ")", 38},
				{lexer.TokenAnd, "AND", 40},
				{lexer.TokenIdent, "c", 44},
				{lexer.TokenEq, "=", 46},
				{lexer.TokenNumber, "3", 48},
			},
		},
		{
			name: "null",
			sql:  "SELECT * FROM s WHERE a IS NULL OR b IS NOT NULL",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenStar, "*", 7},
				{lexer.TokenFrom, "FROM", 9},
				{lexer.TokenIdent, "s", 14},
				{lexer.TokenWhere, "WHERE", 16},
				{lexer.TokenIdent, "a", 22},
				{lexer.TokenIs, "IS", 24},
				{lexer.TokenNull, "NULL", 27},
				{lexer.TokenOr, "OR", 32},
				{lexer.TokenIdent, "b", 35},
				{lexer.TokenIs, "IS", 37},
				{lexer.TokenNot, "NOT", 40},
				{lexer.TokenNull, "NULL", 44},
			},
		},
		{
			name: "quoted_fields",
			sql:  `SELECT "Acct-Status-Type" FROM s WHERE "Framed-IP-Address" = '10.0.0.1'`,
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenQuotedIdent, "Acct-Status-Type", 7},
				{lexer.TokenFrom, "FROM", 26},
				{lexer.TokenIdent, "s", 31},
				{lexer.TokenWhere, "WHERE", 33},
				{lexer.TokenQuotedIdent, "Framed-IP-Address", 39},
				{lexer.TokenEq, "=", 59},
				{lexer.TokenString, "10.0.0.1", 61},
			},
		},
		{
			name: "booleanos",
			sql:  "SELECT * FROM s WHERE flag = TRUE AND other = false",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenStar, "*", 7},
				{lexer.TokenFrom, "FROM", 9},
				{lexer.TokenIdent, "s", 14},
				{lexer.TokenWhere, "WHERE", 16},
				{lexer.TokenIdent, "flag", 22},
				{lexer.TokenEq, "=", 27},
				{lexer.TokenTrue, "TRUE", 29},
				{lexer.TokenAnd, "AND", 34},
				{lexer.TokenIdent, "other", 38},
				{lexer.TokenEq, "=", 44},
				{lexer.TokenFalse, "false", 46},
			},
		},
		{
			name: "case_mista",
			sql:  "select Id from S where Status = 'Stop' limit 10;",
			want: []tok{
				{lexer.TokenSelect, "select", 0},
				{lexer.TokenIdent, "Id", 7},
				{lexer.TokenFrom, "from", 10},
				{lexer.TokenIdent, "S", 15},
				{lexer.TokenWhere, "where", 17},
				{lexer.TokenIdent, "Status", 23},
				{lexer.TokenEq, "=", 30},
				{lexer.TokenString, "Stop", 32},
				{lexer.TokenLimit, "limit", 39},
				{lexer.TokenNumber, "10", 45},
				{lexer.TokenSemicolon, ";", 47},
			},
		},
		{
			// Pos é offset de BYTE: depois de uma quebra de linha ele continua
			// contando do começo do fonte, não da linha.
			name: "multilinha",
			sql:  "SELECT id\n\tFROM s\nWHERE a = 1",
			want: []tok{
				{lexer.TokenSelect, "SELECT", 0},
				{lexer.TokenIdent, "id", 7},
				{lexer.TokenFrom, "FROM", 11},
				{lexer.TokenIdent, "s", 16},
				{lexer.TokenWhere, "WHERE", 18},
				{lexer.TokenIdent, "a", 24},
				{lexer.TokenEq, "=", 26},
				{lexer.TokenNumber, "1", 28},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { wantStream(t, tc.sql, tc.want) })
	}
}

// Os comparadores todos numa query só, com números negativos e fracionários.
func TestLex106Comparadores(t *testing.T) {
	wantStream(t, "SELECT * FROM s WHERE a >= 1 AND b <= -2.5 AND c != 'x' AND d > 0 AND e < 9", []tok{
		{lexer.TokenSelect, "SELECT", 0},
		{lexer.TokenStar, "*", 7},
		{lexer.TokenFrom, "FROM", 9},
		{lexer.TokenIdent, "s", 14},
		{lexer.TokenWhere, "WHERE", 16},
		{lexer.TokenIdent, "a", 22},
		{lexer.TokenGte, ">=", 24},
		{lexer.TokenNumber, "1", 27},
		{lexer.TokenAnd, "AND", 29},
		{lexer.TokenIdent, "b", 33},
		{lexer.TokenLte, "<=", 35},
		{lexer.TokenNumber, "-2.5", 38},
		{lexer.TokenAnd, "AND", 43},
		{lexer.TokenIdent, "c", 47},
		{lexer.TokenNeq, "!=", 49},
		{lexer.TokenString, "x", 52},
		{lexer.TokenAnd, "AND", 56},
		{lexer.TokenIdent, "d", 60},
		{lexer.TokenGt, ">", 62},
		{lexer.TokenNumber, "0", 64},
		{lexer.TokenAnd, "AND", 66},
		{lexer.TokenIdent, "e", 70},
		{lexer.TokenLt, "<", 72},
		{lexer.TokenNumber, "9", 74},
	})
}

// Aliases do YAML são identificadores comuns para o lexer; quem os resolve é
// o binder.
func TestLex106Aliases(t *testing.T) {
	wantStream(t, "SELECT service_id, status FROM s WHERE status = 'Stop'", []tok{
		{lexer.TokenSelect, "SELECT", 0},
		{lexer.TokenIdent, "service_id", 7},
		{lexer.TokenComma, ",", 17},
		{lexer.TokenIdent, "status", 19},
		{lexer.TokenFrom, "FROM", 26},
		{lexer.TokenIdent, "s", 31},
		{lexer.TokenWhere, "WHERE", 33},
		{lexer.TokenIdent, "status", 39},
		{lexer.TokenEq, "=", 46},
		{lexer.TokenString, "Stop", 48},
	})
}
