package lexer_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

// Keyword é case-insensitive, mas Text preserva a grafia do fonte: quem
// escreve a mensagem de erro do parser mostra o que o usuário digitou.
func TestKeywordsCaseInsensitive(t *testing.T) {
	keywords := []struct {
		lower string
		typ   lexer.TokenType
	}{
		{"select", lexer.TokenSelect},
		{"from", lexer.TokenFrom},
		{"where", lexer.TokenWhere},
		{"limit", lexer.TokenLimit},
		{"and", lexer.TokenAnd},
		{"or", lexer.TokenOr},
		{"is", lexer.TokenIs},
		{"not", lexer.TokenNot},
		{"null", lexer.TokenNull},
		{"true", lexer.TokenTrue},
		{"false", lexer.TokenFalse},
	}
	if len(keywords) != 11 {
		t.Fatalf("%d keywords, o contrato fixa 11", len(keywords))
	}

	casings := func(s string) []string {
		upper := []byte(s)
		mixed := []byte(s)
		for i := range upper {
			upper[i] -= 32
		}
		for i := 0; i < len(mixed); i += 2 {
			mixed[i] -= 32
		}
		return []string{s, string(upper), string(mixed)}
	}

	for _, kw := range keywords {
		for _, casing := range casings(kw.lower) {
			t.Run(casing, func(t *testing.T) {
				got := lexAll(t, casing)
				if got[0].Type != kw.typ {
					t.Errorf("%q = %v, want %v", casing, got[0].Type, kw.typ)
				}
				if got[0].Text != casing {
					t.Errorf("Text = %q, want a grafia do fonte %q", got[0].Text, casing)
				}
			})
		}
	}
}

// Palavras que colidem por prefixo ou sufixo com keywords são
// identificadores: o switch por comprimento tem de casar a palavra INTEIRA.
func TestQuaseKeywordsSaoIdent(t *testing.T) {
	cases := []string{
		"nulls", "null_", "selected", "is_active", "_from", "and2", "ORs",
		"tru", "falses", "wher", "limits", "ands", "nots", "iss",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			wantStream(t, sql, []tok{{lexer.TokenIdent, sql, 0}})
		})
	}
}

// Identificador é case-SENSITIVE: status e Status são colunas diferentes, e o
// binder resolve nome exato.
func TestIdentificadoresSaoCaseSensitive(t *testing.T) {
	wantStream(t, "status Status STATUS", []tok{
		{lexer.TokenIdent, "status", 0},
		{lexer.TokenIdent, "Status", 7},
		{lexer.TokenIdent, "STATUS", 14},
	})
}

// O fold ASCII usa |0x20; dígitos e '_' não podem casar por acidente com
// letras (o que aconteceria se o truque fosse aplicado aos dois lados).
func TestFoldNaoConfundeDigitoNemUnderscore(t *testing.T) {
	// '_' é 0x5F; 0x5F|0x20 = 0x7F. Se o fold fosse aplicado aos DOIS lados
	// da comparação, "_" casaria com DEL. Aqui confirmamos que nenhuma dessas
	// palavras vira keyword.
	for _, sql := range []string{"_r", "o_", "an_", "n_ll", "s_lect"} {
		t.Run(sql, func(t *testing.T) {
			wantStream(t, sql, []tok{{lexer.TokenIdent, sql, 0}})
		})
	}
	// E "1s" não é identificador nenhum: número com lixo aderente é recusado
	// no lexer, não empurrado como NUMBER+IDENT para o parser.
	if err := lexUntilErr(t, "1s"); err == nil {
		t.Error("1s deveria falhar")
	}
}
