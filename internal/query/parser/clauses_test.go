package parser_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// clauseOf extrai a cláusula nomeada de um erro de parse, ou "" quando o
// erro é sintático comum.
func clauseOf(t *testing.T, err error) string {
	t.Helper()
	var se *lexer.SyntaxError
	if !errors.As(err, &se) {
		t.Fatalf("erro %v não é *lexer.SyntaxError", err)
	}
	return se.Clause
}

// As cinco cláusulas que a v1 não tem são NOMEADAS, uma a uma.
//
// Eram sete; o OFFSET e o ORDER BY saíram ao serem implementados. A lista encolhe quando uma delas
// passa a existir — manter aqui uma cláusula implementada faria a mensagem de
// recusa mentir, que é o oposto do que ela existe para fazer.
//
// A diferença que isto faz está na ferramenta de SQL: "conteúdo inesperado
// após o fim da query" manda o usuário procurar o erro de digitação que não
// existe, enquanto "ORDER BY não é suportado" diz o que aconteceu. É a mesma
// recusa; muda o que o usuário faz depois.
//
// A posição aponta para o token da CLÁUSULA, não para o seguinte: é ela que a
// ferramenta sublinha.
func TestAsCincoClausulasNaoSuportadasSaoNomeadasUmaAUma(t *testing.T) {
	cases := []struct {
		sql    string
		clause string
		pos    int
	}{
		{"SELECT a FROM x GROUP BY a", "GROUP BY", 16},
		{"SELECT a FROM x JOIN y ON x.a = y.a", "JOIN", 16},
		{"SELECT a FROM x GROUP BY a HAVING a > 1", "GROUP BY", 16},
		{"SELECT a FROM x HAVING a > 1", "HAVING", 16},
		{"SELECT a FROM x UNION SELECT b FROM y", "UNION", 16},
		{"SELECT DISTINCT a FROM x", "DISTINCT", 7},
	}
	for _, c := range cases {
		t.Run(c.clause+"/"+c.sql, func(t *testing.T) {
			_, err := parser.Parse(c.sql)
			if err == nil {
				t.Fatalf("Parse(%q) não falhou", c.sql)
			}
			if got := clauseOf(t, err); got != c.clause {
				t.Errorf("Clause = %q, quer %q (erro: %v)", got, c.clause, err)
			}
			if !strings.Contains(err.Error(), c.clause) {
				t.Errorf("a mensagem não nomeia a cláusula: %v", err)
			}
			var se *lexer.SyntaxError
			_ = errors.As(err, &se)
			if se.Pos != c.pos {
				t.Errorf("Pos = %d, quer %d (o token da cláusula, não o seguinte)", se.Pos, c.pos)
			}
		})
	}
}

// OS TRÊS ORÁCULOS DE NÃO-REGRESSÃO, e a razão de a detecção NÃO passar pelo
// lexer.
//
// Acrescentar ORDER, GROUP, JOIN... a keywordType é a solução óbvia e está
// errada: `order` é hoje um identificador bare válido, e uma query que
// seleciona uma coluna chamada `order` FUNCIONA — pode estar em produção no
// piloto. Virar keyword quebraria essa query para dar uma mensagem melhor a
// outra. Por isso a detecção mora no sítio do erro do parser, e só para token
// bare que já não tinha para onde ir.
func TestColunaChamadaOrderContinuaValida(t *testing.T) {
	cases := []struct{ name, sql string }{
		{"bare na projeção", "SELECT order FROM x"},
		{"entre aspas na projeção", `SELECT "order" FROM x`},
		{"bare no where", "SELECT a FROM x WHERE order = 1"},
		{"as seis mais o offset, bare, na projeção", "SELECT order, group, offset, join, having, distinct, union FROM x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := parser.Parse(c.sql); err != nil {
				t.Fatalf("Parse(%q) falhou: %v", c.sql, err)
			}
		})
	}
}

// O guard de DISTINCT vive dentro da projeção, e não pode disparar para quem
// quotou o nome de propósito.
//
// A ColumnRef não registra se o identificador veio entre aspas, então um guard
// escrito sobre st.Projection[0].Name dispararia aqui — o token tem de ser
// examinado ANTES de virar nome.
func TestColunaChamadaDistinctEntreAspasNaoDisparaAClausula(t *testing.T) {
	_, err := parser.Parse(`SELECT "distinct" x FROM t`)
	if err == nil {
		t.Fatal("quer erro: dois nomes de coluna sem vírgula")
	}
	if c := clauseOf(t, err); c != "" {
		t.Errorf("Clause = %q, quer vazio: o nome estava entre aspas", c)
	}
}

// Um SELECT cuja única coluna se chama distinct continua válido: sem token
// depois dela além do FROM, não há cláusula nenhuma para supor.
func TestColunaChamadaDistinctSozinhaNaProjecaoContinuaValida(t *testing.T) {
	if _, err := parser.Parse("SELECT distinct FROM t"); err != nil {
		t.Fatalf("Parse falhou: %v", err)
	}
}

// A cláusula não suportada continua sendo erro de SINTAXE para quem
// classifica.
//
// É o que garante que errorCode do CLI e printQueryError não precisem de
// uma linha: os dois testam errors.Is(err, ErrSyntax) e leem a posição por
// errors.As. Um tipo de erro NOVO teria quebrado os dois em silêncio — o
// campo, não.
func TestClausulaNaoSuportadaContinuaSendoErroDeSintaxeParaOsClassificadores(t *testing.T) {
	_, err := parser.Parse("SELECT a FROM x GROUP BY a")
	if err == nil {
		t.Fatal("quer erro")
	}
	if !errors.Is(err, parser.ErrSyntax) {
		t.Errorf("errors.Is(ErrSyntax) = false: os classificadores existentes deixam de reconhecer o erro")
	}
	var se *lexer.SyntaxError
	if !errors.As(err, &se) {
		t.Errorf("errors.As(*SyntaxError) = false: a posição some das mensagens do CLI")
	}
}

// Erro sintático comum não ganha cláusula: o campo vazio é o que distingue os
// dois casos, e é o que o servidor testa para escolher o código de wire.
func TestErroSintaticoComumNaoTemClausula(t *testing.T) {
	for _, sql := range []string{"SELECT FROM x", "SELECT a FROM", "SELECT a FROM x WHERE", "SELECT a, FROM x"} {
		_, err := parser.Parse(sql)
		if err == nil {
			t.Fatalf("Parse(%q) não falhou", sql)
		}
		if c := clauseOf(t, err); c != "" {
			t.Errorf("Parse(%q): Clause = %q, quer vazio", sql, c)
		}
	}
}
