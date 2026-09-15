package parser_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// A tabela de erros é CONTRATO: a substring identifica a guarda específica (a
// mensagem é o que o usuário lê para consertar a query) e a posição é o que o
// driver JDBC usa para apontar o byte. Um teste que só afira o sentinela fica
// verde com a guarda errada disparando.
func TestSyntaxErrors(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		pos  int
		msg  string
	}{
		{"vazia", "", 0, "esperado SELECT"},
		{"so espaco", "   ", 3, "esperado SELECT"},
		{"sem select", "FROM x", 0, "esperado SELECT"},
		{"projecao ausente", "SELECT FROM x", 7, "esperado nome de coluna ou '*'"},
		{"keyword como coluna", "SELECT limit FROM x", 7, "esperado nome de coluna ou '*'"},
		{"virgula sem coluna", "SELECT a, FROM x", 10, "esperado nome de coluna"},
		{"estrela com coluna", "SELECT *, a FROM x", 8, "'*' não pode ser combinado com colunas"},
		{"conteudo apos alias", "SELECT a b c FROM x", 11, "esperado ',' ou FROM"},
		{"sem from", "SELECT a", 8, "esperado ',' ou FROM"},
		{"from sem dataset", "SELECT a FROM", 13, "esperado nome do dataset"},
		{"dataset numerico", "SELECT a FROM 3", 14, "esperado nome do dataset"},
		{"where vazio", "SELECT a FROM x WHERE", 21, "esperado predicado"},
		{"parenteses vazios", "SELECT a FROM x WHERE ()", 23, "esperado predicado"},
		{"parenteses sem fechar", "SELECT a FROM x WHERE (a = 1", 28, "esperado ')'"},
		{"literal a esquerda", "SELECT a FROM x WHERE 1 = a", 22, "esperado nome de coluna à esquerda do comparador"},
		{"coluna vs coluna", "SELECT a FROM x WHERE a = b", 26, "comparação entre colunas não é suportada na v1"},
		{"comparador sem operando", "SELECT a FROM x WHERE a =", 25, "esperado literal, TRUE, FALSE ou '?'"},
		{"predicado incompleto", "SELECT a FROM x WHERE a", 23, "esperado comparador, LIKE, IS NULL ou IS NOT NULL"},
		{"like sem padrao", "SELECT a FROM x WHERE a LIKE", 28, "esperado literal, TRUE, FALSE ou '?'"},
		{"like com padrao nao textual", "SELECT a FROM x WHERE a LIKE 1", 29, "o padrão de LIKE é texto ou parâmetro"},
		{"not solto nao vira not like", "SELECT a FROM x WHERE a NOT 1", 24, "esperado comparador, LIKE, IS NULL ou IS NOT NULL"},
		{"is sem null", "SELECT a FROM x WHERE a IS 1", 27, "esperado NULL ou NOT após IS"},
		{"is not sem null", "SELECT a FROM x WHERE a IS NOT 1", 31, "esperado NULL após IS NOT"},
		{"not generico", "SELECT a FROM x WHERE NOT a = 1", 22, "NOT só é suportado em IS NOT NULL e NOT LIKE"},
		{"and sem direita", "SELECT a FROM x WHERE a = 1 AND", 31, "esperado predicado"},
		{"and duplicado", "SELECT a FROM x WHERE a = 1 AND AND b = 2", 32, "esperado predicado"},
		{"or sem direita", "SELECT a FROM x WHERE a = 1 OR", 30, "esperado predicado"},
		{"limit sem valor", "SELECT a FROM x LIMIT", 21, "esperado literal inteiro após LIMIT"},
		{"limit com parametro", "SELECT a FROM x LIMIT ?", 22, "LIMIT não aceita parâmetro"},
		{"limit com string", "SELECT a FROM x LIMIT 'x'", 22, "esperado literal inteiro após LIMIT"},
		{"limit fracionario", "SELECT a FROM x LIMIT 1.5", 22, "LIMIT exige literal inteiro"},
		{"limit negativo", "SELECT a FROM x LIMIT -1", 22, "LIMIT exige literal inteiro sem sinal"},
		// -0 é zero, e mesmo assim é recusado: a gramática recusa o SINAL. A
		// mensagem tem de dizer isso, senão manda procurar um valor negativo
		// que não existe.
		{"limit menos zero", "SELECT a FROM x LIMIT -0", 22, "LIMIT exige literal inteiro sem sinal"},
		// O sinal de MAIS nem chega ao parser: '+' não abre número no lexer
		// (só '-' abre), então a recusa vem de lá. Registrado para que mover
		// essa fronteira apareça como mudança de mensagem, não em silêncio.
		{"limit com mais", "SELECT a FROM x LIMIT +1", 22, "caractere inesperado '+'"},
		{"limit overflow", "SELECT a FROM x LIMIT 99999999999999999999", 22, "inteiro fora do intervalo de int64"},
		{"operando overflow", "SELECT a FROM x WHERE b = 99999999999999999999", 26, "inteiro fora do intervalo de int64"},
		{"where depois de limit", "SELECT a FROM x LIMIT 10 WHERE a = 1", 25, "conteúdo inesperado após o fim da query"},
		// ORDER BY e OFFSET saíam daqui, com "conteúdo inesperado após o fim
		// da query". Hoje são cláusula NOMEADA e vivem em clauses_test.go —
		// a recusa é a mesma, o que muda é o que o usuário faz depois de ler.
		// O caso do `where depois de limit`, logo acima, continua aqui: WHERE
		// fora de ordem é erro de escrita, não recurso ausente.
		{"dois comandos", "SELECT a FROM x; SELECT b FROM y", 17, "apenas um comando por query"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := parser.Parse(tc.sql)
			if err == nil {
				t.Fatalf("Parse(%q) não falhou (stmt = %#v)", tc.sql, st)
			}
			if st != nil {
				t.Errorf("stmt = %#v, want nil junto com o erro", st)
			}
			if !errors.Is(err, parser.ErrSyntax) {
				t.Errorf("erro = %v, want errors.Is ErrSyntax", err)
			}
			var se *parser.SyntaxError
			if !errors.As(err, &se) {
				t.Fatalf("erro = %v, want *SyntaxError", err)
			}
			if se.Pos != tc.pos {
				t.Errorf("Pos = %d, want %d (mensagem: %v)", se.Pos, tc.pos, err)
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("mensagem = %q, want conter %q", err, tc.msg)
			}
		})
	}
}

// Aninhamento além do teto é recusado com posição — sem o limite, um input
// adversarial estoura a pilha.
func TestAninhamentoDemais(t *testing.T) {
	openParens := strings.Repeat("(", parser.MaxExprDepth+1)
	sql := "SELECT a FROM x WHERE " + openParens + "a = 1"

	_, err := parser.Parse(sql)
	if err == nil {
		t.Fatal("aninhamento acima do limite deveria falhar")
	}
	if !strings.Contains(err.Error(), "expressão aninhada demais") {
		t.Errorf("mensagem = %q", err)
	}
	var se *parser.SyntaxError
	if errors.As(err, &se) {
		// A posição é a do parêntese que estourou o limite (o 65º).
		if want := len("SELECT a FROM x WHERE ") + parser.MaxExprDepth; se.Pos != want {
			t.Errorf("Pos = %d, want %d", se.Pos, want)
		}
	}
}

// Erro que nasce no LEXER é repassado cru: embrulhar poria prefixo na
// mensagem e quebraria as asserções de substring do pipeline inteiro.
func TestErroDoLexerChegaSemEmbrulho(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		msg  string
		pos  int
	}{
		{"caractere invalido", "SELECT # FROM x", "caractere inesperado '#'", 7},
		{"numero com zero a esquerda", "SELECT a FROM x WHERE a = 007", "literal numérico inválido", 26},
		{"string aberta", "SELECT a FROM x WHERE a = 'aberta", "literal string sem fechamento", 26},
		{"comentario sql", "SELECT a FROM x -- comentario", "caractere inesperado '-'", 16},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.sql)
			if err == nil {
				t.Fatal("esperava erro")
			}
			// Mesmo sentinela do lexer, sem prefixo adicionado pelo parser.
			if !errors.Is(err, lexer.ErrSyntax) {
				t.Errorf("erro = %v, want lexer.ErrSyntax", err)
			}
			if !strings.HasPrefix(err.Error(), "erro de sintaxe na posição") {
				t.Errorf("mensagem = %q ganhou prefixo do parser", err)
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("mensagem = %q, want conter %q", err, tc.msg)
			}
			var se *parser.SyntaxError
			if errors.As(err, &se) && se.Pos != tc.pos {
				t.Errorf("Pos = %d, want %d", se.Pos, tc.pos)
			}
		})
	}
}

// O caso clássico de recursive descent: um caminho de erro que faz `continue`
// em vez de `return` entra em loop. Se este teste travar, é isso.
func TestErroSempreTermina(t *testing.T) {
	pathologicalQueries := []string{
		"SELECT a FROM x WHERE a = 1 AND AND AND b = 2",
		"SELECT a FROM x WHERE ((((",
		"SELECT ,,,, FROM x",
		"SELECT a FROM x WHERE a IS IS IS NULL",
		"SELECT a FROM x LIMIT LIMIT LIMIT",
	}
	for _, sql := range pathologicalQueries {
		t.Run(sql, func(t *testing.T) {
			if _, err := parser.Parse(sql); err == nil {
				t.Errorf("Parse(%q) não falhou", sql)
			}
		})
	}
}

// O '.' virou token para servir ao nome qualificado do FROM. As mensagens
// abaixo existem para que essa mudança não CUSTE diagnóstico: sem elas, cada
// uma destas entradas cairia numa recusa genérica ("esperado ',' ou FROM",
// "esperado nome do dataset") que não nomeia o ponto nem diz o que fazer.
func TestPontoForaDeLugarTemMensagemPropria(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		msg  string
	}{
		{"from com ponto solto", "SELECT a FROM x.", "esperado nome"},
		{"from comecando com ponto", "SELECT a FROM .x", "esperado nome do dataset"},
		{"from com tres niveis", "SELECT a FROM a.b.c", "dois níveis"},
		{"projecao com tres niveis", "SELECT a.b.c FROM x", "dois níveis"},
		{"projecao com ponto solto", "SELECT a. FROM x", "esperado nome de coluna"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.sql)
			if err == nil {
				t.Fatal("esperava erro")
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, quer conter %q", err, tc.msg)
			}
		})
	}
}

// O nome qualificado no FROM é o que a tabela virtual de introspecção usa.
// Continua LL(1): o '.' É o token de lookahead, e a decisão se toma olhando
// só para ele — ao contrário do BY de GROUP BY, que exigiria um segundo e por
// isso foi recusado.
func TestFromAceitaNomeQualificado(t *testing.T) {
	st, err := parser.Parse("SELECT name FROM _meta.fields")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if st.Namespace != "_meta" {
		t.Errorf("Namespace = %q, quer _meta", st.Namespace)
	}
	if st.Dataset != "fields" {
		t.Errorf("Dataset = %q, quer fields", st.Dataset)
	}
	if st.DatasetPos != len("SELECT name FROM _meta.") {
		t.Errorf("DatasetPos = %d, quer apontar para o nome depois do ponto", st.DatasetPos)
	}
}

// Sem namespace o campo fica vazio: é aditivo, e nenhuma query existente muda
// de forma.
func TestFromSemNamespaceDeixaOCampoVazio(t *testing.T) {
	st, err := parser.Parse("SELECT a FROM x")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if st.Namespace != "" {
		t.Errorf("Namespace = %q, quer vazio", st.Namespace)
	}
}

// `pai.*` expande as colunas extraídas daquele pai. Elas ficam fora do
// SELECT *, então esta é a forma de pedi-las sem saber os nomes.
func TestProjecaoAceitaCurigaQualificado(t *testing.T) {
	st, err := parser.Parse("SELECT id, doc.* FROM eventos")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if st.Star {
		t.Error("Star = true; `doc.*` não é o curinga de topo")
	}
	if len(st.Projection) != 2 {
		t.Fatalf("projeção = %d itens, quer 2", len(st.Projection))
	}
	q := st.Projection[1]
	if q.Qualifier != "doc" || !q.Star {
		t.Errorf("segundo item = %+v, quer qualificador doc com Star", q)
	}
}

// `pai.caminho` endereça a coluna extraída pelo CAMINHO, e não pelo `as`.
func TestProjecaoAceitaCaminhoQualificado(t *testing.T) {
	st, err := parser.Parse("SELECT doc.probeIpNode FROM eventos")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	q := st.Projection[0]
	if q.Qualifier != "doc" || q.Name != "probeIpNode" || q.Star {
		t.Errorf("item = %+v, quer doc/probeIpNode sem Star", q)
	}
}

// O curinga de topo continua sem poder ser combinado, e o qualificado herda a
// mesma regra: `SELECT *, doc.*` colapsaria colunas em silêncio.
func TestCuringaDeTopoNaoSeCombinaComQualificado(t *testing.T) {
	for _, sql := range []string{"SELECT *, doc.* FROM x", "SELECT doc.*, * FROM x"} {
		if _, err := parser.Parse(sql); err == nil {
			t.Errorf("%q foi aceita", sql)
		}
	}
}
