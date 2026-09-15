package binder_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// A repetição preserva as duas colunas e numera o segundo nome de saída.
func TestProjectionDuplicateNamesReceiveSuffixes(t *testing.T) {
	bq := bindOK(t, "SELECT id, status, id FROM radius_sessions")
	if got := columnNames(bq.Columns); strings.Join(got, ",") != "id,status,id_1" {
		t.Fatalf("nomes = %q", got)
	}
	if bq.Columns[0].Slot != bq.Columns[2].Slot {
		t.Fatal("campo repetido não compartilha slot")
	}
}

// O mesmo campo alcançado por nome e por alias NÃO é repetição: são duas chaves
// DISTINTAS no JSON de saída, e nenhuma some. A regra é sobre o nome de saída,
// não sobre o campo — recusar aqui quebraria uma query legítima que a v1 já
// permite escrever, porque o alias existe no schema desde sempre.
func TestNomeEAliasDoMesmoCampoNaoSaoRepeticao(t *testing.T) {
	bq := bindOK(t, "SELECT id, service_id FROM radius_sessions")

	if len(bq.Columns) != 2 {
		t.Fatalf("%d colunas, want 2", len(bq.Columns))
	}
	if bq.Columns[0].Name != "id" || bq.Columns[1].Name != "service_id" {
		t.Errorf("nomes de saída = %q, %q", bq.Columns[0].Name, bq.Columns[1].Name)
	}
	if bq.Columns[0].Field != bq.Columns[1].Field {
		t.Error("nome e alias resolveram para campos diferentes")
	}
	// Duas colunas de saída, UM slot: o executor lê o campo uma vez e a
	// projeção reaproveita. É a única forma de duas colunas compartilharem slot
	// depois que a repetição por nome passou a ser recusada.
	if bq.Columns[0].Slot != bq.Columns[1].Slot {
		t.Errorf("slots diferentes para o mesmo campo: %d e %d", bq.Columns[0].Slot, bq.Columns[1].Slot)
	}
	if bq.Slots.Len() != 1 {
		t.Errorf("Slots.Len = %d, want 1", bq.Slots.Len())
	}
}

// Coluna inexistente repetida é erro de coluna inexistente, não de repetição —
// senão a mensagem mandaria o usuário apagar a duplicata de um nome que ele
// precisa é corrigir.
//
// O que isto recusa é a IMPLEMENTAÇÃO por pré-passada (varrer a projeção
// inteira procurando nome repetido antes de resolver qualquer coluna), que é a
// forma natural de escrever a checagem e inverteria a mensagem. Trocar a ordem
// DENTRO do laço não é observável: com nomes iguais, a primeira ocorrência já
// falha na resolução — verificado por mutação.
func TestColunaInexistenteVenceARepeticao(t *testing.T) {
	err := bindErr(t, "SELECT nao_existe, nao_existe FROM radius_sessions")

	if !strings.Contains(err.Error(), `coluna "nao_existe" não existe no dataset`) {
		t.Errorf("mensagem = %q", err)
	}
	if strings.Contains(err.Error(), "mais de uma vez") {
		t.Errorf("mensagem = %q, want o erro de coluna inexistente", err)
	}
}

// `SELECT *` não passa pela checagem, e não precisa: o schema já garante nome de
// saída único (o spec recusa alias repetido e alias que colide com nome de
// campo, e o campo do dicionário só entra com o nome livre). O teste existe para
// que a checagem nova não passe a recusar o caminho da estrela.
func TestEstrelaNaoEAfetadaPelaChecagemDeRepeticao(t *testing.T) {
	bq := bindOK(t, "SELECT * FROM radius_sessions")

	seen := make(map[string]struct{}, len(bq.Columns))
	for _, c := range bq.Columns {
		if _, duplicate := seen[c.Name]; duplicate {
			t.Errorf("'*' emitiu %q duas vezes", c.Name)
		}
		seen[c.Name] = struct{}{}
	}
	if len(bq.Columns) < 2 {
		t.Fatalf("%d colunas em '*'", len(bq.Columns))
	}
}

// compileWithExtraFields devolve o schema do fixture acrescido de n campos que
// só existem no dicionário (a rota auto), para montar projeção grande o
// bastante para cruzar o limiar em que a checagem troca varredura por mapa.
func compileWithExtraFields(t *testing.T, n int) *schema.Compiled {
	t.Helper()
	spec, err := schema.Parse([]byte(dsYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	d := fakestorage.NewDict()
	for i := range n {
		if _, err := d.LookupOrCreate([]byte(extraName(i))); err != nil {
			t.Fatal(err)
		}
	}
	c, err := spec.Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func extraName(i int) string { return fmt.Sprintf("c%03d", i) }

// A normalização por mapa preserva nomes distintos e numera colisões tanto
// com uma coluna anterior ao limiar quanto com uma coluna posterior.
func TestLargeProjectionDuplicatesReceiveSuffixes(t *testing.T) {
	const columnCount = 200
	sc := compileWithExtraFields(t, columnCount)

	cases := []struct {
		name            string
		duplicateColumn string // coluna reapresentada no fim da projeção
	}{
		{"antes do limiar", extraName(0)},
		{"depois do limiar", extraName(columnCount - 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			names := make([]string, 0, columnCount+1)
			for i := range columnCount {
				names = append(names, extraName(i))
			}
			names = append(names, tc.duplicateColumn)
			sql := "SELECT " + strings.Join(names, ", ") + " FROM radius_sessions"

			st, err := parser.Parse(sql)
			if err != nil {
				t.Fatal(err)
			}
			bq, err := binder.Bind(st, sc, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(bq.Columns) != columnCount+1 {
				t.Fatalf("colunas = %d", len(bq.Columns))
			}
			for i := range columnCount {
				if bq.Columns[i].Name != names[i] {
					t.Fatalf("nome %d mudou: %s", i, bq.Columns[i].Name)
				}
			}
			if got := bq.Columns[columnCount].Name; got != tc.duplicateColumn+"_1" {
				t.Fatalf("nome duplicado = %q", got)
			}
		})
	}
}

// A projeção grande SEM repetição continua bindando: o limiar é detalhe de
// custo, não regra de gramática.
func TestProjecaoGrandeSemRepeticaoBinda(t *testing.T) {
	const columnCount = 200
	sc := compileWithExtraFields(t, columnCount)

	names := make([]string, 0, columnCount)
	for i := range columnCount {
		names = append(names, extraName(i))
	}
	st, err := parser.Parse("SELECT " + strings.Join(names, ", ") + " FROM radius_sessions")
	if err != nil {
		t.Fatal(err)
	}
	bq, err := binder.Bind(st, sc, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if len(bq.Columns) != columnCount {
		t.Errorf("%d colunas, want %d", len(bq.Columns), columnCount)
	}
}
