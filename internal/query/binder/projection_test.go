package binder_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// `SELECT id, id` produz um objeto JSON com a chave repetida. É tecnicamente
// válido, e é exatamente por isso que dói: o consumidor colapsa as duas em
// silêncio, o usuário pede duas colunas e recebe uma, sem aviso nenhum. Recusar
// no bind troca resultado errado silencioso por erro visível.
func TestProjecaoComNomeRepetidoERecusada(t *testing.T) {
	const prefix = "SELECT id, status, "
	err := bindErr(t, prefix+"id FROM radius_sessions")

	if !strings.Contains(err.Error(), `coluna "id" aparece mais de uma vez na projeção`) {
		t.Errorf("mensagem = %q", err)
	}
	// A posição é a da SEGUNDA ocorrência: é ela que o usuário apaga para
	// corrigir a query. Apontar a primeira mandaria remover a coluna certa.
	var be *binder.Error
	if !errors.As(err, &be) {
		t.Fatalf("erro é %T, want *binder.Error", err)
	}
	if be.Pos != len(prefix) {
		t.Errorf("posição = %d, want %d (a segunda ocorrência)", be.Pos, len(prefix))
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

// Projeção grande troca a varredura linear por mapa, e a troca tem duas
// maneiras de vazar uma repetição: esquecer de semear o mapa com o que a
// varredura já tinha visto (repetição de coluna ANTES do limiar) e esquecer de
// alimentá-lo dali em diante (repetição DEPOIS do limiar). Cada subteste cobre
// uma delas.
func TestRepeticaoEmProjecaoGrande(t *testing.T) {
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
			if err == nil {
				t.Fatalf("Bind não falhou: %d colunas", len(bq.Columns))
			}
			if !errors.Is(err, binder.ErrBind) {
				t.Errorf("erro = %v, want errors.Is ErrBind", err)
			}
			want := fmt.Sprintf("coluna %q aparece mais de uma vez na projeção", tc.duplicateColumn)
			if !strings.Contains(err.Error(), want) {
				t.Errorf("mensagem = %q, want conter %q", err, want)
			}
			// A posição continua sendo a da última ocorrência, que é a única
			// coisa que o usuário consegue apagar sem reescrever a query.
			var be *binder.Error
			if !errors.As(err, &be) {
				t.Fatalf("erro é %T, want *binder.Error", err)
			}
			if wantPos := strings.LastIndex(sql, tc.duplicateColumn); be.Pos != wantPos {
				t.Errorf("posição = %d, want %d", be.Pos, wantPos)
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
