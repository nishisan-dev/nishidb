package query_test

import (
	"context"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// colunas abre a query e devolve o metadado da projeção SEM iterar. É o
// caminho que o protocolo remoto usa para emitir a linha de schema antes da
// primeira row.
func colunas(t *testing.T, h *harness, sql string) []executor.ColMeta {
	t.Helper()
	it, err := h.engine.Execute(context.Background(), dataset, sql, nil)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	defer it.Close()
	return it.Columns()
}

// O metadado tem de existir ANTES do primeiro Next, inclusive em dataset
// vazio: quem consome o stream precisa do schema para montar o
// ResultSetMetaData, e um resultado vazio também tem colunas.
func TestColunasSaoConhecidasAntesDaPrimeiraRow(t *testing.T) {
	h := novoHarness(t) // nada ingerido
	cols := colunas(t, h, `SELECT id, status, nas_ip FROM radius_sessions`)

	quer := []executor.ColMeta{
		{Name: "id", Type: types.TypeString},
		{Name: "status", Type: types.TypeString},
		{Name: "nas_ip", Type: types.TypeIPv4},
	}
	if len(cols) != len(quer) {
		t.Fatalf("colunas = %+v, quer %d", cols, len(quer))
	}
	for i, c := range cols {
		if c != quer[i] {
			t.Fatalf("coluna %d = %+v, quer %+v", i, c, quer[i])
		}
	}
}

// O caso que o dado NÃO revela: uma coluna nula em todas as linhas. Sem o
// metadado fora de banda, o driver não teria como tipá-la — é o débito 13, e é
// a razão de a linha de schema existir.
func TestColunaNulaEmTodasAsLinhasAindaTemTipo(t *testing.T) {
	h := semeado(t) // nenhum evento do fixture traz Filter-Id
	cols := colunas(t, h, `SELECT id, filter_id FROM radius_sessions`)

	if len(cols) != 2 {
		t.Fatalf("colunas = %+v", cols)
	}
	if cols[1].Name != "filter_id" || cols[1].Type != types.TypeString {
		t.Fatalf("filter_id = %+v, quer string", cols[1])
	}

	// E o dado, de fato, não revela nada: a coluna é nula em todas as rows.
	for _, l := range h.consultaOK(`SELECT id, filter_id FROM radius_sessions`) {
		if l["filter_id"] != "<null>" {
			t.Fatalf("filter_id = %q, quer <null> — o fixture mudou e o teste perdeu o sentido", l["filter_id"])
		}
	}
}

// O nome do metadado é o de SAÍDA, o mesmo que sai nas chaves do NDJSON.
// Divergir aqui faria o driver casar coluna por um nome que o dado não usa.
func TestONomeDoMetadadoEhODeSaida(t *testing.T) {
	h := novoHarness(t)

	// Projeção explícita: o rótulo é o que o usuário escreveu.
	if cols := colunas(t, h, `SELECT status FROM radius_sessions`); cols[0].Name != "status" {
		t.Fatalf("nome = %q, quer o escrito na projeção", cols[0].Name)
	}
	// '*': o rótulo é o alias, quando existe.
	cols := colunas(t, h, `SELECT * FROM radius_sessions`)
	achou := false
	for _, c := range cols {
		if c.Name == "Acct-Status-Type" {
			t.Fatalf("o '*' rotulou pelo nome do campo, não pelo alias: %+v", c)
		}
		if c.Name == "status" {
			achou = true
		}
	}
	if !achou {
		t.Fatal("o '*' não produziu a coluna 'status' (alias de Acct-Status-Type)")
	}
}

// A cópia é o contrato: o chamador guarda o metadado enquanto itera, e as
// ColView internas são reescritas a cada Next.
func TestColunasDevolveCopiaIndependente(t *testing.T) {
	h := semeado(t)
	it, err := h.engine.Execute(context.Background(), dataset, `SELECT id FROM radius_sessions`, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	antes := it.Columns()
	antes[0].Name = "mexido"
	for it.Next() {
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if depois := it.Columns(); depois[0].Name != "id" {
		t.Fatalf("o metadado interno foi corrompido pelo chamador: %q", depois[0].Name)
	}
}
