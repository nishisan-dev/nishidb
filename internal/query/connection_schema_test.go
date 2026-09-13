package query_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
)

// queryInSchema executa pela conexão por schema, com o handle dado.
func queryInSchema(t *testing.T, handle, sql string) ([]row, error) {
	t.Helper()
	eng := engineWithTwoDatasets(t)
	it, err := eng.ExecuteInSchema(context.Background(), handle, sql, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []row
	for it.Next() {
		l := make(row, it.Row().Len())
		for _, c := range it.Row().Cols() {
			l[c.Name] = render(c)
		}
		out = append(out, l)
	}
	return out, it.Err()
}

// Na conexão por schema o nome simples é a tabela DO SCHEMA DA CONEXÃO, e não
// só a base do handle: o contador abre pelo nome com que a árvore o mostra. O
// conteúdo prova que foi o contador do schema certo que abriu — o outro
// dataset tem um contador de nome diferente.
func TestConnectionSchemaOpensCounterByBareName(t *testing.T) {
	rows, err := queryInSchema(t, dataset, "SELECT olt, total FROM por_olt ORDER BY total ASC")
	if err != nil {
		t.Fatalf("FROM por_olt na conexão telecom: %v", err)
	}
	if got := strings.Join(chaves(rows, "olt"), ","); got != "BR_SPOMO_OLT14,PRCTA_G1M10" {
		t.Fatalf("por_olt = %v", got)
	}

	rows, err = queryInSchema(t, otherDataset, "SELECT id, regiao FROM "+otherDataset+" ORDER BY id ASC")
	if err != nil {
		t.Fatalf("FROM %s na conexão %s: %v", otherDataset, otherSchemaName, err)
	}
	if got := strings.Join(chaves(rows, "regiao"), ","); got != "NORTE,SUL" {
		t.Fatalf("%s = %v", otherDataset, got)
	}
}

// O qualificador igual ao schema da conexão resolve como o nome simples.
func TestConnectionSchemaAcceptsItsOwnQualifier(t *testing.T) {
	if _, err := queryInSchema(t, dataset, "SELECT olt FROM telecom.por_olt"); err != nil {
		t.Fatalf("FROM telecom.por_olt na conexão telecom: %v", err)
	}
	if _, err := queryInSchema(t, otherDataset, "SELECT id FROM "+otherSchemaName+"."+otherDataset); err != nil {
		t.Fatalf("FROM %s.%s na conexão %s: %v", otherSchemaName, otherDataset, otherSchemaName, err)
	}
}

// O schema alheio é RECUSADO, e não resolvido: a árvore da conexão não o
// mostra. A recusa é BIND apontando para o qualificador — é ele o erro, e não
// a tabela, que existe —, nomeia o schema da conexão e oferece os endereços
// que ela abre.
//
// Pelo Execute por dataset o mesmo SQL abre (TestSchemaQualificadoResolveODatasetCerto):
// é a diferença entre os dois escopos, e é por isso que o teste também a cobra.
func TestConnectionSchemaRefusesAnotherSchema(t *testing.T) {
	sql := "SELECT id FROM " + otherSchemaName + "." + otherDataset
	_, err := queryInSchema(t, dataset, sql)
	var be *binder.Error
	if !errors.As(err, &be) {
		t.Fatalf("FROM %s.%s na conexão telecom: erro = %v, quer binder.Error", otherSchemaName, otherDataset, err)
	}
	if be.Pos != strings.Index(sql, otherSchemaName) {
		t.Errorf("posição = %d, quer %d (o qualificador)", be.Pos, strings.Index(sql, otherSchemaName))
	}
	for _, want := range []string{`"` + otherSchemaName + `"`, `"telecom"`, "telecom.por_olt", "telecom." + dataset, "_meta"} {
		if !strings.Contains(be.Error(), want) {
			t.Errorf("a recusa não cita %q: %s", want, be.Error())
		}
	}
	if strings.Contains(be.Error(), otherCounter) {
		t.Errorf("a recusa oferece tabela de outro schema: %s", be.Error())
	}

	if _, err := queryOf(t, engineWithTwoDatasets(t), dataset, sql); err != nil {
		t.Fatalf("pelo Execute por dataset o mesmo SQL tem de abrir: %v", err)
	}
}

// O atalho `_ns.x` continua sendo do handle na conexão por schema.
func TestConnectionSchemaKeepsTheReservedShortcutOnTheHandle(t *testing.T) {
	if _, err := queryInSchema(t, dataset, "SELECT olt FROM _counters.por_olt"); err != nil {
		t.Fatalf("_counters.por_olt pelo handle telecom: %v", err)
	}
	if _, err := queryInSchema(t, dataset, "SELECT name FROM _meta.fields"); err != nil {
		t.Fatalf("_meta.fields pelo handle telecom: %v", err)
	}
	// O contador do outro schema não existe sob este handle.
	if _, err := queryInSchema(t, dataset, "SELECT regiao FROM _counters."+otherCounter); err == nil {
		t.Fatalf("_counters.%s abriu pelo handle telecom", otherCounter)
	}
}

// Nome simples que não é tabela do schema da conexão recusa listando as tabelas
// DELE — e só dele. O nome de uma tabela de outro schema é o caso que importa:
// pelo caminho por dataset ele seria "dataset desconhecido", e aqui tem de ser
// "o schema da conexão não tem essa tabela".
func TestConnectionSchemaBareNameOutsideTheSchemaIsRefused(t *testing.T) {
	_, err := queryInSchema(t, dataset, "SELECT id FROM "+otherDataset)
	var be *binder.Error
	if !errors.As(err, &be) {
		t.Fatalf("FROM %s na conexão telecom: erro = %v, quer binder.Error", otherDataset, err)
	}
	if !strings.Contains(be.Error(), `"telecom"`) || !strings.Contains(be.Error(), "por_olt") {
		t.Errorf("a recusa não nomeia o schema da conexão e as tabelas dele: %s", be.Error())
	}
	if strings.Contains(be.Error(), otherCounter) {
		t.Errorf("a recusa oferece tabela de outro schema: %s", be.Error())
	}
}

// Os limites que o servidor aplica saem do dataset que o ExecuteInSchema abre:
// o nome simples de um contador é do handle, e o schema alheio fica no handle
// (quem recusa é o Execute, logo depois).
func TestTargetDatasetInSchemaFollowsTheConnectionScope(t *testing.T) {
	eng := engineWithTwoDatasets(t)
	cases := []struct{ handle, sql, want string }{
		{dataset, "SELECT olt FROM por_olt", dataset},
		{otherDataset, "SELECT id FROM " + otherDataset, otherDataset},
		{dataset, "SELECT id FROM " + otherSchemaName + "." + otherDataset, dataset},
		{dataset, "SELECT name FROM _meta.fields", dataset},
	}
	for _, c := range cases {
		if got := eng.TargetDatasetInSchema(c.handle, c.sql); got != c.want {
			t.Errorf("TargetDatasetInSchema(%q, %q) = %q, quer %q", c.handle, c.sql, got, c.want)
		}
	}
}
