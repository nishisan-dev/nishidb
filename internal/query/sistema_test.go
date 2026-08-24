package query_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

const yamlComExpiracao = `
version: 1
name: alarmes
materialization:
  key:
    field: id
  mode: merge
  ttl:
    expireAfterWrite: 2h
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    severidade:
      type: int64
`

func harnessComExpiracao(t *testing.T) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte(yamlComExpiracao), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	st := fakestorage.New(fakestorage.Indexes{})
	m := materializer.New(sch, st)
	if r := m.Apply([]byte(`{"id":"A","severidade":3}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	eng := query.NewEngine()
	if err := eng.Register("alarmes", sch, st); err != nil {
		t.Fatal(err)
	}
	return eng
}

func colunasDe(t *testing.T, eng *query.Engine, sql string) []string {
	t.Helper()
	it, err := eng.Execute(context.Background(), "alarmes", sql, nil)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	var out []string
	for _, c := range it.Columns() {
		out = append(out, c.Name)
	}
	return out
}

// SELECT * é "todos os campos do dado", não "todos os campos do payload": a
// contabilidade interna do banco não é resposta a essa pergunta.
func TestSelectEstrelaNaoProjetaOCampoDeSistema(t *testing.T) {
	eng := harnessComExpiracao(t)
	cols := colunasDe(t, eng, "SELECT * FROM alarmes")
	for _, c := range cols {
		if c == schema.CampoInstanteDeEscrita {
			t.Fatalf("SELECT * projetou o campo de sistema: %v", cols)
		}
	}
	if len(cols) != 2 {
		t.Fatalf("colunas = %v, quer id e severidade", cols)
	}
}

// Oculto no *, não inacessível: pedir pelo nome funciona, como ROWID/xmin.
func TestSelectNominalDoCampoDeSistemaFunciona(t *testing.T) {
	eng := harnessComExpiracao(t)
	sql := "SELECT id, " + schema.CampoInstanteDeEscrita + " FROM alarmes"
	it, err := eng.Execute(context.Background(), "alarmes", sql, nil)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	if !it.Next() {
		t.Fatalf("nenhuma linha; err=%v", it.Err())
	}
	v := it.Row().Col(1).Val
	if v.I64 <= 0 {
		t.Fatalf("o campo de sistema veio vazio: %+v", v)
	}
	if cols := it.Columns(); len(cols) != 2 || cols[1].Name != schema.CampoInstanteDeEscrita {
		t.Fatalf("colunas = %+v", cols)
	}
}

// Dataset sem expiração declarada não tem o campo, e pedi-lo é erro de coluna
// inexistente — não uma coluna sempre nula.
func TestSemExpiracaoOCampoDeSistemaNaoExiste(t *testing.T) {
	yaml := strings.Replace(yamlComExpiracao, "  ttl:\n    expireAfterWrite: 2h\n", "", 1)
	spec, err := schema.Parse([]byte(yaml), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	eng := query.NewEngine()
	if err := eng.Register("alarmes", sch, fakestorage.New(fakestorage.Indexes{})); err != nil {
		t.Fatal(err)
	}
	sql := "SELECT " + schema.CampoInstanteDeEscrita + " FROM alarmes"
	if _, err := eng.Execute(context.Background(), "alarmes", sql, nil); err == nil {
		t.Fatal("a coluna de sistema existe num dataset sem expiração")
	}
}
