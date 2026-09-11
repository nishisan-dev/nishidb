package meta_test

import (
	"context"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// targetWithIndexYAML declara dois campos com `index: true` — a única grafia
// desde a D5 — para o teste abaixo verificar a coluna `indexed`.
const targetWithIndexYAML = `version: 1
name: eventos
materialization:
  key:
    field: id
schema:
  include: ["*"]
  fields:
    id:
      type: string
    status:
      type: string
      index: true
    node:
      type: string
      index: true
    livre:
      type: string
`

// TestIndexedRefleteACapacidadeDoMotor: a coluna `indexed` fala do MOTOR, e a
// coluna `index` ao lado fala do que o YAML declara. Antes da D5, um campo
// `index: hash` sobre um Store que só respondia HasBitmap saía com index=hash e
// indexed=false — correto na época, mentira a partir do motor físico (source.go
// documenta o histórico).
//
// Chamava-se TestIndexedSomaAsDuasCapacidadesDeMotor e montava um cenário
// sintético — status só em Bitmap, node só em Hash — para exercitar os dois
// lados da SOMA `HasBitmap(c.ID) || HasHash(c.ID)`. A soma virou uma chamada só
// (fonte.record), e o cenário deixou de ser construível: as duas colunas
// entram no mesmo conjunto indexado. As asserções abaixo são as MESMAS, valor
// por valor, incluindo o vocabulário de wire da coluna `index`.
func TestIndexedRefleteACapacidadeDoMotor(t *testing.T) {
	spec, err := schema.Parse([]byte(targetWithIndexYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	id := func(name string) types.FieldID {
		f, ok := c.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("campo %q não compilou", name)
		}
		return f.ID
	}
	st := fakestorage.New(fakestorage.Indexes{
		Fields: []types.FieldID{id("status"), id("node")},
	})

	visto := map[string][2]string{}
	for _, l := range rowsWith(t, `SELECT name, index, indexed FROM _meta.fields`, c, st) {
		visto[l["name"]] = [2]string{l["index"], l["indexed"]}
	}

	casos := []struct{ name, index, indexed string }{
		{"status", "bitmap", "true"},
		{"node", "bitmap", "true"},
		{"livre", "none", "false"},
		// A key é indexada pela primary, e não por índice secundário: o YAML
		// recusa `index:` nela. Reportá-la como indexada aqui confundiria as
		// duas coisas.
		{"id", "none", "false"},
	}
	for _, caso := range casos {
		got, ok := visto[caso.name]
		if !ok {
			t.Errorf("coluna %q não apareceu em _meta.fields", caso.name)
			continue
		}
		if got[0] != caso.index || got[1] != caso.indexed {
			t.Errorf("%s: index=%q indexed=%q, esperado index=%q indexed=%q",
				caso.name, got[0], got[1], caso.index, caso.indexed)
		}
	}
}

// rowsWith é o `linhas` com Capacidades de verdade. O helper original passa
// nil de propósito — ele testa a tabela sem storage —, e um teste sobre a
// coluna `indexed` precisa exatamente do contrário.
func rowsWith(t *testing.T, sql string, c *schema.Compiled, caps meta.Capacidades) []map[string]string {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	it, err := meta.Open(context.Background(), stmt, c, caps, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	var out []map[string]string
	for it.Next() {
		m := map[string]string{}
		for _, col := range it.Row().Cols() {
			if col.Null {
				m[col.Name] = ""
				continue
			}
			m[col.Name] = text(col.Val)
		}
		out = append(out, m)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	return out
}
