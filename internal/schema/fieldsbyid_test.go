package schema

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const fieldsYAML = `
version: 1
name: fields_ds
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    zeta:
      type: int64
    alpha:
      type: string
      alias: apelido
`

// SELECT * expande por esta lista: a ordem precisa ser a de FieldID, que é a
// mesma ordem do payload — o executor casa payload×slots por merge linear.
func TestFieldsByIDOrdenadoEUnico(t *testing.T) {
	dict := newFakeDict()
	c, err := parseOK(t, fieldsYAML).Compile(dict)
	if err != nil {
		t.Fatal(err)
	}

	fields := c.FieldsByID()
	if len(fields) != 3 {
		t.Fatalf("%d campos, want 3 (id, zeta, alpha)", len(fields))
	}
	for i := 1; i < len(fields); i++ {
		if fields[i].ID <= fields[i-1].ID {
			t.Errorf("fora de ordem na posição %d: %d após %d", i, fields[i].ID, fields[i-1].ID)
		}
	}
	// Campo com alias aparece UMA vez: byName teria duas entradas, byID tem
	// uma — deduplicar de novo aqui seria erro.
	seen := map[types.FieldID]int{}
	for _, f := range fields {
		seen[f.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("campo %d aparece %d vezes", id, n)
		}
	}
	// E o alias continua resolvendo para o mesmo ponteiro.
	byAlias, ok := c.FieldByName([]byte("apelido"))
	if !ok {
		t.Fatal("alias não resolve")
	}
	found := false
	for _, f := range fields {
		if f == byAlias {
			found = true
		}
	}
	if !found {
		t.Error("o campo do alias não está na lista")
	}
}

// O snapshot é do instante da chamada: campo registrado depois não aparece na
// lista antiga, e aparece na nova.
func TestFieldsByIDEhSnapshot(t *testing.T) {
	dict := newFakeDict()
	c, err := parseOK(t, fieldsYAML).Compile(dict)
	if err != nil {
		t.Fatal(err)
	}

	before := c.FieldsByID()
	if _, err := c.ResolveUnknown([]byte("X-Vendor-Attr")); err != nil {
		t.Fatal(err)
	}
	after := c.FieldsByID()

	if len(before) != 3 {
		t.Errorf("a lista antiga mudou: %d campos", len(before))
	}
	if len(after) != 4 {
		t.Fatalf("%d campos após ResolveUnknown, want 4", len(after))
	}
	// O campo novo entra no fim, porque o ID é crescente.
	if after[3].Name != "X-Vendor-Attr" || !after[3].IsAuto {
		t.Errorf("último campo = %+v, want X-Vendor-Attr auto", after[3])
	}
}

func TestFieldsByIDDatasetSemCamposExtras(t *testing.T) {
	const keyOnly = `
version: 1
name: min
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include:
    - id
  fields:
    id:
      type: string
`
	c, err := parseOK(t, keyOnly).Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	fields := c.FieldsByID()
	if len(fields) != 1 || fields[0].Name != "id" {
		t.Errorf("campos = %+v, want só id", fields)
	}
}
