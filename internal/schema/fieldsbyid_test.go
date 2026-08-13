package schema

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const fieldsYAML = `
version: 1
name: fields_ds
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

	campos := c.FieldsByID()
	if len(campos) != 3 {
		t.Fatalf("%d campos, want 3 (id, zeta, alpha)", len(campos))
	}
	for i := 1; i < len(campos); i++ {
		if campos[i].ID <= campos[i-1].ID {
			t.Errorf("fora de ordem na posição %d: %d após %d", i, campos[i].ID, campos[i-1].ID)
		}
	}
	// Campo com alias aparece UMA vez: byName teria duas entradas, byID tem
	// uma — deduplicar de novo aqui seria erro.
	vistos := map[types.FieldID]int{}
	for _, f := range campos {
		vistos[f.ID]++
	}
	for id, n := range vistos {
		if n != 1 {
			t.Errorf("campo %d aparece %d vezes", id, n)
		}
	}
	// E o alias continua resolvendo para o mesmo ponteiro.
	porAlias, ok := c.FieldByName([]byte("apelido"))
	if !ok {
		t.Fatal("alias não resolve")
	}
	achou := false
	for _, f := range campos {
		if f == porAlias {
			achou = true
		}
	}
	if !achou {
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

	antes := c.FieldsByID()
	if _, err := c.ResolveUnknown([]byte("X-Vendor-Attr")); err != nil {
		t.Fatal(err)
	}
	depois := c.FieldsByID()

	if len(antes) != 3 {
		t.Errorf("a lista antiga mudou: %d campos", len(antes))
	}
	if len(depois) != 4 {
		t.Fatalf("%d campos após ResolveUnknown, want 4", len(depois))
	}
	// O campo novo entra no fim, porque o ID é crescente.
	if depois[3].Name != "X-Vendor-Attr" || !depois[3].IsAuto {
		t.Errorf("último campo = %+v, want X-Vendor-Attr auto", depois[3])
	}
}

func TestFieldsByIDDatasetSemCamposExtras(t *testing.T) {
	const soKey = `
version: 1
name: min
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
	c, err := parseOK(t, soKey).Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}
	campos := c.FieldsByID()
	if len(campos) != 1 || campos[0].Name != "id" {
		t.Errorf("campos = %+v, want só id", campos)
	}
}
