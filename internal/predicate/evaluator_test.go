package predicate_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const dsCounterYAML = `
version: 1
name: servicos
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    authenticated:
      type: boolean
    slot:
      type: int64
    olt:
      type: string
`

// payloadOf monta um payload ordenado por FieldID a partir de pares nome→valor,
// resolvendo os ids pelo schema compilado.
func payloadOf(t *testing.T, c *schema.Compiled, fields map[string]types.Value) []byte {
	t.Helper()
	type pair struct {
		id types.FieldID
		v  types.Value
	}
	var pares []pair
	for name, v := range fields {
		f, ok := c.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("campo %q não existe no schema", name)
		}
		pares = append(pares, pair{f.ID, v})
	}
	for i := 1; i < len(pares); i++ {
		for j := i; j > 0 && pares[j-1].id > pares[j].id; j-- {
			pares[j-1], pares[j] = pares[j], pares[j-1]
		}
	}
	var buf []byte
	var err error
	for _, p := range pares {
		if buf, err = encoding.AppendField(buf, p.id, p.v); err != nil {
			t.Fatal(err)
		}
	}
	return buf
}

func TestAvaliadorDePayloadAvaliaOEstadoInteiro(t *testing.T) {
	c := compile(t, dsCounterYAML)
	prog, slots, err := predicate.CompileCondition("counters[0].where", "authenticated = true AND slot > 0", c)
	if err != nil {
		t.Fatal(err)
	}
	av := predicate.NewAvaliadorOfPayload(prog, slots.IDs)

	autenticado := payloadOf(t, c, map[string]types.Value{
		"customerId":    {Type: types.TypeString, Bytes: []byte("A")},
		"authenticated": {Type: types.TypeBool, I64: 1},
		"slot":          {Type: types.TypeInt64, I64: 3},
	})
	if ok, err := av.Evaluate(autenticado); err != nil || !ok {
		t.Fatalf("autenticado com slot 3 tem de casar: %v, %v", ok, err)
	}
	naoAutenticado := payloadOf(t, c, map[string]types.Value{
		"customerId":    {Type: types.TypeString, Bytes: []byte("A")},
		"authenticated": {Type: types.TypeBool, I64: 0},
		"slot":          {Type: types.TypeInt64, I64: 3},
	})
	if ok, err := av.Evaluate(naoAutenticado); err != nil || ok {
		t.Fatalf("não autenticado não pode casar: %v, %v", ok, err)
	}
	// Campo AUSENTE compara falso para todo operador (NULL é ausência nas
	// três camadas) — inclusive quando o resto da condição valeria.
	withoutSlot := payloadOf(t, c, map[string]types.Value{
		"customerId":    {Type: types.TypeString, Bytes: []byte("A")},
		"authenticated": {Type: types.TypeBool, I64: 1},
	})
	if ok, err := av.Evaluate(withoutSlot); err != nil || ok {
		t.Fatalf("sem slot não pode casar: %v, %v", ok, err)
	}
	// O mesmo avaliador é reusado entre payloads: nada do anterior vaza.
	if ok, err := av.Evaluate(autenticado); err != nil || !ok {
		t.Fatalf("reuso do avaliador: %v, %v", ok, err)
	}
}

func TestAvaliadorDePayloadPropagaCorrupcao(t *testing.T) {
	c := compile(t, dsCounterYAML)
	prog, slots, err := predicate.CompileCondition("x", "authenticated = true", c)
	if err != nil {
		t.Fatal(err)
	}
	av := predicate.NewAvaliadorOfPayload(prog, slots.IDs)
	if _, err := av.Evaluate([]byte{0x01}); !errors.Is(err, encoding.ErrCorrupt) {
		t.Fatalf("payload rasgado tem de subir como corrupção, veio %v", err)
	}
}

func TestCompilaCondicaoRecusaCampoDeSistema(t *testing.T) {
	// dsYAML declara TTL, e é a expiração que faz _writtenAt existir como
	// campo de sistema; num dataset sem TTL a recusa seria "coluna não
	// existe", que é outra classe.
	c := compile(t, dsYAML)
	_, _, err := predicate.CompileCondition("counters[0].where", "_writtenAt > 0", c)
	if err == nil || !strings.Contains(err.Error(), "campo de sistema") {
		t.Fatalf("condição sobre campo de sistema tem de ser recusada, veio %v", err)
	}
	if !strings.Contains(err.Error(), "counters[0].where") {
		t.Fatalf("o erro tem de nomear o rótulo de quem compilou: %v", err)
	}
}

func TestCompilaCondicaoRecusaColunaInexistenteEParametro(t *testing.T) {
	c := compile(t, dsCounterYAML)
	if _, _, err := predicate.CompileCondition("x", "naoExiste = 1", c); err == nil {
		t.Fatal("coluna inexistente tem de ser recusada")
	}
	if _, _, err := predicate.CompileCondition("x", "slot = ?", c); err == nil {
		t.Fatal("parâmetro posicional não faz sentido numa condição declarada")
	}
}
