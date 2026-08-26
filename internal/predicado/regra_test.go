package predicado_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/predicado"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const dsYAML = `
version: 1
name: alarmes
materialization:
  key:
    field: id
  ttl:
    - when: "tipo = 2"
      expireAfterWrite: 3m
    - expireAfterWrite: 7d
schema:
  fields:
    id:
      type: string
    tipo:
      type: int64
    sev:
      type: int64
`

func compila(t *testing.T, yaml string) *schema.Compiled {
	t.Helper()
	spec, err := schema.Parse([]byte(yaml), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCompilaRegrasProduzProgramaEFundo(t *testing.T) {
	c := compila(t, dsYAML)
	regras, err := predicado.CompilaRegras(c.RegrasTTL, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(regras) != 2 {
		t.Fatalf("%d regras, quero 2", len(regras))
	}
	if regras[0].Prog == nil || regras[0].Duracao != 3*time.Minute {
		t.Errorf("condicional = %+v", regras[0])
	}
	if regras[1].Prog != nil {
		t.Error("a regra de fundo casa com tudo: não tem programa")
	}
	if regras[1].Duracao != 7*24*time.Hour {
		t.Errorf("fundo = %v", regras[1].Duracao)
	}

	// A regra compilada avalia pelo MESMO código do WHERE.
	slots := predicado.NovosSlots(regras[0].Slots.IDs)
	slots[0].Val, slots[0].Present = types.Value{Type: types.TypeInt64, I64: 2}, true
	if !regras[0].Prog.Avalia(slots, nil) {
		t.Error("tipo = 2 não casou com a regra `tipo = 2`")
	}
	slots[0].Val.I64 = 1
	if regras[0].Prog.Avalia(slots, nil) {
		t.Error("tipo = 1 casou com a regra `tipo = 2`")
	}
}

// A duração não pode depender de si mesma, nem do instante que ela própria
// ajuda a interpretar.
func TestCompilaRegrasRecusaWhenSobreCampoDeSistema(t *testing.T) {
	for _, campo := range []string{schema.CampoInstanteDeEscrita, schema.CampoDePrazo} {
		yaml := strings.Replace(dsYAML, `"tipo = 2"`, `"`+campo+` = 2"`, 1)
		c := compila(t, yaml)
		_, err := predicado.CompilaRegras(c.RegrasTTL, c)
		if err == nil {
			t.Fatalf("condição sobre %q foi aceita", campo)
		}
		if !strings.Contains(err.Error(), campo) {
			t.Errorf("a mensagem precisa nomear o campo: %v", err)
		}
	}
}

func TestCompilaRegrasRecusaWhenQueNaoCompila(t *testing.T) {
	c := compila(t, strings.Replace(dsYAML, `"tipo = 2"`, `"tipo ==== 2"`, 1))
	if _, err := predicado.CompilaRegras(c.RegrasTTL, c); err == nil {
		t.Fatal("condição com sintaxe inválida foi aceita")
	}
}

func TestCompilaRegrasRecusaWhenSobreColunaInexistente(t *testing.T) {
	c := compila(t, strings.Replace(dsYAML, `"tipo = 2"`, `"naoExiste = 2"`, 1))
	if _, err := predicado.CompilaRegras(c.RegrasTTL, c); err == nil {
		t.Fatal("condição sobre coluna inexistente foi aceita")
	}
}

func TestCompilaRegrasRecusaWhenComParametro(t *testing.T) {
	c := compila(t, strings.Replace(dsYAML, `"tipo = 2"`, `"tipo = ?"`, 1))
	_, err := predicado.CompilaRegras(c.RegrasTTL, c)
	if err == nil {
		t.Fatal("parâmetro posicional foi aceito numa regra")
	}
	if !strings.Contains(err.Error(), "parâmetro") {
		t.Errorf("mensagem = %v", err)
	}
}
