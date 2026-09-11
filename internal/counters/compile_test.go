package counters_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const yamlBase = `
version: 1
name: servicos
materialization:
  key:
    field: customerId
  ttl:
    - expireAfterWrite: 48h
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    slot:
      type: int64
    port:
      type: int64
    authenticated:
      type: boolean
    octets:
      type: int64
      merge: sum
    doc:
      type: json
      extract:
        - path: pon
          as: pon
          type: string
`

func load(t *testing.T, yaml string) (*schema.DatasetSpec, *schema.Compiled) {
	t.Helper()
	spec, err := schema.Parse([]byte(yaml), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return spec, sc
}

func idOf(t *testing.T, sc *schema.Compiled, name string) types.FieldID {
	t.Helper()
	f, ok := sc.FieldByName([]byte(name))
	if !ok {
		t.Fatalf("campo %q não existe", name)
	}
	return f.ID
}

func TestCompilaProduzDefinicoesComIDsETipos(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: servicos_por_olt
    groupBy: [olt]
  - name: autenticados_por_pon
    groupBy: [olt, slot, port]
    where: authenticated = true
    maxGroups: 1000
    retainEmptyGroups: never
`)
	defs, err := counters.Compile(spec, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 2 {
		t.Fatalf("%d definições, quero 2", len(defs))
	}
	a := defs[0]
	if a.Name != "servicos_por_olt" || len(a.Columns) != 1 || a.Columns[0] != idOf(t, sc, "olt") ||
		a.Types[0] != types.TypeString || a.Names[0] != "olt" || a.Where != "" || a.NewEvaluator != nil ||
		a.MaxGroups != schema.DefaultMaxGroups || a.RetainEmptyGroups != schema.DefaultRetainEmptyGroups {
		t.Fatalf("servicos_por_olt = %+v", a)
	}
	b := defs[1]
	if b.Name != "autenticados_por_pon" || len(b.Columns) != 3 ||
		b.Columns[0] != idOf(t, sc, "olt") || b.Columns[1] != idOf(t, sc, "slot") || b.Columns[2] != idOf(t, sc, "port") ||
		b.Types[1] != types.TypeInt64 || b.Where != "authenticated = true" || b.NewEvaluator == nil ||
		b.MaxGroups != 1000 || b.RetainEmptyGroups != time.Duration(0) {
		t.Fatalf("autenticados_por_pon = %+v", b)
	}
	// O avaliador funciona sobre um payload real, e cada chamada dá uma
	// instância própria (o writer e a auditoria não compartilham scratch).
	av1, av2 := b.NewEvaluator(), b.NewEvaluator()
	if av1 == av2 {
		t.Fatal("NewEvaluator tem de instanciar um avaliador por chamada")
	}
	var p []byte
	p, _ = encoding.AppendField(p, idOf(t, sc, "authenticated"), types.Value{Type: types.TypeBool, I64: 1})
	if ok, err := av1.Evaluate(p); err != nil || !ok {
		t.Fatalf("avaliador: %v, %v", ok, err)
	}
}

func TestCompilaResolveColunaExtraida(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: por_pon
    groupBy: [pon]
`)
	defs, err := counters.Compile(spec, sc)
	if err != nil {
		t.Fatal(err)
	}
	if defs[0].Columns[0] != idOf(t, sc, "pon") || defs[0].Types[0] != types.TypeString {
		t.Fatalf("%+v", defs[0])
	}
}

func TestCompilaRecusaWhereSobreCampoSomado(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: octets > 0
`)
	_, err := counters.Compile(spec, sc)
	if err == nil || !strings.Contains(err.Error(), "merge: sum") {
		t.Fatalf("where sobre campo somado tem de ser recusado nomeando merge: sum, veio %v", err)
	}
}

func TestCompilaRecusaGroupBySobreCampoSomado(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: x
    groupBy: [octets]
`)
	_, err := counters.Compile(spec, sc)
	if err == nil || !strings.Contains(err.Error(), "merge: sum") {
		t.Fatalf("groupBy sobre campo somado tem de ser recusado nomeando merge: sum, veio %v", err)
	}
}

func TestCompilaRecusaWhereComCampoDeSistema(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: _writtenAt > 0
`)
	_, err := counters.Compile(spec, sc)
	if err == nil || !strings.Contains(err.Error(), "campo de sistema") || !strings.Contains(err.Error(), "counters[0].where") {
		t.Fatalf("where sobre campo de sistema tem de ser recusado nomeando o contador, veio %v", err)
	}
}

func TestCompilaRecusaWhereQueNaoCompila(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: naoExiste = 1
`)
	if _, err := counters.Compile(spec, sc); err == nil {
		t.Fatal("coluna inexistente no where tem de ser recusada")
	}
}

func TestCompiladorDevolveOMesmoSchemaQueOCallbackCompilou(t *testing.T) {
	spec, err := schema.Parse([]byte(yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: authenticated = true
`), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	dict, err := fielddict.Open(filepath.Join(t.TempDir(), "fields.dict"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dict.Close() }()
	comp := counters.NewCompiler(spec)
	if comp.Compiled() != nil {
		t.Fatal("antes do callback não há schema compilado")
	}
	defs, err := comp.Callback()(dict)
	if err != nil {
		t.Fatal(err)
	}
	sc := comp.Compiled()
	if sc == nil {
		t.Fatal("o callback tem de deixar o schema compilado disponível")
	}
	if len(defs) != 1 || defs[0].Columns[0] != idOf(t, sc, "olt") {
		t.Fatalf("as definições têm de vir do MESMO schema: %+v", defs)
	}
	// Os ids saíram do dicionário real, na ordem do Compile.
	if id, ok := dict.Lookup([]byte("olt")); !ok || id != defs[0].Columns[0] {
		t.Fatalf("o id de olt no dicionário (%d, %v) não é o da definição (%d)", id, ok, defs[0].Columns[0])
	}
}

func TestCompiladorSemCountersDevolveNada(t *testing.T) {
	spec, err := schema.Parse([]byte(yamlBase), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	dict, err := fielddict.Open(filepath.Join(t.TempDir(), "fields.dict"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dict.Close() }()
	comp := counters.NewCompiler(spec)
	defs, err := comp.Callback()(dict)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 0 || comp.Compiled() == nil {
		t.Fatalf("sem counters: nenhuma definição, mas o schema compilado: %d, %v", len(defs), comp.Compiled())
	}
}

// TestCompilaLevaOMoldeDeclaradoAteADefinicao fecha o caminho da chave do YAML
// até o texto que sai na tabela: sem ele, `keyFormat` poderia ser aceito pela
// spec e ignorado pelo compilador, e o groupKey continuaria com o separador
// velho sem ninguém reclamar.
func TestCompilaLevaOMoldeDeclaradoAteADefinicao(t *testing.T) {
	spec, sc := load(t, yamlBase+`
counters:
  - name: default
    groupBy: [olt, slot]
  - name: declarado
    groupBy: [olt, slot]
    keyFormat: "{olt}.{slot:02}"
`)
	defs, err := counters.Compile(spec, sc)
	if err != nil {
		t.Fatal(err)
	}
	key, err := valuekey.AppendComposite(nil, []types.Value{
		{Type: types.TypeString, Bytes: []byte("BR_SPOAM_OLT")},
		{Type: types.TypeInt64, I64: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	quero := map[string]string{"default": "BR_SPOAM_OLT|8", "declarado": "BR_SPOAM_OLT.08"}
	for _, def := range defs {
		texto, _, err := counter.KeyText(nil, def.Format, key, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(texto); got != quero[def.Name] {
			t.Errorf("groupKey do contador %q = %q, quero %q", def.Name, got, quero[def.Name])
		}
		if def.Format.Len() != len(def.Columns) {
			t.Errorf("contador %q: molde de %d componentes para %d colunas", def.Name, def.Format.Len(), len(def.Columns))
		}
	}
}
