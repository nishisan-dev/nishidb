package contadores_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/contadores"
	"github.com/nishisan-dev/nishidb/internal/schema"
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

func carrega(t *testing.T, yaml string) (*schema.DatasetSpec, *schema.Compiled) {
	t.Helper()
	spec, err := schema.Parse([]byte(yaml), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return spec, sc
}

func idDe(t *testing.T, sc *schema.Compiled, nome string) types.FieldID {
	t.Helper()
	f, ok := sc.FieldByName([]byte(nome))
	if !ok {
		t.Fatalf("campo %q não existe", nome)
	}
	return f.ID
}

func TestCompilaProduzDefinicoesComIDsETipos(t *testing.T) {
	spec, sc := carrega(t, yamlBase+`
counters:
  - name: servicos_por_olt
    groupBy: [olt]
  - name: autenticados_por_pon
    groupBy: [olt, slot, port]
    where: authenticated = true
    maxGroups: 1000
    retainEmptyGroups: never
`)
	defs, err := contadores.Compila(spec, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 2 {
		t.Fatalf("%d definições, quero 2", len(defs))
	}
	a := defs[0]
	if a.Nome != "servicos_por_olt" || len(a.Colunas) != 1 || a.Colunas[0] != idDe(t, sc, "olt") ||
		a.Tipos[0] != types.TypeString || a.Nomes[0] != "olt" || a.Where != "" || a.NovoAvaliador != nil ||
		a.MaxGroups != schema.MaxGroupsPadrao || a.RetainEmptyGroups != schema.RetainEmptyGroupsPadrao {
		t.Fatalf("servicos_por_olt = %+v", a)
	}
	b := defs[1]
	if b.Nome != "autenticados_por_pon" || len(b.Colunas) != 3 ||
		b.Colunas[0] != idDe(t, sc, "olt") || b.Colunas[1] != idDe(t, sc, "slot") || b.Colunas[2] != idDe(t, sc, "port") ||
		b.Tipos[1] != types.TypeInt64 || b.Where != "authenticated = true" || b.NovoAvaliador == nil ||
		b.MaxGroups != 1000 || b.RetainEmptyGroups != time.Duration(0) {
		t.Fatalf("autenticados_por_pon = %+v", b)
	}
	// O avaliador funciona sobre um payload real, e cada chamada dá uma
	// instância própria (o writer e a auditoria não compartilham scratch).
	av1, av2 := b.NovoAvaliador(), b.NovoAvaliador()
	if av1 == av2 {
		t.Fatal("NovoAvaliador tem de instanciar um avaliador por chamada")
	}
	var p []byte
	p, _ = encoding.AppendField(p, idDe(t, sc, "authenticated"), types.Value{Type: types.TypeBool, I64: 1})
	if ok, err := av1.Avalia(p); err != nil || !ok {
		t.Fatalf("avaliador: %v, %v", ok, err)
	}
}

func TestCompilaResolveColunaExtraida(t *testing.T) {
	spec, sc := carrega(t, yamlBase+`
counters:
  - name: por_pon
    groupBy: [pon]
`)
	defs, err := contadores.Compila(spec, sc)
	if err != nil {
		t.Fatal(err)
	}
	if defs[0].Colunas[0] != idDe(t, sc, "pon") || defs[0].Tipos[0] != types.TypeString {
		t.Fatalf("%+v", defs[0])
	}
}

func TestCompilaRecusaWhereSobreCampoSomado(t *testing.T) {
	spec, sc := carrega(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: octets > 0
`)
	_, err := contadores.Compila(spec, sc)
	if err == nil || !strings.Contains(err.Error(), "merge: sum") {
		t.Fatalf("where sobre campo somado tem de ser recusado nomeando merge: sum, veio %v", err)
	}
}

func TestCompilaRecusaGroupBySobreCampoSomado(t *testing.T) {
	spec, sc := carrega(t, yamlBase+`
counters:
  - name: x
    groupBy: [octets]
`)
	_, err := contadores.Compila(spec, sc)
	if err == nil || !strings.Contains(err.Error(), "merge: sum") {
		t.Fatalf("groupBy sobre campo somado tem de ser recusado nomeando merge: sum, veio %v", err)
	}
}

func TestCompilaRecusaWhereComCampoDeSistema(t *testing.T) {
	spec, sc := carrega(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: _writtenAt > 0
`)
	_, err := contadores.Compila(spec, sc)
	if err == nil || !strings.Contains(err.Error(), "campo de sistema") || !strings.Contains(err.Error(), "counters[0].where") {
		t.Fatalf("where sobre campo de sistema tem de ser recusado nomeando o contador, veio %v", err)
	}
}

func TestCompilaRecusaWhereQueNaoCompila(t *testing.T) {
	spec, sc := carrega(t, yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: naoExiste = 1
`)
	if _, err := contadores.Compila(spec, sc); err == nil {
		t.Fatal("coluna inexistente no where tem de ser recusada")
	}
}

func TestCompiladorDevolveOMesmoSchemaQueOCallbackCompilou(t *testing.T) {
	spec, err := schema.Parse([]byte(yamlBase+`
counters:
  - name: x
    groupBy: [olt]
    where: authenticated = true
`), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	dict, err := fielddict.Open(filepath.Join(t.TempDir(), "fields.dict"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dict.Close() }()
	comp := contadores.NovoCompilador(spec)
	if comp.Compilado() != nil {
		t.Fatal("antes do callback não há schema compilado")
	}
	defs, err := comp.Callback()(dict)
	if err != nil {
		t.Fatal(err)
	}
	sc := comp.Compilado()
	if sc == nil {
		t.Fatal("o callback tem de deixar o schema compilado disponível")
	}
	if len(defs) != 1 || defs[0].Colunas[0] != idDe(t, sc, "olt") {
		t.Fatalf("as definições têm de vir do MESMO schema: %+v", defs)
	}
	// Os ids saíram do dicionário real, na ordem do Compile.
	if id, ok := dict.Lookup([]byte("olt")); !ok || id != defs[0].Colunas[0] {
		t.Fatalf("o id de olt no dicionário (%d, %v) não é o da definição (%d)", id, ok, defs[0].Colunas[0])
	}
}

func TestCompiladorSemCountersDevolveNada(t *testing.T) {
	spec, err := schema.Parse([]byte(yamlBase), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	dict, err := fielddict.Open(filepath.Join(t.TempDir(), "fields.dict"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dict.Close() }()
	comp := contadores.NovoCompilador(spec)
	defs, err := comp.Callback()(dict)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 0 || comp.Compilado() == nil {
		t.Fatalf("sem counters: nenhuma definição, mas o schema compilado: %d, %v", len(defs), comp.Compilado())
	}
}
