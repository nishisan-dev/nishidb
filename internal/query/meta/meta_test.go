package meta_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const alvoYAML = `version: 1
name: eventos
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    status:
      type: string
      index: bitmap
    doc:
      type: json
    livre:
      type: auto
`

func alvo(t *testing.T) *schema.Compiled {
	t.Helper()
	s, err := schema.Parse([]byte(alvoYAML), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// linhas roda a query contra a tabela virtual e devolve as linhas como mapas
// de nome→texto, que é a forma em que as asserções ficam legíveis.
func linhas(t *testing.T, sql string, c *schema.Compiled) []map[string]string {
	t.Helper()
	st, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	if !meta.Reconhece(st) {
		t.Fatalf("Reconhece(%q) = false", sql)
	}
	it, err := meta.Abre(context.Background(), st, c, nil, nil)
	if err != nil {
		t.Fatalf("Abre: %v", err)
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
			m[col.Name] = texto(col.Val)
		}
		out = append(out, m)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	return out
}

func texto(v types.Value) string {
	switch v.Type {
	case types.TypeString:
		return string(v.Bytes)
	case types.TypeBool:
		if v.I64 != 0 {
			return "true"
		}
		return "false"
	default:
		return string(v.Bytes)
	}
}

// A tabela responde "o que este dataset tem", que hoje não tem resposta por
// SQL nenhuma. O campo declarado sai com o tipo declarado e auto=false.
func TestFieldsListaOsCamposDoDataset(t *testing.T) {
	got := linhas(t, "SELECT name, type, auto FROM _meta.fields", alvo(t))
	porNome := map[string]map[string]string{}
	for _, l := range got {
		porNome[l["name"]] = l
	}
	for _, nome := range []string{"id", "status", "doc", "livre"} {
		if _, ok := porNome[nome]; !ok {
			t.Fatalf("campo %q ausente; veio %v", nome, porNome)
		}
	}
	if got := porNome["status"]["type"]; got != "string" {
		t.Errorf("status.type = %q, quer string", got)
	}
	if got := porNome["status"]["auto"]; got != "false" {
		t.Errorf("status.auto = %q, quer false", got)
	}
}

// Campo auto ainda não observado sai com type "unknown", e não com o tipo
// vazio nem com um palpite: o tipo não é desconhecido por falha, é
// desconhecido porque nenhum evento o fixou. É o mesmo vocabulário que o
// /v1/metadata já publica.
func TestCampoAutoSemObservacaoSaiComoUnknown(t *testing.T) {
	got := linhas(t, "SELECT name, type, auto FROM _meta.fields", alvo(t))
	for _, l := range got {
		if l["name"] != "livre" {
			continue
		}
		if l["type"] != "unknown" {
			t.Errorf("livre.type = %q, quer unknown", l["type"])
		}
		if l["auto"] != "true" {
			t.Errorf("livre.auto = %q, quer true", l["auto"])
		}
		return
	}
	t.Fatal("campo livre não apareceu")
}

// A tabela é consultável como qualquer outra: WHERE e projeção passam pelo
// mesmo binder e pelo mesmo executor. É o que dispensa suporte em cliente
// nenhum — DBeaver, CLI e driver já sabem executar SELECT.
func TestFieldsAceitaWhereEProjecao(t *testing.T) {
	got := linhas(t, "SELECT name FROM _meta.fields WHERE index = 'bitmap'", alvo(t))
	if len(got) != 1 || got[0]["name"] != "status" {
		t.Fatalf("filtro por index = %v, quer só status", got)
	}
	if len(got[0]) != 1 {
		t.Errorf("a projeção pediu uma coluna e vieram %d: %v", len(got[0]), got[0])
	}
}

// O binder valida a query contra a tabela virtual, então coluna que não
// existe é recusada — e a mensagem precisa nomear a tabela pelo nome que o
// usuário escreveu, não pelo nome interno do schema sintético.
func TestColunaInexistenteNomeiaATabelaVirtual(t *testing.T) {
	st, err := parser.Parse("SELECT nao_existe FROM _meta.fields")
	if err != nil {
		t.Fatal(err)
	}
	_, err = meta.Abre(context.Background(), st, alvo(t), nil, nil)
	if err == nil {
		t.Fatal("esperava recusa")
	}
	if !strings.Contains(err.Error(), "_meta.fields") {
		t.Errorf("erro = %q, quer nomear _meta.fields", err)
	}
}

// Namespace desconhecido não vira 404 de dataset nem passa batido: a recusa
// nomeia o que existe.
func TestNamespaceDesconhecidoERecusado(t *testing.T) {
	st, err := parser.Parse("SELECT a FROM _outro.coisa")
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Reconhece(st) {
		t.Fatal("Reconhece devia aceitar qualquer namespace reservado para poder recusá-lo com nome")
	}
	if _, err := meta.Abre(context.Background(), st, alvo(t), nil, nil); err == nil {
		t.Fatal("esperava recusa de namespace desconhecido")
	}
}

// Query sem namespace não é da tabela virtual e o motor normal a atende.
func TestReconheceIgnoraQuerySemNamespace(t *testing.T) {
	st, err := parser.Parse("SELECT id FROM eventos")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Reconhece(st) {
		t.Error("Reconhece = true para query sem namespace")
	}
}

var _ executor.RowIterator = (executor.RowIterator)(nil)

const alvoComExtract = `version: 1
name: eventos
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    doc:
      type: json
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
`

func alvoExtract(t *testing.T) *schema.Compiled {
	t.Helper()
	s, err := schema.Parse([]byte(alvoComExtract), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A coluna derivada FICA FORA do SELECT *, então a tabela de metadados é o
// lugar onde ela é descoberta. Escondê-la aqui também deixaria o operador sem
// nenhuma forma de saber que ela existe.
func TestFieldsListaAColunaDerivadaComOPai(t *testing.T) {
	got := linhas(t, "SELECT name, parent FROM _meta.fields WHERE parent = 'doc'", alvoExtract(t))
	if len(got) != 1 {
		t.Fatalf("filtro por parent = %v, quer só a derivada", got)
	}
	if got[0]["name"] != "probe_ip" {
		t.Errorf("name = %q, quer probe_ip", got[0]["name"])
	}
}

// Campo de topo tem parent NULL, e não string vazia: vazio diria "tem pai, e
// o nome dele é nada".
func TestCampoDeTopoTemParentNulo(t *testing.T) {
	got := linhas(t, "SELECT name, parent FROM _meta.fields WHERE parent IS NULL", alvoExtract(t))
	for _, l := range got {
		if l["name"] == "probe_ip" {
			t.Fatal("a derivada apareceu entre as de parent nulo")
		}
	}
	if len(got) < 2 {
		t.Fatalf("campos de topo = %d, quer id e doc pelo menos", len(got))
	}
}
