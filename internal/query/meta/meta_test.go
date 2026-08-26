package meta_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
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
	case types.TypeInt64:
		// Sem este ramo, o `default` abaixo devolvia string(v.Bytes) — vazio
		// para todo tipo que carrega o valor em I64 —, e uma asserção sobre
		// uma coluna int64 comparava "" com "" e passava por vacuidade. Foi
		// assim que `id` conviveu com o helper desde o começo, sem ninguém
		// afirmar nada sobre ela.
		return strconv.FormatInt(v.I64, 10)
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

// alvoComDerivadasYAML declara um campo json com extração e descoberta, que é
// o que faz as colunas de contador terem sobre o que falar.
const alvoComDerivadasYAML = `version: 1
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
      unknownFields: include
      extract:
        - path: tally
          type: int64
`

func alvoComDerivadas(t *testing.T) *schema.Compiled {
	t.Helper()
	s, err := schema.Parse([]byte(alvoComDerivadasYAML), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestContadoresDeColunaSaemPorSQL é a consulta que o documento de melhorias
// pediu, e a razão de as duas colunas existirem: o contador por DATASET diz
// "2.085.138 valores perdidos" e não diz qual das ~1.700 colunas respondeu por
// isso. Sem atribuição, o número é alarme sem endereço.
func TestContadoresDeColunaSaemPorSQL(t *testing.T) {
	c := alvoComDerivadas(t)
	m, err := materializer.New(c, fakestorage.New(fakestorage.Indexes{}))
	if err != nil {
		t.Fatal(err)
	}

	// tally recebe três inteiros e dois textos: 5 ocorrências, 2 divergências.
	for _, ev := range []string{
		`{"id":"A","doc":{"tally":1}}`,
		`{"id":"B","doc":{"tally":2}}`,
		`{"id":"C","doc":{"tally":3}}`,
		`{"id":"D","doc":{"tally":"nao numerico"}}`,
		`{"id":"E","doc":{"tally":"outro texto"}}`,
	} {
		if r := m.Apply([]byte(ev)); r.Err != nil {
			t.Fatalf("Apply(%s): %v", ev, r.Err)
		}
	}

	achou := false
	for _, l := range linhas(t, "SELECT name, occurrences, typeMismatches FROM _meta.fields", c) {
		if l["name"] != "tally" {
			continue
		}
		achou = true
		if l["occurrences"] != "5" {
			t.Errorf("occurrences = %q, quer 5 (toda vez que a chave apareceu)", l["occurrences"])
		}
		if l["typeMismatches"] != "2" {
			t.Errorf("typeMismatches = %q, quer 2", l["typeMismatches"])
		}
	}
	if !achou {
		t.Fatal("a coluna derivada `tally` não apareceu em _meta.fields")
	}
}

// TestDivergenciaEhSubconjuntoDaOcorrencia é a razão de occurrences contar a
// APARIÇÃO da chave e não a materialização bem-sucedida.
//
// Contando o sucesso, o denominador de "2.315 de 2.383" viraria uma soma na
// cabeça de quem consulta — e a tabela existe justamente para que a decisão de
// tipo saia de SQL em vez de script.
func TestDivergenciaEhSubconjuntoDaOcorrencia(t *testing.T) {
	c := alvoComDerivadas(t)
	m, err := materializer.New(c, fakestorage.New(fakestorage.Indexes{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range []string{
		`{"id":"A","doc":{"tally":1}}`,
		`{"id":"B","doc":{"tally":"texto"}}`,
	} {
		if r := m.Apply([]byte(ev)); r.Err != nil {
			t.Fatalf("Apply(%s): %v", ev, r.Err)
		}
	}
	for _, l := range linhas(t, "SELECT name, occurrences, typeMismatches FROM _meta.fields", c) {
		if l["name"] != "tally" {
			continue
		}
		if l["occurrences"] != "2" || l["typeMismatches"] != "1" {
			t.Errorf("occurrences=%q typeMismatches=%q, quer 2 e 1", l["occurrences"], l["typeMismatches"])
		}
	}
}

// Campo de TOPO sai com os contadores NULOS, não zerados.
//
// Zero é indistinguível de "a chave nunca apareceu", e a coluna mentiria com um
// número em vez de calar — o mesmo argumento que já mantém `parent` ausente no
// topo. Efeito colateral bem-vindo: `WHERE typeMismatches > 0` filtra o topo
// naturalmente.
func TestCampoDeTopoTemContadoresNulosENaoZerados(t *testing.T) {
	c := alvoComDerivadas(t)
	for _, l := range linhas(t, "SELECT name, parent, occurrences, typeMismatches, coercions FROM _meta.fields", c) {
		if l["parent"] != "" {
			continue // é derivada
		}
		if l["occurrences"] != "" || l["typeMismatches"] != "" || l["coercions"] != "" {
			t.Errorf("campo de topo %q saiu com occurrences=%q typeMismatches=%q coercions=%q, quer os três NULOS",
				l["name"], l["occurrences"], l["typeMismatches"], l["coercions"])
		}
	}
}

// TestCoercoesSaemPorSQLSeparadasDasDivergencias é o que impede a coerção de
// cegar o painel que o contador por coluna acabou de dar ao operador.
//
// O cenário: `tally` é declarada `int64`, e a origem passa a mandar o número
// como TEXTO. Antes da coerção de tipo isso era divergência — o valor se perdia
// e o `typeMismatches` subia, apontando a coluna. Convertendo, o valor é
// preservado e aquele contador fica em ZERO: sem uma terceira coluna, a
// inconsistência da origem sumiria do SQL sem ter sumido do dado, e a ressalva
// da documentação (`"07"` e `"7"` continuam sendo duas strings) não teria como
// ser medida em dataset nenhum.
//
// As três colunas são afirmações diferentes sobre a mesma chave, e o teste
// afirma as três de uma vez: apareceu 4 vezes, 2 delas coergidas, 1 perdida.
func TestCoercoesSaemPorSQLSeparadasDasDivergencias(t *testing.T) {
	c := alvoComDerivadas(t)
	m, err := materializer.New(c, fakestorage.New(fakestorage.Indexes{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range []string{
		`{"id":"A","doc":{"tally":1}}`,            // nativo
		`{"id":"B","doc":{"tally":"2"}}`,          // coerção: texto num int64
		`{"id":"C","doc":{"tally":"3"}}`,          // coerção
		`{"id":"D","doc":{"tally":"nem numero"}}`, // divergência: não converte
	} {
		if r := m.Apply([]byte(ev)); r.Err != nil {
			t.Fatalf("Apply(%s): %v", ev, r.Err)
		}
	}

	achou := false
	for _, l := range linhas(t, "SELECT name, occurrences, typeMismatches, coercions FROM _meta.fields", c) {
		if l["name"] != "tally" {
			continue
		}
		achou = true
		if l["occurrences"] != "4" {
			t.Errorf("occurrences = %q, quer 4", l["occurrences"])
		}
		if l["coercions"] != "2" {
			t.Errorf("coercions = %q, quer 2 (as duas strings numéricas)", l["coercions"])
		}
		if l["typeMismatches"] != "1" {
			t.Errorf("typeMismatches = %q, quer 1: só o texto que não é número se perdeu", l["typeMismatches"])
		}
	}
	if !achou {
		t.Fatal("a coluna derivada `tally` não apareceu em _meta.fields")
	}
}

// A coluna nova é projetável pelo curinga como qualquer outra, e o teste existe
// porque `SELECT *` é como a tabela é descoberta na prática — quem abre o DBeaver
// não sabe o nome de uma coluna que acabou de nascer.
func TestCoercionsApareceNoCuringaDaTabelaVirtual(t *testing.T) {
	linhas := linhas(t, "SELECT * FROM _meta.fields", alvoComDerivadas(t))
	if len(linhas) == 0 {
		t.Fatal("a tabela virtual não devolveu linha nenhuma")
	}
	if _, ok := linhas[0]["coercions"]; !ok {
		t.Errorf("`coercions` fora do SELECT *; veio %v", linhas[0])
	}
}
