package query_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// O SEGUNDO dataset do motor. O harness sobe um só (`radius_sessions`, no
// schema `telecom`), e com um dataset só nenhum destes testes distingue
// "resolveu o schema" de "usou o dataset da requisição": as duas leituras dão
// a mesma resposta.
const (
	otherDataset    = "eventos"
	otherSchemaName = "rede"
	otherCounter    = "por_regiao"
)

// registerDatasetOfTest registra um dataset BASE sintético, com schemaName
// próprio e duas linhas, sobre o dublê de storage.
//
// As linhas existem para que o teste possa distinguir as duas tabelas base
// pelo CONTEÚDO: um `FROM rede.eventos` que respondesse zero linhas passaria
// igual se o motor tivesse aberto a base errada e ela estivesse vazia — e a
// base do harness está.
func registerDatasetOfTest(t *testing.T, eng *query.Engine, name, schemaName string) {
	t.Helper()
	spec, err := schema.Parse([]byte(`version: 1
name: `+name+`
schemaName: `+schemaName+`
materialization:
  key:
    field: id
schema:
  include: [id, regiao]
  fields:
    id:
      type: string
    regiao:
      type: string
`), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	st := fakestorage.New(fakestorage.Indexes{})
	for i, regiao := range []string{"NORTE", "SUL"} {
		var p []byte
		for _, col := range []struct {
			name string
			v    types.Value
		}{
			{"id", types.Value{Type: types.TypeString, Bytes: []byte(regiao)}},
			{"regiao", types.Value{Type: types.TypeString, Bytes: []byte(regiao)}},
		} {
			f, ok := sc.FieldByName([]byte(col.name))
			if !ok {
				t.Fatal(col.name)
			}
			if p, err = encoding.AppendField(p, f.ID, col.v); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := st.Put([]byte(regiao), int64(i+1), 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Register(name, sc, st); err != nil {
		t.Fatal(err)
	}
}

// engineWithTwoDatasets monta o retrato mínimo em que o roteamento por schema
// pode ser PROVADO: dois datasets, schemas distintos, e um contador em cada um
// com nome que só existe do seu lado.
//
// Os nomes dos contadores não se repetem de propósito: é isso que faz o atalho
// `_counters.<nome>` responder "essa tabela não é deste dataset" quando o
// namespace reservado é usado através do handle errado — a asserção que separa
// "o atalho continua sendo do dataset da requisição" de "o atalho virou busca
// global".
func engineWithTwoDatasets(t *testing.T) *query.Engine {
	t.Helper()
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, dataset, schema.CountersNamespace, "por_olt")
	registerDatasetOfTest(t, h.engine, otherDataset, otherSchemaName)
	tableVirtualOfTest(t, h.engine, otherDataset, schema.CountersNamespace, otherCounter)
	return h.engine
}

// queryOf executa contra o handle DADO, e não contra o do harness: o handle é
// metade do que estes testes medem.
func queryOf(t *testing.T, eng *query.Engine, handle, sql string) ([]row, error) {
	t.Helper()
	it, err := eng.Execute(context.Background(), handle, sql, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	var out []row
	for it.Next() {
		l := make(row, it.Row().Len())
		for _, c := range it.Row().Cols() {
			l[c.Name] = render(c)
		}
		out = append(out, l)
	}
	return out, it.Err()
}

func queryOfOK(t *testing.T, eng *query.Engine, handle, sql string) []row {
	t.Helper()
	rows, err := queryOf(t, eng, handle, sql)
	if err != nil {
		t.Fatalf("query %q no handle %q: %v", sql, handle, err)
	}
	return rows
}

// TestSchemaQualificadoResolveODatasetCerto é o ganho que a mudança existe
// para entregar. O par publicado pelo catálogo — `(telecom, por_olt)` — passa
// a ser ENDEREÇO: `FROM telecom.por_olt` abre a tabela do dataset dono do
// schema `telecom`, venha a requisição pelo handle que vier.
//
// Antes disto o qualificador não endereçava nada: `FROM _counters.por_olt` não
// nomeia dataset nenhum e caía sempre no handle da conexão, de modo que, com
// dois datasets no servidor, as views de um deles eram inalcançáveis da
// conexão do outro. E a tabela base do outro dataset era inalcançável de
// qualquer forma — o binder recusava o nome por não bater com o handle.
//
// As duas formas são cobradas porque o roteamento as abre por caminhos
// diferentes: a base pelo `record` do dataset, a view pelo registro de tabelas
// virtuais. Uma só passando esconderia metade da fiação.
func TestSchemaQualificadoResolveODatasetCerto(t *testing.T) {
	eng := engineWithTwoDatasets(t)

	// View do outro dataset, pelo handle deste.
	rows := queryOfOK(t, eng, otherDataset, "SELECT olt, total FROM telecom.por_olt ORDER BY total ASC")
	if got := strings.Join(chaves(rows, "olt"), ","); got != "BR_SPOMO_OLT14,PRCTA_G1M10" {
		t.Fatalf("telecom.por_olt pelo handle %q = %v", otherDataset, got)
	}

	// Tabela base do outro dataset, pelo handle deste. O conteúdo é o que
	// prova que a base aberta foi a de `rede`: a do harness está vazia.
	rows = queryOfOK(t, eng, dataset, "SELECT id, regiao FROM "+otherSchemaName+"."+otherDataset+" ORDER BY id ASC")
	if got := strings.Join(chaves(rows, "regiao"), ","); got != "NORTE,SUL" {
		t.Fatalf("%s.%s pelo handle %q = %v", otherSchemaName, otherDataset, dataset, got)
	}

	// E a simétrica: a view de `rede`, pelo handle de `telecom`.
	rows = queryOfOK(t, eng, dataset, "SELECT groupKey FROM "+otherSchemaName+"."+otherCounter)
	if len(rows) != 2 {
		t.Fatalf("%s.%s pelo handle %q = %v", otherSchemaName, otherCounter, dataset, rows)
	}

	// O schema do PRÓPRIO dataset também é endereço — seria absurdo que só o
	// alheio resolvesse, e é a forma que o DBeaver gera na conexão comum.
	rows = queryOfOK(t, eng, dataset, "SELECT olt FROM telecom.por_olt")
	if len(rows) != 2 {
		t.Fatalf("telecom.por_olt pelo handle do próprio dataset = %v", rows)
	}
}

// TestAtalhoSemSchemaUsaODatasetDaRequisicao: o namespace reservado continua
// significando "o dataset da requisição", e não vira busca global por tabela.
//
// Os dois lados são cobrados: o atalho ALCANÇA a view do próprio dataset e NÃO
// alcança a do outro. Só o primeiro passaria igual se o atalho tivesse virado
// busca global, que é exatamente o erro que esta mudança poderia introduzir.
func TestAtalhoSemSchemaUsaODatasetDaRequisicao(t *testing.T) {
	eng := engineWithTwoDatasets(t)

	rows := queryOfOK(t, eng, otherDataset, "SELECT groupKey FROM "+schema.CountersNamespace+"."+otherCounter)
	if len(rows) != 2 {
		t.Fatalf("o atalho tem de alcançar a view do próprio dataset: %v", rows)
	}

	_, err := queryOf(t, eng, otherDataset, "SELECT olt FROM "+schema.CountersNamespace+".por_olt")
	if err == nil {
		t.Fatal("o atalho não pode alcançar a view de outro dataset")
	}
	if !strings.Contains(err.Error(), otherDataset) || !strings.Contains(err.Error(), otherCounter) {
		t.Fatalf("a recusa tem de nomear o dataset da requisição e as views que ele tem: %v", err)
	}

	// A `_meta.fields` é o outro atalho, e descreve o dataset da requisição
	// pela definição. As colunas do handle é que têm de sair.
	names := chaves(queryOfOK(t, eng, otherDataset, "SELECT name FROM _meta.fields ORDER BY name ASC"), "name")
	if got := strings.Join(names, ","); got != "id,regiao" {
		t.Fatalf("_meta.fields no handle %q = %v", otherDataset, got)
	}
}

// TestSchemaDesconhecidoListaOsExistentes: recusa que não nomeia o que existe
// manda o operador adivinhar. O par recusado é o do catálogo, então a lista
// tem de ser a dos schemas que o catálogo publica.
//
// A recusa é erro de QUEM ESCREVEU A QUERY, e sai tipada como tal: sem o tipo,
// o servidor não a distingue de um bug e responde INTERNAL/500 a quem só
// digitou um nome errado (issue #35). A posição aponta o nome DEPOIS do ponto,
// como nas recusas de namespace.
func TestSchemaDesconhecidoListaOsExistentes(t *testing.T) {
	eng := engineWithTwoDatasets(t)

	const sql = "SELECT a FROM nao_existe.coisa"
	_, err := queryOf(t, eng, dataset, sql)
	if err == nil {
		t.Fatal("schema desconhecido tem de ser recusado")
	}
	for _, quero := range []string{"nao_existe", "telecom", otherSchemaName} {
		if !strings.Contains(err.Error(), quero) {
			t.Errorf("a recusa tem de nomear o schema pedido e listar os que existem, e não cita %q: %v", quero, err)
		}
	}
	assertRefusalOfBind(t, err, sql, "coisa")
}

// TestSchemaConhecidoSemATabelaListaAsDoSchema: o schema existe e a tabela
// não. É recusa DIFERENTE da anterior — quem errou o nome da tabela não tem o
// que fazer com a lista de schemas —, e por isso lista as tabelas DAQUELE
// schema.
func TestSchemaConhecidoSemATabelaListaAsDoSchema(t *testing.T) {
	eng := engineWithTwoDatasets(t)

	const sql = "SELECT a FROM telecom.nao_existe"
	_, err := queryOf(t, eng, dataset, sql)
	if err == nil {
		t.Fatal("tabela inexistente no schema tem de ser recusada")
	}
	for _, quero := range []string{"nao_existe", dataset, "por_olt"} {
		if !strings.Contains(err.Error(), quero) {
			t.Errorf("a recusa tem de listar as tabelas do schema, e não cita %q: %v", quero, err)
		}
	}
	// A lista é do schema PEDIDO. Vazar as tabelas do outro schema mandaria o
	// operador escrever um FROM que não resolve.
	if strings.Contains(err.Error(), otherCounter) {
		t.Errorf("a lista tem de ser só do schema pedido: %v", err)
	}
	assertRefusalOfBind(t, err, sql, "nao_existe")
}

// TestQualificadorQueNaoEhSchemaEhRecusado guarda a metade que um teste de
// caminho feliz não vê: o qualificador passa a ser LIDO, e não mais ignorado.
//
// Até aqui `FROM qualquercoisa.radius_sessions` era aceito e idêntico a
// `FROM radius_sessions` — o roteamento só desviava namespace com prefixo `_`
// e o binder conferia apenas o nome depois do ponto. Um motor que continuasse
// ignorando o qualificador passaria em todo teste de `FROM telecom.x` do
// arquivo, porque `telecom` é o schema do dataset do handle; é esta recusa que
// distingue "resolveu o schema" de "descartou o prefixo".
func TestQualificadorQueNaoEhSchemaEhRecusado(t *testing.T) {
	eng := engineWithTwoDatasets(t)

	const sql = "SELECT a FROM qualquercoisa." + dataset
	_, err := queryOf(t, eng, dataset, sql)
	if err == nil {
		t.Fatal("qualificador que não é schema declarado não pode abrir a tabela base")
	}
	if !strings.Contains(err.Error(), "qualquercoisa") {
		t.Fatalf("a recusa tem de nomear o qualificador recusado: %v", err)
	}
	assertRefusalOfBind(t, err, sql, dataset)
}

// assertRefusalOfBind cobra o contrato de erro das recusas de roteamento: da
// família do bind (senão o servidor responde INTERNAL/500) e com a posição
// apontando o token ofensor, que é o nome DEPOIS do ponto.
func assertRefusalOfBind(t *testing.T, err error, sql, ofensor string) {
	t.Helper()
	if !errors.Is(err, binder.ErrBind) {
		t.Fatalf("a recusa tem de ser da família do bind: %v", err)
	}
	var be *binder.Error
	if !errors.As(err, &be) {
		t.Fatalf("a recusa tem de carregar a posição para o cliente destacar o trecho: %v", err)
	}
	if be.Pos < 0 || be.Pos > len(sql) || !strings.HasPrefix(sql[be.Pos:], ofensor) {
		t.Fatalf("a posição %d tem de apontar %q em %q", be.Pos, ofensor, sql)
	}
}
