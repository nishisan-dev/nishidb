package binder_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// compilaYAML monta um Compiled a partir de um YAML próprio — os helpers do
// pacote usam a spec compartilhada, que não declara extract.
func compilaYAML(t *testing.T, yml string) *schema.Compiled {
	t.Helper()
	spec, err := schema.Parse([]byte(yml), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func bindComYAML(t *testing.T, yml, sql string) (*binder.BoundQuery, error) {
	t.Helper()
	st, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	return binder.Bind(st, compilaYAML(t, yml), nil)
}

const yamlAninhado = `version: 1
name: eventos
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  maxNestedCols: 2
  fields:
    id:
      type: string
    doc:
      type: json
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
        - path: alarme.id
          as: alarme_id
          type: string
    outro:
      type: json
      extract:
        - path: x
          type: string
`

// `pai.*` expande as colunas extraídas daquele pai, em ordem de FieldID — a
// mesma do SELECT *, pela mesma razão: a projeção precisa ser estável entre
// execuções.
func TestCuringaQualificadoExpandeAsDerivadas(t *testing.T) {
	bq, err := bindComYAML(t, yamlAninhado, "SELECT id, doc.* FROM eventos")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	quer := []string{"id", "probe_ip", "alarme_id"}
	if len(bq.Columns) != len(quer) {
		t.Fatalf("colunas = %d, quer %d", len(bq.Columns), len(quer))
	}
	for i, n := range quer {
		if bq.Columns[i].Name != n {
			t.Errorf("coluna %d = %q, quer %q", i, bq.Columns[i].Name, n)
		}
	}
}

// A coluna derivada também é endereçável pelo CAMINHO, que é como ela aparece
// dentro do documento — inclusive o caminho aninhado.
func TestCaminhoQualificadoResolveAColuna(t *testing.T) {
	bq, err := bindComYAML(t, yamlAninhado, "SELECT doc.probeIpNode FROM eventos")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if len(bq.Columns) != 1 || bq.Columns[0].Name != "doc.probeIpNode" {
		t.Fatalf("colunas = %+v", bq.Columns)
	}
	if bq.Columns[0].Field.Name != "probe_ip" {
		t.Errorf("resolveu para %q, quer probe_ip", bq.Columns[0].Field.Name)
	}
}

func TestRecusasDoQualificado(t *testing.T) {
	casos := []struct {
		nome string
		sql  string
		quer string
	}{
		{"pai inexistente", "SELECT nada.* FROM eventos", "nada"},
		{"pai sem extracoes", "SELECT id.* FROM eventos", "não declara extract"},
		{"caminho inexistente", "SELECT doc.naoExiste FROM eventos", "naoExiste"},
		{"acima do teto", "SELECT doc.*, outro.* FROM eventos", "maxNestedCols"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			_, err := bindComYAML(t, yamlAninhado, tc.sql)
			if err == nil {
				t.Fatal("esperava recusa")
			}
			if !strings.Contains(err.Error(), tc.quer) {
				t.Errorf("erro = %q, quer conter %q", err, tc.quer)
			}
		})
	}
}

// Ao estourar o teto, RECUSAR nomeando o limite — nunca truncar. Truncar
// entregaria um resultado com colunas faltando e cara de completo, que é o
// defeito que o produto fecha em toda parte. O erro é acionável: pede-se as
// colunas pelo nome.
func TestTetoDeColunasAninhadasRecusaEmVezDeTruncar(t *testing.T) {
	_, err := bindComYAML(t, yamlAninhado, "SELECT doc.*, outro.* FROM eventos")
	if err == nil {
		t.Fatal("esperava recusa")
	}
	for _, quer := range []string{"3", "maxNestedCols", "2"} {
		if !strings.Contains(err.Error(), quer) {
			t.Errorf("erro = %q, quer conter %q (pedido, chave e teto)", err, quer)
		}
	}
}

// O teto vale para TODA coluna aninhada da projeção, e não só para a
// expansão: misturar `pai.*` com `pai.caminho` somava sem conferir, e três
// colunas passavam sob um teto de duas. Foi a verificação com o binário que
// pegou — a expansão sozinha respeitava o limite, então nenhum teste do
// caminho feliz acusava.
func TestTetoContaTambemOCaminhoQualificado(t *testing.T) {
	_, err := bindComYAML(t, yamlAninhado, "SELECT doc.*, doc.probeIpNode FROM eventos")
	if err == nil {
		t.Fatal("três colunas aninhadas passaram sob um teto de duas")
	}
	if !strings.Contains(err.Error(), "maxNestedCols") {
		t.Errorf("erro = %q, quer nomear o teto", err)
	}
}

const yamlSoDescobre = `version: 1
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
`

// Pai que DESCOBRE mas ainda não viu nenhuma chave: a recusa não pode dizer
// "não declara extract", porque ele declara descoberta — a mensagem mandaria
// o operador escrever um extract que ele não precisa. O que falta é evento.
func TestPaiQueDescobreEAindaNaoViuNadaDizOQueFalta(t *testing.T) {
	_, err := bindComYAML(t, yamlSoDescobre, "SELECT doc.* FROM eventos")
	if err == nil {
		t.Fatal("esperava recusa: nada foi descoberto ainda")
	}
	if strings.Contains(err.Error(), "não declara extract") {
		t.Errorf("erro = %q; ele DECLARA descoberta, a mensagem manda corrigir a coisa errada", err)
	}
	if !strings.Contains(err.Error(), "eventos") {
		t.Errorf("erro = %q, quer dizer que as colunas aparecem conforme os eventos chegam", err)
	}
}

// Filtrar por caminho tem de funcionar como projetar por caminho. Sem isso a
// gramática fica torta: `SELECT doc.probeIpNode` vale e
// `WHERE doc.probeIpNode = 'x'` não, para a mesma coluna.
func TestWhereAceitaCaminhoQualificado(t *testing.T) {
	bq, err := bindComYAML(t, yamlAninhado, "SELECT id FROM eventos WHERE doc.probeIpNode = '10.0.0.1'")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if bq.Where == nil {
		t.Fatal("WHERE não ligou")
	}
}

func TestWhereQualificadoRecusaCaminhoInexistente(t *testing.T) {
	_, err := bindComYAML(t, yamlAninhado, "SELECT id FROM eventos WHERE doc.naoExiste = 'x'")
	if err == nil {
		t.Fatal("esperava recusa")
	}
	if !strings.Contains(err.Error(), "naoExiste") {
		t.Errorf("erro = %q, quer nomear o caminho", err)
	}
}

// IS NULL também: a coluna aninhada é ausente na maioria das linhas, então
// perguntar por presença é o filtro mais natural sobre ela.
func TestWhereQualificadoAceitaIsNull(t *testing.T) {
	if _, err := bindComYAML(t, yamlAninhado, "SELECT id FROM eventos WHERE doc.probeIpNode IS NULL"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
}
