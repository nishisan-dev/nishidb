// Package meta serve as tabelas virtuais do namespace `_meta` — hoje só
// `_meta.fields`, que responde "o que este dataset tem" por SQL.
//
// Por SQL, e não por rota HTTP nova: uma tabela não exige suporte em cliente
// nenhum. DBeaver, CLI e driver já sabem executar SELECT e desenhar o
// resultado; uma rota precisaria de código em cada um.
//
// A tabela é um Store VIRTUAL por trás do mesmo binder, planner e executor de
// qualquer consulta. É isso que lhe dá WHERE, LIMIT e projeção sem gramática
// nova nem caminho de execução paralelo — e o que garante que ela recuse
// coluna inexistente com a mesma mensagem que o resto do produto.
//
// A resposta é ESTADO CORRENTE, não declaração: para campo autodescoberto o
// tipo se fixa na primeira observação, então duas execuções em momentos
// diferentes podem devolver conjuntos diferentes. É a mesma ressalva que o
// `SELECT *` já carrega.
package meta

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Namespace é o qualificador reservado. Nome de dataset não pode começar com
// "_" justamente para que ele nunca colida — a recusa vive na compilação da
// spec.
const Namespace = "_meta"

// TableFields é a única tabela virtual da v1.
const TableFields = "fields"

// Capacidades é o que a tabela precisa saber do storage do dataset alvo.
// Interface, e não o *storage.Store, porque a tabela não lê dado nenhum: ela
// só quer saber se um índice EXISTE. nil é aceito e vale "não sei", que
// reporta indexed=false.
//
// UM método, e não mais o par HasBitmap/HasHash somado em source.record: existe
// um motor de índice só (spec §3.1), e a soma de duas respostas idênticas era o
// resto do dia em que a coluna `indexed` mentia — enquanto só havia HasBitmap,
// uma coluna com `index: hash` saía com indexed=false, o que era verdade (não
// havia motor) e virou mentira no instante em que o motor entrou.
type Capacidades interface {
	Indexed(types.FieldID) bool
}

// Reconhece informa se o statement endereça o namespace reservado.
//
// Ele aceita QUALQUER namespace começando com "_", e não só o "_meta", para
// que um `FROM _outro.coisa` seja recusado aqui, nomeando o que existe, em
// vez de virar "dataset desconhecido" — que mandaria o usuário procurar um
// dataset que ele nunca quis.
func Reconhece(st *parser.SelectStmt) bool {
	return st != nil && strings.HasPrefix(st.Namespace, "_")
}

// Open executa o statement contra a tabela virtual que descreve alvo.
func Open(ctx context.Context, st *parser.SelectStmt, target *schema.Compiled, caps Capacidades, params []types.Value) (executor.RowIterator, error) {
	if target == nil {
		return nil, fmt.Errorf("%s: dataset alvo não informado", Namespace)
	}
	// As duas recusas abaixo são erro de quem escreveu a query — o FROM aponta
	// para o que não existe —, e por isso saem tipadas por binder.ErrorAt, como
	// as do roteamento no motor (internal/query/engine.go). Erro sem tipo aqui
	// chegaria ao servidor como INTERNAL/500 (issue #35). O `alvo == nil`
	// acima NÃO é tipado de propósito: aquilo é chamador que não passou o
	// dataset, e 500 é a resposta honesta.
	if st.Namespace != Namespace {
		// Quem roteia os OUTROS namespaces reservados (`_counters`) é o motor,
		// pelo registro de tabelas virtuais; chegar aqui com outro namespace
		// é chamador que não passou pelo motor.
		return nil, binder.ErrorAt(st.DatasetPos, "namespace %q não existe; este ponto serve só %q (os demais namespaces são registrados no motor)", st.Namespace, Namespace)
	}
	if st.Dataset != TableFields {
		return nil, binder.ErrorAt(st.DatasetPos, "%s não tem tabela %q; existe %s.%s", Namespace, st.Dataset, Namespace, TableFields)
	}

	sch, err := schemaOfTable()
	if err != nil {
		return nil, err
	}
	bq, err := binder.Bind(st, sch, params)
	if err != nil {
		return nil, rename(err)
	}
	store := newSource(target, caps, sch)
	plan := planner.New(store, nil).Plan(bq)
	return executor.Open(ctx, bq, plan, executor.Options{Store: store})
}

// rename troca o nome interno do schema sintético pelo nome que o usuário
// escreveu. Sem isto, "coluna desconhecida em fields" mandaria procurar uma
// tabela chamada `fields`, que não existe em lugar nenhum.
func rename(err error) error {
	var be *binder.Error
	if !errorsAs(err, &be) {
		return err
	}
	msg := strings.ReplaceAll(be.Error(), `"`+TableFields+`"`, `"`+Namespace+`.`+TableFields+`"`)
	if !strings.Contains(msg, Namespace+"."+TableFields) {
		msg += " (em " + Namespace + "." + TableFields + ")"
	}
	return &errOfTable{msg: msg, err: err}
}

type errOfTable struct {
	msg string
	err error
}

func (e *errOfTable) Error() string { return e.msg }
func (e *errOfTable) Unwrap() error { return e.err }

// especOfTable é a spec da tabela virtual, escrita no MESMO YAML de um
// dataset de verdade. Reusar o compilador em vez de montar um *schema.Compiled
// à mão é o que garante que a tabela obedeça às mesmas regras — tipos, chave,
// ordem determinística de FieldID — que qualquer outro dataset.
//
// `occurrences`, `typeMismatches` e `coercions` são TRÊS colunas cruas e não
// uma razão, porque a razão não compõe: `2.315 de 2.383` e `93 de 3.334` são
// problemas de ordens diferentes, e o numerador sozinho não os distingue. A
// divisão fica com quem consulta. As três são NULL para campo de topo — ver o
// record().
//
// `coercions` é uma coluna à parte de `typeMismatches`, e não uma soma com ela,
// porque as duas afirmações pedem remédios diferentes: divergiu é "o valor se
// perdeu, a coluna está incompleta"; coergiu é "o valor está lá, mas a origem
// escreve a mesma coisa de duas formas e um único literal não a encontra
// inteira". Ela também é o que impede a coerção de cegar o diagnóstico: sem
// `coercions`, um número passando a entrar numa coluna de texto zeraria o
// `typeMismatches` daquela coluna e a inconsistência da origem sumiria do painel
// sem ter sumido do dado.
//
// `type` e `auto` são DUAS colunas de propósito, e reusam o vocabulário que o
// /v1/metadata já publica. Dizer só `auto` jogaria fora a informação que se foi
// buscar ali: campo autodescoberto TEM tipo fixado depois da primeira
// observação.
const especOfTable = `version: 1
name: ` + TableFields + `
schemaName: ` + schema.SyntheticSchemaName + `
materialization:
  key:
    field: name
schema:
  include: [name, type, auto, index, indexed, parent, id, occurrences, typeMismatches, coercions]
  fields:
    name:
      type: string
    type:
      type: string
    auto:
      type: boolean
    index:
      type: string
    indexed:
      type: boolean
    parent:
      type: string
    id:
      type: int64
    occurrences:
      type: int64
    typeMismatches:
      type: int64
    coercions:
      type: int64
`

var (
	oneVez     sync.Once
	schCache   *schema.Compiled
	schErr     error
	columnsIDs map[string]types.FieldID
)

// SchemaOfFields devolve o schema compilado da `_meta.fields` — o MESMO que o
// Open usa para bindar a consulta.
//
// Exportado para o catálogo do motor, que precisa das colunas desta tabela e
// não tem como alcançá-las: a `fields` é fixa deste pacote e não está no
// registro de tabelas virtuais. Sem isto o catálogo redeclararia a lista de
// colunas e divergiria dela na primeira mudança daqui.
func SchemaOfFields() (*schema.Compiled, error) { return schemaOfTable() }

func schemaOfTable() (*schema.Compiled, error) {
	oneVez.Do(func() {
		s, err := schema.Parse([]byte(especOfTable), schema.BuiltinDefaults)
		if err != nil {
			schErr = fmt.Errorf("%s: spec da tabela virtual inválida: %w", Namespace, err)
			return
		}
		c, err := s.Compile(newDicionario())
		if err != nil {
			schErr = fmt.Errorf("%s: compilando a tabela virtual: %w", Namespace, err)
			return
		}
		schCache = c
		columnsIDs = map[string]types.FieldID{}
		for _, f := range c.FieldsByID() {
			columnsIDs[f.Name] = f.ID
		}
	})
	return schCache, schErr
}
