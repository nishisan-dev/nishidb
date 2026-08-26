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

// TabelaFields é a única tabela virtual da v1.
const TabelaFields = "fields"

// Capacidades é o que a tabela precisa saber do storage do dataset alvo.
// Interface, e não o *storage.Store, porque a tabela não lê dado nenhum: ela
// só quer saber se um índice EXISTE. nil é aceito e vale "não sei", que
// reporta indexed=false.
//
// Os DOIS motores estão aqui, e é por isso que a coluna `indexed` volta a
// significar alguma coisa. Enquanto só havia HasBitmap, uma coluna com
// `index: hash` saía com indexed=false — o que era verdade (não havia motor) e
// virou mentira no instante em que o motor entrou. Um Store que não implemente
// HasHash continua sendo aceito: a asserção de tipo em fonte.record trata a
// ausência como "não tem".
type Capacidades interface {
	HasBitmap(types.FieldID) bool
	HasHash(types.FieldID) bool
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

// Abre executa o statement contra a tabela virtual que descreve alvo.
func Abre(ctx context.Context, st *parser.SelectStmt, alvo *schema.Compiled, caps Capacidades, params []types.Value) (executor.RowIterator, error) {
	if alvo == nil {
		return nil, fmt.Errorf("%s: dataset alvo não informado", Namespace)
	}
	if st.Namespace != Namespace {
		return nil, fmt.Errorf("namespace %q não existe; o único reservado é %q", st.Namespace, Namespace)
	}
	if st.Dataset != TabelaFields {
		return nil, fmt.Errorf("%s não tem tabela %q; existe %s.%s", Namespace, st.Dataset, Namespace, TabelaFields)
	}

	sch, err := schemaDaTabela()
	if err != nil {
		return nil, err
	}
	bq, err := binder.Bind(st, sch, params)
	if err != nil {
		return nil, renomeia(err)
	}
	store := novaFonte(alvo, caps, sch)
	plan := planner.New(store, nil).Plan(bq)
	return executor.Open(ctx, bq, plan, executor.Options{Store: store})
}

// renomeia troca o nome interno do schema sintético pelo nome que o usuário
// escreveu. Sem isto, "coluna desconhecida em fields" mandaria procurar uma
// tabela chamada `fields`, que não existe em lugar nenhum.
func renomeia(err error) error {
	var be *binder.Error
	if !errorsAs(err, &be) {
		return err
	}
	msg := strings.ReplaceAll(be.Error(), `"`+TabelaFields+`"`, `"`+Namespace+`.`+TabelaFields+`"`)
	if !strings.Contains(msg, Namespace+"."+TabelaFields) {
		msg += " (em " + Namespace + "." + TabelaFields + ")"
	}
	return &erroDaTabela{msg: msg, err: err}
}

type erroDaTabela struct {
	msg string
	err error
}

func (e *erroDaTabela) Error() string { return e.msg }
func (e *erroDaTabela) Unwrap() error { return e.err }

// especDaTabela é a spec da tabela virtual, escrita no MESMO YAML de um
// dataset de verdade. Reusar o compilador em vez de montar um *schema.Compiled
// à mão é o que garante que a tabela obedeça às mesmas regras — tipos, chave,
// ordem determinística de FieldID — que qualquer outro dataset.
//
// `occurrences` e `typeMismatches` são DUAS colunas cruas e não uma razão,
// porque a razão não compõe: `2.315 de 2.383` e `93 de 3.334` são problemas de
// ordens diferentes, e o numerador sozinho não os distingue. A divisão fica com
// quem consulta. As duas são NULL para campo de topo — ver o record().
//
// `type` e `auto` são DUAS colunas de propósito, e reusam o vocabulário que o
// /v1/metadata já publica. Dizer só `auto` jogaria fora a informação que se foi
// buscar ali: campo autodescoberto TEM tipo fixado depois da primeira
// observação.
const especDaTabela = `version: 1
name: ` + TabelaFields + `
materialization:
  key:
    field: name
schema:
  include: [name, type, auto, index, indexed, parent, id, occurrences, typeMismatches]
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
`

var (
	umaVez     sync.Once
	schCache   *schema.Compiled
	schErr     error
	colunasIDs map[string]types.FieldID
)

func schemaDaTabela() (*schema.Compiled, error) {
	umaVez.Do(func() {
		s, err := schema.Parse([]byte(especDaTabela), schema.DefaultsPadrao)
		if err != nil {
			schErr = fmt.Errorf("%s: spec da tabela virtual inválida: %w", Namespace, err)
			return
		}
		c, err := s.Compile(novoDicionario())
		if err != nil {
			schErr = fmt.Errorf("%s: compilando a tabela virtual: %w", Namespace, err)
			return
		}
		schCache = c
		colunasIDs = map[string]types.FieldID{}
		for _, f := range c.FieldsByID() {
			colunasIDs[f.Name] = f.ID
		}
	})
	return schCache, schErr
}
