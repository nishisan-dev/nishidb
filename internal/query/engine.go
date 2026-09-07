// Package query costura o pipeline SQL v1: lexer → parser → binder → planner
// → executor. É o único lugar onde as cinco camadas se conhecem; sem ele cada
// teste e cada chamador recosturariam o pipeline à mão, e a primeira
// divergência de costura viraria bug de resultado.
package query

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Store é o recorte do storage que a engine inteira consome: o que o executor
// lê MAIS a capability que o planner consulta.
//
// A união é necessária: com executor.Store sozinho o planner não teria
// Indexado e não poderia fazer o capability check — escolheria um plano
// indexado que falharia em runtime, que é justamente o que a degradação para
// full scan existe para evitar.
type Store interface {
	executor.Store
	planner.Store
}

type registro struct {
	sch     *schema.Compiled
	store   Store
	planner *planner.Planner
	// virtuais são as tabelas virtuais registradas sob este dataset, por
	// namespace e por tabela — hoje as `_counters.<nome>`. Cada uma é um
	// registro completo (schema sintético, store virtual, planner), e passa
	// pelo MESMO pipeline que um dataset de verdade.
	virtuais map[string]map[string]*registro
}

// Engine mantém os datasets registrados. Register é de configuração; Execute é
// seguro para uso concorrente.
type Engine struct {
	mu        sync.RWMutex
	datasets  map[string]*registro
	metrics   executor.Metrics
	planStats planner.Metrics
}

func NewEngine() *Engine {
	return &Engine{datasets: make(map[string]*registro)}
}

// Register associa um dataset compilado a um store. O planner é criado UMA vez
// por dataset — as capabilities do store são resolvidas aqui, fora do caminho
// de query.
func (e *Engine) Register(name string, sch *schema.Compiled, st Store) error {
	switch {
	case name == "":
		return fmt.Errorf("query: dataset sem nome")
	case sch == nil:
		return fmt.Errorf("query: dataset %q sem schema compilado", name)
	case st == nil:
		return fmt.Errorf("query: dataset %q sem store", name)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, existe := e.datasets[name]; existe {
		return fmt.Errorf("query: dataset %q já registrado", name)
	}
	e.datasets[name] = &registro{sch: sch, store: st, planner: planner.New(st, &e.planStats)}
	return nil
}

// RegistraVirtual associa `namespace.tabela`, sob o dataset já registrado, a
// um schema sintético e um store virtual.
//
// A tabela entra no MESMO pipeline de um dataset de verdade — binder, planner
// com métricas, executor com top-N e orçamento —, o que é uma diferença
// deliberada em relação ao `_meta`: as consultas do caso de uso dos contadores
// (`ORDER BY total ASC LIMIT 5` sobre milhares de grupos) são exatamente as que
// o caminho de top-N e as métricas de plano existem para servir. O `_meta`
// continua fora do registro: ele descreve o dataset alvo por consulta e não
// tem store fixo.
//
// O namespace tem de começar com "_", que é o prefixo que a compilação da spec
// reserva; o nome da tabela tem de ser o Name do schema sintético, porque é
// contra ele que o binder confere o FROM.
func (e *Engine) RegistraVirtual(dataset, namespace, tabela string, sch *schema.Compiled, st Store) error {
	switch {
	case !strings.HasPrefix(namespace, "_"):
		return fmt.Errorf("query: namespace virtual %q sem o prefixo reservado \"_\"", namespace)
	case tabela == "":
		return fmt.Errorf("query: tabela virtual sem nome em %s", namespace)
	case sch == nil:
		return fmt.Errorf("query: tabela virtual %s.%s sem schema compilado", namespace, tabela)
	case sch.Name != tabela:
		return fmt.Errorf("query: tabela virtual %s.%s com schema chamado %q; o binder confere o FROM contra o Name", namespace, tabela, sch.Name)
	case st == nil:
		return fmt.Errorf("query: tabela virtual %s.%s sem store", namespace, tabela)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	reg, ok := e.datasets[dataset]
	if !ok {
		return fmt.Errorf("query: dataset %q não registrado; a tabela virtual %s.%s não tem sob o que viver", dataset, namespace, tabela)
	}
	reg.declaraNamespace(namespace)
	if _, existe := reg.virtuais[namespace][tabela]; existe {
		return fmt.Errorf("query: tabela virtual %s.%s já registrada em %q", namespace, tabela, dataset)
	}
	reg.virtuais[namespace][tabela] = &registro{sch: sch, store: st, planner: planner.New(st, &e.planStats)}
	return nil
}

// DeclaraNamespace registra que o namespace existe sob o dataset, mesmo sem
// tabela nenhuma. É o que separa "o dataset não declara contador" de "esse
// namespace não existe": um `FROM _counters.x` num dataset sem `counters:`
// tem de responder o primeiro, e um `FROM _outro.x` o segundo.
func (e *Engine) DeclaraNamespace(dataset, namespace string) error {
	if !strings.HasPrefix(namespace, "_") {
		return fmt.Errorf("query: namespace virtual %q sem o prefixo reservado \"_\"", namespace)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	reg, ok := e.datasets[dataset]
	if !ok {
		return fmt.Errorf("query: dataset %q não registrado", dataset)
	}
	reg.declaraNamespace(namespace)
	return nil
}

func (reg *registro) declaraNamespace(namespace string) {
	if reg.virtuais == nil {
		reg.virtuais = make(map[string]map[string]*registro)
	}
	if reg.virtuais[namespace] == nil {
		reg.virtuais[namespace] = make(map[string]*registro)
	}
}

// namespacesConhecidos lista o que um `FROM _x.y` pode endereçar, para a
// mensagem de recusa: o `_meta` fixo mais os registrados neste dataset.
func (reg *registro) namespacesConhecidos() []string {
	out := []string{meta.Namespace}
	for ns := range reg.virtuais {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// tabelaVirtual resolve `namespace.tabela` sob o dataset, com as três recusas
// nomeando o que existe — ninguém é mandado procurar um dataset que nunca quis.
//
// As três são erro DE QUEM ESCREVEU A QUERY, e por isso saem tipadas por
// binder.ErrorAt: o FROM aponta para algo que não existe, que é a mesma família
// de "coluna desconhecida" — esta, na MESMA tabela virtual, já sai como BIND
// pelo renomeiaTabelaVirtual. Com fmt.Errorf cru o servidor não tinha como
// distingui-las de um bug e respondia INTERNAL/500, subindo a série de erro
// interno por conta de um nome digitado errado (issue #35).
//
// pos é a posição do nome DEPOIS do ponto (parser.SelectStmt.DatasetPos), que
// é o token ofensor: o cliente destaca a tabela, não o qualificador.
func (reg *registro) tabelaVirtual(dataset, namespace, tabela string, pos int) (*registro, error) {
	// O `_meta` existe sempre, registrado ou não: a `fields` é fixa e mora
	// no pacote meta, fora deste registro.
	porTabela, ok := reg.virtuais[namespace]
	if !ok && namespace != meta.Namespace {
		return nil, binder.ErrorAt(pos, "namespace %q não existe; existem %s", namespace, strings.Join(reg.namespacesConhecidos(), " e "))
	}
	v, ok := porTabela[tabela]
	if !ok {
		nomes := make([]string, 0, len(porTabela)+1)
		for n := range porTabela {
			nomes = append(nomes, n)
		}
		if namespace == meta.Namespace {
			nomes = append(nomes, meta.TabelaFields)
		}
		if len(nomes) == 0 {
			return nil, binder.ErrorAt(pos, "o dataset %q não declara tabela nenhuma em %s", dataset, namespace)
		}
		sort.Strings(nomes)
		return nil, binder.ErrorAt(pos, "%s não tem tabela %q no dataset %q; existem: %s", namespace, tabela, dataset, strings.Join(nomes, ", "))
	}
	return v, nil
}

// renomeiaTabelaVirtual troca o nome interno do schema sintético pelo nome
// qualificado que o usuário escreveu nas mensagens do binder — o mesmo
// remédio do `_meta`: "coluna desconhecida em x" mandaria procurar uma tabela
// chamada x, que não existe em lugar nenhum.
func renomeiaTabelaVirtual(err error, namespace, tabela string) error {
	var be *binder.Error
	if !errors.As(err, &be) {
		return err
	}
	qualificado := namespace + "." + tabela
	msg := strings.ReplaceAll(be.Error(), `"`+tabela+`"`, `"`+qualificado+`"`)
	if !strings.Contains(msg, qualificado) {
		msg += " (em " + qualificado + ")"
	}
	return &erroDeTabelaVirtual{msg: msg, err: err}
}

type erroDeTabelaVirtual struct {
	msg string
	err error
}

func (e *erroDeTabelaVirtual) Error() string { return e.msg }
func (e *erroDeTabelaVirtual) Unwrap() error { return e.err }

// Execute compila e abre a query. Erros de sintaxe e de bind saem AQUI, antes
// de qualquer leitura de dado.
//
// O dataset vem por argumento E dentro do SQL (`FROM x`): o binder exige que
// os dois batam, senão a query rodaria contra um schema que não é o que o
// chamador pediu.
func (e *Engine) Execute(ctx context.Context, dataset, sql string, params []types.Value) (executor.RowIterator, error) {
	e.mu.RLock()
	reg, ok := e.datasets[dataset]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("query: dataset %q não registrado", dataset)
	}

	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	// O namespace reservado desvia para a tabela virtual, que descreve o
	// dataset nomeado NA REQUISIÇÃO. O desvio é aqui, e não no servidor,
	// porque este é o ponto por onde o servidor e o CLI passam os dois — pô-lo
	// numa camada acima faria a introspecção existir só para quem falasse HTTP.
	if meta.Reconhece(stmt) {
		// `_meta.fields` é a tabela fixa do pacote meta, que descreve o
		// dataset pela definição. As DEMAIS tabelas do `_meta` (`counters`) e
		// os outros namespaces reservados são tabelas REGISTRADAS sob o
		// dataset, e correm pelo mesmo pipeline que ele.
		if stmt.Namespace == meta.Namespace && stmt.Dataset == meta.TabelaFields {
			caps, _ := reg.store.(meta.Capacidades)
			return meta.Abre(ctx, stmt, reg.sch, caps, params)
		}
		e.mu.RLock()
		v, err := reg.tabelaVirtual(dataset, stmt.Namespace, stmt.Dataset, stmt.DatasetPos)
		e.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		bq, err := binder.Bind(stmt, v.sch, params)
		if err != nil {
			return nil, renomeiaTabelaVirtual(err, stmt.Namespace, stmt.Dataset)
		}
		plan := v.planner.Plan(bq)
		return executor.Open(ctx, bq, plan, executor.Options{Store: v.store, Metrics: &e.metrics})
	}
	bq, err := binder.Bind(stmt, reg.sch, params)
	if err != nil {
		return nil, err
	}
	plan := reg.planner.Plan(bq)
	return executor.Open(ctx, bq, plan, executor.Options{Store: reg.store, Metrics: &e.metrics})
}

// Metrics são os contadores do executor (diagnóstico).
func (e *Engine) Metrics() *executor.Metrics { return &e.metrics }

// PlanMetrics são os contadores de plano, onde vive a métrica normativa
// query.full_scan (§64).
func (e *Engine) PlanMetrics() *planner.Metrics { return &e.planStats }
