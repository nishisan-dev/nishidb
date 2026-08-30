// Package query costura o pipeline SQL v1: lexer → parser → binder → planner
// → executor. É o único lugar onde as cinco camadas se conhecem; sem ele cada
// teste e cada chamador recosturariam o pipeline à mão, e a primeira
// divergência de costura viraria bug de resultado.
package query

import (
	"context"
	"fmt"
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
		caps, _ := reg.store.(meta.Capacidades)
		return meta.Abre(ctx, stmt, reg.sch, caps, params)
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
