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
// Indexed e não poderia fazer o capability check — escolheria um plano
// indexado que falharia em runtime, que é justamente o que a degradação para
// full scan existe para evitar.
type Store interface {
	executor.Store
	planner.Store
}

type record struct {
	sch     *schema.Compiled
	store   Store
	planner *planner.Planner
	// virtualTables são as tabelas virtuais registradas sob este dataset, por
	// namespace e por tabela — as `_counters.<nome>` mais as do `_meta` que
	// dependem do storage (`counters`, `kafka`). Cada uma é um registro
	// completo (schema sintético, store virtual, planner), e passa pelo MESMO
	// pipeline que um dataset de verdade.
	virtualTables map[string]map[string]*record
}

// Engine mantém os datasets registrados. Register é de configuração; Execute é
// seguro para uso concorrente.
type Engine struct {
	mu        sync.RWMutex
	datasets  map[string]*record
	metrics   executor.Metrics
	planStats planner.Metrics
}

// NewEngine ...
func NewEngine() *Engine {
	return &Engine{datasets: make(map[string]*record)}
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
	if _, exists := e.datasets[name]; exists {
		return fmt.Errorf("query: dataset %q já registrado", name)
	}
	e.datasets[name] = &record{sch: sch, store: st, planner: planner.New(st, &e.planStats)}
	return nil
}

// RegisterVirtual associa `namespace.tabela`, sob o dataset já registrado, a
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
func (e *Engine) RegisterVirtual(dataset, namespace, table string, sch *schema.Compiled, st Store) error {
	switch {
	case !strings.HasPrefix(namespace, "_"):
		return fmt.Errorf("query: namespace virtual %q sem o prefixo reservado \"_\"", namespace)
	case table == "":
		return fmt.Errorf("query: tabela virtual sem nome em %s", namespace)
	case sch == nil:
		return fmt.Errorf("query: tabela virtual %s.%s sem schema compilado", namespace, table)
	case sch.Name != table:
		return fmt.Errorf("query: tabela virtual %s.%s com schema chamado %q; o binder confere o FROM contra o Name", namespace, table, sch.Name)
	case st == nil:
		return fmt.Errorf("query: tabela virtual %s.%s sem store", namespace, table)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	reg, ok := e.datasets[dataset]
	if !ok {
		return fmt.Errorf("query: dataset %q não registrado; a tabela virtual %s.%s não tem sob o que viver", dataset, namespace, table)
	}
	reg.declaraNamespace(namespace)
	if _, exists := reg.virtualTables[namespace][table]; exists {
		return fmt.Errorf("query: tabela virtual %s.%s já registrada em %q", namespace, table, dataset)
	}
	reg.virtualTables[namespace][table] = &record{sch: sch, store: st, planner: planner.New(st, &e.planStats)}
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

func (reg *record) declaraNamespace(namespace string) {
	if reg.virtualTables == nil {
		reg.virtualTables = make(map[string]map[string]*record)
	}
	if reg.virtualTables[namespace] == nil {
		reg.virtualTables[namespace] = make(map[string]*record)
	}
}

// namespacesConhecidos lista o que um `FROM _x.y` pode endereçar, para a
// mensagem de recusa: o `_meta` fixo mais os registrados neste dataset.
func (reg *record) namespacesConhecidos() []string {
	out := []string{meta.Namespace}
	for ns := range reg.virtualTables {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// tableVirtual resolve `namespace.tabela` sob o dataset, com as três recusas
// nomeando o que existe — ninguém é mandado procurar um dataset que nunca quis.
//
// As três são erro DE QUEM ESCREVEU A QUERY, e por isso saem tipadas por
// binder.ErrorAt: o FROM aponta para algo que não existe, que é a mesma família
// de "coluna desconhecida" — esta, na MESMA tabela virtual, já sai como BIND
// pelo renameTableVirtual. Com fmt.Errorf cru o servidor não tinha como
// distingui-las de um bug e respondia INTERNAL/500, subindo a série de erro
// interno por conta de um nome digitado errado (issue #35).
//
// pos é a posição do nome DEPOIS do ponto (parser.SelectStmt.DatasetPos), que
// é o token ofensor: o cliente destaca a tabela, não o qualificador.
func tableVirtual(tableVirtual *record, dataset, namespace, table string, pos int) (*record, error) {
	// O `_meta` existe sempre, registrado ou não: a `fields` é fixa e mora
	// no pacote meta, fora deste registro.
	byTable, ok := tableVirtual.virtualTables[namespace]
	if !ok && namespace != meta.Namespace {
		return nil, binder.ErrorAt(pos, "namespace %q não existe; existem %s", namespace, strings.Join(tableVirtual.namespacesConhecidos(), " e "))
	}
	v, ok := byTable[table]
	if !ok {
		nomes := make([]string, 0, len(byTable)+1)
		for n := range byTable {
			nomes = append(nomes, n)
		}
		if namespace == meta.Namespace {
			nomes = append(nomes, meta.TableFields)
		}
		if len(nomes) == 0 {
			return nil, binder.ErrorAt(pos, "o dataset %q não declara tabela nenhuma em %s", dataset, namespace)
		}
		sort.Strings(nomes)
		return nil, binder.ErrorAt(pos, "%s não tem tabela %q no dataset %q; existem: %s", namespace, table, dataset, strings.Join(nomes, ", "))
	}
	return v, nil
}

// renameTableVirtual troca o nome interno do schema sintético pelo nome
// qualificado que o usuário escreveu nas mensagens do binder — o mesmo
// remédio do `_meta`: "coluna desconhecida em x" mandaria procurar uma tabela
// chamada x, que não existe em lugar nenhum.
func renameTableVirtual(err error, namespace, table string) error {
	var be *binder.Error
	if !errors.As(err, &be) {
		return err
	}
	qualificado := namespace + "." + table
	msg := strings.ReplaceAll(be.Error(), `"`+table+`"`, `"`+qualificado+`"`)
	if !strings.Contains(msg, qualificado) {
		msg += " (em " + qualificado + ")"
	}
	return &errOfTableVirtual{msg: msg, err: err}
}

type errOfTableVirtual struct {
	msg string
	err error
}

func (e *errOfTableVirtual) Error() string { return e.msg }
func (e *errOfTableVirtual) Unwrap() error { return e.err }

// schemaQualifiedTarget é o endereço INTERNO de uma tabela endereçada pelo
// schema declarado: o dataset registrado que a hospeda e o namespace por onde
// o Execute a abre — vazio para a tabela base, que é aberta pelo nome.
type schemaQualifiedTarget struct {
	dataset   string
	namespace string
}

// resolveSchemaQualified traduz o par publicado pelo catálogo — `(Schema,
// Name)` — no endereço interno que o Execute já sabe abrir.
//
// A tradução sai da enumeração do catálogo (catalogEntries), e não de uma
// varredura própria dos registros, de propósito: ela é a ÚNICA derivação de
// "que tabelas cada dataset publica, em que schema cada uma aparece e por qual
// namespace cada uma abre", e o godoc do Catalog se compromete com isso ("Toda
// tabela listada aqui É aberta pelo Execute, e tabela que ele abre não fica de
// fora"). Com uma segunda varredura aqui a promessa viraria coincidência: na
// primeira tabela que entrasse só num dos dois lados, o catálogo e o FROM
// discordariam sem nada estar quebrado — a ferramenta mostraria uma tabela que
// a consulta recusa, ou o contrário.
//
// O que ela NÃO pede é a montagem das colunas, e essa é a diferença entre uma
// consulta qualificada custar microssegundos e custar meio milissegundo: ver o
// godoc de catalogEntries e o BenchmarkExecuteFromQualified.
//
// O laço é sobre TODOS os datasets registrados, e é isso que faz o schema ser
// endereço de servidor e não de conexão. O custo é uma enumeração por dataset
// por consulta QUALIFICADA — os datasets são unidades e as tabelas de cada um
// também, e o caminho já paga parse, bind e plano.
//
// A lista sai SEM os schemas de prefixo "_", e é só isso que o filtro faz: ele
// não é a barreira que impede `_meta` de ser endereço qualificado. Quem impede
// são dois outros fatos, e os dois estão fora daqui — o Execute desvia todo
// `FROM _x.y` pelo meta.Reconhece ANTES de chegar nesta função, e um dataset
// não consegue declarar schemaName com "_" (schema.validate recusa o prefixo
// reservado). Sem o filtro, o único efeito seria a recusa de schema
// desconhecido OFERECER `_meta` numa lista de endereços que não endereçam
// nada.
//
// pos é a posição do nome DEPOIS do ponto (parser.SelectStmt.DatasetPos), o
// mesmo token ofensor que as recusas de namespace apontam.
func (e *Engine) resolveSchemaQualified(schemaName, table string, pos int) (schemaQualifiedTarget, error) {
	e.mu.RLock()
	names := make([]string, 0, len(e.datasets))
	for n := range e.datasets {
		names = append(names, n)
	}
	e.mu.RUnlock()
	// A ordem alfabética é o que faz a lista das recusas e o desempate de uma
	// eventual ambiguidade saírem sempre iguais: o laço de um mapa não tem
	// ordem, e uma mensagem que muda entre duas execuções idênticas manda o
	// operador procurar uma diferença que não existe.
	sort.Strings(names)

	var (
		found          []schemaQualifiedTarget
		schemas        []string
		tablesOfSchema []string
		seenSchema     = make(map[string]bool, len(names))
	)
	for _, n := range names {
		entries, err := e.catalogEntries(n)
		if err != nil {
			return schemaQualifiedTarget{}, err
		}
		for _, entry := range entries {
			tb := entry.info
			if strings.HasPrefix(tb.Schema, "_") {
				continue
			}
			if !seenSchema[tb.Schema] {
				seenSchema[tb.Schema] = true
				schemas = append(schemas, tb.Schema)
			}
			if tb.Schema != schemaName {
				continue
			}
			tablesOfSchema = append(tablesOfSchema, tb.Name)
			if tb.Name == table {
				found = append(found, schemaQualifiedTarget{dataset: n, namespace: tb.Namespace})
			}
		}
	}

	switch {
	case len(found) == 1:
		return found[0], nil
	case len(found) > 1:
		// A configuração fecha as duas colisões que se sabe construir — dois
		// datasets no mesmo schemaName (config.ValidateSchemaUniqueness) e um
		// contador com o nome do próprio dataset (schema.validateCounters) —,
		// mas a tabela irmã do histórico herda o schema do base e tem o nome
		// derivado por sufixo configurável, então a unicidade do par não é
		// garantida por construção. Escolher um dos dois em silêncio seria a
		// consulta respondendo de outra tabela sem nada acusar; a recusa nomeia
		// os datasets para que o YAML possa ser corrigido.
		donos := make([]string, 0, len(found))
		for _, f := range found {
			donos = append(donos, f.dataset)
		}
		return schemaQualifiedTarget{}, binder.ErrorAt(pos,
			"%s.%s é ambíguo: os datasets %s publicam essa mesma tabela no mesmo schema",
			schemaName, table, strings.Join(donos, " e "))
	case !seenSchema[schemaName]:
		sort.Strings(schemas)
		return schemaQualifiedTarget{}, binder.ErrorAt(pos,
			"schema %q não existe; existem: %s", schemaName, strings.Join(schemas, ", "))
	default:
		sort.Strings(tablesOfSchema)
		return schemaQualifiedTarget{}, binder.ErrorAt(pos,
			"o schema %q não tem tabela %q; existem: %s",
			schemaName, table, strings.Join(tablesOfSchema, ", "))
	}
}

// Execute compila e abre a query. Erros de sintaxe e de bind saem AQUI, antes
// de qualquer leitura de dado.
//
// O FROM tem TRÊS formas, e cada uma resolve o dataset alvo de um jeito:
//
//   - `FROM x` — nome simples. O dataset é o do argumento, e o binder exige que
//     os dois batam: sem isso a query rodaria contra um schema que não é o que
//     o chamador pediu.
//   - `FROM _ns.x` — namespace reservado. É atalho, e significa "a tabela
//     virtual x DO DATASET DA REQUISIÇÃO"; ele não nomeia dataset nenhum.
//   - `FROM schema.x` — schema declarado. É ENDEREÇO COMPLETO: o schemaName é
//     único no servidor (config.ValidateSchemaUniqueness), então o par
//     (schema, tabela) identifica uma tabela e só uma, em qualquer dataset.
//
// Na terceira forma o dataset do argumento deixa de ser o alvo e passa a ser
// só o default das outras duas — é o papel que um "current schema" tem numa
// conexão SQL. A regra do binder não é furada por isso: ela continua rodando,
// agora contra o dataset DONO do schema, que é resolvido antes da comparação.
// O que ela protege é endereço AMBÍGUO — `FROM y` com o handle `x` são dois
// nomes e nenhum jeito de saber qual o usuário quis —, e o qualificado não tem
// essa ambiguidade. Também não há fronteira de autorização a atravessar: os
// papéis do servidor são READ e ADMIN por ROTA (internal/server/auth.go), não
// por dataset, e quem pode consultar já podia mandar outro dataset no campo da
// requisição.
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
	// O desvio é aqui, e não no servidor, porque este é o ponto por onde o
	// servidor e o CLI passam os dois — pô-lo numa camada acima faria a
	// introspecção existir só para quem falasse HTTP.
	switch {
	case meta.Reconhece(stmt):
		// `_meta.fields` é a tabela fixa do pacote meta, que descreve o
		// dataset pela definição. As DEMAIS tabelas do `_meta` (`counters`) e
		// os outros namespaces reservados são tabelas REGISTRADAS sob o
		// dataset, e correm pelo mesmo pipeline que ele.
		if stmt.Namespace == meta.Namespace && stmt.Dataset == meta.TableFields {
			caps, _ := reg.store.(meta.Capacidades)
			return meta.Open(ctx, stmt, reg.sch, caps, params)
		}
		return e.openVirtualTable(ctx, reg, dataset, stmt.Namespace, stmt, params)

	case stmt.Namespace != "":
		target, err := e.resolveSchemaQualified(stmt.Namespace, stmt.Dataset, stmt.DatasetPos)
		if err != nil {
			return nil, err
		}
		e.mu.RLock()
		owner, ok := e.datasets[target.dataset]
		e.mu.RUnlock()
		if !ok {
			// O dataset saiu do registro entre a resolução e agora. Não há
			// caminho que faça isso hoje (Register não tem par que remova),
			// mas o mapa é lido fora do lock em dois momentos e a alternativa
			// seria desreferenciar nil.
			return nil, fmt.Errorf("query: dataset %q não registrado", target.dataset)
		}
		if target.namespace == "" {
			return e.openTable(ctx, owner, stmt, params)
		}
		return e.openVirtualTable(ctx, owner, target.dataset, target.namespace, stmt, params)

	default:
		return e.openTable(ctx, reg, stmt, params)
	}
}

// openTable binda, planeja e abre contra a tabela BASE do registro.
func (e *Engine) openTable(ctx context.Context, reg *record, stmt *parser.SelectStmt, params []types.Value) (executor.RowIterator, error) {
	bq, err := binder.Bind(stmt, reg.sch, params)
	if err != nil {
		return nil, err
	}
	plan := reg.planner.Plan(bq)
	return executor.Open(ctx, bq, plan, executor.Options{Store: reg.store, Metrics: &e.metrics})
}

// openVirtualTable resolve a tabela virtual sob o registro e a abre pelo MESMO
// pipeline da base.
//
// O namespace vem por parâmetro porque ele nem sempre é o que o usuário
// escreveu: pelo atalho `_counters.x` os dois coincidem, mas por
// `FROM telecom.x` o qualificador escrito é o schema e o namespace que abre a
// tabela vem do catálogo. Quem renomeia as mensagens do binder, esse sim, usa
// o que o usuário escreveu — mandar procurar `_counters.x` quem digitou
// `telecom.x` seria apontar um endereço que ele não escolheu.
func (e *Engine) openVirtualTable(ctx context.Context, reg *record, dataset, namespace string, stmt *parser.SelectStmt, params []types.Value) (executor.RowIterator, error) {
	e.mu.RLock()
	v, err := tableVirtual(reg, dataset, namespace, stmt.Dataset, stmt.DatasetPos)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	bq, err := binder.Bind(stmt, v.sch, params)
	if err != nil {
		return nil, renameTableVirtual(err, stmt.Namespace, stmt.Dataset)
	}
	plan := v.planner.Plan(bq)
	return executor.Open(ctx, bq, plan, executor.Options{Store: v.store, Metrics: &e.metrics})
}

// Metrics são os contadores do executor (diagnóstico).
func (e *Engine) Metrics() *executor.Metrics { return &e.metrics }

// PlanMetrics são os contadores de plano, onde vive a métrica normativa
// query.full_scan (§64).
func (e *Engine) PlanMetrics() *planner.Metrics { return &e.planStats }
