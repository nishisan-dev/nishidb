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
	// adminOnly marca a tabela virtual que só uma consulta com WithAdmin no
	// contexto pode abrir (RegisterAdminVirtual).
	adminOnly bool
}

// ErrForbidden recusa a consulta a uma tabela só-ADMIN feita sem a marca de
// ADMIN no contexto.
var ErrForbidden = errors.New("query: this table requires the ADMIN role")

type adminKey struct{}

// WithAdmin marca o contexto de uma consulta feita por um principal ADMIN. É
// a ÚNICA forma de abrir uma tabela registrada por RegisterAdminVirtual.
//
// A barreira mora no motor, e não em quem chama, pela mesma razão do desvio do
// `_meta` no Execute: este é o ponto por onde todo caminho de consulta passa, e
// um contexto sem a marca — o do plano loopback, o de um usuário READ, o de um
// chamador que esqueceu — cai na recusa sem código extra em lugar nenhum.
func WithAdmin(ctx context.Context) context.Context {
	return context.WithValue(ctx, adminKey{}, true)
}

func isAdmin(ctx context.Context) bool {
	v, _ := ctx.Value(adminKey{}).(bool)
	return v
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
	return e.registerVirtual(dataset, namespace, table, sch, st, false)
}

// RegisterAdminVirtual é RegisterVirtual para uma tabela que só uma consulta
// com WithAdmin no contexto pode abrir; as demais recebem ErrForbidden. É o
// caso do `_meta.users`.
func (e *Engine) RegisterAdminVirtual(dataset, namespace, table string, sch *schema.Compiled, st Store) error {
	return e.registerVirtual(dataset, namespace, table, sch, st, true)
}

func (e *Engine) registerVirtual(dataset, namespace, table string, sch *schema.Compiled, st Store, adminOnly bool) error {
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
	reg.virtualTables[namespace][table] = &record{sch: sch, store: st, planner: planner.New(st, &e.planStats), adminOnly: adminOnly}
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
// scope é o schema da CONEXÃO. Vazio, o laço é sobre TODOS os datasets
// registrados, e o schema é endereço de servidor: é o caminho do handle por
// dataset, que só o client Python ainda fala (débito 123). Preenchido, o laço
// pula os datasets de outro schema — quem garante que o par pedido É do escopo
// é o chamador (resolveFrom), que recusa o qualificador alheio antes, com a
// posição dele. O custo é uma enumeração por dataset alcançável por consulta
// que resolve — os datasets são unidades e as tabelas de cada um também, e o
// caminho já paga parse, bind e plano.
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
func (e *Engine) resolveSchemaQualified(scope, schemaName, table string, pos int) (schemaQualifiedTarget, error) {
	names := e.datasetsInScope(scope)

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
		// Esta é a REDE, não a barreira. A barreira é a configuração, e ela
		// recusa na subida as três colisões construíveis por YAML: dois
		// datasets no mesmo schemaName (config.ValidateSchemaUniqueness) e um
		// contador com o nome do próprio dataset ou com o da tabela irmã do
		// histórico, que herda o schema do base (schema.validateCounters). Um
		// YAML aceito não chega aqui.
		//
		// A rede existe porque a unicidade do par é sustentada por uma LISTA
		// de recusas, e não pela forma da estrutura: quem registrar uma tabela
		// virtual direto pelo RegisterVirtual não passa por nenhuma delas.
		// Escolher um dos dois em silêncio seria a consulta respondendo de
		// outra tabela sem nada acusar; a recusa nomeia os datasets para que o
		// YAML possa ser corrigido.
		owners := make([]string, 0, len(found))
		for _, f := range found {
			owners = append(owners, f.dataset)
		}
		return schemaQualifiedTarget{}, binder.ErrorAt(pos,
			"%s.%s é ambíguo: os datasets %s publicam essa mesma tabela no mesmo schema",
			schemaName, table, strings.Join(owners, " e "))
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

// datasetsInScope lista, em ordem alfabética, os datasets registrados cujo
// schema declarado é scope — ou todos, com scope vazio.
//
// A ordem alfabética é o que faz a lista das recusas e o desempate de uma
// eventual ambiguidade saírem sempre iguais: o laço de um mapa não tem ordem, e
// uma mensagem que muda entre duas execuções idênticas manda o operador
// procurar uma diferença que não existe.
func (e *Engine) datasetsInScope(scope string) []string {
	e.mu.RLock()
	names := make([]string, 0, len(e.datasets))
	for n, reg := range e.datasets {
		if scope == "" || reg.sch.SchemaName == scope {
			names = append(names, n)
		}
	}
	e.mu.RUnlock()
	sort.Strings(names)
	return names
}

// schemaTables lista, qualificados e em ordem, os endereços que a conexão ao
// schema abre fora do `_meta` — é a lista que a recusa do schema alheio
// oferece. Sai da MESMA enumeração do catálogo que a resolução lê.
func (e *Engine) schemaTables(schemaName string) ([]string, error) {
	var out []string
	for _, n := range e.datasetsInScope(schemaName) {
		entries, err := e.catalogEntries(n)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.info.Schema == schemaName {
				out = append(out, schemaName+"."+entry.info.Name)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// resolveFrom decide para onde o FROM aponta, e é a ÚNICA decisão de rota: o
// Execute e o TargetDataset a chamam, para que a consulta e os limites que o
// servidor aplica a ela não possam discordar sobre o dono.
//
// resolved=false quer dizer "o próprio handle, pela tabela base" — o nome
// simples no caminho por dataset. O atalho `_ns.x` NÃO passa por aqui: quem o
// desvia é o chamador, antes, porque ele não nomeia dataset nenhum.
//
// Com scoped, o schema da conexão é o do handle, e as duas formas que nomeiam
// tabela resolvem DENTRO dele: `FROM x` é o par (schema da conexão, x) e
// `FROM s.x` só é aceito com s igual ao schema da conexão. O qualificador
// alheio é recusado apontando para ele, e não resolvido: a árvore da conexão
// só mostra o schema dela, e um FROM que abrisse o que a árvore esconde seria o
// falso negativo que a promessa do Catalog proíbe.
func (e *Engine) resolveFrom(reg *record, scoped bool, stmt *parser.SelectStmt) (schemaQualifiedTarget, bool, error) {
	if !scoped {
		if stmt.Namespace == "" {
			return schemaQualifiedTarget{}, false, nil
		}
		target, err := e.resolveSchemaQualified("", stmt.Namespace, stmt.Dataset, stmt.DatasetPos)
		return target, err == nil, err
	}
	scope := reg.sch.SchemaName
	if stmt.Namespace != "" && stmt.Namespace != scope {
		tables, err := e.schemaTables(scope)
		if err != nil {
			return schemaQualifiedTarget{}, false, err
		}
		return schemaQualifiedTarget{}, false, binder.ErrorAt(stmt.NamespacePos,
			"schema %q is outside this connection, whose schema is %q; it addresses %s and the %s tables",
			stmt.Namespace, scope, strings.Join(tables, ", "), meta.Namespace)
	}
	target, err := e.resolveSchemaQualified(scope, scope, stmt.Dataset, stmt.DatasetPos)
	return target, err == nil, err
}

// Execute compila e abre a query pelo handle de DATASET. Erros de sintaxe e de
// bind saem AQUI, antes de qualquer leitura de dado.
//
// É o caminho do CLI local e do client Python, que ainda endereça o servidor
// por dataset (débito 123); a conexão por schema — JDBC e `nishidb query
// --server` — entra pelo ExecuteInSchema.
//
// O FROM tem TRÊS formas, e cada uma resolve o dataset alvo de um jeito:
//
//   - `FROM x` — nome simples. O dataset é o do argumento, e o binder exige que
//     os dois batam: sem isso a query rodaria contra um schema que não é o que
//     o chamador pediu.
//   - `FROM _ns.x` — namespace reservado. É atalho, e significa "a tabela
//     virtual x DO DATASET DA REQUISIÇÃO"; ele não nomeia dataset nenhum.
//   - `FROM schema.x` — schema declarado. É ENDEREÇO COMPLETO: o par
//     (schema, tabela) endereça uma tabela em qualquer dataset do servidor.
//
// Que o par endereça UMA tabela e não duas é sustentado por três recusas de
// CONFIGURAÇÃO, todas na subida: dois datasets não declaram o mesmo schemaName
// (config.ValidateSchemaUniqueness) e um contador não se chama como o próprio
// dataset nem como a tabela irmã do histórico (schema.validateCounters), que
// herda o schema do base. Não é propriedade da estrutura — por isso a
// resolução ainda recusa o par ambíguo, como rede.
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
	return e.execute(ctx, dataset, false, sql, params)
}

// ExecuteInSchema é o Execute da CONEXÃO POR SCHEMA: o handle é o dataset base
// do schema, e o escopo de resolução é o schema declarado por ele.
//
// As três formas do FROM mudam de significado assim:
//
//   - `FROM x` — a tabela x DO SCHEMA DA CONEXÃO: a base, a irmã do histórico
//     ou um contador, pelo nome com que a árvore a mostra.
//   - `FROM _ns.x` — o atalho, sem mudança: a tabela virtual x do handle.
//   - `FROM s.x` — endereço completo, aceito só com s igual ao schema da
//     conexão; o schema alheio é recusado (BIND), nomeando o schema da conexão
//     e os endereços que ela abre.
//
// É o que faz a árvore e o SQL dizerem a mesma coisa: o /v1/metadata?schema=
// publica o schema da conexão e o `_meta`, e o que ele não publica esta função
// não abre. Prova: TestTheConnectionTreeAndTheConnectionSqlAgree, em
// internal/server.
func (e *Engine) ExecuteInSchema(ctx context.Context, dataset, sql string, params []types.Value) (executor.RowIterator, error) {
	return e.execute(ctx, dataset, true, sql, params)
}

func (e *Engine) execute(ctx context.Context, dataset string, scoped bool, sql string, params []types.Value) (executor.RowIterator, error) {
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
	}

	target, resolved, err := e.resolveFrom(reg, scoped, stmt)
	switch {
	case err != nil:
		return nil, err
	case !resolved:
		return e.openTable(ctx, reg, stmt, params)
	}
	e.mu.RLock()
	owner, ok := e.datasets[target.dataset]
	e.mu.RUnlock()
	if !ok {
		// O dataset saiu do registro entre a resolução e agora. Não há
		// caminho que faça isso hoje (Register não tem par que remova), mas o
		// mapa é lido fora do lock em dois momentos e a alternativa seria
		// desreferenciar nil.
		return nil, fmt.Errorf("query: dataset %q não registrado", target.dataset)
	}
	if target.namespace == "" {
		return e.openTable(ctx, owner, stmt, params)
	}
	return e.openVirtualTable(ctx, owner, target.dataset, target.namespace, stmt, params)
}

// TargetDataset devolve o dataset que esta consulta vai LER: o dono do schema,
// quando o FROM é qualificado por um schema declarado, e o handle recebido em
// todo o resto.
//
// Ela existe para quem precisa saber o alvo ANTES de chamar o Execute, e hoje
// isso é um chamador só: o servidor, que aplica `query.maxRows`,
// `query.timeout` e `query.maxTimeout` — declarados POR DATASET no YAML — antes
// de abrir o iterador, e precisa aplicar os do dataset de que o YAML fala. Sem
// isto, um handle com teto alto seria porta para ler, sob os limites dele, um
// dataset cujo YAML declara teto baixo.
//
// Ela REUSA o parser e o resolveSchemaQualified do Execute, e não uma segunda
// leitura do SQL: o preço é um parse a mais por consulta no servidor, e a
// alternativa — o servidor decidindo por conta própria quem é o dono — seria
// uma segunda fonte de verdade sobre onde cada tabela mora, que é exatamente o
// que a resolução pelo catálogo existe para não ter.
//
// SQL que não compila, schema que não resolve e tabela que não existe devolvem
// o handle, e não um erro: quem recusa e explica é o Execute, logo em seguida,
// com a posição do token ofensor. Duplicar a recusa aqui daria duas mensagens
// para o mesmo defeito, e a dela seria a pior.
func (e *Engine) TargetDataset(dataset, sql string) string {
	return e.targetDataset(dataset, false, sql)
}

// TargetDatasetInSchema é o TargetDataset do ExecuteInSchema: a mesma
// resolução com o escopo do schema da conexão, para que o servidor aplique os
// limites do dataset que o ExecuteInSchema vai de fato abrir.
func (e *Engine) TargetDatasetInSchema(dataset, sql string) string {
	return e.targetDataset(dataset, true, sql)
}

func (e *Engine) targetDataset(dataset string, scoped bool, sql string) string {
	e.mu.RLock()
	reg, ok := e.datasets[dataset]
	e.mu.RUnlock()
	if !ok {
		return dataset
	}
	stmt, err := parser.Parse(sql)
	if err != nil || stmt == nil {
		return dataset
	}
	// O atalho `_ns.x` não nomeia dataset nenhum — significa "o da requisição"
	// —, e é o mesmo desvio que o Execute faz primeiro.
	if meta.Reconhece(stmt) {
		return dataset
	}
	target, resolved, err := e.resolveFrom(reg, scoped, stmt)
	if err != nil || !resolved {
		return dataset
	}
	return target.dataset
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
	// A recusa vem antes do bind: nem o schema da tabela, pela mensagem de uma
	// coluna inexistente, chega a quem não pode lê-la.
	if v.adminOnly && !isAdmin(ctx) {
		return nil, fmt.Errorf("%w: %s.%s", ErrForbidden, namespace, stmt.Dataset)
	}
	bq, err := binder.Bind(stmt, v.sch, params)
	if err != nil {
		// Pelo nome simples da conexão por schema não há qualificador escrito,
		// e o nome que o binder usa já é o que o usuário digitou.
		if stmt.Namespace == "" {
			return nil, err
		}
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
