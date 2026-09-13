package query

import (
	"fmt"
	"sort"

	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// TableKind é como a tabela se apresenta a um cliente SQL.
type TableKind string

const (
	// KindTable é a tabela base do dataset — a única que materializa evento.
	KindTable TableKind = "TABLE"
	// KindView cobre o contador E a tabela de meta. Não existe KindSystemTable:
	// o spike do DBeaver mostrou que objeto de sistema fica escondido por
	// default, e tabela que existe para ser descoberta não pode nascer
	// invisível.
	KindView TableKind = "VIEW"
)

// TableInfo descreve UMA tabela consultável.
type TableInfo struct {
	// Schema é onde a tabela aparece na árvore do cliente: o schema declarado
	// pelo dataset para a base e para os contadores, e o namespace `_meta`
	// para as tabelas de meta — que descrevem o motor, não o dado.
	Schema string
	// Name é o nome da tabela DENTRO do Schema.
	Name string
	// Namespace é o espaço de nome reservado por onde o ATALHO abre a tabela
	// (`_counters`, `_meta`) — a forma que significa "no dataset da
	// requisição" —, e sai VAZIO para a tabela base, que pelo atalho é aberta
	// só pelo nome, sem prefixo.
	//
	// Ele é publicado porque não é reconstruível a partir de (Schema, Name):
	// quem precisar montar o atalho de uma view — o /v1/metadata e a
	// `_meta.tables` — não teria como chegar a `_counters.por_olt` sabendo
	// apenas `telecom` e `por_olt`. O par (Schema, Name) é o OUTRO endereço, e
	// o FROM o aceita direto QUANDO o Schema é o declarado pelo dataset; nas
	// linhas do `_meta` o atalho é o único endereço que existe (ver o godoc do
	// Catalog).
	Namespace string
	Kind      TableKind
	// Description é o texto que o cliente mostra ao lado da tabela. Hoje sai
	// vazia em TODA linha: nem a spec do dataset nem o registro de tabela
	// virtual declaram descrição, e o catálogo não inventa uma. O campo existe
	// porque o wire do /v1/metadata e a `_meta.tables` o publicam, e passa a
	// ter conteúdo quando houver onde declará-lo.
	Description string
	// Columns são as colunas DESCRITÍVEIS da tabela, pela mesma regra do
	// /v1/metadata (FieldsForMetadata): as de topo mais as derivadas, sem os
	// campos de sistema, em ordem crescente de FieldID.
	//
	// É deliberadamente um SUPERSET do que o `SELECT *` devolve — a coluna
	// derivada fica fora do `SELECT *` e está aqui, porque esconder também
	// aqui deixaria o operador sem forma nenhuma de saber que ela existe, e
	// ela foi declarada justamente para ser consultada pelo nome.
	Columns []*schema.CompiledField
}

// Catalog devolve TODA tabela consultável do dataset, em ordem estável: a base
// primeiro, depois os contadores (no schema do dataset) e por último as tabelas
// do `_meta`, cada grupo por nome.
//
// A ordem é contrato. Uma ferramenta que agrupa a árvore pela sequência em que
// as linhas chegam mostraria as pastas embaralhadas a cada reconexão sem ela —
// e o mapa de tabelas virtuais não tem ordem nenhuma.
//
// O catálogo ESPELHA o roteamento do Execute, e é isso que o mantém honesto: a
// base sai do `record.sch`, as views saem do `record.virtualTables` — o mesmo
// mapa que o tableVirtual consulta — e a `_meta.fields` é acrescentada à parte
// porque ela é fixa do pacote meta e não está no registro, exatamente como o
// desvio do Execute a trata. Toda tabela listada aqui É aberta pelo Execute, e
// tabela que ele abre não fica de fora.
//
// O par (Schema, Name) É o endereço que o FROM aceita, com uma ressalva que
// muda de linha para linha: o contador publicado como `(telecom, por_olt)` abre
// por `FROM telecom.por_olt` — e por `FROM por_olt` — numa conexão ao schema
// `telecom` (ExecuteInSchema), e o mesmo vale para a base; numa conexão a outro
// schema o par é recusado, porque a árvore dela não o mostra. No caminho por
// dataset (Execute, débito 123) o par abre de qualquer handle. As linhas do
// `_meta` que esta função também publica — `(_meta, fields)`, `(_meta, tables)`
// e as registradas (`counters`, `kafka`) — NÃO são endereço de servidor:
// `_meta` tem o prefixo reservado, o Execute desvia todo `FROM _x.y` para o
// atalho antes de chegar à resolução por schema, e o atalho significa sempre
// "no dataset da requisição". Quem resolve o par do schema declarado é o
// resolveSchemaQualified (engine.go), e ele resolve LENDO ESTA ENUMERAÇÃO — não
// há segunda varredura dos registros, justamente para que o catálogo e o
// roteamento não possam discordar.
//
// O Namespace continua publicado porque ele é o outro endereço, o do atalho:
// `FROM _counters.por_olt` significa "no dataset da requisição", e é a forma
// que o `_meta` usa. Ele não é reconstruível a partir de (Schema, Name).
//
// As COLUNAS são montadas aqui e não na enumeração: FieldsForMetadata aloca um
// slice de todos os campos e o ordena, e quem só precisa saber onde a tabela
// mora não pode pagar isso. Ver catalogEntries.
//
// Os DOIS consumidores saem daqui: a `_meta.tables` (metatables.go) e o campo
// `tables` do /v1/metadata (internal/server/documents.go, que é de onde o
// driver JDBC monta a árvore do cliente SQL). É uma função única justamente
// para que as duas não derivem cada uma a sua lista — duas derivações do mesmo
// fato divergem, e a divergência aparece como uma ferramenta que discorda do
// SQL sem nada estar quebrado. Prova:
// TestMetadataEMetaTablesNaoPodemDiscordarDoCatalogo, em internal/server.
func (e *Engine) Catalog(dataset string) ([]TableInfo, error) {
	entries, err := e.catalogEntries(dataset)
	if err != nil {
		return nil, err
	}
	out := make([]TableInfo, 0, len(entries))
	for _, entry := range entries {
		info := entry.info
		if info.Columns, err = entry.columns(); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// catalogEntries é a ENUMERAÇÃO: onde cada tabela mora (Schema), como se chama
// (Name), por qual atalho abre (Namespace) e o que ela é (Kind) — na ordem do
// contrato, e sem tocar no dicionário de campos.
//
// A separação entre enumerar e montar colunas existe por MEDIDA, não por
// estética. Enquanto o Catalog era um bloco só, a resolução do `FROM
// <schema>.<tabela>` — que usa apenas Schema, Name e Namespace — pagava um
// FieldsForMetadata por tabela de cada dataset, e essa função aloca um slice
// com todos os campos e o ORDENA. Num dataset de 4.000 colunas a forma
// qualificada custava ~480 µs e ~35 KiB contra ~4 µs e 1,5 KiB da forma
// simples, e é a qualificada que o DBeaver gera sozinho a partir do catálogo:
// era o caminho quente do cliente-alvo pagando por colunas que ninguém lia.
// Ver BenchmarkExecuteFromQualified.
//
// Continua havendo uma derivação só: o Catalog é esta enumeração MAIS as
// colunas, e a resolução é esta enumeração e nada mais. Não há segunda
// varredura dos registros a divergir desta.
func (e *Engine) catalogEntries(dataset string) ([]catalogEntry, error) {
	e.mu.RLock()
	reg, ok := e.datasets[dataset]
	var virtualTables []virtualTableRef
	if ok {
		virtualTables = reg.listVirtualTables()
	}
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("query: dataset %q não registrado", dataset)
	}

	// O nome da base é o Name do COMPILADO, e não a chave do registro: é
	// contra o Name que o binder confere o FROM, então é ele o nome pelo qual
	// a tabela pode ser aberta.
	entries := make([]catalogEntry, 0, len(virtualTables)+2)
	entries = append(entries, catalogEntry{
		info: TableInfo{
			Schema: reg.sch.SchemaName,
			Name:   reg.sch.Name,
			Kind:   KindTable,
		},
		rank: rankBase,
		sch:  reg.sch,
	})
	entries = append(entries, catalogEntry{
		info: TableInfo{
			Schema:    meta.Namespace,
			Name:      meta.TableFields,
			Namespace: meta.Namespace,
			Kind:      KindView,
		},
		rank: rankMeta,
	})
	for _, v := range virtualTables {
		// Registro homônimo da tabela fixa é inalcançável: o Execute desvia
		// `_meta.fields` para o pacote meta ANTES de olhar o registro. Listá-lo
		// poria duas linhas iguais na árvore, uma delas com colunas que
		// nenhuma consulta devolve.
		if v.namespace == meta.Namespace && v.table == meta.TableFields {
			continue
		}
		// O contador vive no schema do DATASET — é tabela do usuário, e é lá
		// que a ferramenta o procura ao lado da base. A spec sintética dele
		// declara o schema `_virtual`, que é sentinela interno e não nome de
		// árvore.
		tableSchema := reg.sch.SchemaName
		rank := rankVirtual
		if v.namespace == meta.Namespace {
			tableSchema = meta.Namespace
			rank = rankMeta
		}
		entries = append(entries, catalogEntry{
			info: TableInfo{
				Schema:    tableSchema,
				Name:      v.table,
				Namespace: v.namespace,
				Kind:      KindView,
			},
			rank: rank,
			sch:  v.sch,
		})
	}

	// O namespace desempata o que schema e nome não separam: duas tabelas
	// homônimas em namespaces diferentes sairiam em ordem de mapa sem ele, e a
	// ordem voltaria a depender de sorte.
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		switch {
		case a.rank != b.rank:
			return a.rank < b.rank
		case a.info.Schema != b.info.Schema:
			return a.info.Schema < b.info.Schema
		case a.info.Name != b.info.Name:
			return a.info.Name < b.info.Name
		default:
			return a.info.Namespace < b.info.Namespace
		}
	})
	return entries, nil
}

// Os três grupos do contrato de ordem, na sequência em que saem.
const (
	rankBase = iota
	rankVirtual
	rankMeta
)

// catalogEntry carrega, além da linha publicada, o que a enumeração sabe e a
// linha não publica: o grupo, que é o único critério de ordem que não cabe
// nela, e de onde as colunas SAIRIAM se alguém as pedisse. O namespace é
// critério de desempate e mora na linha, porque o consumidor precisa dele para
// montar o FROM.
//
// info.Columns sai SEMPRE vazio daqui: quem as quer é o Catalog, e ele as pede
// pelo método columns.
type catalogEntry struct {
	info TableInfo
	rank int
	// sch é o compilado de onde as colunas desta linha saem, e é NIL na linha
	// da `_meta.fields`: as colunas dela são fixas do pacote meta e não vêm de
	// registro nenhum — é o mesmo fato que faz o Execute desviá-la para o meta
	// antes de olhar o registro do dataset.
	sch *schema.Compiled
}

// columns monta as colunas DESCRITÍVEIS da linha, pela regra do /v1/metadata.
//
// É onde mora o custo que a enumeração não paga: FieldsForMetadata aloca um
// slice com todos os campos conhecidos e o ordena por FieldID.
func (c catalogEntry) columns() ([]*schema.CompiledField, error) {
	if c.sch == nil {
		return meta.ColumnsOfFields()
	}
	return c.sch.FieldsForMetadata(), nil
}

type virtualTableRef struct {
	namespace string
	table     string
	sch       *schema.Compiled
}

// listVirtualTables achata o mapa de dois níveis num slice. Ele existe para que
// a varredura do mapa aconteça SOB o lock do motor e o resto do catálogo fora
// dele — o RegisterVirtual escreve nesses mapas, e lê-los enquanto se monta a
// resposta seria corrida.
func (reg *record) listVirtualTables() []virtualTableRef {
	out := make([]virtualTableRef, 0, len(reg.virtualTables))
	for ns, byTable := range reg.virtualTables {
		for table, v := range byTable {
			out = append(out, virtualTableRef{namespace: ns, table: table, sch: v.sch})
		}
	}
	return out
}
