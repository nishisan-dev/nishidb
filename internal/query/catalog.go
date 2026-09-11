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
	// Namespace é o espaço de nome virtual por onde o FROM abre a tabela hoje
	// (`_counters`, `_meta`), e sai VAZIO para a tabela base — que é aberta
	// pelo nome, sem prefixo. Ele é publicado porque o par (Schema, Name) não
	// é endereço: quem precisar montar o FROM de uma view — o /v1/metadata e a
	// `_meta.tables` — não teria como reconstruir `_counters.por_olt` a partir
	// de `telecom` e `por_olt`.
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
// Mas o par (Schema, Name) ainda NÃO é o endereço que o FROM aceita: o
// contador publicado como `(telecom, por_olt)` só abre hoje por
// `FROM _counters.por_olt`, porque o motor resolve a view pelo NAMESPACE e não
// pelo schema. Por isso a linha também carrega o Namespace — é com ele que se
// monta o FROM que funciona. Fazer o schema declarado ser aceito no FROM é a
// tarefa seguinte do ciclo; até lá, quem consome esta lista para executar SQL
// tem de usar o Namespace, e o Schema é só o lugar na árvore do cliente.
//
// Por ora o único consumidor é o teste desta função: o /v1/metadata e a
// `_meta.tables` passam a sair daqui nas tarefas seguintes do ciclo. Ela nasce
// como função única justamente para que as duas não derivem cada uma a sua
// lista — duas derivações do mesmo fato divergem, e a divergência aparece como
// uma ferramenta que discorda do SQL sem nada estar quebrado.
func (e *Engine) Catalog(dataset string) ([]TableInfo, error) {
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

	fieldsColumns, err := meta.ColumnsOfFields()
	if err != nil {
		return nil, err
	}

	// O nome da base é o Name do COMPILADO, e não a chave do registro: é
	// contra o Name que o binder confere o FROM, então é ele o nome pelo qual
	// a tabela pode ser aberta.
	entries := make([]catalogEntry, 0, len(virtualTables)+2)
	entries = append(entries, catalogEntry{
		info: TableInfo{
			Schema:  reg.sch.SchemaName,
			Name:    reg.sch.Name,
			Kind:    KindTable,
			Columns: reg.sch.FieldsForMetadata(),
		},
		rank: rankBase,
	})
	entries = append(entries, catalogEntry{
		info: TableInfo{
			Schema:    meta.Namespace,
			Name:      meta.TableFields,
			Namespace: meta.Namespace,
			Kind:      KindView,
			Columns:   fieldsColumns,
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
				Columns:   v.sch.FieldsForMetadata(),
			},
			rank: rank,
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

	out := make([]TableInfo, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.info)
	}
	return out, nil
}

// Os três grupos do contrato de ordem, na sequência em que saem.
const (
	rankBase = iota
	rankVirtual
	rankMeta
)

// catalogEntry carrega, além da linha publicada, o único critério de ordem que
// não cabe nela: o grupo. O namespace é critério de desempate e mora na linha,
// porque o consumidor precisa dele para montar o FROM.
type catalogEntry struct {
	info TableInfo
	rank int
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
