package query

import (
	"fmt"
	"sort"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TableMetaTables é a tabela `_meta.tables`: uma linha por tabela consultável
// do dataset. É a resposta por SQL para "que tabelas existem" — e antes dela a
// lista COMPLETA não saía de lugar nenhum: o `/v1/metadata` devolvia os
// datasets registrados no servidor, o base e a irmã histórica, com as colunas
// de cada um, e nada mais. O que faltava só aparecia por engano, na mensagem de
// recusa de um `FROM` para tabela desconhecida.
//
// O `/v1/metadata` publica a mesma lista desde que ganhou o campo `tables`
// (internal/server/documents.go), e as duas saem do MESMO Catalog: esta tabela
// é o canal de quem fala SQL, e aquele campo o de quem monta árvore. Duas
// derivações da mesma enumeração divergiriam.
//
// Ela mora no pacote query, e não em `internal/query/meta` ao lado da
// `fields`, porque o conteúdo dela é o Catalog — e `internal/query/meta` não
// pode importar query, que já importa meta. E não mora num pacote à parte como
// a `_meta.counters` e a `_meta.kafka`, porque, ao contrário das duas, ela não
// costura o motor a subsistema nenhum: o que ela publica o motor já sabe
// sozinho.
const TableMetaTables = "tables"

// specOfMetaTables é a spec da tabela, no MESMO YAML de um dataset de verdade,
// como as outras três tabelas sintéticas. Reusar o compilador é o que garante
// que ela obedeça às mesmas regras — tipos, chave, ordem determinística de
// FieldID — que qualquer outro dataset.
//
// A key é `id`, e NÃO `name`: o nome não é único dentro do dataset. A
// compilação da spec recusa QUATRO colisões no nome de um contador — o prefixo
// `_`, que é a reserva do produto; o nome do próprio dataset; o nome da tabela
// irmã do histórico, que herda o schema do base; e o nome repetido entre dois
// contadores (internal/schema/counter.go) —, e nenhuma das quatro
// alcança o nome de uma tabela de meta: um contador chamado `tables` é aceito e
// apareceria aqui ao lado desta própria tabela, com o mesmo `name` e schemas
// diferentes. Duas linhas de mesma key fariam o point lookup
// devolver uma e a varredura devolver duas — o motor discordando de si mesmo
// conforme o plano escolhido. `id` é o endereço do ATALHO
// (`_counters.por_olt`, `_meta.tables`, ou só o nome na tabela base) — o FROM
// também aceita o par `schema.tabela`, que é o que as colunas `schema` e
// `name` dão. `id` é único porque o registro de tabelas virtuais é um mapa por
// namespace e por tabela, e o RegisterVirtual recusa a segunda tabela de mesmo
// nome no mesmo namespace.
//
// `columns` é a CONTAGEM de colunas descritíveis, não a lista: quem quer os
// nomes tem a `_meta.fields`, e uma lista junta por separador seria um segundo
// formato a parsear para responder o que a outra tabela já responde em linhas.
//
// `namespace` é NULL na tabela base — pelo atalho ela é aberta só pelo nome, e
// ausência é o que NULL significa nas três camadas deste produto. Não quer
// dizer que ela não tenha qualificador: `schema.base` a abre como abre
// qualquer outra. `schema` nunca é NULL: a spec exige `schemaName` de todo
// dataset e as tabelas de meta saem no `_meta`.
const specOfMetaTables = `version: 1
name: ` + TableMetaTables + `
schemaName: ` + schema.SyntheticSchemaName + `
materialization:
  key:
    field: id
schema:
  include: [id, name, schema, namespace, kind, columns, description]
  fields:
    id:
      type: string
    name:
      type: string
    schema:
      type: string
    namespace:
      type: string
    kind:
      type: string
    columns:
      type: int64
    description:
      type: string
`

// metaTablesColumn identifica cada coluna da tabela, para a linha ser montada
// por um switch em vez de por nome.
type metaTablesColumn uint8

const (
	mtID metaTablesColumn = iota
	mtName
	mtSchema
	mtNamespace
	mtKind
	mtColumns
	mtDescription
)

// metaTablesColumnNames casa, posição a posição, com as constantes acima.
var metaTablesColumnNames = [...]string{
	"id", "name", "schema", "namespace", "kind", "columns", "description",
}

// MetaTables monta o schema sintético e o Store virtual de `_meta.tables` sobre
// o catálogo do dataset dado. O dataset não precisa estar registrado ainda: o
// catálogo é lido a cada consulta, não agora.
//
// É exportada para o gate de alocação, que mede a linha sem passar pelo
// pipeline; quem registra a tabela usa RegisterMetaTables.
func (e *Engine) MetaTables(dataset string) (*schema.Compiled, Store, error) {
	spec, err := schema.Parse([]byte(specOfMetaTables), schema.BuiltinDefaults)
	if err != nil {
		return nil, nil, fmt.Errorf("query: spec da tabela %s.%s inválida: %w", meta.Namespace, TableMetaTables, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, nil, fmt.Errorf("query: compilando a tabela %s.%s: %w", meta.Namespace, TableMetaTables, err)
	}
	f := &metaTablesSource{
		eng:     e,
		dataset: dataset,
		columns: make([]metaTablesColumnWithID, 0, len(metaTablesColumnNames)),
		open:    make(map[uint64]*metaTablesRows),
	}
	for k, name := range metaTablesColumnNames {
		field, ok := sc.FieldByName([]byte(name))
		if !ok {
			return nil, nil, fmt.Errorf("query: coluna %q sem FieldID no schema sintético de %s.%s", name, meta.Namespace, TableMetaTables)
		}
		f.columns = append(f.columns, metaTablesColumnWithID{id: field.ID, k: metaTablesColumn(k)})
	}
	// O payload exige FieldID crescente; a ordem é resolvida UMA vez aqui, e a
	// linha é montada sem ordenar.
	sort.Slice(f.columns, func(i, j int) bool { return f.columns[i].id < f.columns[j].id })
	return sc, f, nil
}

// RegisterMetaTables declara `_meta.tables` sob o dataset já registrado no
// motor.
//
// Chamado por todo caminho que registra um dataset CONSULTÁVEL, e não só os
// que registram um dataset base: o servidor (internal/server/server.go), o
// `nishidb query --config` (internal/cli/query.go) e a tabela histórica irmã
// (internal/server/history.go) — que não é base, mas é consultável por direito
// (`FROM <irma>` funciona) e por isso publica o próprio catálogo.
//
// A TABELA responde igual em todo caminho; o CONTEÚDO dela é o que cada um
// registrou, e nisso os caminhos não são simétricos. No dataset BASE a lista
// coincide entre servidor e CLI: os dois chamam counters.Register e
// metakafka.Register ao lado desta função. Na IRMÃ não coincide: o CLI chama
// counters.Register sem condição (internal/cli/query.go), e a `_meta.counters`
// e a `_meta.counter_metrics` são registradas sempre, com contador ou sem e com
// métrica ou sem (internal/counters/table.go), então as duas aparecem na
// listagem da irmã pelo CLI e não aparecem na do servidor, que na irmã só
// chama esta função. A assimetria é da FIAÇÃO e é anterior a esta
// tabela — que está contando a verdade sobre cada processo; está registrada
// como débito 117, com os dois lados travados por teste
// (TestMetaTablesRespondeNaTabelaIrmaAbertaPeloServidor e
// TestQueryLocalListaMetaCountersNaIrmaQueOServidorNaoLista).
//
// A ordem em relação a esses dois não importa, e é de propósito: as linhas são
// montadas a cada consulta, a partir do Catalog do momento. É isso que faz a
// tabela se INCLUIR na resposta — perguntar "que tabelas existem" e não ver a
// própria pergunta listada deixaria o catálogo incompleto na primeira linha — e
// o que a impede de mentir sobre qualquer tabela registrada depois dela.
func (e *Engine) RegisterMetaTables(dataset string) error {
	sc, store, err := e.MetaTables(dataset)
	if err != nil {
		return err
	}
	return e.RegisterVirtual(dataset, meta.Namespace, TableMetaTables, sc, store)
}

type metaTablesColumnWithID struct {
	id types.FieldID
	k  metaTablesColumn
}

// metaTablesRow é uma tabela do catálogo como linha: os textos são calculados
// uma vez, na montagem do snapshot, para que a linha não aloque.
type metaTablesRow struct {
	id          []byte
	name        []byte
	schemaName  []byte
	namespace   []byte // vazio = tabela base (aberta sem qualificador) → NULL
	kind        []byte
	columns     int64
	description []byte // vazio → NULL; hoje é toda linha (ver TableInfo.Description)
}

// metaTablesRows é o catálogo congelado de UMA consulta: o conjunto de linhas,
// o índice por key e o erro que a montagem devolveu, se devolveu.
//
// Ele é por snapshot, e não do Store, porque o catálogo do motor pode mudar
// entre duas consultas — o RegisterVirtual escreve no registro sem parar o
// mundo. Congelar na abertura é o que dá a esta tabela o mesmo isolamento que
// qualquer outra: a consulta enxerga o catálogo de T0 do começo ao fim, em vez
// de linhas que aparecem e somem no meio da varredura.
type metaTablesRows struct {
	rows []metaTablesRow
	byID map[string]int
	err  error
}

// metaTablesSource é o Store virtual de `_meta.tables`.
type metaTablesSource struct {
	eng     *Engine
	dataset string
	columns []metaTablesColumnWithID // em ordem crescente de FieldID

	// mu guarda os snapshots ABERTOS, por época. A época é atribuída aqui, e é
	// o único canal por onde OpenSnapshot entrega o que congelou às demais
	// chamadas: a assinatura do Store passa só o types.Snapshot.
	mu    sync.Mutex
	epoch uint64
	open  map[uint64]*metaTablesRows
}

// OpenSnapshot congela o catálogo desta consulta e devolve o limite de linhas.
//
// Ele não pode devolver erro, e o Catalog pode falhar — o erro fica guardado no
// snapshot e sai pelo Err() do iterador e pelo retorno do point lookup. Uma
// falha silenciosa aqui viraria "nenhuma tabela", que é uma resposta plausível
// e errada.
func (f *metaTablesSource) OpenSnapshot() types.Snapshot {
	s := f.freeze()
	f.mu.Lock()
	f.epoch++
	epoch := f.epoch
	f.open[epoch] = s
	f.mu.Unlock()
	return types.Snapshot{Epoch: epoch, LimitRowID: types.RowID(len(s.rows) + 1)}
}

// CloseSnapshot solta o catálogo congelado. Sem isto o Store acumularia uma
// cópia do catálogo por consulta, pela vida do processo.
func (f *metaTablesSource) CloseSnapshot(snap types.Snapshot) {
	f.mu.Lock()
	delete(f.open, snap.Epoch)
	f.mu.Unlock()
}

// freeze monta as linhas a partir do catálogo corrente. Os textos saem prontos:
// o que a linha faz depois é só copiá-los para o payload.
func (f *metaTablesSource) freeze() *metaTablesRows {
	tables, err := f.eng.Catalog(f.dataset)
	if err != nil {
		return &metaTablesRows{err: err}
	}
	s := &metaTablesRows{
		rows: make([]metaTablesRow, 0, len(tables)),
		byID: make(map[string]int, len(tables)),
	}
	for _, t := range tables {
		id := t.Name
		if t.Namespace != "" {
			id = t.Namespace + "." + t.Name
		}
		if _, dup := s.byID[id]; dup {
			return &metaTablesRows{err: fmt.Errorf("query: %s.%s: duas tabelas endereçadas por %q", meta.Namespace, TableMetaTables, id)}
		}
		s.byID[id] = len(s.rows)
		s.rows = append(s.rows, metaTablesRow{
			id:          []byte(id),
			name:        []byte(t.Name),
			schemaName:  []byte(t.Schema),
			namespace:   []byte(t.Namespace),
			kind:        []byte(t.Kind),
			columns:     int64(len(t.Columns)),
			description: []byte(t.Description),
		})
	}
	return s
}

// rowsOf resolve o catálogo congelado de um snapshot. Época desconhecida é
// chamador que usa um snapshot já fechado, ou que nunca o abriu neste Store —
// bug, e por isso erro nomeando a época em vez de "nenhuma linha".
func (f *metaTablesSource) rowsOf(snap types.Snapshot) (*metaTablesRows, error) {
	f.mu.Lock()
	s, ok := f.open[snap.Epoch]
	f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("query: %s.%s: snapshot %d não está aberto", meta.Namespace, TableMetaTables, snap.Epoch)
	}
	return s, s.err
}

func (f *metaTablesSource) ScanLive(snap types.Snapshot) types.RowIDIterator {
	s, err := f.rowsOf(snap)
	if err != nil {
		return &metaTablesIterator{err: err}
	}
	return &metaTablesIterator{limit: types.RowID(len(s.rows) + 1)}
}

func (f *metaTablesSource) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	s, err := f.rowsOf(snap)
	if err != nil {
		return types.Record{}, buf, false, err
	}
	if id == 0 || id >= snap.LimitRowID || int(id) > len(s.rows) {
		return types.Record{}, buf, false, nil
	}
	return f.record(s, id, buf)
}

func (f *metaTablesSource) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	s, err := f.rowsOf(snap)
	if err != nil {
		return types.Record{}, buf, false, err
	}
	i, ok := s.byID[string(key)]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.GetByRowID(snap, types.RowID(i+1), buf)
}

// Indexed responde SEMPRE false, como nas outras tabelas virtuais: sem índice
// declarado o planner varre as linhas — uma por tabela do dataset — e a key (o
// endereço do FROM) dá o point lookup por conta própria.
func (f *metaTablesSource) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: rede de segurança, como no _meta e no _counters.
func (f *metaTablesSource) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("query: a tabela %s.%s não tem índice (campo %d)", meta.Namespace, TableMetaTables, field)
}

// record codifica UMA tabela do catálogo como linha. Coluna ausente é NULL: o
// namespace na tabela base, e a descrição enquanto nada a declara.
func (f *metaTablesSource) record(s *metaTablesRows, row types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	l := s.rows[row-1]
	buf = buf[:0]
	var err error
	for _, col := range f.columns {
		var v types.Value
		switch col.k {
		case mtID:
			v = types.Value{Type: types.TypeString, Bytes: l.id}
		case mtName:
			v = types.Value{Type: types.TypeString, Bytes: l.name}
		case mtSchema:
			v = types.Value{Type: types.TypeString, Bytes: l.schemaName}
		case mtNamespace:
			if len(l.namespace) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: l.namespace}
		case mtKind:
			v = types.Value{Type: types.TypeString, Bytes: l.kind}
		case mtColumns:
			v = types.Value{Type: types.TypeInt64, I64: l.columns}
		case mtDescription:
			if len(l.description) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: l.description}
		}
		if buf, err = encoding.AppendField(buf, col.id, v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("query: codificando a coluna %s da tabela %q: %w", metaTablesColumnNames[col.k], l.id, err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: l.id, Payload: buf}, buf, true, nil
}

// metaTablesIterator percorre 1..limite-1: toda linha é viva. Com erro na
// montagem ele não entrega linha nenhuma e o Err() diz por quê.
type metaTablesIterator struct {
	limit types.RowID
	id    types.RowID
	err   error
}

func (it *metaTablesIterator) Next() bool {
	if it.id+1 < it.limit {
		it.id++
		return true
	}
	it.id = it.limit
	return false
}

func (it *metaTablesIterator) RowID() types.RowID { return it.id }
func (it *metaTablesIterator) Err() error         { return it.err }
func (it *metaTablesIterator) Close() error       { return nil }
