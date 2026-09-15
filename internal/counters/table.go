package counters

import (
	"fmt"
	"math"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// maxGroupingColumns é o teto de colunas do groupBy que a tabela virtual
// monta sem alocar: a linha é composta num array de pilha desse tamanho mais
// as colunas fixas e as das métricas (maxRowColumns).
const maxGroupingColumns = 16

// maxAggregatesPerMetric é o número de agregados distintos que uma métrica de
// janela pode declarar: avg, min e max. Repetir um agregado repete a coluna
// gerada, e isso a recusa de colisão do TableSchema pega; agregado fora dos três
// não tem tipo de coluna e também é recusado ali. Por isso uma métrica nunca
// gera mais colunas que este número.
const maxAggregatesPerMetric = 3

// maxRowColumns é o teto de colunas de UMA linha da tabela e o tamanho do array
// de pilha em que record a compõe: o groupBy no teto (maxGroupingColumns), as
// duas fixas (groupKey e total) e as geradas pelas métricas —
// schema.MaxCounterMetrics métricas com os maxAggregatesPerMetric agregados
// cada. Os três tetos são conferidos no TableSchema, e é isso que garante que a
// linha nunca transborda o array nem precisa de uma fatia do heap por grupo.
const maxRowColumns = maxGroupingColumns + 2 + schema.MaxCounterMetrics*maxAggregatesPerMetric

// Reader é o que a tabela virtual precisa do contador. Interface, e não o
// *counter.Counter, para que o teste da tabela e o do contador não se
// conheçam além do contrato — *counter.Counter a satisfaz.
type Reader interface {
	// Limit é o maior id de grupo emitido mais um.
	Limit() uint32
	// Group devolve a chave composta, o texto (groupKey), o total e os refs do
	// grupo, e se ele está vivo. Chave e texto são estáveis.
	Group(id uint32) (key, text []byte, total, refs int64, live bool)
	// ByText resolve o groupKey de um grupo vivo para o id.
	ByText(text []byte) (uint32, bool)
}

// MetricReader lê o valor publicado de UMA métrica de janela.
//
// A interface é declarada aqui, no consumidor, e não no pacote que amostra: a
// tabela só precisa ler o publicado, e o amostrador tem relógio — importá-lo
// traria o relógio para o pacote que cola schema, motor e storage.
type MetricReader interface {
	// Aggregate devolve o publicado do grupo; samples == 0 é "sem valor" (NULL).
	// Os retornos chamam-se minimum e maximum, e não min e max, para não
	// sombrear as funções embutidas; o nome não entra no tipo do método.
	//
	// Com samples > 0, avg é FINITO — nem NaN nem ±Inf. A garantia é de quem
	// implementa (o amostrador, que divide uma soma por uma contagem positiva),
	// e não da tabela: ela grava o double como veio, sem conferir por linha,
	// porque a conferência pagaria no caminho de toda consulta e de todo ciclo
	// do alarme o que é propriedade do produtor. Um NaN que escapasse chegaria
	// ao SELECT e à condição do alarme como valor, e não como NULL.
	Aggregate(id uint32) (samples uint64, avg float64, minimum, maximum int64)
	// Usage alimenta _meta.counter_metrics; sampledAtUnixMs == 0 é "nunca amostrou".
	Usage() (groups, heapBytes, diskBytes, sampledAtUnixMs int64)
}

// MetricSource resolve a métrica pelo nome. Resolvido UMA vez na construção
// da tabela, nunca por linha.
//
// Uma MetricSource nil é válida: as colunas das métricas continuam no schema e
// saem sempre NULL, e `_meta.counter_metrics` lista as definições com o uso
// NULL. O schema não pode depender de o laço de amostragem estar de pé, porque
// a tabela e a condição de um alarme precisam do MESMO schema haja amostrador
// ou não — um `auth_5m_avg < 1` que compilasse só com o amostrador vivo seria
// recusado ou aceito conforme a ordem de subida, e não conforme o YAML.
//
// Com a fonte presente, uma métrica declarada que ela não resolve é erro na
// montagem, e não coluna NULL: seria a tabela calando para sempre uma métrica
// que o operador declarou.
type MetricSource interface {
	Metric(counter, metric string) (MetricReader, bool)
}

// Table monta o schema sintético e o Store virtual de `_counters.<nome>`.
//
// O schema é compilado do MESMO YAML de um dataset de verdade, como o `_meta`
// faz: reusar o compilador é o que garante que a tabela obedeça às mesmas
// regras — tipos, chave, ordem determinística de FieldID — que qualquer outro
// dataset, e o que dá WHERE, ORDER BY, LIMIT e projeção sem gramática nova.
//
// A tabela tem a coluna `groupKey` como key (o texto canônico do grupo — a
// key da v1 é uma coluna string única, e um groupBy composto não tem coluna
// que identifique a linha), as colunas do groupBy abertas e tipadas, `total`
// e as colunas das métricas de janela (`<metric>_<aggregate>`). Um grupo que
// chegou a zero permanece com total 0 (é o equipamento que apagou por inteiro,
// e é o caso que interessa); um grupo aposentado não aparece.
//
// A tabela lê o PRESENTE: os totais e os agregados das métricas não são
// versionados pelo journal do snapshot. O que o snapshot congela é o CONJUNTO
// de grupos — um grupo que nasce no meio da varredura não entra no universo
// dela.
//
// metrics são as métricas declaradas do contador, e src resolve o leitor de
// cada uma (ver MetricSource, inclusive sobre src nil).
func Table(def counter.Definition, l Reader, metrics []schema.CounterMetricSpec, src MetricSource) (*schema.Compiled, query.Store, error) {
	sc, err := TableSchema(def, metrics)
	if err != nil {
		return nil, nil, err
	}
	f := &source{l: l, columns: make([]types.FieldID, len(def.Names))}
	if f.idKey, err = idOfColumn(sc, schema.CounterKeyColumn); err != nil {
		return nil, nil, err
	}
	if f.idTotal, err = idOfColumn(sc, schema.CounterTotalColumn); err != nil {
		return nil, nil, err
	}
	for i, n := range def.Names {
		if f.columns[i], err = idOfColumn(sc, n); err != nil {
			return nil, nil, err
		}
	}
	if f.metrics, err = bindMetrics(sc, def.Name, metrics, src); err != nil {
		return nil, nil, err
	}
	return sc, f, nil
}

// TableSchema monta e compila o schema sintético da tabela de um contador, sem
// o Store virtual.
//
// Existe separada da Table porque quem compila uma condição sobre as colunas do
// contador — o `when` de um alarme — precisa do schema e não tem Reader para
// dar. Exigir um obrigaria o declarante a inventar um leitor vazio só para
// compilar texto, e um leitor inventado é a porta para a condição acabar
// compilada contra um schema que não é o da tabela de verdade.
//
// As colunas das métricas entram depois de `total`, na ordem declarada (métrica,
// depois agregado): `avg` como double, `min` e `max` como int64. Todas são
// nuláveis — sem amostra na janela a coluna fica ausente da linha.
func TableSchema(def counter.Definition, metrics []schema.CounterMetricSpec) (*schema.Compiled, error) {
	if len(def.Columns) == 0 || len(def.Columns) > maxGroupingColumns {
		return nil, fmt.Errorf("counters: %q has %d grouping columns; the table accepts 1 to %d", def.Name, len(def.Columns), maxGroupingColumns)
	}
	if len(def.Names) != len(def.Columns) || len(def.Types) != len(def.Columns) {
		return nil, fmt.Errorf("counters: %q has Names, Types and Columns of different lengths", def.Name)
	}
	// O teto de métricas é conferido aqui, e não só no YAML, porque é dele que
	// sai o tamanho do array de pilha de record (maxRowColumns): uma Definition
	// montada sem YAML com métricas demais transbordaria a linha.
	if len(metrics) > schema.MaxCounterMetrics {
		return nil, fmt.Errorf("counters: %q declares %d metrics; the table accepts at most %d", def.Name, len(metrics), schema.MaxCounterMetrics)
	}
	// A colisão sai da MESMA lista de colunas que a validação do YAML usa
	// (schema.CounterColumns), e não de uma relação própria de colunas fixas.
	// Aqui ela é defensiva: toda Definition compilada de um YAML já foi
	// recusada na subida se colidisse. Fica porque a Definition é montável sem
	// YAML, e sem ela o schema sintético declararia a mesma coluna duas vezes.
	if first, second, found := schema.FindCounterColumnCollision(def.Names, metrics); found {
		return nil, fmt.Errorf("counters: %q: %s collides with %s", def.Name, second, first)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "version: 1\nname: %s\nschemaName: %s\nmaterialization:\n  key:\n    field: %s\nschema:\n  include: [%s",
		def.Name, schema.SyntheticSchemaName, schema.CounterKeyColumn, schema.CounterKeyColumn)
	for _, n := range def.Names {
		fmt.Fprintf(&sb, ", %s", n)
	}
	fmt.Fprintf(&sb, ", %s", schema.CounterTotalColumn)
	for _, m := range metrics {
		for _, col := range m.Columns() {
			fmt.Fprintf(&sb, ", %s", col)
		}
	}
	fmt.Fprintf(&sb, "]\n  fields:\n    %s:\n      type: string\n", schema.CounterKeyColumn)
	for i, n := range def.Names {
		kind, err := typeName(def.Types[i])
		if err != nil {
			return nil, fmt.Errorf("counters: %q, column %q: %w", def.Name, n, err)
		}
		fmt.Fprintf(&sb, "    %s:\n      type: %s\n", n, kind)
	}
	fmt.Fprintf(&sb, "    %s:\n      type: int64\n", schema.CounterTotalColumn)
	for _, m := range metrics {
		for _, a := range m.Aggregates {
			agg, err := aggregateOf(a)
			if err != nil {
				return nil, fmt.Errorf("counters: %q, metric %q: %w", def.Name, m.Name, err)
			}
			fmt.Fprintf(&sb, "    %s:\n      type: %s\n", schema.MetricColumnName(m.Name, a), agg.typeName())
		}
	}

	spec, err := schema.Parse([]byte(sb.String()), schema.BuiltinDefaults)
	if err != nil {
		return nil, fmt.Errorf("counters: invalid spec for table %s.%s: %w", schema.CountersNamespace, def.Name, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, fmt.Errorf("counters: compiling table %s.%s: %w", schema.CountersNamespace, def.Name, err)
	}
	return sc, nil
}

func typeName(t types.FieldType) (string, error) {
	switch t {
	case types.TypeString:
		return "string", nil
	case types.TypeInt64:
		return "int64", nil
	case types.TypeBool:
		return "boolean", nil
	default:
		return "", fmt.Errorf("type %#x has no column in the counter table (groupBy accepts string, int64 and boolean)", t)
	}
}

func idOfColumn(sc *schema.Compiled, name string) (types.FieldID, error) {
	f, ok := sc.FieldByName([]byte(name))
	if !ok {
		return 0, fmt.Errorf("counters: column %q has no FieldID in the synthetic schema", name)
	}
	return f.ID, nil
}

// aggregate identifica o agregado de uma coluna de métrica, para a linha ser
// montada por um switch em vez de por comparação de texto.
type aggregate uint8

const (
	aggregateAvg aggregate = iota
	aggregateMin
	aggregateMax
)

// aggregateOf traduz o agregado declarado. O YAML já recusou o desconhecido;
// a recusa aqui é para a Definition montada sem ele.
func aggregateOf(name string) (aggregate, error) {
	switch name {
	case schema.AggregateAvg:
		return aggregateAvg, nil
	case schema.AggregateMin:
		return aggregateMin, nil
	case schema.AggregateMax:
		return aggregateMax, nil
	default:
		return 0, fmt.Errorf("aggregate %q has no column in the counter table (known: %s, %s, %s)",
			name, schema.AggregateAvg, schema.AggregateMin, schema.AggregateMax)
	}
}

// typeName é o tipo da coluna do agregado: a média é fracionária, e o mínimo e
// o máximo são valores que o `total` de fato teve, por isso inteiros como ele.
func (a aggregate) typeName() string {
	if a == aggregateAvg {
		return "double"
	}
	return "int64"
}

// metricBinding é uma métrica ligada às suas colunas: o leitor resolvido na
// montagem e o FieldID de cada agregado declarado, na ordem declarada.
type metricBinding struct {
	reader  MetricReader
	columns []metricColumn
}

// metricColumn é uma coluna gerada por uma métrica.
type metricColumn struct {
	id  types.FieldID
	agg aggregate
}

// bindMetrics resolve o leitor de cada métrica UMA vez, e os FieldIDs das
// colunas dela. Com src nil não há ligação nenhuma: as colunas existem no
// schema e a linha nunca as escreve (ver MetricSource).
func bindMetrics(sc *schema.Compiled, counterName string, metrics []schema.CounterMetricSpec, src MetricSource) ([]metricBinding, error) {
	if src == nil || len(metrics) == 0 {
		return nil, nil
	}
	out := make([]metricBinding, 0, len(metrics))
	for _, m := range metrics {
		reader, ok := src.Metric(counterName, m.Name)
		if !ok || reader == nil {
			return nil, fmt.Errorf("counters: the metric source has no reader for metric %q of counter %q", m.Name, counterName)
		}
		b := metricBinding{reader: reader, columns: make([]metricColumn, 0, len(m.Aggregates))}
		for _, a := range m.Aggregates {
			agg, err := aggregateOf(a)
			if err != nil {
				return nil, fmt.Errorf("counters: %q, metric %q: %w", counterName, m.Name, err)
			}
			id, err := idOfColumn(sc, schema.MetricColumnName(m.Name, a))
			if err != nil {
				return nil, err
			}
			b.columns = append(b.columns, metricColumn{id: id, agg: agg})
		}
		out = append(out, b)
	}
	return out, nil
}

// Register declara o namespace `_counters` sob o dataset, registra a tabela
// de cada contador aberto no Store, a `_meta.counters` que os lista e a
// `_meta.counter_metrics` que lista as métricas deles — as duas de `_meta`
// SEMPRE, mesmo sem contador ou sem métrica nenhuma: "quais existem" tem
// resposta quando é nenhum. Chamado por todo caminho que registra um dataset no
// motor — servidor e CLI — para que `FROM _counters.<nome>` e as tabelas de
// `_meta` funcionem da mesma forma nos dois.
//
// specs são os contadores declarados no YAML do dataset; cada contador aberto é
// casado com o seu pelo nome, que é de onde vêm as métricas. Um contador aberto
// sem spec é erro de programação — o Store só abre o que o YAML declara —, e
// segui-lo sem métricas publicaria uma tabela com schema diferente do que o
// alarme compila. src é a fonte das métricas; nil é válido (ver MetricSource).
func Register(eng *query.Engine, dataset string, st *storage.Store, specs []schema.CounterSpec, src MetricSource) error {
	if err := eng.DeclaraNamespace(dataset, schema.CountersNamespace); err != nil {
		return err
	}
	specByName := make(map[string]int, len(specs))
	for i, s := range specs {
		specByName[s.Name] = i
	}
	cs := st.Counters()
	listed := make([]ListedCounter, 0, len(cs))
	opened := make([]schema.CounterSpec, 0, len(cs))
	for _, c := range cs {
		def := c.Def()
		i, ok := specByName[def.Name]
		if !ok {
			return fmt.Errorf("counters: counter %q is open in dataset %q but has no declared spec", def.Name, dataset)
		}
		sc, store, err := Table(def, c, specs[i].Metrics, src)
		if err != nil {
			return err
		}
		if err := eng.RegisterVirtual(dataset, schema.CountersNamespace, def.Name, sc, store); err != nil {
			return err
		}
		listed = append(listed, c)
		opened = append(opened, specs[i])
	}
	msc, metaStore, err := TableMeta(listed)
	if err != nil {
		return err
	}
	if err := eng.RegisterVirtual(dataset, meta.Namespace, TableMetaCounters, msc, metaStore); err != nil {
		return err
	}
	// Só as métricas dos contadores ABERTOS, na ordem deles: a coluna `counter`
	// aponta para uma linha de `_meta.counters`, e um contador declarado que o
	// Store não abriu não está lá.
	mmsc, metricsStore, err := TableMetaMetrics(opened, src)
	if err != nil {
		return err
	}
	return eng.RegisterVirtual(dataset, meta.Namespace, TableMetaCounterMetrics, mmsc, metricsStore)
}

// source é o Store virtual: cada rowID é um id de grupo, e o record é
// codificado sob demanda a partir do que o contador tem em memória.
type source struct {
	l       Reader
	idKey   types.FieldID
	idTotal types.FieldID
	columns []types.FieldID // FieldID de cada coluna do groupBy, na ordem declarada
	metrics []metricBinding // só as métricas com leitor; vazio sem MetricSource
}

// OpenSnapshot congela o conjunto de grupos: ids a partir do limite nasceram
// depois da consulta. Época e instante ficam zerados e são ignorados — os
// totais são lidos no presente.
func (f *source) OpenSnapshot() types.Snapshot {
	return types.Snapshot{LimitRowID: types.RowID(f.l.Limit())}
}

// CloseSnapshot é no-op: a fonte não retém nada.
func (f *source) CloseSnapshot(types.Snapshot) {}

func (f *source) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &iterator{l: f.l, limit: uint32(snap.LimitRowID)}
}

func (f *source) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if id == 0 || id >= snap.LimitRowID {
		return types.Record{}, buf, false, nil
	}
	group := uint32(id)
	key, text, total, _, live := f.l.Group(group)
	if !live {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, group, key, text, total, buf)
}

func (f *source) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	id, ok := f.l.ByText(key)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.GetByRowID(snap, types.RowID(id), buf)
}

// Indexed responde SEMPRE false: a tabela não declara índice, e o planner
// degrada para a varredura dos grupos — que sobre milhares de linhas é o
// plano certo. A key (groupKey) dá o point lookup por conta própria.
func (f *source) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: sem índice declarado o planner não escolhe plano
// indexado. Rede de segurança, como no _meta.
func (f *source) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("counters: the virtual table has no index (field %d)", field)
}

type pair struct {
	id types.FieldID
	v  types.Value
}

// record codifica UM grupo como uma linha. O payload exige FieldID crescente;
// os pares são ordenados num array de pilha, sem alocar — a linha é montada
// por grupo a cada consulta do alarme.
func (f *source) record(row types.RowID, group uint32, key, text []byte, total int64, buf []byte) (types.Record, []byte, bool, error) {
	var pairs [maxRowColumns]pair
	n := 0
	pairs[n] = pair{f.idKey, types.Value{Type: types.TypeString, Bytes: text}}
	n++
	pairs[n] = pair{f.idTotal, types.Value{Type: types.TypeInt64, I64: total}}
	n++
	err := valuekey.WalkComposite(key, func(i int, v types.Value) error {
		if i >= len(f.columns) {
			return fmt.Errorf("counters: the key of group %d has more components than the groupBy", row)
		}
		pairs[n] = pair{f.columns[i], v}
		n++
		return nil
	})
	if err != nil {
		return types.Record{}, buf, false, err
	}
	if n != len(f.columns)+2 {
		return types.Record{}, buf, false, fmt.Errorf("counters: the key of group %d has %d components, the groupBy has %d", row, n-2, len(f.columns))
	}
	for _, m := range f.metrics {
		samples, avg, lowest, highest := m.reader.Aggregate(group)
		if samples == 0 {
			// NULL é ausência: sem amostra na janela as colunas não entram no
			// payload, e é assim que `IS NULL` as acha. Escrever 0 afirmaria uma
			// média de zero autenticados que ninguém mediu — e é justamente o
			// valor que um `auth_5m_avg < 1` dispararia.
			continue
		}
		for _, c := range m.columns {
			v := types.Value{Type: types.TypeInt64}
			switch c.agg {
			case aggregateAvg:
				v = types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(avg))}
			case aggregateMin:
				v.I64 = lowest
			case aggregateMax:
				v.I64 = highest
			}
			pairs[n] = pair{c.id, v}
			n++
		}
	}
	for i := 1; i < n; i++ {
		for j := i; j > 0 && pairs[j-1].id > pairs[j].id; j-- {
			pairs[j-1], pairs[j] = pairs[j], pairs[j-1]
		}
	}
	buf = buf[:0]
	for i := 0; i < n; i++ {
		if buf, err = encoding.AppendField(buf, pairs[i].id, pairs[i].v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("counters: encoding column %d of group %d: %w", pairs[i].id, row, err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: text, Payload: buf}, buf, true, nil
}

// iterator percorre os ids de grupo vivos abaixo do limite congelado.
type iterator struct {
	l     Reader
	limit uint32
	id    uint32
}

func (it *iterator) Next() bool {
	for it.id+1 < it.limit {
		it.id++
		if _, _, _, _, live := it.l.Group(it.id); live {
			return true
		}
	}
	it.id = it.limit
	return false
}

func (it *iterator) RowID() types.RowID { return types.RowID(it.id) }
func (it *iterator) Err() error         { return nil }
func (it *iterator) Close() error       { return nil }
