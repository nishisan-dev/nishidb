package counters

import (
	"fmt"
	"sort"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TableMetaCounterMetrics é a tabela `_meta.counter_metrics`: uma linha por
// métrica de janela dos contadores do dataset consultado, com a definição e o
// custo corrente. Responde por SQL "que métricas existem, de que contador, com
// que janela e que colunas elas geram" — a coluna gerada é o nome que o SELECT
// e a condição do alarme citam, e sem esta tabela quem escreve a condição teria
// de reconstruí-lo a partir do YAML.
//
// Mora aqui, ao lado da `_meta.counters`, pela mesma razão dela: a definição
// vem da spec do contador e o uso vem do amostrador, e o pacote que já cola
// schema, motor e fontes vivas é este.
const TableMetaCounterMetrics = "counter_metrics"

// metricIDSeparator separa contador e métrica na key `id`. Nome de contador e
// de métrica não aceitam `|`, então a key é inequívoca.
const metricIDSeparator = "|"

// metricListSeparator separa os itens das colunas `aggregates` e `columns`.
const metricListSeparator = ","

// metricsMetaTableSpec é a spec da tabela, no MESMO YAML de um dataset de
// verdade, como a `_meta.counters`.
//
// A key é `id` (`<counter>|<metric>`), e não o nome da métrica: o nome é livre
// por contador, e dois contadores podem declarar `auth_5m` cada um.
//
// `groups`, `heapBytes`, `diskBytes` e `sampledAt` são o uso lido do
// amostrador, e são NULL sem ele — o CLI local e um processo sem laço de
// amostragem não medem nada, e zero seria uma medida inventada. `sampledAt` é
// NULL também antes da primeira amostra: "nunca amostrou" e "amostrou agora"
// são afirmações diferentes.
const metricsMetaTableSpec = `version: 1
name: ` + TableMetaCounterMetrics + `
schemaName: ` + schema.SyntheticSchemaName + `
materialization:
  key:
    field: id
schema:
  include: [id, counter, name, type, source, windowMs, bucketMs, aggregates, columns, groups, heapBytes, diskBytes, sampledAt]
  fields:
    id:
      type: string
    counter:
      type: string
    name:
      type: string
    type:
      type: string
    source:
      type: string
    windowMs:
      type: int64
    bucketMs:
      type: int64
    aggregates:
      type: string
    columns:
      type: string
    groups:
      type: int64
    heapBytes:
      type: int64
    diskBytes:
      type: int64
    sampledAt:
      type: timestamp
`

// metricsMetaColumn identifica cada coluna da tabela, para a linha ser montada
// por um switch em vez de por nome.
type metricsMetaColumn uint8

const (
	mmID metricsMetaColumn = iota
	mmCounter
	mmName
	mmType
	mmSource
	mmWindowMs
	mmBucketMs
	mmAggregates
	mmColumns
	mmGroups
	mmHeapBytes
	mmDiskBytes
	mmSampledAt
)

// metricsMetaColumnNames casa, posição a posição, com as constantes acima.
var metricsMetaColumnNames = [...]string{
	"id", "counter", "name", "type", "source", "windowMs", "bucketMs",
	"aggregates", "columns", "groups", "heapBytes", "diskBytes", "sampledAt",
}

// TableMetaMetrics monta o schema sintético e o Store virtual de
// `_meta.counter_metrics` sobre os contadores dados, na ordem dada, com as
// métricas de cada um na ordem declarada. A lista pode ser vazia, e os
// contadores podem não ter métrica: a tabela existe em todo dataset, e
// "nenhuma" é uma resposta.
//
// O conjunto de linhas é fixo pela vida do processo — métrica só nasce na
// abertura do dataset —, então o rowID é a posição mais um, a key resolve por
// mapa, e os textos são calculados aqui. O que muda entre duas consultas é o
// uso, lido no presente a cada linha.
//
// src resolve o leitor de cada métrica UMA vez, aqui; nil deixa o uso NULL (ver
// MetricSource).
func TableMetaMetrics(specs []schema.CounterSpec, src MetricSource) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(metricsMetaTableSpec), schema.BuiltinDefaults)
	if err != nil {
		return nil, nil, fmt.Errorf("counters: invalid spec for table %s.%s: %w", meta.Namespace, TableMetaCounterMetrics, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, nil, fmt.Errorf("counters: compiling table %s.%s: %w", meta.Namespace, TableMetaCounterMetrics, err)
	}
	f := &metricsMetaSource{
		byID:    make(map[string]int),
		columns: make([]metricsMetaColumnWithID, 0, len(metricsMetaColumnNames)),
	}
	for k, name := range metricsMetaColumnNames {
		id, err := idOfColumn(sc, name)
		if err != nil {
			return nil, nil, err
		}
		f.columns = append(f.columns, metricsMetaColumnWithID{id: id, k: metricsMetaColumn(k)})
	}
	// O payload exige FieldID crescente; a ordem é resolvida UMA vez aqui, e a
	// linha é montada sem ordenar.
	sort.Slice(f.columns, func(i, j int) bool { return f.columns[i].id < f.columns[j].id })
	for _, cs := range specs {
		for _, m := range cs.Metrics {
			row, err := newMetricsMetaRow(cs.Name, m, src)
			if err != nil {
				return nil, nil, err
			}
			if _, exists := f.byID[string(row.id)]; exists {
				return nil, nil, fmt.Errorf("counters: metric %q of counter %q is declared twice", m.Name, cs.Name)
			}
			f.byID[string(row.id)] = len(f.rows)
			f.rows = append(f.rows, row)
		}
	}
	return sc, f, nil
}

// newMetricsMetaRow calcula os textos e os números fixos de uma métrica.
func newMetricsMetaRow(counterName string, m schema.CounterMetricSpec, src MetricSource) (metricsMetaRow, error) {
	row := metricsMetaRow{
		id:         []byte(counterName + metricIDSeparator + m.Name),
		counter:    []byte(counterName),
		name:       []byte(m.Name),
		kind:       []byte(m.Type),
		source:     []byte(m.Source),
		windowMs:   m.Window.Milliseconds(),
		bucketMs:   (m.Window / schema.MetricWindowBuckets).Milliseconds(),
		aggregates: []byte(strings.Join(m.Aggregates, metricListSeparator)),
		columns:    []byte(strings.Join(m.Columns(), metricListSeparator)),
	}
	if src == nil {
		return row, nil
	}
	reader, ok := src.Metric(counterName, m.Name)
	if !ok || reader == nil {
		return row, fmt.Errorf("counters: the metric source has no reader for metric %q of counter %q", m.Name, counterName)
	}
	row.reader = reader
	return row, nil
}

type metricsMetaColumnWithID struct {
	id types.FieldID
	k  metricsMetaColumn
}

// metricsMetaRow é uma métrica como linha: os textos são calculados uma vez, na
// montagem, para que a linha não aloque.
type metricsMetaRow struct {
	reader     MetricReader // nil sem MetricSource: o uso sai NULL
	id         []byte
	counter    []byte
	name       []byte
	kind       []byte
	source     []byte
	windowMs   int64
	bucketMs   int64
	aggregates []byte
	columns    []byte
}

// metricsMetaSource é o Store virtual de `_meta.counter_metrics`.
type metricsMetaSource struct {
	rows    []metricsMetaRow
	byID    map[string]int
	columns []metricsMetaColumnWithID // em ordem crescente de FieldID
}

// OpenSnapshot devolve o limite de linhas; o conjunto é fixo, então não há o
// que congelar além dele. Época e instante ficam zerados e são ignorados.
func (f *metricsMetaSource) OpenSnapshot() types.Snapshot {
	return types.Snapshot{LimitRowID: types.RowID(len(f.rows) + 1)}
}

// CloseSnapshot é no-op: a fonte não retém nada.
func (f *metricsMetaSource) CloseSnapshot(types.Snapshot) {}

func (f *metricsMetaSource) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &metaIterator{limit: snap.LimitRowID}
}

func (f *metricsMetaSource) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if id == 0 || id >= snap.LimitRowID || int(id) > len(f.rows) {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, buf)
}

func (f *metricsMetaSource) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	i, ok := f.byID[string(key)]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.GetByRowID(snap, types.RowID(i+1), buf)
}

// Indexed responde SEMPRE false, como nas outras tabelas virtuais: sem índice
// declarado o planner varre as linhas — uma por métrica — e a key (o id) dá o
// point lookup por conta própria.
func (f *metricsMetaSource) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: rede de segurança, como no _meta e no _counters.
func (f *metricsMetaSource) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("counters: table %s.%s has no index (field %d)", meta.Namespace, TableMetaCounterMetrics, field)
}

// record codifica UMA métrica como uma linha, lendo o uso no presente. Coluna
// ausente é NULL: o uso inteiro sem leitor, e `sampledAt` antes da primeira
// amostra.
func (f *metricsMetaSource) record(row types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	l := &f.rows[row-1]
	var groups, heapBytes, diskBytes, sampledAtUnixMs int64
	if l.reader != nil {
		groups, heapBytes, diskBytes, sampledAtUnixMs = l.reader.Usage()
	}
	buf = buf[:0]
	var err error
	for _, col := range f.columns {
		var v types.Value
		switch col.k {
		case mmID:
			v = types.Value{Type: types.TypeString, Bytes: l.id}
		case mmCounter:
			v = types.Value{Type: types.TypeString, Bytes: l.counter}
		case mmName:
			v = types.Value{Type: types.TypeString, Bytes: l.name}
		case mmType:
			v = types.Value{Type: types.TypeString, Bytes: l.kind}
		case mmSource:
			v = types.Value{Type: types.TypeString, Bytes: l.source}
		case mmWindowMs:
			v = types.Value{Type: types.TypeInt64, I64: l.windowMs}
		case mmBucketMs:
			v = types.Value{Type: types.TypeInt64, I64: l.bucketMs}
		case mmAggregates:
			v = types.Value{Type: types.TypeString, Bytes: l.aggregates}
		case mmColumns:
			v = types.Value{Type: types.TypeString, Bytes: l.columns}
		case mmGroups:
			if l.reader == nil {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: groups}
		case mmHeapBytes:
			if l.reader == nil {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: heapBytes}
		case mmDiskBytes:
			if l.reader == nil {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: diskBytes}
		case mmSampledAt:
			if l.reader == nil || sampledAtUnixMs == 0 {
				continue
			}
			v = types.Value{Type: types.TypeTimestamp, I64: sampledAtUnixMs}
		}
		if buf, err = encoding.AppendField(buf, col.id, v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("counters: encoding column %s of metric %q: %w", metricsMetaColumnNames[col.k], l.id, err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: l.id, Payload: buf}, buf, true, nil
}
