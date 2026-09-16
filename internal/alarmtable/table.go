// Package alarmtable serve a view `<schema>.alarms`, atalho `_counters.alarms`:
// os alarmes em observação no dataset consultado, um por par (alarme, grupo).
//
// Mora fora de `internal/alarm` e fora de `internal/query` porque precisa dos
// dois lados, e nenhum dos dois pode passar a importar o outro — é o mesmo
// argumento do `internal/counters` e do `internal/metakafka`.
//
// # Por que sob `_counters`, e não sob `_meta`
//
// A tabela DERIVA dos contadores — todo alarme observa um `_counters.<nome>` —
// e o catálogo já publica toda tabela virtual fora de `_meta` no schema do
// dataset (`internal/query/catalog.go`, `catalogEntries`). `_meta` descreve o
// MOTOR (kafka, counters, tabelas, usuários); esta tabela descreve o DADO — o
// mesmo corte que já separa `_counters.<nome>` de `_meta.counters`. Por isso
// ela não é mais `_meta.alarms`: a quebra é deliberada, registrada no plano
// `planning/alarmes-como-view-do-schema.md`, e sem legado — a v0.9.1, única
// release publicada até aqui, aceita a mudança de contrato.
//
// O nome `alarms` é RESERVADO (`schema.ReservedAlarmsTable`): nenhum contador,
// dataset ou tabela histórica pode se chamar assim, para que a view nunca
// colida com uma tabela de verdade.
//
// # O que ela responde e o que as métricas não respondem
//
// As séries de alarme dão os agregados: quantos grupos em observação, quantos
// disparados, quantos disparos desde a subida. Esta tabela dá o DETALHE — QUAL
// equipamento, desde quando, com que valor disparou, e se o prazo já se
// cumpriu. É a diferença entre "há três OLTs alarmadas" e "a OLT_14 está assim
// desde 03:14, com auth_5m_avg = 0.6".
package alarmtable

import (
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TableName é o nome da view, sempre `schema.ReservedAlarmsTable`.
const TableName = schema.ReservedAlarmsTable

// Estados da coluna `state`. Enum fechado, em inglês, como os do evento.
const (
	// StatePending: a condição vale e o prazo ainda não se cumpriu. "Vai
	// alarmar em quarenta segundos" é informação, e é o que esta linha dá.
	StatePending = "pending"
	// StateFiring: o disparo já foi anunciado.
	StateFiring = "firing"
)

// tableSpec é a spec da tabela, no MESMO YAML de um dataset de verdade,
// como `_counters.<nome>` e `_meta.kafka`. Reusar o compilador é o que
// garante que ela obedeça às mesmas regras — tipos, chave, ordem determinística
// de FieldID — que qualquer outro dataset, e o que dá WHERE, ORDER BY, LIMIT e
// projeção sem gramática nova.
//
// `schemaName` fica `schema.SyntheticSchemaName`: é o nome interno da spec, só
// para a validação — a view em si é publicada sob o schema do DATASET, pelo
// `RegisterVirtual` em `Register`. É por isso que a recusa de dataset chamado
// `alarms` isenta `SchemaName == SyntheticSchemaName` (`internal/schema/spec.go`):
// a spec sintética desta view se chama `alarms` e não pode recusar a si mesma.
//
// A key é `id`, composta de contador, alarme e grupo: a key da v1 é uma coluna
// string única, e nenhuma das três identifica a linha sozinha. Os nomes de
// contador e de alarme são [a-z0-9_]+, então o separador `|` só pode aparecer
// dentro do groupKey — o que mantém a decomposição sem ambiguidade e a key
// única.
//
// `condition`, `qualifyCondition` e `sustainedForMs` repetem em toda linha do
// mesmo alarme, e isso é deliberado: quem consulta a tabela não tem o YAML à
// mão, e "total baixo" sem o limiar não é acionável.
//
// `qualifyCondition` é NULL quando o alarme não declara `qualifyWhen`: NULL é
// ausência, e uma string vazia afirmaria uma condição de qualificação vazia que
// ninguém escreveu. É o que deixa `WHERE qualifyCondition IS NULL` separar os
// dois tipos de alarme.
//
// `valueColumn`, `firedAt` e `firingValue` descrevem o disparo:
//   - `valueColumn` é SEMPRE preenchida, pendente ou disparado — é propriedade
//     da REGRA (`value:` no YAML, `total` por padrão), como `condition`;
//   - `firedAt` é o instante da transição para o disparo, e fica NULL enquanto
//     pending: não há disparo a descrever ainda. Não é o mesmo que `since` — um
//     alarme restaurado (`restored=true`) volta pending e dispara de novo, com
//     `firedAt` descrevendo esse NOVO disparo, não o original;
//   - `firingValue` é o valor de `valueColumn` capturado NESSE disparo, e fica
//     NULL enquanto pending, ou se a coluna estava ausente do payload que
//     decidiu — uma métrica sem amostra na janela, por exemplo. É `double`
//     mesmo para uma coluna `int64` (`total`, `_min`, `_max`): a conversão
//     perde precisão acima de 2^53, e é o preço de uma coluna só para os dois
//     tipos de origem.
const tableSpec = `version: 1
name: ` + TableName + `
schemaName: ` + schema.SyntheticSchemaName + `
materialization:
  key:
    field: id
schema:
  include: [id, counter, alarm, groupKey, state, since, condition, qualifyCondition, sustainedForMs, valueColumn, firedAt, firingValue]
  fields:
    id:
      type: string
    counter:
      type: string
    alarm:
      type: string
    groupKey:
      type: string
    state:
      type: string
    since:
      type: timestamp
    condition:
      type: string
    qualifyCondition:
      type: string
    sustainedForMs:
      type: int64
    valueColumn:
      type: string
    firedAt:
      type: timestamp
    firingValue:
      type: double
`

// column é a posição de cada coluna, e columnNames as nomeia na mesma
// ordem. Uma lista só, para que acrescentar coluna seja uma linha em cada.
type column int

const (
	cID column = iota
	cCounter
	cAlarm
	cGroupKey
	cState
	cSince
	cCondition
	cQualifyCondition
	cSustainedForMs
	cValueColumn
	cFiredAt
	cFiringValue
)

var columnNames = []string{
	"id", "counter", "alarm", "groupKey", "state", "since", "condition", "qualifyCondition", "sustainedForMs",
	"valueColumn", "firedAt", "firingValue",
}

// Source é o que a tabela precisa do dataset, RESOLVIDO NA HORA.
//
// Groups é um closure, e não a fatia: o registro acontece na abertura do
// dataset, quando o runner pode nem existir, e o retrato TROCA a cada passada.
// Uma fatia capturada no registro descreveria para sempre o estado da subida.
//
// nil é aceito e vale "nenhum alarme declarado" — que é uma resposta, não um
// erro: `SELECT * FROM alarms` nunca pode falhar por isso.
type Source struct {
	Groups func() []alarm.ActiveGroup
}

// Table monta o schema sintético e o Store virtual da view.
func Table(f Source) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(tableSpec), schema.BuiltinDefaults)
	if err != nil {
		return nil, nil, fmt.Errorf("alarmtable: spec da tabela %s.%s inválida: %w", schema.CountersNamespace, TableName, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, nil, fmt.Errorf("alarmtable: compilando a tabela %s.%s: %w", schema.CountersNamespace, TableName, err)
	}
	fo := &source{f: f, open: map[uint64][]alarm.ActiveGroup{}}
	fo.columns = make([]columnWithID, 0, len(columnNames))
	for k, name := range columnNames {
		field, ok := sc.FieldByName([]byte(name))
		if !ok {
			return nil, nil, fmt.Errorf("alarmtable: coluna %q sem FieldID no schema sintético", name)
		}
		fo.columns = append(fo.columns, columnWithID{id: field.ID, k: column(k)})
	}
	// O payload exige FieldID CRESCENTE, e a ordem do `include` não garante
	// isso — o dicionário atribui os ids por conta própria. A ordem é resolvida
	// UMA vez aqui, e a linha é montada sem ordenar. Sem este passo, o leitor
	// para no primeiro campo fora de ordem e as colunas seguintes chegam como
	// NULL, sem erro em camada nenhuma.
	slices.SortFunc(fo.columns, func(a, b columnWithID) int { return int(a.id) - int(b.id) })
	return sc, fo, nil
}

// columnWithID casa a posição da coluna com o FieldID que o schema lhe deu.
type columnWithID struct {
	id types.FieldID
	k  column
}

// Register declara a view sob o dataset já registrado no motor, dentro de
// `schema.CountersNamespace` — o mesmo namespace de `_counters.<nome>`.
//
// Chamado SEMPRE, inclusive num dataset sem alarme declarado: "este dataset não
// observa nada" é uma resposta, e a tabela vazia a dá.
func Register(eng *query.Engine, dataset string, f Source) error {
	sc, store, err := Table(f)
	if err != nil {
		return err
	}
	return eng.RegisterVirtual(dataset, schema.CountersNamespace, TableName, sc, store)
}

// source é o Store virtual. Registrada UMA vez no motor, serve TODAS as
// consultas concorrentes — então nada do que uma consulta enxerga mora em campo
// mutável compartilhado.
type source struct {
	f Source
	// columns está em ordem CRESCENTE de FieldID, que é a ordem em que o
	// payload tem de ser escrito.
	columns []columnWithID

	mu        sync.Mutex
	nextEpoch uint64
	open      map[uint64][]alarm.ActiveGroup
}

// OpenSnapshot congela o retrato DESTA consulta.
//
// Congelar é barato aqui porque o retrato já vem imutável do runner: a fatia é
// publicada por uma passada e substituída pela seguinte, nunca alterada. O que
// esta função guarda é o ponteiro, para que uma passada no meio da consulta não
// troque o universo dela pela metade.
func (f *source) OpenSnapshot() types.Snapshot {
	var groups []alarm.ActiveGroup
	if f.f.Groups != nil {
		groups = f.f.Groups()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextEpoch++
	epoch := f.nextEpoch
	f.open[epoch] = groups
	// rowID é o índice mais um: eles valem DENTRO deste retrato, e o número
	// não é coluna — nenhuma consulta o enxerga.
	return types.Snapshot{Epoch: epoch, LimitRowID: types.RowID(len(groups)) + 1}
}

// CloseSnapshot devolve o retrato. O executor fecha o par em todos os caminhos
// de saída, inclusive no Open que falha depois de abrir.
func (f *source) CloseSnapshot(snap types.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.open, snap.Epoch)
}

func (f *source) snapshotOf(snap types.Snapshot) ([]alarm.ActiveGroup, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.open[snap.Epoch]
	return g, ok
}

func (f *source) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &iterator{limit: snap.LimitRowID}
}

func (f *source) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	groups, ok := f.snapshotOf(snap)
	if !ok || id < 1 || int(id) > len(groups) {
		return types.Record{}, buf, false, nil
	}
	rec, buf, err := f.record(id, groups[id-1], buf)
	return rec, buf, err == nil, err
}

func (f *source) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	groups, ok := f.snapshotOf(snap)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	for i, g := range groups {
		if string(idOf(g)) == string(key) {
			rec, buf, err := f.record(types.RowID(i)+1, g, buf)
			return rec, buf, err == nil, err
		}
	}
	return types.Record{}, buf, false, nil
}

// Indexed é sempre false: a tabela tem no máximo tantas linhas quantos alarmes
// ativos, e um índice sobre isso seria estrutura para uma varredura que já é
// trivial.
func (f *source) Indexed(types.FieldID) bool { return false }

func (f *source) Lookup(types.FieldID, types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("alarmtable: %w", types.ErrInvalidArgument)
}

// idOf compõe a key da linha.
func idOf(g alarm.ActiveGroup) []byte {
	out := make([]byte, 0, len(g.Counter)+len(g.Alarm)+len(g.GroupKey)+2)
	out = append(out, g.Counter...)
	out = append(out, '|')
	out = append(out, g.Alarm...)
	out = append(out, '|')
	return append(out, g.GroupKey...)
}

// record monta a linha. Os campos saem em ordem crescente de FieldID, que é o
// que o payload exige — e a ordem de declaração no `include` já a produz,
// porque o dicionário em memória atribui os ids na ordem em que os lê.
func (f *source) record(row types.RowID, g alarm.ActiveGroup, buf []byte) (types.Record, []byte, error) {
	id := idOf(g)
	state := StatePending
	if g.Firing {
		state = StateFiring
	}
	values := [...]types.Value{
		cID:               {Type: types.TypeString, Bytes: id},
		cCounter:          {Type: types.TypeString, Bytes: []byte(g.Counter)},
		cAlarm:            {Type: types.TypeString, Bytes: []byte(g.Alarm)},
		cGroupKey:         {Type: types.TypeString, Bytes: []byte(g.GroupKey)},
		cState:            {Type: types.TypeString, Bytes: []byte(state)},
		cSince:            {Type: types.TypeTimestamp, I64: g.Since.UnixMilli()},
		cCondition:        {Type: types.TypeString, Bytes: []byte(g.Condition)},
		cQualifyCondition: {Type: types.TypeString, Bytes: []byte(g.QualifyCondition)},
		cSustainedForMs:   {Type: types.TypeInt64, I64: g.SustainedFor.Milliseconds()},
		cValueColumn:      {Type: types.TypeString, Bytes: []byte(g.ValueColumn)},
		cFiredAt:          {Type: types.TypeTimestamp, I64: g.FiredAt.UnixMilli()},
		cFiringValue:      {Type: types.TypeDouble, I64: int64(math.Float64bits(g.FiringValue))},
	}
	buf = buf[:0]
	var err error
	for _, c := range f.columns {
		// Sem qualifyWhen declarado a coluna fica fora do payload: NULL é
		// ausência (ver tableSpec).
		if c.k == cQualifyCondition && g.QualifyCondition == "" {
			continue
		}
		// firedAt e firingValue descrevem o disparo: enquanto pending não há o
		// que descrever, e as duas ficam fora do payload (NULL). firingValue
		// também fica fora quando a coluna estava ausente do payload que
		// decidiu o disparo — HasFiringValue é o que distingue "não disparou"
		// de "disparou, mas o valor não existia".
		if c.k == cFiredAt && g.FiredAt.IsZero() {
			continue
		}
		if c.k == cFiringValue && (g.FiredAt.IsZero() || !g.HasFiringValue) {
			continue
		}
		if buf, err = encoding.AppendField(buf, c.id, values[c.k]); err != nil {
			return types.Record{}, buf, fmt.Errorf("alarmtable: montando a linha %q: %w", id, err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: id, Payload: buf}, buf, nil
}

// iterator percorre os rowIDs do retrato.
type iterator struct {
	next  types.RowID
	limit types.RowID
}

func (it *iterator) Next() bool {
	it.next++
	return it.next < it.limit
}
func (it *iterator) RowID() types.RowID { return it.next }
func (it *iterator) Err() error         { return nil }
func (it *iterator) Close() error       { return nil }
