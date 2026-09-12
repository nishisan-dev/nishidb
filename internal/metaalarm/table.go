// Package metaalarm serve a tabela virtual `_meta.alarms`: os alarmes em
// observação no dataset consultado, um por par (alarme, grupo).
//
// Mora fora de `internal/alarm` e fora de `internal/query` porque precisa dos
// dois lados, e nenhum dos dois pode passar a importar o outro — é o mesmo
// argumento do `internal/counters` e do `internal/metakafka`.
//
// # O que ela responde e o que as métricas não respondem
//
// As séries de alarme dão os agregados: quantos grupos em observação, quantos
// disparados, quantos disparos desde a subida. Esta tabela dá o DETALHE — QUAL
// equipamento, desde quando, e se o prazo já se cumpriu. É a diferença entre
// "há três OLTs alarmadas" e "a OLT_14 está assim desde 03:14".
package metaalarm

import (
	"fmt"
	"slices"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TableMetaAlarms é o nome da tabela sob o `_meta`.
const TableMetaAlarms = "alarms"

// Estados da coluna `state`. Enum fechado, em inglês, como os do evento.
const (
	// StatePending: a condição vale e o prazo ainda não se cumpriu. "Vai
	// alarmar em quarenta segundos" é informação, e é o que esta linha dá.
	StatePending = "pending"
	// StateFiring: o disparo já foi anunciado.
	StateFiring = "firing"
)

// especOfTable é a spec da tabela, no MESMO YAML de um dataset de verdade,
// como o `_meta.counters` e o `_meta.kafka`. Reusar o compilador é o que
// garante que ela obedeça às mesmas regras — tipos, chave, ordem determinística
// de FieldID — que qualquer outro dataset, e o que dá WHERE, ORDER BY, LIMIT e
// projeção sem gramática nova.
//
// A key é `id`, composta de contador, alarme e grupo: a key da v1 é uma coluna
// string única, e nenhuma das três identifica a linha sozinha. Os nomes de
// contador e de alarme são [a-z0-9_]+, então o separador `|` só pode aparecer
// dentro do groupKey — o que mantém a decomposição sem ambiguidade e a key
// única.
//
// `condition` e `sustainedForMs` repetem em toda linha do mesmo alarme, e isso
// é deliberado: quem consulta a tabela não tem o YAML à mão, e "total baixo"
// sem o limiar não é acionável.
const especOfTable = `version: 1
name: ` + TableMetaAlarms + `
schemaName: ` + schema.SyntheticSchemaName + `
materialization:
  key:
    field: id
schema:
  include: [id, counter, alarm, groupKey, state, since, condition, sustainedForMs]
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
    sustainedForMs:
      type: int64
`

// column é a posição de cada coluna, e nomesDasColumns as nomeia na mesma
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
	cSustainedForMs
)

var nomesDasColumns = []string{
	"id", "counter", "alarm", "groupKey", "state", "since", "condition", "sustainedForMs",
}

// Source é o que a tabela precisa do dataset, RESOLVIDO NA HORA.
//
// Groups é um closure, e não a fatia: o registro acontece na abertura do
// dataset, quando o runner pode nem existir, e o retrato TROCA a cada passada.
// Uma fatia capturada no registro descreveria para sempre o estado da subida.
//
// nil é aceito e vale "nenhum alarme declarado" — que é uma resposta, não um
// erro: `SELECT * FROM _meta.alarms` nunca pode falhar por isso.
type Source struct {
	Groups func() []alarm.ActiveGroup
}

// Table monta o schema sintético e o Store virtual de `_meta.alarms`.
func Table(f Source) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(especOfTable), schema.BuiltinDefaults)
	if err != nil {
		return nil, nil, fmt.Errorf("metaalarm: spec da tabela %s.%s inválida: %w", meta.Namespace, TableMetaAlarms, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, nil, fmt.Errorf("metaalarm: compilando a tabela %s.%s: %w", meta.Namespace, TableMetaAlarms, err)
	}
	fo := &source{f: f, abertos: map[uint64][]alarm.ActiveGroup{}}
	fo.columns = make([]columnWithID, 0, len(nomesDasColumns))
	for k, name := range nomesDasColumns {
		field, ok := sc.FieldByName([]byte(name))
		if !ok {
			return nil, nil, fmt.Errorf("metaalarm: coluna %q sem FieldID no schema sintético", name)
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

// Register declara `_meta.alarms` sob o dataset já registrado no motor.
//
// Chamado SEMPRE, inclusive num dataset sem alarme declarado: "este dataset não
// observa nada" é uma resposta, e a tabela vazia a dá.
func Register(eng *query.Engine, dataset string, f Source) error {
	sc, store, err := Table(f)
	if err != nil {
		return err
	}
	return eng.RegisterVirtual(dataset, meta.Namespace, TableMetaAlarms, sc, store)
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
	proxEpoca uint64
	abertos   map[uint64][]alarm.ActiveGroup
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
	f.proxEpoca++
	epoca := f.proxEpoca
	f.abertos[epoca] = groups
	// rowID é o índice mais um: eles valem DENTRO deste retrato, e o número
	// não é coluna — nenhuma consulta o enxerga.
	return types.Snapshot{Epoch: epoca, LimitRowID: types.RowID(len(groups)) + 1}
}

// CloseSnapshot devolve o retrato. O executor fecha o par em todos os caminhos
// de saída, inclusive no Open que falha depois de abrir.
func (f *source) CloseSnapshot(snap types.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.abertos, snap.Epoch)
}

func (f *source) retratoOf(snap types.Snapshot) ([]alarm.ActiveGroup, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.abertos[snap.Epoch]
	return g, ok
}

func (f *source) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &iterator{limit: snap.LimitRowID}
}

func (f *source) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	groups, ok := f.retratoOf(snap)
	if !ok || id < 1 || int(id) > len(groups) {
		return types.Record{}, buf, false, nil
	}
	rec, buf, err := f.record(id, groups[id-1], buf)
	return rec, buf, err == nil, err
}

func (f *source) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	groups, ok := f.retratoOf(snap)
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
	return nil, fmt.Errorf("metaalarm: %w", types.ErrInvalidArgument)
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
	estado := StatePending
	if g.Firing {
		estado = StateFiring
	}
	valores := [...]types.Value{
		cID:             {Type: types.TypeString, Bytes: id},
		cCounter:        {Type: types.TypeString, Bytes: []byte(g.Counter)},
		cAlarm:          {Type: types.TypeString, Bytes: []byte(g.Alarm)},
		cGroupKey:       {Type: types.TypeString, Bytes: []byte(g.GroupKey)},
		cState:          {Type: types.TypeString, Bytes: []byte(estado)},
		cSince:          {Type: types.TypeTimestamp, I64: g.Since.UnixMilli()},
		cCondition:      {Type: types.TypeString, Bytes: []byte(g.Condition)},
		cSustainedForMs: {Type: types.TypeInt64, I64: g.SustainedFor.Milliseconds()},
	}
	buf = buf[:0]
	var err error
	for _, c := range f.columns {
		if buf, err = encoding.AppendField(buf, c.id, valores[c.k]); err != nil {
			return types.Record{}, buf, fmt.Errorf("metaalarm: montando a linha %q: %w", id, err)
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
