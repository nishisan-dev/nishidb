package metakafka

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// KeyOfSummary é o `id` da linha de resumo — a que responde quando não há
// partição nenhuma. Nunca colide com o id de uma partição, que é o número em
// decimal e portanto só tem dígitos.
const KeyOfSummary = "source"

// especOfTable é a spec da tabela, no MESMO YAML de um dataset de verdade,
// como o `_meta.fields` e o `_meta.counters`. Reusar o compilador é o que
// garante que ela obedeça às mesmas regras — tipos, chave, ordem
// determinística de FieldID — que qualquer outro dataset.
//
// AS DUAS CLASSES DE COLUNA, e é isso que faz a linha de resumo e as linhas
// por partição serem a mesma tabela:
//
//   - de DATASET (topic…enabled, state, detail, a fila, o desfecho, as
//     partições, o watchdog): repetem em toda linha. Uma linha por partição
//     que não trouxesse o estado do dataset obrigaria uma segunda consulta
//     para responder "e a fila, está cheia?".
//   - de PARTIÇÃO (partition, appliedOffset, committedOffset, highWatermark,
//     lag, markAt): são NULL na linha de resumo. markAt é de partição, e não
//     de dataset, porque carimba o HWM e o lag, que só existem por partição.
//
// A key é `id`, e não `partition`: a key tem de ser string (a spec só suporta
// isso) e tem de existir em TODA linha, e `partition` é NULL no resumo.
const especOfTable = `version: 1
name: ` + TableMetaKafka + `
materialization:
  key:
    field: id
schema:
  include: [id, topic, groupId, bootstrap, partition, enabled, state, detail,
            appliedOffset, committedOffset, highWatermark, lag, markAt,
            lastBatchAt, queueRecords, queueBytes, events, applied, stale,
            discarded, failed, partitionsAssigned, partitionsTotal,
            watchdogTrips, watchdogGaveUp]
  fields:
    id:
      type: string
    topic:
      type: string
    groupId:
      type: string
    bootstrap:
      type: string
    partition:
      type: int64
    enabled:
      type: boolean
    state:
      type: string
    detail:
      type: string
    appliedOffset:
      type: int64
    committedOffset:
      type: int64
    highWatermark:
      type: int64
    lag:
      type: int64
    markAt:
      type: timestamp
    lastBatchAt:
      type: timestamp
    queueRecords:
      type: int64
    queueBytes:
      type: int64
    events:
      type: int64
    applied:
      type: int64
    stale:
      type: int64
    discarded:
      type: int64
    failed:
      type: int64
    partitionsAssigned:
      type: int64
    partitionsTotal:
      type: int64
    watchdogTrips:
      type: int64
    watchdogGaveUp:
      type: boolean
`

// column identifica cada column, para a linha ser montada por um switch em vez
// de por nome.
type column uint8

const (
	colID column = iota
	colTopic
	colGroupID
	colBootstrap
	colPartition
	colEnabled
	colState
	colDetail
	colOffsetAplicado
	colOffsetDuravel
	colEndOfLog
	colLag
	colMarkIn
	colUltimoLote
	colFilaRegistros
	colFilaBytes
	colEventos
	colAplicados
	colStale
	colDescartados
	colFalhas
	colPartitionsAtribuidas
	colPartitionsOfTopic
	colWatchdogTrips
	colWatchdogDesistiu
)

// nomesDasColumns casa, posição a posição, com as constantes acima.
var nomesDasColumns = [...]string{
	"id", "topic", "groupId", "bootstrap", "partition", "enabled", "state",
	"detail", "appliedOffset", "committedOffset", "highWatermark", "lag",
	"markAt", "lastBatchAt", "queueRecords", "queueBytes", "events", "applied",
	"stale", "discarded", "failed", "partitionsAssigned", "partitionsTotal",
	"watchdogTrips", "watchdogGaveUp",
}

type columnWithID struct {
	id types.FieldID
	k  column
}

// Table monta o schema sintético e o Store virtual de `_meta.kafka`.
func Table(f Source) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(especOfTable), schema.BuiltinDefaults)
	if err != nil {
		return nil, nil, fmt.Errorf("metakafka: spec da tabela %s.%s inválida: %w", meta.Namespace, TableMetaKafka, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, nil, fmt.Errorf("metakafka: compilando a tabela %s.%s: %w", meta.Namespace, TableMetaKafka, err)
	}
	fo := &source{
		f:         f,
		topic:     []byte(f.Declarada.Topic),
		grupo:     []byte(f.Declarada.GroupID),
		bootstrap: []byte(f.Declarada.Bootstrap),
		abertos:   map[uint64]*retrato{},
		columns:   make([]columnWithID, 0, len(nomesDasColumns)),
	}
	for k, name := range nomesDasColumns {
		field, ok := sc.FieldByName([]byte(name))
		if !ok {
			return nil, nil, columnError(name)
		}
		fo.columns = append(fo.columns, columnWithID{id: field.ID, k: column(k)})
	}
	// O payload exige FieldID crescente; a ordem é resolvida UMA vez aqui, e a
	// linha é montada sem ordenar.
	sort.Slice(fo.columns, func(i, j int) bool { return fo.columns[i].id < fo.columns[j].id })
	return sc, fo, nil
}

// comum são as colunas de dataset — iguais em toda linha do resultado.
type comum struct {
	state, detalhe    string
	hasConsumidor     bool
	filaRecords       int
	filaBytes         int64
	ultimoLote        time.Time
	desfecho          kafka.Desfecho
	partitionsAtrib   int64
	partitionsOfTopic int64
	trips             uint64
	desistiu          bool
	hasVigia          bool
}

// row é uma row do retrato: a de resumo (Summary=true) ou a de uma partição.
type row struct {
	Summary  bool
	Position kafka.PositionOfPartition
	Duravel  int64
	HasDurav bool
	MarkIn   time.Time
}

// retrato é o que UMA consulta enxerga: colhido no OpenSnapshot, IMUTÁVEL, e
// devolvido no CloseSnapshot.
type retrato struct {
	comum comum
	rows  map[types.RowID]row
	limit types.RowID
}

// source é o Store virtual. Registrada UMA vez no motor, serve TODAS as
// consultas concorrentes — então nada do que UMA consulta enxerga mora em
// campo mutável compartilhado.
type source struct {
	f                       Source
	columns                 []columnWithID // em ordem crescente de FieldID
	topic, grupo, bootstrap []byte

	mu        sync.Mutex
	proxEpoca uint64
	abertos   map[uint64]*retrato

	// medindo é o voo único da marca d'água: uma consulta por vez fala com o
	// broker. Ver OpenSnapshot.
	medindo atomic.Bool
}

// OpenSnapshot congela o universo DESTA consulta: o conjunto de linhas E os
// números.
//
// Aqui a tabela diverge do `_counters`, que congela só o conjunto e lê os
// totais no presente — e a divergência é o motivo de ela existir. As colunas
// desta são uma MEDIÇÃO COM CARIMBO: markAt diz quando o broker respondeu, e
// `lag == highWatermark - appliedOffset` é um invariante que o coletor do
// /metrics tornou testável dentro de uma raspagem. Lendo no presente linha a
// linha, um resultado de três partições poderia carregar três markAt
// diferentes e um lag que não fecha com o HWM da mesma linha — exatamente o
// painel que discorda de si mesmo.
//
// O retrato é guardado por ÉPOCA, mintada aqui. O executor fecha o par
// OpenSnapshot/CloseSnapshot em todos os caminhos de saída, inclusive no Open
// que falha depois de abrir.
func (f *source) OpenSnapshot() types.Snapshot {
	var inst kafka.Instantaneo
	c := f.consumidor()
	if c != nil {
		inst = c.Instantaneo(f.contextoOfMark())
	}

	r := &retrato{
		comum: f.colheComum(c != nil, inst),
		rows:  map[types.RowID]row{},
	}
	duraveis, hasDuraveis := f.colheDuraveis()

	// Os rowIDs valem DENTRO deste retrato e são atribuídos na ordem em que
	// as partições vêm, que já é crescente. Um registro estável entre
	// consultas não teria o que proteger: o retrato é imutável e privado, e o
	// rowID não é coluna — nenhuma consulta enxerga o número.
	//
	// A linha de resumo existe SÓ quando não há partição nenhuma — com
	// partições, ela seria uma linha a mais no resultado —, e por isso as
	// duas formas nunca coexistem: a numeração a partir de 2 não evita
	// colisão (não há uma a evitar), só deixa o resumo sempre no rowID 1.
	for i, p := range inst.Partitions {
		l := row{Position: p, MarkIn: inst.MarkIn}
		if hasDuraveis {
			if off, ok := duraveis.Offsets[p.Partition]; ok {
				l.Duravel, l.HasDurav = off, true
			}
		}
		r.rows[types.RowID(i)+2] = l
	}
	if len(r.rows) == 0 {
		r.rows[1] = row{Summary: true}
	}
	r.limit = 2 + types.RowID(len(inst.Partitions))

	f.mu.Lock()
	defer f.mu.Unlock()
	f.proxEpoca++
	epoca := f.proxEpoca
	f.abertos[epoca] = r
	return types.Snapshot{Epoch: epoca, LimitRowID: r.limit}
}

// contextoOfMark dá à ida ao broker um prazo PRÓPRIO, derivado do
// context.Background(): a interface do query.Store não recebe context, e este
// é o único jeito de a chamada ter teto. O preço está registrado: o
// cancelamento da consulta não alcança essa ida, e o pior caso é segurar a
// goroutine por kafka.PrazoDaMarcaDagua depois de o cliente desistir — o mesmo
// teto que o scrape do /metrics já aceita.
//
// O VOO ÚNICO limita o dano: uma consulta por vez fala com o broker. Quem não
// pega o lugar recebe um contexto já cancelado, o que faz a atualização sair
// na hora e a consulta reusar a última marca — ou publicar NULL, se estiver
// vencida, que é a resposta honesta. Sem isto, com o broker fora do ar, N
// consultas simultâneas gastariam o prazo inteiro cada uma.
func (f *source) contextoOfMark() context.Context {
	if !f.medindo.CompareAndSwap(false, true) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	ctx, cancel := context.WithTimeout(context.Background(), kafka.PrazoDaMarcaDagua)
	go func() {
		<-ctx.Done()
		cancel()
		f.medindo.Store(false)
	}()
	return ctx
}

func (f *source) consumidor() Consumido {
	if f.f.Consumidor == nil {
		return nil
	}
	return f.f.Consumidor()
}

func (f *source) colheDuraveis() (checkpoint.SourceOffsets, bool) {
	if f.f.Duraveis == nil {
		return checkpoint.SourceOffsets{}, false
	}
	o, ok, err := f.f.Duraveis()
	// Arquivo ilegível é ausência de offset, não offset zero: as duas
	// respostas do tri-estado que não são um número viram NULL.
	if err != nil || !ok {
		return checkpoint.SourceOffsets{}, false
	}
	return o, true
}

func (f *source) colheComum(hasConsumidor bool, inst kafka.Instantaneo) comum {
	c := comum{hasConsumidor: hasConsumidor}
	if f.f.State != nil {
		c.state, c.detalhe = f.f.State()
	} else {
		c.state, c.detalhe = DeclaredState(f.f.Declarada)
	}
	if hasConsumidor {
		c.filaRecords, c.filaBytes = inst.FilaRecords, inst.FilaBytes
		c.ultimoLote = inst.UltimoLote
		c.desfecho = inst.Desfecho
		c.partitionsAtrib, c.partitionsOfTopic = inst.PartitionsAtribuidas, inst.PartitionsOfTopic
		if inst.Pausa != nil && c.detalhe == "" {
			c.detalhe = inst.Pausa.Error()
		}
	}
	if f.f.Vigia != nil {
		c.trips, c.desistiu, c.hasVigia = f.f.Vigia()
	}
	return c
}

// CloseSnapshot devolve o retrato desta consulta. O registro de partições NÃO
// é podado: ele é a identidade dos rowIDs.
func (f *source) CloseSnapshot(snap types.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.abertos, snap.Epoch)
}

func (f *source) retratoOf(snap types.Snapshot) (*retrato, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.abertos[snap.Epoch]
	return r, ok
}

func (f *source) ScanLive(snap types.Snapshot) types.RowIDIterator {
	r, ok := f.retratoOf(snap)
	if !ok {
		return &iterador{}
	}
	ids := make([]types.RowID, 0, len(r.rows))
	for id := range r.rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return &iterador{ids: ids}
}

func (f *source) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	r, ok := f.retratoOf(snap)
	if !ok {
		// Época desconhecida é DEFEITO de pareamento, não linha ausente:
		// devolver ok=false faria a linha sumir do resultado sem nada acusar.
		return types.Record{}, buf, false, fmt.Errorf("metakafka: snapshot %d não está aberto", snap.Epoch)
	}
	l, ok := r.rows[id]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, r, l, buf)
}

func (f *source) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	r, ok := f.retratoOf(snap)
	if !ok {
		return types.Record{}, buf, false, fmt.Errorf("metakafka: snapshot %d não está aberto", snap.Epoch)
	}
	// A busca é sobre as linhas DO RETRATO, e não sobre o registro de
	// partições: o point lookup tem de enxergar exatamente o que o scan
	// enxerga, senão devolveria falso negativo numa linha que o scan traria.
	for id, l := range r.rows {
		if equalsKey(key, l) {
			return f.record(id, r, l, buf)
		}
	}
	return types.Record{}, buf, false, nil
}

// equalsKey compara a key pedida com o `id` da linha SEM alocar: o resumo é
// a literal fixa, e a partição é o número em decimal.
func equalsKey(key []byte, l row) bool {
	if l.Summary {
		return string(key) == KeyOfSummary
	}
	n, ok := decimal(key)
	return ok && n == int64(l.Position.Partition)
}

// decimal lê um inteiro não negativo de bytes ASCII. Devolve false para vazio,
// sinal, não dígito ou estouro — nenhum desses é um número de partição.
func decimal(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 10 {
		return 0, false
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

// Indexed responde SEMPRE false, como nas outras tabelas virtuais: sem índice
// declarado o planner varre as linhas — uma por partição — e a key dá o point
// lookup por conta própria.
func (f *source) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: rede de segurança, como no _meta e no _counters.
func (f *source) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("metakafka: a tabela %s.%s não tem índice (campo %d)", meta.Namespace, TableMetaKafka, field)
}

type iterador struct {
	ids []types.RowID
	i   int
}

func (it *iterador) Next() bool {
	if it.i < len(it.ids) {
		it.i++
		return true
	}
	return false
}

func (it *iterador) RowID() types.RowID {
	if it.i == 0 || it.i > len(it.ids) {
		return 0
	}
	return it.ids[it.i-1]
}

func (it *iterador) Err() error   { return nil }
func (it *iterador) Close() error { return nil }

// record codifica UMA linha.
//
// A regra que governa o método inteiro: COLUNA AUSENTE É NULL, e ausência
// nunca vira zero. Sem consumidor, as colunas vivas — fila, desfecho,
// partições — são NULL, porque "não há o que medir" e "medi e deu zero" são
// afirmações diferentes e o operador precisa das duas. Na linha de resumo, as
// colunas de partição saem NULL pela mesma razão. E o fim do log e o lag saem
// NULL quando a marca d'água venceu ou o broker não respondeu por aquela
// partição — um zero ali diria "está em dia" a quem perdeu o broker.
func (f *source) record(row types.RowID, r *retrato, l row, buf []byte) (types.Record, []byte, bool, error) {
	buf = buf[:0]
	key := keyOfRow(l)
	var err error
	for _, col := range f.columns {
		var v types.Value
		switch col.k {
		case colID:
			v = types.Value{Type: types.TypeString, Bytes: key}
		case colTopic:
			if len(f.topic) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: f.topic}
		case colGroupID:
			if len(f.grupo) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: f.grupo}
		case colBootstrap:
			if len(f.bootstrap) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: f.bootstrap}
		case colEnabled:
			// Nunca NULL: um dataset sem bloco Kafka não consome, e isso é um
			// fato, não uma ausência de medida.
			v = types.Value{Type: types.TypeBool, I64: booleano(f.f.Declarada.Declarado && f.f.Declarada.Habilitado)}
		case colState:
			if r.comum.state == "" {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: []byte(r.comum.state)}
		case colDetail:
			if r.comum.detalhe == "" {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: []byte(r.comum.detalhe)}
		case colPartition:
			if l.Summary {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: int64(l.Position.Partition)}
		case colOffsetAplicado:
			if l.Summary {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Position.Aplicado}
		case colOffsetDuravel:
			if l.Summary || !l.HasDurav {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Duravel}
		case colEndOfLog:
			if l.Summary || !l.Position.TemEnd {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Position.EndDoLog}
		case colLag:
			if l.Summary || !l.Position.TemEnd {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Position.Atraso}
		case colMarkIn:
			if l.Summary || !l.Position.TemEnd || l.MarkIn.IsZero() {
				continue
			}
			v = types.Value{Type: types.TypeTimestamp, I64: l.MarkIn.UnixMilli()}
		case colUltimoLote:
			if !r.comum.hasConsumidor || r.comum.ultimoLote.IsZero() {
				continue
			}
			v = types.Value{Type: types.TypeTimestamp, I64: r.comum.ultimoLote.UnixMilli()}
		case colFilaRegistros:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: int64(r.comum.filaRecords)}
		case colFilaBytes:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.filaBytes}
		case colEventos:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Eventos}
		case colAplicados:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Aplicados}
		case colStale:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Stale}
		case colDescartados:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Descartados}
		case colFalhas:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Falhas}
		case colPartitionsAtribuidas:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.partitionsAtrib}
		case colPartitionsOfTopic:
			if !r.comum.hasConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.partitionsOfTopic}
		case colWatchdogTrips:
			// NULL sem goroutine de ingestão: "não há watchdog" e "nunca
			// recriou" são afirmações diferentes, e um zero as confundiria.
			if !r.comum.hasVigia {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: int64(r.comum.trips)} //nolint:gosec // contagem de recriações, nunca perto do teto do int64
		case colWatchdogDesistiu:
			if !r.comum.hasVigia {
				continue
			}
			v = types.Value{Type: types.TypeBool, I64: booleano(r.comum.desistiu)}
		}
		if buf, err = encoding.AppendField(buf, col.id, v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("metakafka: codificando a coluna %s: %w", nomesDasColumns[col.k], err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: key, Payload: buf}, buf, true, nil
}

// keyOfRow devolve o `id`: a literal do resumo, ou a partição em decimal.
func keyOfRow(l row) []byte {
	if l.Summary {
		return []byte(KeyOfSummary)
	}
	return []byte(strconv.Itoa(int(l.Position.Partition)))
}

// booleano converte para a representação de bool do payload (0 ou 1).
func booleano(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
