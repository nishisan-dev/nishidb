package metareplication

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// tableSpec é a spec da tabela, no MESMO YAML de um dataset de verdade, como
// a `_meta.kafka`: reusar o compilador é o que garante tipos, chave e ordem
// determinística de FieldID iguais às de qualquer outro dataset.
//
// AS DUAS CLASSES DE COLUNA:
//
//   - de DESTINO (id, topic, state, epoch, cursor, checkpointCursor, os dois
//     atrasos e o histórico): uma por linha, do publicador daquele destino.
//   - de FEED (produced, floor, feedDiskBytes, feedCacheBytes,
//     feedSegments): o feed é UM por dataset e alimenta todos os destinos, e
//     por isso repete em toda linha. Uma linha que não as trouxesse obrigaria
//     uma segunda consulta para responder "o destino está atrás de quê?".
//
// As sequências e os bytes saem como int64: é o inteiro do SQL v1, e nenhum
// deles chega perto do teto.
const tableSpec = `version: 1
name: ` + TableName + `
schemaName: ` + schema.SyntheticSchemaName + `
materialization:
  key:
    field: id
schema:
  include: [id, topic, state, epoch, cursor, checkpointCursor, produced, floor,
            feedDiskBytes, feedCacheBytes, feedSegments, lagSequences, lagBytes,
            lastResetReason, lastSnapshotAt, lastSuccessAt, lastError]
  fields:
    id:
      type: string
    topic:
      type: string
    state:
      type: string
    epoch:
      type: string
    cursor:
      type: int64
    checkpointCursor:
      type: int64
    produced:
      type: int64
    floor:
      type: int64
    feedDiskBytes:
      type: int64
    feedCacheBytes:
      type: int64
    feedSegments:
      type: int64
    lagSequences:
      type: int64
    lagBytes:
      type: int64
    lastResetReason:
      type: string
    lastSnapshotAt:
      type: timestamp
    lastSuccessAt:
      type: timestamp
    lastError:
      type: string
`

// column identifica cada coluna, para a linha ser montada por um switch em
// vez de por nome.
type column uint8

const (
	colID column = iota
	colTopic
	colState
	colEpoch
	colCursor
	colCheckpointCursor
	colProduced
	colFloor
	colFeedDiskBytes
	colFeedCacheBytes
	colFeedSegments
	colLagSequences
	colLagBytes
	colLastResetReason
	colLastSnapshotAt
	colLastSuccessAt
	colLastError
)

// columnNames casa, posição a posição, com as constantes acima.
var columnNames = [...]string{
	"id", "topic", "state", "epoch", "cursor", "checkpointCursor", "produced",
	"floor", "feedDiskBytes", "feedCacheBytes", "feedSegments", "lagSequences",
	"lagBytes", "lastResetReason", "lastSnapshotAt", "lastSuccessAt", "lastError",
}

type columnWithID struct {
	id types.FieldID
	k  column
}

// Table monta o schema sintético e o Store virtual de `_meta.replication`.
func Table(src Source) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(tableSpec), schema.BuiltinDefaults)
	if err != nil {
		return nil, nil, fmt.Errorf("metareplication: invalid spec for table %s.%s: %w", meta.Namespace, TableName, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, nil, fmt.Errorf("metareplication: compiling table %s.%s: %w", meta.Namespace, TableName, err)
	}
	st := &store{src: src, open: map[uint64]*picture{}, columns: make([]columnWithID, 0, len(columnNames))}
	for k, name := range columnNames {
		field, ok := sc.FieldByName([]byte(name))
		if !ok {
			return nil, nil, columnError(name)
		}
		st.columns = append(st.columns, columnWithID{id: field.ID, k: column(k)})
	}
	// O payload exige FieldID crescente; a ordem é resolvida UMA vez aqui.
	sort.Slice(st.columns, func(i, j int) bool { return st.columns[i].id < st.columns[j].id })
	return sc, st, nil
}

// row é a linha de um destino: do publicador vivo (live) ou do .ckpt lido
// sem processo.
type row struct {
	live  bool
	stats replicationkafka.Stats
	ckpt  CheckpointRow
}

// id devolve o nome do destino da linha.
func (r *row) id() string {
	if r.live {
		return r.stats.Destination
	}
	return r.ckpt.Destination
}

// picture é o que UMA consulta enxerga: colhido no OpenSnapshot, IMUTÁVEL, e
// devolvido no CloseSnapshot. Congelar na abertura é o que faz as colunas da
// mesma linha saírem do MESMO Stats: lido linha a linha no presente, o
// lagBytes de uma linha poderia ser de um instante e o cursor de outro.
type picture struct {
	feed    replication.Status
	hasFeed bool
	rows    []row
}

// store é o Store virtual. Registrado UMA vez no motor, serve TODAS as
// consultas concorrentes: o que uma consulta enxerga mora no picture dela,
// guardado por época.
type store struct {
	src     Source
	columns []columnWithID // em ordem crescente de FieldID

	mu    sync.Mutex
	epoch uint64
	open  map[uint64]*picture
}

// OpenSnapshot colhe o retrato DESTA consulta. Os rowIDs são 1..n na ordem
// das linhas (a de `replication.targets`), e valem só dentro do retrato.
func (s *store) OpenSnapshot() types.Snapshot {
	p := &picture{}
	if s.src.Feed != nil {
		p.feed, p.hasFeed = s.src.Feed()
	}
	switch {
	case s.src.Destinations != nil:
		for _, st := range s.src.Destinations() {
			p.rows = append(p.rows, row{live: true, stats: st})
		}
	case s.src.Checkpoints != nil:
		for _, c := range s.src.Checkpoints() {
			p.rows = append(p.rows, row{ckpt: c})
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	s.open[s.epoch] = p
	return types.Snapshot{Epoch: s.epoch, LimitRowID: types.RowID(len(p.rows)) + 1}
}

// CloseSnapshot devolve o retrato desta consulta.
func (s *store) CloseSnapshot(snap types.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.open, snap.Epoch)
}

func (s *store) pictureOf(snap types.Snapshot) (*picture, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.open[snap.Epoch]
	return p, ok
}

func (s *store) ScanLive(snap types.Snapshot) types.RowIDIterator {
	p, ok := s.pictureOf(snap)
	if !ok {
		return &iterator{}
	}
	return &iterator{n: len(p.rows)}
}

func (s *store) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	p, ok := s.pictureOf(snap)
	if !ok {
		// Época desconhecida é DEFEITO de pareamento, não linha ausente:
		// ok=false faria a linha sumir do resultado sem nada acusar.
		return types.Record{}, buf, false, fmt.Errorf("metareplication: snapshot %d is not open", snap.Epoch)
	}
	if id < 1 || int(id) > len(p.rows) {
		return types.Record{}, buf, false, nil
	}
	return s.record(id, p, &p.rows[id-1], buf)
}

func (s *store) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	p, ok := s.pictureOf(snap)
	if !ok {
		return types.Record{}, buf, false, fmt.Errorf("metareplication: snapshot %d is not open", snap.Epoch)
	}
	// Sobre as linhas DO RETRATO: o point lookup enxerga exatamente o que o
	// scan enxerga.
	for i := range p.rows {
		if p.rows[i].id() == string(key) {
			return s.record(types.RowID(i)+1, p, &p.rows[i], buf)
		}
	}
	return types.Record{}, buf, false, nil
}

// Indexed responde SEMPRE false, como nas outras tabelas virtuais: o planner
// varre as linhas — uma por destino — e a key dá o point lookup.
func (s *store) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: rede de segurança, como no _meta e no _counters.
func (s *store) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("metareplication: table %s.%s has no index (field %d)", meta.Namespace, TableName, field)
}

// iterator percorre os rowIDs 1..n.
type iterator struct {
	n, i int
}

func (it *iterator) Next() bool {
	if it.i < it.n {
		it.i++
		return true
	}
	return false
}

func (it *iterator) RowID() types.RowID {
	if it.i == 0 || it.i > it.n {
		return 0
	}
	return types.RowID(it.i)
}

func (it *iterator) Err() error   { return nil }
func (it *iterator) Close() error { return nil }

// record codifica UMA linha.
//
// A regra que governa o método: COLUNA AUSENTE É NULL, e ausência nunca vira
// zero. Sem feed, as colunas do feed são NULL. Na linha lida do .ckpt (sem
// publicador no ar), state, cursor, os atrasos, feedCacheBytes,
// lastResetReason e lastSuccessAt são NULL: são o estado de um processo que
// não existe, e um zero ali diria "em dia" sobre um destino que ninguém está
// medindo. Texto vazio e instante zero ("nunca houve") também são NULL, e o
// checkpointCursor do destino que ainda não tem .ckpt (Stats.CheckpointFound
// falso, ou CheckpointRow.Found falso) também: zero ali seria um ponto
// gravado que não existe.
func (s *store) record(id types.RowID, p *picture, r *row, buf []byte) (types.Record, []byte, bool, error) {
	buf = buf[:0]
	key := []byte(r.id())
	var err error
	for _, col := range s.columns {
		var v types.Value
		switch col.k {
		case colID:
			v = types.Value{Type: types.TypeString, Bytes: key}
		case colTopic:
			topic := r.ckpt.Topic
			if r.live {
				topic = r.stats.Topic
			}
			if topic == "" {
				continue
			}
			v = text(topic)
		case colState:
			if !r.live || r.stats.State == "" {
				continue
			}
			v = text(r.stats.State)
		case colEpoch:
			epoch := r.ckpt.Checkpoint.Epoch
			if r.live {
				epoch = r.stats.Epoch
			}
			if epoch == "" {
				continue
			}
			v = text(epoch)
		case colCursor:
			if !r.live {
				continue
			}
			v = integer(r.stats.Cursor)
		case colCheckpointCursor:
			switch {
			case r.live && r.stats.CheckpointFound:
				v = integer(r.stats.CheckpointCursor)
			case !r.live && r.ckpt.Found:
				v = integer(r.ckpt.Checkpoint.Cursor)
			default:
				continue
			}
		case colProduced:
			if !p.hasFeed {
				continue
			}
			v = integer(p.feed.Produced)
		case colFloor:
			if !p.hasFeed {
				continue
			}
			v = integer(p.feed.AvailableAfter)
		case colFeedDiskBytes:
			if !p.hasFeed {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: p.feed.DiskBytes}
		case colFeedCacheBytes:
			// A cache é memória do processo: sem publicador vivo não há o que
			// medir, mesmo com o feed lido do disco.
			if !p.hasFeed || !r.live {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: p.feed.BufferedBytes}
		case colFeedSegments:
			if !p.hasFeed {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: int64(p.feed.DiskSegments)}
		case colLagSequences:
			if !r.live {
				continue
			}
			v = integer(r.stats.LagSequences)
		case colLagBytes:
			if !r.live {
				continue
			}
			v = integer(r.stats.LagBytes)
		case colLastResetReason:
			if !r.live || r.stats.LastResetReason == "" {
				continue
			}
			v = text(r.stats.LastResetReason)
		case colLastSnapshotAt:
			switch {
			case r.live && r.stats.LastSnapshotAt != 0:
				v = timestampOfSeconds(r.stats.LastSnapshotAt)
			case !r.live && !r.ckpt.Checkpoint.LastSnapshotAt.IsZero():
				v = types.Value{Type: types.TypeTimestamp, I64: r.ckpt.Checkpoint.LastSnapshotAt.UnixMilli()}
			default:
				continue
			}
		case colLastSuccessAt:
			if !r.live || r.stats.LastSuccessTimestamp == 0 {
				continue
			}
			v = timestampOfSeconds(r.stats.LastSuccessTimestamp)
		case colLastError:
			msg := r.stats.LastError
			if !r.live {
				msg = ""
				if r.ckpt.Err != nil {
					msg = "checkpoint unreadable: " + r.ckpt.Err.Error()
				}
			}
			if msg == "" {
				continue
			}
			v = text(msg)
		}
		if buf, err = encoding.AppendField(buf, col.id, v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("metareplication: encoding column %s: %w", columnNames[col.k], err)
		}
	}
	return types.Record{RowID: id, Sequence: int64(id), Key: key, Payload: buf}, buf, true, nil
}

func text(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }

// integer converte a sequência ou a contagem de bytes para o int64 da coluna,
// saturando no teto (que nenhuma delas alcança).
func integer(v uint64) types.Value {
	if v > math.MaxInt64 {
		return types.Value{Type: types.TypeInt64, I64: math.MaxInt64}
	}
	return types.Value{Type: types.TypeInt64, I64: int64(v)}
}

// timestampOfSeconds converte o instante em segundos Unix (float, como o
// Stats do publicador o guarda) para os milissegundos da coluna timestamp.
func timestampOfSeconds(seconds float64) types.Value {
	return types.Value{Type: types.TypeTimestamp, I64: int64(math.Round(seconds * 1000))}
}
