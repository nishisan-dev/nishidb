// Package replication implementa o feed de mudanças compartilhado pelos
// seguidores HTTP e pelos publicadores Kafka de um dataset: um feed limitado
// em memória (cache) apoiado num log de segmentos em disco, com retenção por
// bytes. O Publish, que roda sob o wmu do Store, nunca faz I/O de rede nem de
// disco: quem grava é uma goroutine própria do feed.
package replication

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrResyncRequired means the requested cursor is no longer available.
var ErrResyncRequired = errors.New("replication: RESYNC_REQUIRED")

// ErrClosed means the change feed has been shut down.
var ErrClosed = errors.New("replication: closed")

var (
	// errCloseDeadline: o gravador não drenou a fila dentro do prazo do Close,
	// e o `state` não recebeu a marca limpa.
	errCloseDeadline = errors.New("replication: feed writer did not drain before the close deadline")
	// errLateMutation: uma mutação chegou ao feed depois do Close (ou durante
	// ele), e o `state` não recebeu a marca limpa.
	errLateMutation = errors.New("replication: a change reached the feed after it closed; the next start is a crash")
	// errAlreadyResumed é um segundo Resume no mesmo feed.
	errAlreadyResumed = errors.New("replication: feed already resumed")
)

const (
	// Faixa do tamanho de segmento: maxDiskBytes/8, entre 1 MiB e 64 MiB. Com
	// o piso de 8 MiB de disco há sempre ao menos 8 segmentos, e a retenção
	// apaga 1/8 do log de cada vez.
	minSegmentBytes = 1 << 20
	maxSegmentBytes = 64 << 20

	// minDiskBytes é o piso absoluto de MaxDiskBytes.
	minDiskBytes = 8 << 20

	// Retentativa do gravador depois de erro de disco: de 100 ms a 5 s,
	// dobrando a cada falha seguida.
	writerMinBackoff = 100 * time.Millisecond
	writerMaxBackoff = 5 * time.Second
	// diskWarnInterval é o espaçamento mínimo entre dois WARN de erro de disco.
	diskWarnInterval = 5 * time.Second

	// readChunk é a leitura mínima do leitor de disco: quadros pequenos saem
	// de um mesmo pread.
	readChunk = 64 << 10
)

// Operation identifies the materialized change represented by an entry.
type Operation uint8

const (
	// Insert adds the first materialized version of a key.
	Insert Operation = iota + 1
	// Update replaces the materialized version of an existing key.
	Update
	// Delete writes a tombstone for a key.
	Delete
	// Schema precedes records using newly discovered fields.
	Schema
	// Reset replaces a query projection, never a base or history table.
	Reset
	// Retain advances the history retention floor (Record.RowID).
	Retain
)

// Entry is one ordered materialized change in the feed.
type Entry struct {
	Sequence  uint64       `json:"sequence"`
	Table     string       `json:"table"`
	Operation Operation    `json:"operation"`
	Record    types.Record `json:"record"`
	// At é o instante em que a mudança foi publicada no feed. Quando a
	// coalescência substitui uma entrada pendente, vale o instante da ÚLTIMA
	// publicação, junto com o último estado.
	At time.Time `json:"at"`
}

// Options governam o feed de um dataset.
//
// O teto de memória (MaxBufferedBytes) inclui o intervalo ativo, os lotes
// ainda não gravados e os lotes despejados que um leitor ainda segura. Os
// lotes que um leitor lê do disco são dele e não entram na conta. O teto
// REAL de memória do feed é, portanto, MaxBufferedBytes + o buffer de
// codificação do gravador (até 2 × MaxBatchBytes retido entre lotes; um
// quadro maior só vive durante a própria gravação) + um lote, e o buffer de
// leitura dele, por leitor que lê do disco.
type Options struct {
	Interval         time.Duration
	MaxMessages      int
	MaxBatchBytes    int64
	MaxBufferedBytes int64
	// MaxDiskBytes é o teto do log de segmentos em disco: passado dele, a
	// retenção apaga os segmentos mais antigos e o piso do feed sobe.
	MaxDiskBytes int64
	// ExcludeHistory só é anunciado (Status.HistoryExcluded); quem deixa a
	// tabela histórica fora do feed é a fiação do servidor. O zero vale o
	// legado: a histórica é publicada.
	ExcludeHistory bool
	// Logger recebe os WARN e ERROR do feed. Nil descarta.
	Logger *logging.Logger
}

// DefaultOptions returns the bounded feed defaults used by configuration.
func DefaultOptions() Options {
	return Options{
		Interval:         5 * time.Second,
		MaxMessages:      10000,
		MaxBatchBytes:    8 << 20,
		MaxBufferedBytes: 64 << 20,
		MaxDiskBytes:     2 << 30,
	}
}

// Validate confere intervalo e tetos antes de o feed abrir.
//
// Pisos, todos comparados por divisão (um maxBatchBytes gigante não
// transborda int64 e aprova o par por engano):
//   - MaxBufferedBytes >= 3 × MaxBatchBytes: com o gravador assíncrono
//     coexistem na memória o intervalo ativo, um lote na fila de gravação e
//     um lote arrendado já despejado. Abaixo disso a próxima escrita não acha
//     espaço e o feed invalida o intervalo mesmo com o disco saudável;
//   - MaxDiskBytes >= 8 MiB e >= 4 × MaxBatchBytes: o log precisa comportar
//     alguns segmentos, e um quadro é do tamanho de um lote.
func (o Options) Validate() error {
	if o.Interval < 0 || o.Interval > time.Minute || o.Interval%time.Second != 0 {
		return errors.New("replication: flushInterval must be an integer from 0s to 60s")
	}
	if o.MaxMessages < 1 || o.MaxBatchBytes < 1 {
		return errors.New("replication: maxMessages and maxBatchBytes must be positive")
	}
	if o.MaxBufferedBytes/3 < o.MaxBatchBytes {
		return errors.New("replication: maxBufferedBytes must be at least 3 x maxBatchBytes (active interval, one batch being written and one leased)")
	}
	if o.MaxDiskBytes < minDiskBytes {
		return errors.New("replication: maxDiskBytes must be at least 8 MiB")
	}
	if o.MaxDiskBytes/4 < o.MaxBatchBytes {
		return errors.New("replication: maxDiskBytes must be at least 4 x maxBatchBytes")
	}
	return nil
}

// segmentBytesFor é o teto de segmento do log: maxDiskBytes/8, entre 1 MiB e
// 64 MiB.
func segmentBytesFor(maxDiskBytes int64) int64 {
	return min(max(maxDiskBytes/8, minSegmentBytes), maxSegmentBytes)
}

// Batch is an immutable, leased group of feed entries.
type Batch struct {
	Epoch    string    `json:"epoch"`
	From     uint64    `json:"from"`
	To       uint64    `json:"to"`
	At       time.Time `json:"at"`
	Reason   string    `json:"reason"`
	Messages int       `json:"messages"`
	Entries  []Entry   `json:"entries"`
	// EndPos é a posição acumulada, em bytes de quadro desde o início da
	// geração, do fim deste lote. Fica fora do wire HTTP: é o que o
	// publicador usa para o atraso em bytes.
	EndPos uint64 `json:"-"`
	bytes  int64
	refs   int
	// retained: o lote ainda está no cache. Despejado (ou descartado por
	// invalidação/rotação) com refs > 0, a memória dele só sai da conta no
	// último release.
	retained bool
	// gen é a geração interna em que o lote nasceu (ver Stream.gen).
	gen uint64
	// persisted: o gravador já pôs o lote no log. Só lote gravado sai do cache.
	persisted bool
}

type identity struct {
	table string
	row   types.RowID
}
type pending struct {
	entry  Entry
	memory []byte
}

// Status describes the feed cursor, retention window and flush counters.
type Status struct {
	Epoch           string `json:"epoch"`
	Produced        uint64 `json:"produced"`
	AvailableAfter  uint64 `json:"availableAfter"`
	BufferedBytes   int64  `json:"bufferedBytes"`
	InputMessages   uint64 `json:"inputMessages"`
	OutputMessages  uint64 `json:"outputMessages"`
	EvictedBatches  uint64 `json:"evictedBatches"`
	Flushes         uint64 `json:"flushes"`
	LastFlushReason string `json:"lastFlushReason"`
	// Invalidations conta as vezes em que o feed descartou o intervalo
	// retido e forçou os seguidores a ressincronizar: por Invalidate, por
	// memória cheia de lotes ainda não gravados no Publish, por quadro que o
	// gravador não consegue codificar ou por quadro corrompido lido do disco.
	Invalidations uint64 `json:"invalidations"`
	// DiskBytes e DiskSegments são o tamanho do log de segmentos neste
	// instante (cabeçalhos incluídos).
	DiskBytes    int64 `json:"-"`
	DiskSegments int   `json:"-"`
	// DiskErrors conta as falhas de disco do feed (gravação, religação do
	// log, limpeza da retenção, leitura). Cada retentativa que falha conta.
	DiskErrors uint64 `json:"-"`
	// EndPos é a posição em bytes do fim do último lote fechado.
	EndPos uint64 `json:"-"`
	// HistoryExcluded anuncia que a tabela histórica não é publicada no feed.
	// Ausente no wire vale o legado (histórica publicada).
	HistoryExcluded bool `json:"historyExcluded,omitempty"`
}

// OriginKind diz de onde a geração corrente do feed veio.
type OriginKind uint8

const (
	// OriginFresh é o feed que nunca tinha subido (sem `state`).
	OriginFresh OriginKind = iota + 1
	// OriginClean é a retomada da mesma epoch depois de um fechamento limpo.
	OriginClean
	// OriginCrash é a geração nova depois de queda, fechamento sem marca limpa,
	// impressão divergente, log inválido ou Publish antes do Resume.
	OriginCrash
	// OriginRotated é a geração nova pedida em operação (Rotate).
	OriginRotated
)

// Origin descreve a geração corrente: o tipo, a epoch anterior (quando
// conhecida e diferente) e a sequence em que a retomada limpa recomeçou.
type Origin struct {
	Kind            OriginKind
	PreviousEpoch   string
	ResumedSequence uint64
}

// opened é o que o Open leu, consumido pelo Resume.
type opened struct {
	state        feedState
	stateFound   bool
	stateCorrupt bool
	// log é o log aberto quando o `state` era limpo; logCorrupt marca o log
	// incoerente (errCorruptLog), que o Resume trata como queda.
	log        *diskLog
	logCorrupt bool
}

// Stream é o feed de um dataset: cache limitado em memória, log de segmentos
// em disco e uma goroutine de gravação.
//
// Ciclo de vida: Open (só leitura) → Resume (decide a geração e grava o
// `state` sem marca limpa) → Publish/Acquire → Close (drena, sincroniza e,
// com impressão, grava a marca limpa) → Wait (fim da vida: junta as
// goroutines e fecha o log). Depois do Close os leitores continuam sendo
// servidos, da memória e do disco, até esgotar; o log só fecha no Wait.
//
// Concorrência: mu protege o estado em memória e nunca é segurado durante
// I/O. A ordem de locks é stateMu → mu → (mu do diskLog). Nada sob mu chama
// readAt nem release de um handle de segmento.
type Stream struct {
	mu           sync.Mutex
	opts         Options
	dir          string
	log          *logging.Logger
	segmentBytes int64

	open opened

	resumed bool
	origin  Origin
	disk    *diskLog
	// prePublishes conta os Publish (e Invalidate) antes do Resume: forçam
	// OriginCrash.
	prePublishes uint64

	epoch    string
	epochRaw [16]byte
	// gen é a geração INTERNA: incrementa a cada invalidação e a cada
	// Rotate. genFloor e genEndPos são a base do log dessa geração (o bind).
	gen                 uint64
	genFloor, genEndPos uint64
	sequence, floor     uint64
	endPos              uint64

	active             list.List
	last               map[identity]*list.Element
	first              uint64
	started            time.Time
	count              int
	activeBytes, bytes int64
	// batches é o cache, em ordem; queue é a fila do gravador (todo lote da
	// fila também está em batches até ser gravado ou descartado).
	batches []*Batch
	queue   []*Batch
	changed chan struct{}
	closed  bool
	stats   Status

	// Gravador. wake é sinalizado a cada lote novo; kick a cada mudança de
	// geração, pedido de parada ou aborto (só kick interrompe o backoff).
	wake, kick  chan struct{}
	writerDone  chan struct{}
	writerStop  bool
	writerAbort bool
	// writerGen é a geração interna a que o log está ligado.
	writerGen uint64

	// Mutação tardia: late é um Publish (ou Invalidate) depois do Close.
	late     bool
	lateOnce sync.Once
	lateDone chan struct{}
	// stateMu serializa as gravações do `state`.
	stateMu   sync.Mutex
	logClosed bool
	waitOnce  sync.Once

	// closeOnce e closeErr: o Close roda uma vez, e toda chamada devolve o
	// erro dela.
	closeOnce sync.Once
	closeErr  error

	// Ganchos de teste (nil em produção). writeHook roda no gravador com o
	// quadro codificado, antes do append; um erro conta como falha de
	// gravação. readHook roda no leitor de disco antes da primeira leitura,
	// fora de mu. closeStateHook roda no Close entre a leitura de `late` e a
	// gravação do `state`; lateStateHook, na goroutine do Publish tardio,
	// antes de ela gravar o `state`.
	writeHook      func([]byte) error
	readHook       func()
	closeStateHook func()
	lateStateHook  func()
	// resumeHook roda no começo de cada decisão do Resume, fora de mu, como
	// se fosse o I/O dela: é onde o teste encaixa um Publish concorrente.
	resumeHook func()
}

// Open abre o feed em dir SÓ PARA LEITURA: lê o `state` e, se ele marca um
// fechamento limpo, os segmentos. Não cria, não grava e não apaga nada; quem
// grava é o Resume. Um `state` ilegível (CRC) ou um log incoerente
// (errCorruptLog) não falham o Open: viram queda no Resume. Erro de I/O de
// verdade (permissão, por exemplo) falha.
func Open(dir string, o Options) (*Stream, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	s := &Stream{
		opts: o, dir: dir, log: o.Logger, segmentBytes: segmentBytesFor(o.MaxDiskBytes),
		last: make(map[identity]*list.Element), changed: make(chan struct{}),
		wake: make(chan struct{}, 1), kick: make(chan struct{}, 1),
	}
	if s.log == nil {
		s.log = logging.Discard()
	}
	st, ok, err := readFeedState(dir)
	switch {
	case errors.Is(err, errCorruptState):
		s.open.stateCorrupt = true
	case err != nil:
		return nil, err
	case ok:
		s.open.state, s.open.stateFound = st, true
	}
	// Sem a marca limpa o Resume apaga tudo: não há por que ler os segmentos.
	if ok && st.Clean {
		l, err := openDiskLog(dir, s.segmentBytes, o.MaxDiskBytes)
		switch {
		case errors.Is(err, errCorruptLog):
			s.open.logCorrupt = true
		case err != nil:
			return nil, err
		default:
			s.open.log = l
		}
	}
	return s, nil
}

func newEpoch() [16]byte {
	var id [16]byte
	// crypto/rand.Read não falha desde o Go 1.24 (aborta o processo).
	_, _ = rand.Read(id[:])
	return id
}

// Resume decide a geração do feed e é o primeiro passo que grava:
//   - sem `state`: OriginFresh, epoch nova;
//   - `state` limpo, impressão igual a fp, log coerente e cauda conferida
//     (validateTail): OriginClean, a MESMA epoch, sequence e posição em bytes
//     continuam, e ResumedSequence é a sequence gravada;
//   - qualquer outro caso, inclusive um Publish antes do Resume: OriginCrash.
//     Os segmentos são apagados, nasce epoch nova e a sequence volta a 0;
//     PreviousEpoch é a epoch do `state`, se legível.
//
// Antes de voltar, regrava o `state` SEM a marca limpa, de forma durável
// (atomicfile): uma queda daqui em diante é sempre queda. Cria o diretório do
// feed se preciso e liga a goroutine de gravação.
func (s *Stream) Resume(fp Fingerprint) (Origin, error) {
	s.mu.Lock()
	switch {
	case s.closed:
		s.mu.Unlock()
		return Origin{}, ErrClosed
	case s.resumed:
		s.mu.Unlock()
		return Origin{}, errAlreadyResumed
	}
	pre := s.prePublishes
	o := s.open
	s.open = opened{}
	s.mu.Unlock()

	forceCrash := pre > 0
	for {
		origin, d, st, err := s.prepareGeneration(o, fp, forceCrash)
		o.log = nil // consumido: aberto, fechado ou trocado por d
		if err != nil {
			return Origin{}, err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = d.close()
			return Origin{}, ErrClosed
		}
		if s.prePublishes != pre {
			// Um Publish chegou durante a decisão: a retomada limpa deixou
			// de ser verdade. Refaz como queda.
			pre = s.prePublishes
			if origin.Kind == OriginClean {
				s.mu.Unlock()
				_ = d.close()
				forceCrash = true
				continue
			}
		}
		if pre > 0 && origin.Kind == OriginFresh {
			origin.Kind = OriginCrash
		}
		s.installLocked(origin, d, st)
		s.mu.Unlock()
		if pre > 0 {
			s.log.Error("replication.feed.publish_before_resume",
				"changes reached the replication feed before it resumed; the feed starts a new generation and consumers resynchronize",
				"changes", pre, "epoch", epochString(st.Epoch))
		}
		return origin, nil
	}
}

// prepareGeneration faz o I/O do Resume, fora de mu: decide a origem, deixa o
// log pronto (ligado à geração) e grava o `state` sem a marca limpa.
func (s *Stream) prepareGeneration(o opened, fp Fingerprint, forceCrash bool) (Origin, *diskLog, feedState, error) {
	if s.resumeHook != nil {
		s.resumeHook()
	}
	if !forceCrash && o.stateFound && o.state.Clean && o.log != nil && fp.Equal(o.state.Fingerprint) {
		st := o.state
		if err := o.log.bind(st.Epoch, st.Floor, st.EndPos); err == nil {
			if err := o.log.validateTail(st.Epoch, st.Sequence, st.EndPos); err == nil {
				st.Floor = max(st.Floor, o.log.floor())
				st.Clean, st.Fingerprint = false, Fingerprint{}
				if err := s.writeState(st); err != nil {
					_ = o.log.close()
					return Origin{}, nil, feedState{}, err
				}
				return Origin{Kind: OriginClean, ResumedSequence: st.Sequence}, o.log, st, nil
			}
		}
	}
	if o.log != nil {
		_ = o.log.close()
	}
	origin := Origin{Kind: OriginCrash}
	switch {
	case o.stateFound:
		origin.PreviousEpoch = epochString(o.state.Epoch)
	case !o.stateCorrupt && !forceCrash:
		origin.Kind = OriginFresh
	}
	if err := removeSegments(s.dir); err != nil {
		return Origin{}, nil, feedState{}, err
	}
	d, err := createDiskLog(s.dir, s.segmentBytes, s.opts.MaxDiskBytes)
	if err != nil {
		return Origin{}, nil, feedState{}, err
	}
	st := feedState{Epoch: newEpoch()}
	if err := d.bind(st.Epoch, 0, 0); err != nil {
		_ = d.close()
		return Origin{}, nil, feedState{}, err
	}
	if err := s.writeState(st); err != nil {
		_ = d.close()
		return Origin{}, nil, feedState{}, err
	}
	return origin, d, st, nil
}

// installLocked liga a geração decidida pelo Resume e sobe o gravador.
func (s *Stream) installLocked(origin Origin, d *diskLog, st feedState) {
	s.origin = origin
	s.disk = d
	s.epochRaw = st.Epoch
	s.epoch = epochString(st.Epoch)
	s.sequence, s.floor, s.endPos = st.Sequence, st.Floor, st.EndPos
	s.gen, s.genFloor, s.genEndPos, s.writerGen = 0, st.Floor, st.EndPos, 0
	s.resumed = true
	s.writerDone = make(chan struct{})
	go s.runWriter()
}

// writeState grava o `state` sob stateMu.
func (s *Stream) writeState(st feedState) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return writeFeedState(s.dir, st)
}

// stateLocked é o retrato da geração corrente para o `state`.
func (s *Stream) stateLocked() feedState {
	return feedState{Epoch: s.epochRaw, Sequence: s.sequence, Floor: s.floor, EndPos: s.endPos}
}

// Cursor returns the current generation and produced sequence.
func (s *Stream) Cursor() (string, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.sequence
}

// Position devolve a geração, a sequence produzida e a posição em bytes do
// fim do último lote fechado, lidas juntas.
func (s *Stream) Position() (epoch string, sequence, endPos uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.sequence, s.endPos
}

// Origin devolve de onde a geração corrente veio (Resume ou Rotate).
func (s *Stream) Origin() Origin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.origin
}

func entryBytes(e Entry) int64 {
	return int64(192 + len(e.Table) + len(e.Record.Key) + len(e.Record.Payload))
}

// Publish copies the borrowed record. Replacing a pending operation moves it to
// its last occurrence, without crossing another operation of that entity.
// History rows have distinct RowIDs and consequently never coalesce.
//
// Nunca faz I/O, nunca espera leitor nem disco e nunca devolve erro: o lote
// fechado entra na fila do gravador. Sem espaço na memória e só com lotes
// ainda não gravados para despejar, o feed invalida o intervalo (lacuna).
//
// Antes do Resume não registra nada (devolve 0) e força a queda no Resume.
// Depois do Close não registra nada e derruba a marca limpa do `state` (ver
// markLateLocked).
func (s *Stream) Publish(table string, op Operation, r types.Record, at time.Time) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.markLateLocked()
		return s.sequence
	}
	if !s.resumed {
		s.prePublishes++
		return 0
	}
	// A sequence nova só é consumida DEPOIS do fechamento por bytes abaixo:
	// o lote que fecha ali termina na sequence anterior. Consumida antes, o
	// lote declarava To = a sequence desta entrada sem contê-la, e o
	// consumidor, que descarta tudo <= cursor, perdia esta entrada no lote
	// seguinte.
	seq := s.sequence + 1
	s.stats.InputMessages++
	e := Entry{Sequence: seq, Table: table, Operation: op, Record: r, At: at}
	id := identity{table, r.RowID}
	n := s.last[id]
	if op >= Schema || (n != nil && n.Value.(*pending).entry.Operation != op) {
		n = nil
		// The previous operation is a hard boundary. Do not leave its list
		// element indexed, otherwise a later operation of the same kind could
		// incorrectly deduplicate across the boundary.
		delete(s.last, id)
	}
	oldBytes := int64(0)
	if n != nil {
		oldBytes = entryBytes(n.Value.(*pending).entry)
	}
	size := entryBytes(e)
	if s.count > 0 && s.activeBytes-oldBytes+size > s.opts.MaxBatchBytes {
		s.flushLocked(at, "bytes")
		n = nil
		oldBytes = 0
	}
	s.sequence = seq
	if !s.makeRoomLocked(size - oldBytes) {
		// O espaço restante é de lotes ainda não gravados ou arrendados, ou o
		// registro sozinho passa do teto. Perde o intervalo de forma
		// explícita; nunca trava o banco nem publica um buraco invisível.
		s.invalidateLocked()
		return s.sequence
	}
	if s.count == 0 {
		s.first = e.Sequence
		s.started = at
	}
	var p *pending
	if n == nil {
		p = &pending{}
		n = s.active.PushBack(p)
	} else {
		p = n.Value.(*pending)
		s.active.MoveToBack(n)
	}
	// Capacity is also bounded: shrinking an outlier releases its allocation.
	length := len(r.Key) + len(r.Payload)
	if cap(p.memory) < length || cap(p.memory) > length*2 {
		p.memory = make([]byte, length)
	} else {
		p.memory = p.memory[:length]
	}
	copy(p.memory, r.Key)
	copy(p.memory[len(r.Key):], r.Payload)
	e.Record.Key = p.memory[:len(r.Key):len(r.Key)]
	e.Record.Payload = p.memory[len(r.Key):]
	p.entry = e
	if op < Schema {
		s.last[id] = n
	}
	s.activeBytes += size - oldBytes
	s.bytes += size - oldBytes
	s.count++
	switch {
	case s.opts.Interval == 0:
		s.flushLocked(at, "immediate")
	case s.count >= s.opts.MaxMessages:
		s.flushLocked(at, "messages")
	case s.activeBytes >= s.opts.MaxBatchBytes:
		s.flushLocked(at, "bytes")
	}
	return e.Sequence
}

// markLateLocked registra uma mutação depois do Close. Não faz I/O (roda sob
// o wmu do Store): liga late, e uma única goroutine regrava o `state` sem a
// marca limpa e emite o ERROR. O Close confere late de novo DEPOIS de gravar
// a marca limpa, e a desfaz se ela mudou: nenhum Publish tardio fica coberto
// por clean=true, nem antes nem depois de o Close voltar.
func (s *Stream) markLateLocked() {
	s.late = true
	if !s.resumed {
		// Antes do Resume nada foi gravado: a próxima subida não tem como
		// confundir este feed com um fechamento limpo.
		return
	}
	s.lateOnce.Do(func() {
		s.lateDone = make(chan struct{})
		go s.recordLate()
	})
}

func (s *Stream) recordLate() {
	defer close(s.lateDone)
	if s.lateStateHook != nil {
		s.lateStateHook()
	}
	s.stateMu.Lock()
	s.mu.Lock()
	st := s.stateLocked()
	s.mu.Unlock()
	err := writeFeedState(s.dir, st)
	s.stateMu.Unlock()
	args := []any{"epoch", epochString(st.Epoch), "sequence", st.Sequence}
	if err != nil {
		args = append(args, "error", err.Error())
	}
	s.log.Error("replication.feed.publish_after_close",
		"a change reached the replication feed after it closed and was not published; the next start opens a new generation and consumers resynchronize",
		args...)
}

func (s *Stream) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (s *Stream) kickWriterLocked() { signal(s.kick) }

// makeRoomLocked despeja do começo do cache lotes JÁ GRAVADOS até caber need.
// Despejar não cria lacuna: o lote está no disco. Para no primeiro lote ainda
// não gravado (o cache é contíguo).
func (s *Stream) makeRoomLocked(need int64) bool {
	for s.bytes+need > s.opts.MaxBufferedBytes && len(s.batches) > 0 && s.batches[0].persisted {
		b := s.batches[0]
		s.batches[0] = nil
		s.batches = s.batches[1:]
		b.retained = false
		s.stats.EvictedBatches++
		if b.refs == 0 {
			s.bytes -= b.bytes
		}
	}
	return s.bytes+need <= s.opts.MaxBufferedBytes
}

// dropAllLocked descarta o intervalo ativo, o cache e a fila do gravador. Os
// lotes da fila saem da conta E do heap (nada mais aponta para eles, salvo o
// que o gravador estiver codificando agora); os arrendados saem da conta no
// último release.
func (s *Stream) dropAllLocked() {
	s.bytes -= s.activeBytes
	s.active.Init()
	clear(s.last)
	s.count = 0
	s.activeBytes = 0
	for _, b := range s.batches {
		b.retained = false
		if b.refs == 0 {
			s.bytes -= b.bytes
		}
	}
	clear(s.batches)
	s.batches = nil
	clear(s.queue)
	s.queue = nil
}

// invalidateLocked declara a lacuna: mesma epoch, o piso salta para o
// produzido e a geração interna muda. O gravador descarta qualquer lote da
// geração velha (mesmo o que já tirou da fila) e apaga o log antes do próximo
// quadro, de modo que lote velho nunca fica gravado depois da invalidação.
func (s *Stream) invalidateLocked() {
	s.dropAllLocked()
	s.floor = s.sequence
	s.gen++
	s.genFloor, s.genEndPos = s.floor, s.endPos
	s.stats.Invalidations++
	s.kickWriterLocked()
	s.notifyLocked()
}

// Invalidate is used if a producer can no longer guarantee complete capture.
// Antes do Resume força a queda; depois do Close derruba a marca limpa.
func (s *Stream) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		s.markLateLocked()
	case !s.resumed:
		s.prePublishes++
	default:
		s.invalidateLocked()
	}
}

// Rotate abre uma geração nova em operação (rewind, por exemplo): epoch
// nova, sequence e posição em bytes de volta a 0, cache e fila descartados,
// leitores acordados (e mandados ressincronizar). O wipe do log fica com o
// gravador. Regrava o `state`, sem a marca limpa, com a epoch nova: uma queda
// depois do Rotate reporta a epoch rotacionada como anterior. No-op antes do
// Resume e depois do Close.
//
// Faz I/O SÍNCRONO: a regravação do `state` (writeFeedState, com fsync do
// arquivo e do diretório) acontece antes de voltar. Por isso o Rotate NUNCA
// pode ser chamado sob o wmu do Store nem de dentro do Publish: é chamada do
// publicador, na decisão de rewind da subida.
func (s *Stream) Rotate(reason string) {
	raw := newEpoch()
	s.mu.Lock()
	if !s.resumed || s.closed {
		s.mu.Unlock()
		return
	}
	old := s.epoch
	s.dropAllLocked()
	s.epochRaw, s.epoch = raw, epochString(raw)
	s.sequence, s.floor, s.endPos = 0, 0, 0
	s.gen++
	s.genFloor, s.genEndPos = 0, 0
	s.origin = Origin{Kind: OriginRotated, PreviousEpoch: old}
	s.kickWriterLocked()
	s.notifyLocked()
	s.mu.Unlock()

	s.stateMu.Lock()
	s.mu.Lock()
	// Fechado no meio: o Close grava o `state` certo (ele lê a epoch nova).
	closed, st := s.closed, s.stateLocked()
	s.mu.Unlock()
	var err error
	if !closed {
		err = writeFeedState(s.dir, st)
	}
	s.stateMu.Unlock()
	if err != nil {
		s.mu.Lock()
		s.stats.DiskErrors++
		s.mu.Unlock()
		s.log.Warn("replication.feed.disk_error",
			"the replication feed could not record its new generation; a crash now reports the generation before the rotation",
			"stage", "state", "error", err.Error())
	}
	s.log.Info("replication.feed.rotated", "the replication feed started a new generation",
		"reason", reason, "previousEpoch", old, "epoch", epochString(raw))
}

// flushLocked fecha o intervalo ativo num lote e o põe no cache e na fila do
// gravador. A posição em bytes do lote é calculada aqui (frameSize), sem
// codificar: o gravador codifica fora de mu.
func (s *Stream) flushLocked(at time.Time, reason string) {
	if s.count == 0 {
		return
	}
	b := &Batch{Epoch: s.epoch, From: s.first, To: s.sequence, At: at, Reason: reason, Messages: s.count, bytes: s.activeBytes, retained: true, gen: s.gen, Entries: make([]Entry, 0, s.active.Len())}
	for n := s.active.Front(); n != nil; n = n.Next() {
		b.Entries = append(b.Entries, n.Value.(*pending).entry)
	}
	s.endPos += uint64(frameSize(b))
	b.EndPos = s.endPos
	s.batches = append(s.batches, b)
	s.queue = append(s.queue, b)
	s.stats.OutputMessages += uint64(len(b.Entries))
	s.stats.Flushes++
	s.stats.LastFlushReason = reason
	s.active.Init()
	clear(s.last)
	s.count = 0
	s.activeBytes = 0
	signal(s.wake)
	s.notifyLocked()
}

// Flush closes the active interval and returns the produced sequence.
func (s *Stream) Flush(at time.Time, reason string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.flushLocked(at, reason)
	}
	return s.sequence
}

// Tick closes an interval whose configured delay has elapsed.
func (s *Stream) Tick(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && s.count > 0 && at.Sub(s.started) >= s.opts.Interval {
		s.flushLocked(at, "interval")
	}
}

// Start runs the interval timer. A zero interval is event-driven and needs no
// goroutine; message and byte thresholds still flush synchronously in Publish.
func (s *Stream) Start(ctx context.Context) {
	if s.opts.Interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(s.opts.Interval)
		defer t.Stop()
		for {
			select {
			case now := <-t.C:
				s.Tick(now)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// runWriter é a goroutine de gravação: religa o log a cada geração nova
// (wipe e bind) e grava os lotes da fila em ordem, fora de mu. Erro de disco
// não chega ao Store: uma falha de ESCRITA deixa o lote na memória e a
// gravação é retentada com backoff, e a lacuna só vem quando a memória lota
// (no Publish). Uma falha de DURABILIDADE (errLogFailed, fdatasync na
// rotação) é a exceção: o log fica falho, e a lacuna é declarada na hora.
func (s *Stream) runWriter() {
	defer close(s.writerDone)
	var buf []byte
	var backoff time.Duration
	var lastWarn time.Time
	for {
		// Depois de cada lote: um buffer que cresceu para um quadro acima de
		// 2 × MaxBatchBytes não fica retido no heap do gravador (a mesma
		// disciplina do snapshotArenaKeepBytes da exportação).
		if cap(buf) > 2*int(s.opts.MaxBatchBytes) {
			buf = nil
		}
		s.mu.Lock()
		for !s.writerAbort && s.writerGen == s.gen && len(s.queue) == 0 && !s.writerStop {
			s.mu.Unlock()
			select {
			case <-s.wake:
			case <-s.kick:
			}
			s.mu.Lock()
		}
		if s.writerAbort || (s.writerStop && s.writerGen == s.gen && len(s.queue) == 0) {
			s.mu.Unlock()
			return
		}
		if s.writerGen != s.gen {
			gen, raw, floor, endPos := s.gen, s.epochRaw, s.genFloor, s.genEndPos
			s.mu.Unlock()
			if err := s.rebind(raw, floor, endPos); err != nil {
				if errors.Is(err, errLogClosed) {
					return
				}
				s.diskError("rebind", err, &backoff, &lastWarn)
				continue
			}
			s.mu.Lock()
			s.writerGen = gen
			s.mu.Unlock()
			backoff = 0
			continue
		}
		b := s.queue[0]
		hook := s.writeHook
		s.mu.Unlock()

		var err error
		buf, err = appendFrame(buf[:0], b, b.EndPos)
		if err != nil {
			// Campo que não cabe no prefixo do quadro: nunca vai caber.
			s.gap(b, "frame_too_large", err)
			buf = nil
			continue
		}
		if hook != nil {
			if err := hook(buf); err != nil {
				s.diskError("write", err, &backoff, &lastWarn)
				continue
			}
		}
		s.mu.Lock()
		stale := b.gen != s.gen || s.writerAbort
		s.mu.Unlock()
		if stale {
			// Invalidado (ou rotacionado) enquanto era codificado: nem chega
			// ao disco.
			continue
		}
		_, err = s.disk.append(buf, b.From, b.To, b.EndPos)
		switch {
		case err == nil || errors.Is(err, errRetentionUnlink):
			if err != nil {
				s.diskError("retention", err, nil, &lastWarn)
			}
			backoff = 0
			s.mu.Lock()
			if b.gen == s.gen && len(s.queue) > 0 && s.queue[0] == b {
				b.persisted = true
				s.queue[0] = nil
				s.queue = s.queue[1:]
				s.floor = max(s.floor, s.disk.floor())
			}
			s.mu.Unlock()
		case errors.Is(err, errLogClosed):
			return
		case errors.Is(err, errLogFailed):
			// fdatasync falhou: o log fica falho até o wipe, e retentar
			// devolveria o mesmo erro para sempre. A lacuna vem NA HORA, e a
			// religação da geração nova apaga o log e o cura.
			s.gap(b, "durability", err)
		case errors.Is(err, errNotContiguous), errors.Is(err, errBadAppend), errors.Is(err, errLogUnbound):
			// O log recusou o quadro sem gravar: repetir daria o mesmo erro.
			// A lacuna religa o log numa base nova.
			s.gap(b, "append_rejected", err)
		default:
			s.diskError("write", err, &backoff, &lastWarn)
		}
	}
}

// rebind apaga o log e o liga à geração nova.
func (s *Stream) rebind(epoch [16]byte, floor, endPos uint64) error {
	if err := s.disk.wipe(); err != nil {
		return err
	}
	return s.disk.bind(epoch, floor, endPos)
}

// gap invalida o intervalo por um quadro que o log nunca vai aceitar (se o
// lote ainda é da geração corrente) e avisa.
func (s *Stream) gap(b *Batch, cause string, err error) {
	s.mu.Lock()
	if b.gen == s.gen {
		s.invalidateLocked()
	}
	s.mu.Unlock()
	s.log.Warn("replication.feed.gap",
		"the replication feed dropped its retained interval; consumers resynchronize",
		"cause", cause, "from", b.From, "to", b.To, "error", err.Error())
}

// diskError conta a falha, emite o WARN no máximo a cada diskWarnInterval e,
// com backoff, espera antes da retentativa (interrompida só por mudança de
// geração, parada ou aborto, nunca por lote novo).
func (s *Stream) diskError(stage string, err error, backoff *time.Duration, lastWarn *time.Time) {
	s.mu.Lock()
	s.stats.DiskErrors++
	total := s.stats.DiskErrors
	s.mu.Unlock()
	if backoff != nil {
		*backoff = min(max(2**backoff, writerMinBackoff), writerMaxBackoff)
	}
	if now := time.Now(); lastWarn.IsZero() || now.Sub(*lastWarn) >= diskWarnInterval {
		*lastWarn = now
		args := []any{"stage", stage, "error", err.Error(), "diskErrors", total}
		if backoff != nil {
			args = append(args, "retryIn", backoff.String())
		}
		s.log.Warn("replication.feed.disk_error",
			"the replication feed could not write to disk; batches stay in memory and the write is retried",
			args...)
	}
	if backoff == nil {
		return
	}
	t := time.NewTimer(*backoff)
	defer t.Stop()
	// O laço principal relê o estado (geração, parada, aborto) sob mu antes
	// de cada passo, então o sinal consumido aqui não precisa ser devolvido.
	// Devolvê-lo faria cada retentativa seguinte pular o backoff.
	select {
	case <-t.C:
	case <-s.kick:
	}
}

// Acquire devolve um lote arrendado. Release tem de ser chamado mesmo que a
// rede caia.
//
// Do cache, o lote é compartilhado (sem cópia por seguidor) e conta na
// memória do feed até o último release. Do disco, quadros consecutivos são
// agrupados num lote do leitor, até MaxBatchBytes e MaxMessages (sempre ao
// menos um quadro); esse lote não entra no cache nem na conta, e o release
// dele não faz nada. A busca no log (locate) acontece sob mu, sem syscall; a
// leitura e o release do segmento, fora dele.
//
// Cursor abaixo do piso, de outra epoch ou além do produzido:
// ErrResyncRequired. Um quadro corrompido no disco vira lacuna (invalidação)
// e ErrResyncRequired.
//
// Depois do Close, a cauda continua sendo servida (da memória e do disco):
// o Close fecha o intervalo ativo justamente para que ele seja entregue.
// Esgotada a cauda, devolve ErrClosed na hora — nunca espera em s.changed,
// que ninguém mais fecha. Depois do Wait o log está fechado, e o que ainda
// estivesse só no disco também vira ErrClosed.
func (s *Stream) Acquire(ctx context.Context, epoch string, after uint64) (*Batch, func(), error) {
	for {
		s.mu.Lock()
		if !s.resumed || epoch != s.epoch || after > s.sequence {
			s.mu.Unlock()
			return nil, nil, ErrResyncRequired
		}
		if b := s.cachedLocked(after); b != nil {
			b.refs++
			s.mu.Unlock()
			var once sync.Once
			return b, func() {
				once.Do(func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					b.refs--
					if b.refs == 0 && !b.retained {
						s.bytes -= b.bytes
					}
				})
			}, nil
		}
		if after < s.floor {
			s.mu.Unlock()
			return nil, nil, ErrResyncRequired
		}
		if after < s.closedToLocked() {
			// O que falta está só no disco (gravado e despejado do cache).
			if s.logClosed {
				s.mu.Unlock()
				return nil, nil, ErrClosed
			}
			h, off, ok := s.disk.locate(after)
			gen, hook := s.gen, s.readHook
			s.mu.Unlock()
			if !ok {
				return nil, nil, s.lostCursor()
			}
			b, err := s.readDisk(h, off, after, epoch, hook)
			h.release()
			if err != nil {
				return nil, nil, s.diskReadFailed(gen, err)
			}
			s.mu.Lock()
			current := s.gen == gen
			s.mu.Unlock()
			if !current {
				return nil, nil, ErrResyncRequired
			}
			if b != nil {
				return b, func() {}, nil
			}
			// Nada depois de after no segmento: só numa corrida com o
			// gravador. Espera a próxima mudança em vez de girar.
			s.mu.Lock()
		}
		if s.closed {
			s.mu.Unlock()
			return nil, nil, ErrClosed
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-ch:
		}
	}
}

// cachedLocked devolve o lote do cache que continua o cursor, ou nil se o
// cursor está antes do começo do cache (o resto está no disco) ou no fim.
func (s *Stream) cachedLocked(after uint64) *Batch {
	for _, b := range s.batches {
		if b.To <= after {
			continue
		}
		if b.From > after+1 {
			return nil
		}
		return b
	}
	return nil
}

// closedToLocked é a última sequence já fechada num lote.
func (s *Stream) closedToLocked() uint64 {
	if s.count > 0 {
		return s.first - 1
	}
	return s.sequence
}

// availableAfterLocked é o menor cursor servível: o piso do disco, ou o
// começo do cache quando ele vai mais para trás (cache maior que o disco).
func (s *Stream) availableAfterLocked() uint64 {
	if len(s.batches) > 0 && s.batches[0].From-1 < s.floor {
		return s.batches[0].From - 1
	}
	return s.floor
}

// lostCursor classifica um locate que não achou segmento: log já fechado
// (Wait) é ErrClosed; o resto é cursor abaixo do piso.
func (s *Stream) lostCursor() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logClosed {
		return ErrClosed
	}
	return ErrResyncRequired
}

// diskReadFailed trata uma leitura do disco que falhou: quadro corrompido ou
// erro de I/O tornam o intervalo inservível, e a lacuna é declarada (se a
// geração ainda é a mesma).
func (s *Stream) diskReadFailed(gen uint64, err error) error {
	if errors.Is(err, errReleased) {
		return ErrResyncRequired
	}
	s.mu.Lock()
	if s.logClosed {
		s.mu.Unlock()
		return ErrClosed
	}
	invalidated := s.gen == gen
	if invalidated {
		if !errors.Is(err, errCorruptFrame) {
			s.stats.DiskErrors++
		}
		s.invalidateLocked()
	}
	s.mu.Unlock()
	if invalidated {
		s.log.Warn("replication.feed.gap",
			"the replication feed could not read a retained batch from disk; consumers resynchronize",
			"cause", "disk_read", "error", err.Error())
	}
	return ErrResyncRequired
}

// segmentReader lê quadros inteiros de um segmento arrendado, a partir de
// off, com leituras de no mínimo readChunk bytes.
type segmentReader struct {
	h        *segmentHandle
	off      int64 // offset no segmento de data[pos]
	data     []byte
	pos, end int
}

// next devolve os bytes do próximo quadro (cabeçalho e corpo, válidos até a
// próxima chamada) ou io.EOF no fim publicado do segmento.
func (r *segmentReader) next() ([]byte, error) {
	if err := r.fill(frameHeaderSize); err != nil {
		return nil, err
	}
	hdr := r.data[r.pos : r.pos+frameHeaderSize]
	if [4]byte(hdr[0:4]) != frameMagic {
		return nil, errCorruptFrame
	}
	size := frameHeaderSize + int(binary.LittleEndian.Uint32(hdr[4:8]))
	// Um tamanho que não cabe no que resta do segmento publicado é lixo
	// (bytes corrompidos no meio do arquivo): recusa SEM alocar, senão um
	// bodyLen adulterado pediria até 4 GiB ao heap antes de ler um byte.
	if size < frameHeaderSize+frameFixedBody || int64(size) > r.h.size()-r.off {
		return nil, errCorruptFrame
	}
	if err := r.fill(size); err != nil {
		if errors.Is(err, io.EOF) {
			// O fim publicado nunca corta um quadro.
			return nil, errCorruptFrame
		}
		return nil, err
	}
	raw := r.data[r.pos : r.pos+size]
	r.pos += size
	r.off += int64(size)
	return raw, nil
}

// fill garante n bytes a partir de pos.
func (r *segmentReader) fill(n int) error {
	if r.end-r.pos >= n {
		return nil
	}
	copy(r.data, r.data[r.pos:r.end])
	r.end -= r.pos
	r.pos = 0
	if len(r.data) < n {
		grown := make([]byte, max(n, readChunk))
		copy(grown, r.data[:r.end])
		r.data = grown
	}
	for r.end < n {
		m, err := r.h.readAt(r.data[r.end:], r.off+int64(r.end))
		r.end += m
		if r.end >= n {
			return nil
		}
		if err != nil {
			return err
		}
		if m == 0 {
			return io.EOF
		}
	}
	return nil
}

// readDisk lê, a partir de off, os quadros com To > after e os agrupa num
// lote do leitor até MaxBatchBytes e MaxMessages, aceitando sempre o
// primeiro. Para no fim do segmento. Devolve nil sem erro se o segmento não
// tem quadro depois de after.
func (s *Stream) readDisk(h *segmentHandle, off int64, after uint64, epoch string, hook func()) (*Batch, error) {
	if hook != nil {
		hook()
	}
	r := segmentReader{h: h, off: off}
	var out *Batch
	for {
		raw, err := r.next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		// To está no corpo, logo depois de From (next garante o corpo fixo):
		// o quadro que o cursor já consumiu nem é decodificado.
		if out == nil && binary.LittleEndian.Uint64(raw[frameHeaderSize+8:frameHeaderSize+16]) <= after {
			continue
		}
		f, _, err := decodeFrame(raw)
		if err != nil {
			return nil, err
		}
		b := f.batch(epoch)
		if out == nil {
			out = b
			continue
		}
		if f.From != out.To+1 || out.bytes+b.bytes > s.opts.MaxBatchBytes || out.Messages+b.Messages > s.opts.MaxMessages {
			return out, nil
		}
		out.Entries = append(out.Entries, b.Entries...)
		out.To, out.At, out.Reason, out.EndPos = b.To, b.At, b.Reason, b.EndPos
		out.Messages += b.Messages
		out.bytes += b.bytes
	}
}

// Check validates that a cursor is still covered by the retained feed.
func (s *Stream) Check(epoch string, after uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	floor := s.availableAfterLocked()
	if !s.resumed || epoch != s.epoch || after < floor || after > s.sequence {
		return fmt.Errorf("%w: cursor %d, available (%d,%d]", ErrResyncRequired, after, floor, s.sequence)
	}
	return nil
}

// Status returns a point-in-time feed snapshot.
func (s *Stream) Status() Status {
	s.mu.Lock()
	out := s.stats
	out.Epoch = s.epoch
	out.Produced = s.sequence
	out.AvailableAfter = s.availableAfterLocked()
	out.BufferedBytes = s.bytes
	out.EndPos = s.endPos
	out.HistoryExcluded = s.opts.ExcludeHistory
	disk := s.disk
	s.mu.Unlock()
	if disk != nil {
		out.DiskBytes = disk.bytes()
		out.DiskSegments = disk.segments()
	}
	return out
}

// Close fecha o feed, nesta ordem:
//  1. sob mu, marca closed, fecha o intervalo ativo e acorda os leitores;
//  2. espera o gravador drenar a fila, até deadline;
//  3. sincroniza o log (fdatasync do segmento corrente e fsync do diretório);
//  4. com fp != nil, grava o `state` com a marca limpa e a impressão.
//
// A marca limpa só é gravada com tudo isso em ordem e sem mutação tardia
// (Publish ou Invalidate depois do passo 1): late é lido sob stateMu e
// conferido de novo DEPOIS da gravação, que é desfeita se ele mudou. fp nil
// fecha sem marca limpa (a próxima subida é queda). Prazo vencido não grava
// nada e devolve erro.
//
// Antes do Resume não toca o disco. Roda uma vez: toda chamada seguinte
// espera a primeira terminar e devolve o MESMO erro dela (fp e deadline das
// seguintes são ignorados). Os leitores continuam sendo servidos até esgotar
// a cauda; o log só fecha no Wait.
func (s *Stream) Close(fp *Fingerprint, deadline time.Duration) error {
	s.closeOnce.Do(func() { s.closeErr = s.close(fp, deadline) })
	return s.closeErr
}

func (s *Stream) close(fp *Fingerprint, deadline time.Duration) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if !s.resumed {
		s.logClosed = true
		l := s.open.log
		s.open.log = nil
		s.notifyLocked()
		s.mu.Unlock()
		if l != nil {
			_ = l.close()
		}
		return nil
	}
	s.flushLocked(time.Now(), "close")
	s.writerStop = true
	s.kickWriterLocked()
	s.notifyLocked()
	done := s.writerDone
	s.mu.Unlock()

	if !waitDone(done, deadline) {
		s.mu.Lock()
		s.writerAbort = true
		s.kickWriterLocked()
		s.mu.Unlock()
		return errCloseDeadline
	}
	syncErr := s.disk.sync()
	if fp == nil {
		return syncErr
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.mu.Lock()
	st, late := s.stateLocked(), s.late
	s.mu.Unlock()
	if s.closeStateHook != nil {
		s.closeStateHook()
	}
	if syncErr != nil {
		return syncErr
	}
	if late {
		return errLateMutation
	}
	st.Clean = true
	st.Fingerprint = Fingerprint{Stores: append([]StorePosition(nil), fp.Stores...)}
	if err := writeFeedState(s.dir, st); err != nil {
		return err
	}
	s.mu.Lock()
	late = s.late
	s.mu.Unlock()
	if late {
		st.Clean, st.Fingerprint = false, Fingerprint{}
		if err := writeFeedState(s.dir, st); err != nil {
			return err
		}
		return errLateMutation
	}
	return nil
}

// waitDone espera done até d; d <= 0 só confere se já terminou.
func waitDone(done <-chan struct{}, d time.Duration) bool {
	select {
	case <-done:
		return true
	default:
	}
	if d <= 0 {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// Wait é o fim da vida do feed: espera o gravador e a goroutine do Publish
// tardio e só então fecha o log. Chame depois do Close e depois que os
// leitores (publicadores, seguidores) drenaram a cauda: a partir daqui o que
// estiver só no disco devolve ErrClosed. Sem Close antes, não faz nada.
// Idempotente.
func (s *Stream) Wait() {
	s.mu.Lock()
	closed, writer, late := s.closed, s.writerDone, s.lateDone
	s.mu.Unlock()
	if !closed {
		return
	}
	if writer != nil {
		<-writer
	}
	if late != nil {
		<-late
	}
	s.waitOnce.Do(func() {
		s.mu.Lock()
		s.logClosed = true
		d := s.disk
		s.mu.Unlock()
		if d != nil {
			_ = d.close()
		}
	})
}
