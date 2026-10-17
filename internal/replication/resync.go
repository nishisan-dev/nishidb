package replication

import (
	"errors"
	"fmt"
	"sync"
)

// ReplicaState is the externally visible lifecycle of a follower.
type ReplicaState string

const (
	// ReplicaEmpty is the initial state before the first snapshot.
	ReplicaEmpty ReplicaState = "empty"
	// ReplicaSyncing is the state while a snapshot is being applied.
	ReplicaSyncing ReplicaState = "syncing"
	// ReplicaCatchingUp is the state after commit and before the tail catches up.
	ReplicaCatchingUp ReplicaState = "catching_up"
	// ReplicaFollowing means the follower has a contiguous current cursor.
	ReplicaFollowing ReplicaState = "following"
	// ReplicaResyncRequired means the current cursor cannot be continued.
	ReplicaResyncRequired ReplicaState = "resync_required"
	// ReplicaDisconnected means the master request failed and will be retried.
	ReplicaDisconnected ReplicaState = "disconnected"
	// ReplicaHistoryUnavailable means the follower declares the history table
	// and the master does not publish it (replication.publishHistory: false).
	ReplicaHistoryUnavailable ReplicaState = "history_unavailable"
)

// ErrGenerationChanged means the follower must discard its cursor and resync.
var ErrGenerationChanged = errors.New("replication: master generation changed")

// ErrApplyFailed é a falha LOCAL ao aplicar uma entrada do tail (o Store do
// follower recusou). Não é desconexão: o follower vai para resync_required e
// a rodada seguinte baixa um snapshot. Chega embrulhado num *ApplyError, que
// diz qual entrada falhou.
var ErrApplyFailed = errors.New("replication: follower could not apply an entry")

// ApplyError é a falha local de uma entrada do tail: a sequence e a tabela
// dela, e a causa. errors.Is reconhece ErrApplyFailed e a causa.
type ApplyError struct {
	Sequence uint64
	Table    string
	Err      error
}

func (e *ApplyError) Error() string {
	return fmt.Sprintf("replication: follower could not apply sequence %d of table %s: %v", e.Sequence, e.Table, e.Err)
}

// Unwrap expõe o sentinela e a causa.
func (e *ApplyError) Unwrap() []error { return []error{ErrApplyFailed, e.Err} }

// ErrHistoryNotPublished é a recusa do follower que exige a tabela histórica
// diante de um master que a deixa fora do feed: nada é aplicado, e o follower
// não fica com a histórica vazia em silêncio.
var ErrHistoryNotPublished = errors.New("replication: the master does not publish the history table")

// Snapshot é a passagem lógica entre o exportador de snapshot e um follower.
// O Cursor é lido ANTES da varredura (ver ExportSnapshot), e as entradas são
// estados materializados, não eventos crus. Como a exportação anda em blocos,
// cada um com um snapshot próprio, uma entrada pode estar num estado MAIS
// NOVO que o Cursor; os lotes depois do Cursor são o tail, que reaplica essas
// mudanças por cima, e o resultado converge.
type Snapshot struct {
	Epoch   string  `json:"epoch"`
	Cursor  uint64  `json:"cursor"`
	Entries []Entry `json:"entries"`
}

// Replica applies a snapshot and its tail to an external generation. The
// callbacks are deliberately small: the storage layer owns how a generation
// is built and atomically published, while this type enforces cursor rules.
type Replica struct {
	mu             sync.Mutex
	state          ReplicaState
	epoch          string
	applied        uint64
	snapshotCursor uint64
	begin          func(Snapshot) error
	apply          func(Entry) error
	commit         func() error
	abort          func()
	// resumable diz que a desconexão corrente veio de catching_up ou
	// following: o cursor aplicado continua contíguo, e com a MESMA epoch no
	// master o tail retoma dele sem snapshot (o reinício limpo do master).
	// Só vale no estado disconnected, e só o Disconnect o escreve: toda
	// entrada nesse estado decide de novo, a partir do estado de onde veio.
	resumable bool
}

// NewReplica creates a cursor-enforcing follower state machine.
func NewReplica(begin func(Snapshot) error, apply func(Entry) error, commit func() error, abort func()) *Replica {
	return &Replica{state: ReplicaEmpty, begin: begin, apply: apply, commit: commit, abort: abort}
}

// State returns the lifecycle state, master epoch and applied cursor.
func (r *Replica) State() (ReplicaState, string, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.epoch, r.applied
}

// StartSnapshot invalidates any partial generation and starts a new one. The
// active generation remains owned by the caller, so reads can continue during
// this operation.
func (r *Replica) StartSnapshot(s Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.Epoch == "" {
		return errors.New("replication: snapshot without epoch")
	}
	if r.abort != nil {
		r.abort()
	}
	r.state, r.epoch, r.applied, r.snapshotCursor = ReplicaSyncing, s.Epoch, 0, s.Cursor
	if r.begin != nil {
		if err := r.begin(s); err != nil {
			r.state = ReplicaResyncRequired
			return err
		}
	}
	for _, e := range s.Entries {
		if r.apply != nil {
			if err := r.apply(e); err != nil {
				r.state = ReplicaResyncRequired
				if r.abort != nil {
					r.abort()
				}
				return err
			}
		}
	}
	r.applied = s.Cursor
	return nil
}

// FinishSnapshot publishes the staged generation. It must be called after the
// exporter has confirmed that its cursor is still covered by the feed.
func (r *Replica) FinishSnapshot() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != ReplicaSyncing {
		return fmt.Errorf("replication: snapshot is not active")
	}
	if r.commit != nil {
		if err := r.commit(); err != nil {
			r.state = ReplicaResyncRequired
			if r.abort != nil {
				r.abort()
			}
			return err
		}
	}
	r.state = ReplicaCatchingUp
	return nil
}

// MarkFollowing closes the short catch-up phase when a cursor handshake proves
// that the snapshot already reaches the master's current feed position. It is
// separate from FinishSnapshot so callers can keep queries blocked while a
// non-empty tail is still pending.
func (r *Replica) MarkFollowing() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != ReplicaCatchingUp {
		return fmt.Errorf("replication: follower state %s cannot become following", r.state)
	}
	r.state = ReplicaFollowing
	return nil
}

// tailLocked diz se o estado corrente continua pelo tail: catching_up,
// following, ou a desconexão que veio de um dos dois (resumable).
func (r *Replica) tailLocked() bool {
	return r.state == ReplicaCatchingUp || r.state == ReplicaFollowing ||
		(r.state == ReplicaDisconnected && r.resumable)
}

// TailCursor devolve a epoch e o cursor de onde o tail continua, com ok=false
// quando o estado exige snapshot. A desconexão que veio de catching_up ou
// following continua pelo tail: é o que deixa o follower atravessar o
// reinício limpo do master, que mantém a epoch, sem baixar um snapshot.
func (r *Replica) TailCursor() (epoch string, cursor uint64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.epoch, r.applied, r.tailLocked()
}

// ApplyBatch enforces generation and contiguous sequence. A gap is never
// skipped: the caller must request a new snapshot.
//
// Também aplica na desconexão retomável (ver TailCursor): o lote contíguo
// prova que o master manteve a geração, e o follower volta a following.
func (r *Replica) ApplyBatch(b *Batch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b == nil {
		return errors.New("replication: nil batch")
	}
	if b.Epoch != r.epoch {
		r.state = ReplicaResyncRequired
		return ErrGenerationChanged
	}
	if !r.tailLocked() {
		return fmt.Errorf("replication: follower state %s cannot apply a batch", r.state)
	}
	if b.To <= r.applied {
		return nil
	}
	if b.From > r.applied+1 {
		r.state = ReplicaResyncRequired
		return ErrResyncRequired
	}
	for _, e := range b.Entries {
		if e.Sequence <= r.applied {
			continue
		}
		if r.apply != nil {
			if err := r.apply(e); err != nil {
				// Falha LOCAL no meio do lote: parte dele pode ter sido
				// aplicada sem o cursor andar, e repetir o tail daria o mesmo
				// erro para sempre se ele for determinístico. O caminho é o
				// snapshot.
				r.state = ReplicaResyncRequired
				return &ApplyError{Sequence: e.Sequence, Table: e.Table, Err: err}
			}
		}
	}
	// Deduplication can remove intermediate source sequences. The batch's To
	// is the authoritative cursor, not the last entry's sequence.
	r.applied = b.To
	r.state = ReplicaFollowing
	return nil
}

// Disconnect marks a non-empty replica unavailable while the master is down.
//
// Lembra se o estado anterior era catching_up ou following (resumable): o
// cursor aplicado continua valendo, e a próxima rodada tenta o tail antes do
// snapshot. Uma desconexão repetida mantém o que a primeira lembrou; vinda
// de qualquer outro estado, esquece.
func (r *Replica) Disconnect() {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
	case ReplicaEmpty:
		return
	case ReplicaCatchingUp, ReplicaFollowing:
		r.resumable = true
	case ReplicaDisconnected:
	default:
		r.resumable = false
	}
	r.state = ReplicaDisconnected
}

// HistoryUnavailable marca o follower que exige a tabela histórica diante de
// um master que não a publica. Devolve true só na transição, para que o
// chamador emita o ERROR uma vez por ocorrência, e não a cada retentativa.
// A geração ativa fica como está: nada foi aplicado.
func (r *Replica) HistoryUnavailable() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := r.state != ReplicaHistoryUnavailable
	r.state = ReplicaHistoryUnavailable
	return changed
}

// RequireResync discards the staged generation and requests a new snapshot.
func (r *Replica) RequireResync() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.abort != nil {
		r.abort()
	}
	r.state = ReplicaResyncRequired
}
