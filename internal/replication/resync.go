package replication

import (
	"errors"
	"fmt"
	"sync"
)

// ReplicaState is the externally visible lifecycle of a follower.
type ReplicaState string

const (
	ReplicaEmpty          ReplicaState = "empty"
	ReplicaSyncing        ReplicaState = "syncing"
	ReplicaCatchingUp     ReplicaState = "catching_up"
	ReplicaFollowing      ReplicaState = "following"
	ReplicaResyncRequired ReplicaState = "resync_required"
	ReplicaDisconnected   ReplicaState = "disconnected"
)

var ErrGenerationChanged = errors.New("replication: master generation changed")

// Snapshot is the logical handoff between a snapshot exporter and a follower.
// Cursor is captured at the same point as the records; batches after Cursor
// are the catch-up tail. The records are materialized entries, not raw events.
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
}

func NewReplica(begin func(Snapshot) error, apply func(Entry) error, commit func() error, abort func()) *Replica {
	return &Replica{state: ReplicaEmpty, begin: begin, apply: apply, commit: commit, abort: abort}
}

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

// ApplyBatch enforces generation and contiguous sequence. A gap is never
// skipped: the caller must request a new snapshot.
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
	if r.state != ReplicaCatchingUp && r.state != ReplicaFollowing {
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
				return err
			}
		}
	}
	// Deduplication can remove intermediate source sequences. The batch's To
	// is the authoritative cursor, not the last entry's sequence.
	r.applied = b.To
	r.state = ReplicaFollowing
	return nil
}

func (r *Replica) Disconnect() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != ReplicaEmpty {
		r.state = ReplicaDisconnected
	}
}

func (r *Replica) RequireResync() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.abort != nil {
		r.abort()
	}
	r.state = ReplicaResyncRequired
}
