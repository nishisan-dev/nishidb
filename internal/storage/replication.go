package storage

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ApplyReplication applies one materialized feed entry. It deliberately uses
// the Store's normal write path so primary, secondary indexes, TTL metadata and
// counters remain subject to the same commit point as local ingestion.
// Callers must serialize entries in feed order.
func (s *Store) ApplyReplication(e replication.Entry) error {
	if s.EhHistory() {
		if e.Operation == replication.Retain {
			if e.Record.RowID > s.RowIDBase() {
				s.rowIDBase.Store(uint64(e.Record.RowID))
			}
			return nil
		}
		if e.Operation != replication.Insert {
			return fmt.Errorf("storage: history accepts only replicated inserts")
		}
		return s.ApendaReplication(e.Record.RowID, e.Record.Payload)
	}
	seq := int64(e.Sequence)
	// Snapshot entries have no feed sequence; their record sequence is the
	// master's materialization sequence. Prefer it whenever present so a
	// resync cannot move an existing follower row backwards.
	if e.Record.Sequence != 0 {
		seq = e.Record.Sequence
	}
	switch e.Operation {
	case replication.Insert, replication.Update:
		_, _, err := s.Put(e.Record.Key, seq, e.Record.Flags&^types.FlagTombstone, e.Record.Payload, nil)
		return err
	case replication.Delete:
		return s.applyReplicationDelete(e.Record.Key, seq)
	default:
		return fmt.Errorf("storage: unsupported replicated operation %d", e.Operation)
	}
}

func (s *Store) applyReplicationDelete(key []byte, seq int64) error {
	prev, buf, presence, err := s.State(key, nil)
	_ = buf
	if err != nil {
		return err
	}
	if presence == types.PresenceAbsent {
		return nil
	}
	tomb, err := s.buildLapide(prev.Payload)
	if err != nil {
		return err
	}
	_, _, err = s.Put(key, seq, types.FlagTombstone, tomb, &prev)
	return err
}
