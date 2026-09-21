package server

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// pruneFollowerBase removes live rows absent from a completed master snapshot.
// Tombstones are not sent for rows older than the snapshot cursor, so pruning
// is what makes a resync converge after an offline period that included deletes.
func pruneFollowerBase(d *dataset, seen map[string]struct{}) error {
	snap := d.st.OpenSnapshot()
	it := d.st.ScanLive(snap)
	var buf []byte
	type staleRow struct {
		key []byte
		seq int64
	}
	var stale []staleRow
	for it.Next() {
		rec, next, live, err := d.st.GetByRowID(snap, it.RowID(), buf)
		buf = next
		if err != nil {
			_ = it.Close()
			d.st.CloseSnapshot(snap)
			return err
		}
		if live {
			key := append([]byte(nil), rec.Key...)
			if _, ok := seen[string(key)]; !ok {
				stale = append(stale, staleRow{key: key, seq: rec.Sequence})
			}
		}
	}
	if err := it.Err(); err != nil {
		_ = it.Close()
		d.st.CloseSnapshot(snap)
		return err
	}
	_ = it.Close()
	d.st.CloseSnapshot(snap)
	for _, row := range stale {
		seq := row.seq
		if seq < int64(^uint64(0)>>1) {
			seq++
		}
		if err := d.st.ApplyReplication(replication.Entry{Sequence: uint64(seq), Operation: replication.Delete, Record: types.Record{Sequence: seq, Key: row.key}}); err != nil {
			return fmt.Errorf("prune follower row: %w", err)
		}
	}
	return nil
}
