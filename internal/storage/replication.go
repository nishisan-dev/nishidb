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
	// A sequence de materialização do master (Record.Sequence) tem
	// preferência sempre que existe, para que um resync não faça uma linha do
	// follower andar para trás. Entradas do tail levam a sequence do feed, e
	// as do snapshot levam o cursor₀ da exportação — então, com
	// Record.Sequence == 0 (dataset sem `sequence:`), o follower grava o
	// cursor₀ ou a sequence do feed. Nenhum stale check lê esse valor: o Put
	// não compara sequences, e o único leitor no follower é a poda
	// (pruneFollowerBase), que só o usa para dar à lápide uma sequence maior.
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

// applyReplicationDelete grava a lápide de uma key apagada no master.
//
// A lápide é montada FORA do wmu (o Put o toma depois), e por isso num buffer
// novo, e não no tempBuffer do writer: no follower a compaction expira linhas
// com TTL e monta a sua lápide no tempBuffer sob o wmu, em paralelo a este
// caminho. Montar a lápide dentro do wmu exigiria um Put com a lápide
// derivada lá dentro — um contrato novo no caminho de escrita só para poupar
// uma alocação que o State acima já faz por delete.
func (s *Store) applyReplicationDelete(key []byte, seq int64) error {
	prev, _, presence, err := s.State(key, nil)
	if err != nil {
		return err
	}
	if presence == types.PresenceAbsent {
		return nil
	}
	tomb, err := s.appendLapide(nil, prev.Payload)
	if err != nil {
		return err
	}
	_, _, err = s.Put(key, seq, types.FlagTombstone, tomb, &prev)
	return err
}
