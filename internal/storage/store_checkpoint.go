package storage

import (
	"path/filepath"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
)

// Checkpoint executa o protocolo do design §10.2, no goroutine do writer:
//
//  1. fdatasync do segmento corrente (records ≤ S' ficam duráveis)
//  2. aplica o overlay de locations ao mmap (invariante §6.1: nenhuma
//     página de índice referencia record não durável)
//  3. msync de primary, locations, sequences e bitmaps
//  4. manifest atômico com S'
//
// Exclusivo do writer — não concorre com Put nem consigo mesmo. Leitores
// seguem operando normalmente durante todo o checkpoint.
func (s *Store) Checkpoint() error {
	if err := s.segW.Sync(); err != nil {
		return err
	}
	segID, off := s.segW.Current()

	if err := s.locs.ApplyOverlay(); err != nil {
		return err
	}
	if err := s.prim.Msync(); err != nil {
		return err
	}
	if err := s.locs.Msync(); err != nil {
		return err
	}
	if err := s.seqs.Msync(); err != nil {
		return err
	}
	for _, ix := range s.bitmaps {
		if err := ix.Msync(); err != nil {
			return err
		}
	}

	segs, err := segment.ListSegments(filepath.Join(s.opts.Dir, "data"))
	if err != nil {
		return err
	}
	m := checkpoint.Manifest{
		Epoch:               s.epoch + 1,
		CreatedAtUnixMs:     time.Now().UnixMilli(),
		NextRowID:           s.nextRowID.Load(),
		RowCount:            s.rowCount.Load(),
		CurrentSegment:      segID,
		DurableOffset:       uint64(off),
		PrimarySlotCount:    s.prim.SlotCount(),
		PrimaryUsed:         s.prim.Used(),
		HashKind:            1,
		FieldDictValidBytes: s.dict.SizeBytes(),
	}
	for _, ref := range segs {
		m.Segments = append(m.Segments, checkpoint.SegmentInfo{ID: ref.ID, SizeBytes: ref.SizeBytes})
	}
	for field, ix := range s.bitmaps {
		m.Bitmaps = append(m.Bitmaps, checkpoint.BitmapInfo{
			FieldID:              field,
			ValuesDictValidBytes: ix.DictSizeBytes(),
		})
	}
	if err := checkpoint.Write(s.manifestPath(), &m); err != nil {
		return err
	}
	s.epoch = m.Epoch
	return nil
}

func (s *Store) manifestPath() string {
	return filepath.Join(s.opts.Dir, "checkpoint", "manifest.bin")
}
