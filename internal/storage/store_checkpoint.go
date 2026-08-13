package storage

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
)

// Checkpoint executa o protocolo do design §10.2:
//
//  1. fdatasync do segmento corrente (records ≤ S' ficam duráveis)
//  2. aplica o overlay de locations ao mmap (invariante §6.1: nenhuma
//     página de índice referencia record não durável)
//  3. msync de primary, locations, sequences e bitmaps
//  4. manifest atômico com S'
//
// Serializado com Put/Sync pelo mutex do writer; leitores seguem operando
// normalmente durante todo o checkpoint.
//
// Taxonomia de erros: falha de DURABILIDADE (segW.Sync, qualquer Msync,
// escrita/fsync do manifest) é STICKY — o kernel reporta EIO de writeback
// UMA vez e o limpa, então uma retentativa "bem-sucedida" gravaria um
// manifest declarando durável o que se perdeu (fsyncgate). Erro LÓGICO
// (ApplyOverlay, ex.: ENOSPC no grow do índice) é retentável: o manifest
// anterior permanece autoritativo.
//
// Os dicionários (fields.dict/values.dict) NÃO participam do checkpoint:
// a durabilidade deles é por entrada, no momento da criação
// (fsync-before-first-use, design §4/§8, travada por teste nos dois
// pacotes). Nenhum batching de Sync de dicionário pode ser introduzido
// aqui — quebraria o replay do tail de forma silenciosa.
func (s *Store) Checkpoint() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	if err := s.traceStep("segSync", s.segW.Sync); err != nil {
		return s.failWriter(err)
	}
	segID, off := s.segW.Current()

	if err := s.traceStep("applyOverlay", s.locs.ApplyOverlay); err != nil {
		return err // erro lógico: retentável, manifest antigo governa
	}
	if err := s.traceStep("msyncPrimary", s.prim.Msync); err != nil {
		return s.failWriter(err)
	}
	if err := s.traceStep("msyncLocations", s.locs.Msync); err != nil {
		return s.failWriter(err)
	}
	if err := s.traceStep("msyncSequences", s.seqs.Msync); err != nil {
		return s.failWriter(err)
	}
	if err := s.traceStep("msyncBitmaps", func() error {
		for field, ix := range s.bitmaps {
			if err := ix.Msync(); err != nil {
				return fmt.Errorf("storage: msync do bitmap %d: %w", field, err)
			}
		}
		return nil
	}); err != nil {
		return s.failWriter(err)
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
		HashKind:            s.prim.HashKind(),
		FieldDictValidBytes: s.dict.SizeBytes(),
	}
	for _, ref := range segs {
		info := checkpoint.SegmentInfo{ID: ref.ID, SizeBytes: ref.SizeBytes}
		if t := s.segTally[ref.ID]; t != nil {
			// Liveness real do que o writer viu: updates decrementam o
			// segmento do record anterior no Put. Segmentos anteriores ao
			// restart são semeados pela recovery (S12, design §14).
			info.TotalRecords = t.totalRecords
			info.LiveRecords = t.liveRecords
			info.LiveBytes = t.liveBytes
		}
		m.Segments = append(m.Segments, info)
	}
	for field, ix := range s.bitmaps {
		m.Bitmaps = append(m.Bitmaps, checkpoint.BitmapInfo{
			FieldID:              field,
			ValuesDictValidBytes: ix.DictSizeBytes(),
		})
	}
	// Ordem determinística: dois checkpoints do mesmo estado produzem
	// manifests idênticos byte a byte.
	slices.SortFunc(m.Bitmaps, func(a, b checkpoint.BitmapInfo) int {
		return cmp.Compare(a.FieldID, b.FieldID)
	})
	if err := s.traceStep("manifest", func() error {
		return checkpoint.Write(s.manifestPath(), &m)
	}); err != nil {
		return s.failWriter(err)
	}
	s.epoch = m.Epoch
	return nil
}

// traceStep executa a operação e, em sucesso, emite o evento no seam de
// teste da ordem de durabilidade (nil em produção). Amarrar o trace à
// execução da operação garante que remover/reordenar a operação também
// remove/reordena o evento — é o que dá poder de detecção às mutações.
func (s *Store) traceStep(name string, op func() error) error {
	err := op()
	if err == nil && s.cpTrace != nil {
		s.cpTrace(name)
	}
	return err
}

func (s *Store) manifestPath() string {
	return filepath.Join(s.opts.Dir, "checkpoint", "manifest.bin")
}

// segStats acumula os contadores de um segmento conhecidos pelo writer.
// liveRecords/liveBytes decrementam quando um update torna garbage o record
// anterior daquele segmento.
type segStats struct {
	totalRecords uint64
	liveRecords  uint64
	liveBytes    uint64
}
