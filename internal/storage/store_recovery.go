package storage

import (
	"fmt"
	"log"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// recoverFrom executa a recovery do design §10.3 a partir de um manifest
// válido: seeds (§14.15), verificação da cadeia de segmentos (com adoção de
// órfão de rotação), tail scan com replay e writer posicionado no fim
// válido (truncando o lixo). Roda sem leitores ativos.
func (s *Store) recoverFrom(m *checkpoint.Manifest, refs []segment.SegmentRef, dataDir string) error {
	// Seeds do manifest (§14.15).
	s.epoch = m.Epoch
	s.nextRowID.Store(m.NextRowID)
	s.rowCount.Store(m.RowCount)
	for _, si := range m.Segments {
		s.segTally[si.ID] = &segStats{
			totalRecords: si.TotalRecords,
			liveRecords:  si.LiveRecords,
			liveBytes:    si.LiveBytes,
		}
	}
	// PrimarySlotCount/HashKind do manifest são ADVISORY: a fonte de
	// verdade é o header do primary.idx (um rehash pós-checkpoint deixa o
	// manifest para trás — divergência é esperada, não erro).
	if m.PrimarySlotCount != s.prim.SlotCount() {
		log.Printf("nishidb: manifest declara %d slots na primary; header tem %d (rehash pós-checkpoint; header prevalece)",
			m.PrimarySlotCount, s.prim.SlotCount())
	}
	if m.HashKind != s.prim.HashKind() {
		log.Printf("nishidb: manifest declara hashKind %d; índice usa %d (header prevalece)",
			m.HashKind, s.prim.HashKind())
	}

	// Conjunto de segmentos: todos os do manifest precisam existir no
	// diretório; segmentos além do manifest (criados por rotação
	// pós-checkpoint, inclusive o órfão de uma rotação interrompida) são
	// ADOTADOS para o tail scan.
	byID := make(map[uint32]segment.SegmentRef, len(refs))
	maxID := m.CurrentSegment
	for _, ref := range refs {
		byID[ref.ID] = ref
		if ref.ID > maxID {
			maxID = ref.ID
		}
	}
	for _, si := range m.Segments {
		if _, present := byID[si.ID]; !present {
			return fmt.Errorf("%w: segmento %d do manifest ausente em %s", ErrDatasetCorrupt, si.ID, dataDir)
		}
	}
	if _, present := byID[m.CurrentSegment]; !present {
		return fmt.Errorf("%w: segmento corrente %d do manifest ausente em %s",
			ErrDatasetCorrupt, m.CurrentSegment, dataDir)
	}

	// Tail scan com replay: o segmento corrente a partir de S
	// (durableOffset); os posteriores, do fim do header.
	st := &replayState{baseNextRowID: m.NextRowID, novas: make(map[types.RowID]struct{})}
	lastValidEnd := int64(segment.HeaderSize)
	for id := m.CurrentSegment; id <= maxID; id++ {
		ref, present := byID[id]
		if !present {
			return fmt.Errorf("%w: buraco na cadeia de segmentos: %d ausente entre %d e %d",
				ErrDatasetCorrupt, id, m.CurrentSegment, maxID)
		}
		from := int64(segment.HeaderSize)
		if id == m.CurrentSegment {
			from = int64(m.DurableOffset)
			// S fora do arquivo é incoerência fatal: o OpenWriter posiciona
			// o writer com Truncate(offset), e ftruncate ALÉM do fim ESTENDE
			// o arquivo com zeros em vez de cortar — o writer passaria a
			// escrever depois de um buraco, e o replay seguinte pararia nele.
			if from < segment.HeaderSize || from > ref.SizeBytes {
				return fmt.Errorf("%w: manifest declara durableOffset %d no segmento %d, que tem %d bytes",
					ErrDatasetCorrupt, from, id, ref.SizeBytes)
			}
		}
		validEnd, err := s.segR.ScanFrom(id, from, func(off uint32, rec types.Record) error {
			return s.replayRecord(st, id, off, rec)
		})
		if err != nil {
			return err
		}
		if id != maxID && validEnd != ref.SizeBytes {
			// Só o ÚLTIMO segmento pode ter tail rasgado: a rotação sela o
			// anterior com fdatasync antes de criar o próximo. Parar no meio
			// da cadeia é corrupção — truncar ali descartaria os segmentos
			// seguintes inteiros.
			return fmt.Errorf("%w: corrupção no meio da cadeia: segmento %d válido até %d de %d bytes, mas o log continua no %d",
				ErrDatasetCorrupt, id, validEnd, ref.SizeBytes, id+1)
		}
		lastValidEnd = validEnd
	}

	// Writer no fim válido do último segmento (o OpenWriter trunca o lixo
	// além dele — spec §52).
	w, err := segment.OpenWriter(dataDir, s.opts.SegmentSize, maxID, lastValidEnd)
	if err != nil {
		return err
	}
	s.segW = w
	s.curSegID.Store(maxID)
	return nil
}

// replayState carrega o que o replay precisa lembrar entre records.
type replayState struct {
	// baseNextRowID é o nextRowID do manifest: um rowId >= isto nasceu
	// depois do ponto S, logo o manifest não o contabilizou em RowCount.
	baseNextRowID uint64
	// novas desduplica as rows criadas dentro do tail (create seguido de
	// updates no mesmo intervalo não pode contar duas vezes). É bounded
	// pelas rows CRIADAS num intervalo de checkpoint — mesma ordem de
	// grandeza do overlay de locations (§15.3), não do dataset.
	novas map[types.RowID]struct{}
}

// contaComoNova decide se este record representa uma row criada após o
// ponto S. Não dá para usar "não tem location": um checkpoint interrompido
// entre o ApplyOverlay e o manifest deixa a location da row nova já no
// mmap, e o manifest antigo (que governa) não a contou.
func (st *replayState) contaComoNova(id types.RowID) bool {
	if uint64(id) < st.baseNextRowID {
		return false // já existia em S; o manifest já a contabilizou
	}
	if _, visto := st.novas[id]; visto {
		return false
	}
	st.novas[id] = struct{}{}
	return true
}

// replayRecord aplica um record do tail pelo MESMO code path do write
// (applyRecord, §15A Option A) — o que impede que restart e escrita
// divirjam em silêncio. As diferenças do §10.3 (location direto no mmap,
// primary por insert-if-absent) estão no modo applyReplay.
func (s *Store) replayRecord(st *replayState, seg, off uint32, rec types.Record) error {
	id := rec.RowID
	// nextRowID = max(manifest.NextRowID, maiorRowIdVisto+1): nunca regride
	// e nunca reutiliza um id (risco 9 do §14 — ABA de rowId).
	if uint64(id)+1 > s.nextRowID.Load() {
		s.nextRowID.Store(uint64(id) + 1)
	}
	// Estado anterior da row: base do diff de bitmaps e do decremento de
	// liveness. No replay forward ele é sempre o correto — o estado de S ou
	// um record já replayado (§10.2).
	var prevPayload []byte
	oldPacked, had := s.locs.Get(id)
	if had {
		oseg, ooff, _ := rowindex.Unpack(oldPacked)
		prevRec, buf, err := s.segR.ReadRecord(oseg, ooff, s.prevBuf)
		s.prevBuf = buf
		if err != nil {
			return fmt.Errorf("%w: replay da row %d: estado anterior ilegível: %w", ErrDatasetCorrupt, id, err)
		}
		prevPayload = prevRec.Payload
	}
	return s.applyRecord(recordApply{
		rowID:       id,
		key:         rec.Key,
		seq:         rec.Sequence,
		flags:       rec.Flags,
		payload:     rec.Payload,
		prevPayload: prevPayload,
		hadPrevious: had,
		countsAsNew: st.contaComoNova(id),
		seg:         seg,
		off:         off,
		recLen:      encoding.RecordHeaderSize + len(rec.Key) + len(rec.Payload),
		mode:        applyReplay,
	})
}
