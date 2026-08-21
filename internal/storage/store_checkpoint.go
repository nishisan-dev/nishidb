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
	inicio := time.Now()
	err := s.checkpoint()
	if err != nil {
		return err
	}
	// Só o SUCESSO conta. Um checkpoint que falhou não tem duração
	// interpretável (parou num passo arbitrário) e, principalmente, não pode
	// mexer no "último sucesso" — é justamente esse relógio parado que revela
	// um writer em modo falho gravando nada há meia hora.
	//
	// A medição inclui a espera pelo mutex do writer de propósito: para quem
	// opera, o custo do checkpoint É o stall total da ingestão, não só o
	// tempo de fsync.
	if s.cpDuracao != nil {
		s.cpDuracao.Observa(time.Since(inicio).Seconds())
		s.cpUltimo.Set(float64(time.Now().Unix()))
	}
	return nil
}

// limitesDeCheckpoint cobre a faixa observada no M1 (checkpoint de dataset de
// 8M com msync de índices) e vai até 30s, que é o intervalo padrão do §78: um
// checkpoint que leve mais que o próprio intervalo é a definição de saturação.
var limitesDeCheckpoint = []float64{
	0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
}

func (s *Store) checkpoint() error {
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
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
	// Este ListSegments é o único ponto do código que sabe QUAIS segmentos
	// existem, e ele roda sob o mutex do writer: a listagem é um snapshot
	// consistente, nenhuma rotação se intercala entre ela e o que vem abaixo.
	// Aproveitar isso para reconciliar os dois mapas de contabilidade — e não
	// só para montar o manifest — é o que faz o Stats parar de mentir nas duas
	// pontas.
	//
	// segSize é RECONSTRUÍDO, não mesclado: um segmento aposentado pela
	// compaction (§53-55) some do diretório, e atualizar entrada por entrada
	// deixaria o tamanho do arquivo removido somando para sempre.
	clear(s.segSize)
	for _, ref := range segs {
		s.segSize[ref.ID] = ref.SizeBytes
		info := checkpoint.SegmentInfo{ID: ref.ID, SizeBytes: ref.SizeBytes}
		t := s.segTally[ref.ID]
		if t == nil {
			// Segmento que existe em disco e que o writer nunca tocou. Não é
			// hipótese: TODO dataset novo nasce assim — o segmento inicial é
			// criado na abertura e o tally só ganha entrada no applyRecord.
			// Sem semear aqui, o Stats de um dataset recém-provisionado
			// reportaria `segments=0` com `current_segment=1`, que é snapshot
			// incoerente vazando para o /v1/stats e para a
			// nishidb_segments_total. Os zeros que entram no manifest são os
			// mesmos de antes, quando o `t != nil` simplesmente falhava.
			t = &segStats{}
			s.segTally[ref.ID] = t
		}
		// Liveness real do que o writer viu: updates decrementam o
		// segmento do record anterior no Put. Segmentos anteriores ao
		// restart são semeados pela recovery (S12, design §14).
		info.TotalRecords = t.totalRecords
		info.LiveRecords = t.liveRecords
		info.LiveBytes = t.liveBytes
		m.Segments = append(m.Segments, info)
	}
	// E a poda, que é a outra metade da mesma reconciliação: o tally não perde
	// entrada por conta própria em lugar nenhum do código. Sem isto, o
	// segmento que a compaction aposentar continuaria contado pelo Stats para
	// sempre — o arquivo sai do diretório, o manifest para de nomeá-lo, o
	// StorageBytes dele some, e SegmentsTotal fica marcando um segmento que
	// não existe mais. É a mesma direção da decisão D4 (a varredura tem
	// autoridade sobre o tally do segmento que ela esvaziou), aplicada ao
	// único fato que o diretório conhece melhor que o writer: se o segmento
	// ainda está lá.
	for id := range s.segTally {
		if _, existe := s.segSize[id]; !existe {
			delete(s.segTally, id)
		}
	}
	// Recarga dos espelhos que o Stats lê sem tomar o wmu. É este laço — a
	// varredura que o Stats fazia sob o lock do writer — trazido para dentro
	// da seção crítica onde ele sempre pertenceu, e o Stats fica com três
	// Load atômicos.
	//
	// O ponto é aqui, e não no fim do checkpoint, porque é aqui que os dois
	// mapas voltaram a concordar sobre QUAIS segmentos existem. E é dos
	// mapas, não do manifest, que os espelhos são cópia: se a gravação
	// abaixo falhar, o estado em memória continua sendo este, e é este que o
	// Stats tem de descrever.
	//
	// A assimetria do segSize é a mesma de antes, e continua deliberada:
	// segmento que o writer conhece mas que o último ListSegments não viu
	// contribui com zero bytes, o que mantém StorageBytes num limite
	// INFERIOR e nunca inventa disco que não existe.
	var (
		segmentos  uint32
		vivoTotal  uint64
		discoTotal int64
	)
	for id, t := range s.segTally {
		segmentos++
		vivoTotal += t.liveBytes
		discoTotal += s.segSize[id]
	}
	s.segCount.Store(segmentos)
	s.liveBytesAll.Store(vivoTotal)
	s.diskBytesAll.Store(discoTotal)
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

	// Passo 8: a posição no tópico, DEPOIS do manifest — nunca antes.
	//
	// São dois arquivos e dois renames, sem transação entre eles, e é a ORDEM
	// que decide qual estado um crash entre os dois pode produzir:
	//
	//	manifest → offsets:  offset ATRÁS dos dados. O seek reprocessa uma
	//	                     janela já aplicada, que converge (§25 descarta por
	//	                     sequence; sem sequence, reaplicar na mesma ordem de
	//	                     partição produz o mesmo payload). Custo: lixo.
	//	offsets → manifest:  offset ADIANTE dos dados. Os eventos entre os dois
	//	                     pontos nunca mais são lidos. PERDA DEFINITIVA.
	//
	// O §48 já fixa essa direção ("persist → durable checkpoint → commit
	// Kafka offset"); aqui ela vira invariante de código, travada por teste.
	if s.srcOffsets != nil {
		if err := s.traceStep("sourceOffsets", func() error {
			return checkpoint.WriteOffsets(s.offsetsPath(), *s.srcOffsets)
		}); err != nil {
			return s.failWriter(err)
		}
	}
	return nil
}

// SetSourceOffsets publica a posição no tópico que o PRÓXIMO checkpoint vai
// tornar durável. Exclusivo do writer único (§27).
//
// Guarda uma cópia: o chamador mantém o mapa vivo entre os lotes, e
// compartilhar a referência faria o checkpoint gravar um estado mais recente
// que o dos dados que ele acabou de tornar duráveis — exatamente a inversão
// que o §48 proíbe.
func (s *Store) SetSourceOffsets(o checkpoint.SourceOffsets) error {
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	c := o.Clone()
	s.srcOffsets = &c
	return nil
}

// SourceOffsets devolve a posição persistida.
//
// ok=false e err=nil significa dataset que nunca consumiu de Kafka. err != nil
// significa que o arquivo EXISTIA e estava ilegível — que não é a mesma coisa,
// e tratar os dois casos igual é o que faria um restore parcial reprocessar o
// tópico retido inteiro (com autoOffsetReset: earliest) ou pular tudo (latest),
// nos dois casos sem erro em camada nenhuma.
func (s *Store) SourceOffsets() (checkpoint.SourceOffsets, bool, error) {
	if err := s.entra(); err != nil {
		return checkpoint.SourceOffsets{}, false, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.srcOffsetsErr != nil {
		return checkpoint.SourceOffsets{}, false, s.srcOffsetsErr
	}
	if s.srcOffsets == nil {
		return checkpoint.SourceOffsets{}, false, nil
	}
	return s.srcOffsets.Clone(), true, nil
}

// offsetsPath é o caminho do kafka.offsets (§50, §83).
func (s *Store) offsetsPath() string {
	return filepath.Join(s.opts.Dir, "checkpoint", checkpoint.OffsetsFileName)
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
