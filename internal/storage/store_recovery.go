package storage

import (
	"fmt"

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
	var (
		segmentosDoManifest uint32
		vivoDoManifest      uint64
	)
	for _, si := range m.Segments {
		s.segTally[si.ID] = &segStats{
			totalRecords: si.TotalRecords,
			liveRecords:  si.LiveRecords,
			liveBytes:    si.LiveBytes,
		}
		segmentosDoManifest++
		vivoDoManifest += si.LiveBytes
		// O segSize NÃO é semeado aqui de propósito: o Open faz um Checkpoint
		// incondicional antes de devolver o Store, e esse checkpoint zera o
		// mapa e o repovoa pelo ListSegments. Semear aqui seria escrita que
		// nenhuma leitura alcança — não existe Stats() possível entre este
		// ponto e o checkpoint, porque o Store ainda não saiu do Open.
	}
	// Os espelhos do tally, ao contrário, PRECISAM da base do manifest, e o
	// argumento "ninguém observa o intervalo" não os cobria. O replay do tail
	// abaixo passa pelo applyRecord, que desconta do agregado a baixa
	// CLAMPADA contra o tally — e o tally acabou de ser semeado com os
	// liveBytes do manifest. Sem a mesma base no espelho, a primeira baixa
	// subtrai mais do que existe: medido, o fim do recoverFrom deixava
	// liveBytesAll em 18446744073709551544 com os tallies somando 568, ou
	// seja underflow de uint64 no código de produção, e não num mutante.
	//
	// Que o Checkpoint do Open recarregue o absoluto logo depois não conserta
	// o defeito, apenas o esconde: uma invariante que só vale porque o
	// próximo passo a reescreve não é invariante, é coincidência de ordem —
	// e a compaction do §53-55 vai mexer nesses mesmos mapas fora do Open.
	// O espelho de disco fica em zero porque o segSize também fica: cada
	// espelho continua sendo cópia fiel do SEU mapa.
	s.segCount.Store(segmentosDoManifest)
	s.liveBytesAll.Store(vivoDoManifest)
	// PrimarySlotCount/HashKind do manifest são ADVISORY: a fonte de
	// verdade é o header do primary.idx (um rehash pós-checkpoint deixa o
	// manifest para trás — divergência é esperada, não erro).
	if m.PrimarySlotCount != s.prim.SlotCount() {
		s.opts.Log.Info("recovery.primary_slots_divergem",
			"manifest declara outro número de slots na primary; o header prevalece",
			"manifest_slots", m.PrimarySlotCount, "header_slots", s.prim.SlotCount(),
			"causa", "rehash pós-checkpoint")
	}
	if m.HashKind != s.prim.HashKind() {
		s.opts.Log.Info("recovery.hashkind_diverge",
			"manifest declara outro hashKind; o header prevalece",
			"manifest_hashkind", m.HashKind, "header_hashkind", s.prim.HashKind())
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
	declaradoNoManifest := make(map[uint32]int64, len(m.Segments))
	for _, si := range m.Segments {
		ref, present := byID[si.ID]
		if !present {
			return fmt.Errorf("%w: segmento %d do manifest ausente em %s", ErrDatasetCorrupt, si.ID, dataDir)
		}
		// Segmentos só crescem: o manifest registra o tamanho no instante do
		// checkpoint e o writer apenas appenda depois. Um arquivo MENOR que o
		// registrado encolheu por fora (truncamento externo, falha de FS), e
		// os records que faltam são irrecuperáveis — seguir daqui replayaria
		// um prefixo mutilado como se fosse o dataset inteiro.
		if ref.SizeBytes < si.SizeBytes {
			return fmt.Errorf("%w: segmento %d encolheu de %d para %d bytes (truncamento externo, não falha do engine)",
				ErrDatasetCorrupt, si.ID, si.SizeBytes, ref.SizeBytes)
		}
		declaradoNoManifest[si.ID] = si.SizeBytes
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
		if ref.SizeBytes < segment.HeaderSize {
			// Arquivo sem header: crash entre o O_CREATE|O_EXCL e o
			// writeHeader do openNew. É reparável (o OpenWriter completa o
			// header adiante), mas SÓ se o manifest não o conhecer com
			// dados: um segmento que continha records e foi zerado por
			// truncamento externo é byte a byte idêntico a este, e reparar
			// apagaria a evidência de uma perda real.
			if declarado := declaradoNoManifest[id]; declarado > segment.HeaderSize {
				return fmt.Errorf("%w: segmento %d está com %d bytes, mas o manifest o declara com %d (arquivo encolheu)",
					ErrDatasetCorrupt, id, ref.SizeBytes, declarado)
			}
			if id != maxID {
				return fmt.Errorf("%w: segmento %d sem header no meio da cadeia (o último é o %d)",
					ErrDatasetCorrupt, id, maxID)
			}
			lastValidEnd = segment.HeaderSize
			break
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
	// Único instante em que os espelhos podem ser lidos como a RECOVERY os
	// deixou: o Checkpoint incondicional do Open recarrega os três em valor
	// absoluto logo adiante, e depois dele nenhuma leitura de Stats
	// distingue a semeadura acima de um agregado furado. É por isso que o
	// underflow de uint64 medido aqui (liveBytesAll em 1,8e19 contra 568 nos
	// tallies) atravessou a suíte inteira até a revisão da Task 1b: não
	// existia ponto de observação antes da recarga. Nil em produção.
	if s.opts.traceRecovery != nil {
		s.opts.traceRecovery(s)
	}
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
//
// Resíduo conhecido: um rowId reservado por um Put que falhou ANTES do
// Append não gera record no tail, então nunca é contado — RowCount fica
// abaixo de nextRowID por esses ids. É o comportamento desejado (a row de
// fato não existe), mas significa que RowCount é ESTATÍSTICA e não
// invariante: não use a diferença entre RowCount e nextRowID como prova de
// nada. Visibilidade e lookup não dependem dele.
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
	// liveness.
	//
	// O §10.2 afirma que esse prev está sempre correto no replay forward,
	// mas a premissa NÃO vale quando a location está à frente do ponto S —
	// situação normal de crash, porque o checkpoint msynca as locations
	// (passo 3) antes dos bitmaps e antes do manifest (passo 4). Nesse caso
	// a location nomeia este mesmo record (ou um posterior do tail): usá-lo
	// como "anterior" faria o diff comparar o record consigo mesmo,
	// suprimindo o Set — falso negativo permanente no bitmap, que a
	// revalidação do §44 não recupera — e decrementaria a liveness de um
	// record VIVO.
	//
	// Só uma location ESTRITAMENTE atrás do cursor do replay é passado
	// legítimo (estado de S ou record já replayado). Fora disso o prev é
	// desconhecido, e prev desconhecido é tratado como ausente: liga todas
	// as postings do payload novo (Set é idempotente), não limpa nenhuma
	// (Clear a menos = falso positivo, tolerado pelo §44) e não decrementa
	// liveness.
	//
	// Esse ramo deixa DOIS resíduos, ambos permanentes e da mesma raiz.
	//
	// Resíduo 1, no bitmap: o bit do valor que a row tinha em S continua
	// ligado — POSTING ÓRFÃ. Nenhum update seguinte a apaga, porque o Clear
	// de cada update parte do prev que a location nomeia, que é o record do
	// tail e nunca o de antes dele; só uma varredura (compaction/rebuild,
	// M4) reconstrói o bitmap. NÃO é falso negativo: o valor corrente está
	// nas postings, que é a única propriedade da qual o §44 depende, então
	// o custo é candidato a mais varrido, nunca resultado errado. Medido em
	// TestPostingOrfaDoReplayNaoConverge; débito 45.
	//
	// Resíduo 2, no tally: quando a row já existia em S e o tail traz
	// um update cuja location está à frente, o record antigo continua
	// contado como vivo — LiveRecords fica ACIMA do real (2 onde o certo é
	// 1, com LiveBytes em 127 onde o certo é 63). É a direção segura da
	// mesma assimetria do §14.16: superestimar liveness só adia a
	// compaction, enquanto subestimar faria a compaction descartar dado
	// vivo.
	//
	// O resíduo é PERMANENTE — nenhum checkpoint futuro o desconta. O
	// decremento do applyRecord (§10.1 passo 3) identifica o record que
	// virou garbage por s.locs.Get(rowID), e essa location já aponta para o
	// record do tail: o que cada update seguinte desconta é o record
	// anterior a ELE, nunca o órfão deixado aqui. Medido em
	// TestResiduoDeLivenessDoReplayNaoConverge: um terceiro update com
	// checkpoint completo mantém 2 records / 128 bytes vivos onde o certo é
	// 1 / 64. Só uma varredura do segmento (compaction, M4) reconstrói o
	// tally — e é justamente a compaction que consome este número no
	// minimumLiveRatio, o que faz do erro um adiamento, nunca uma perda.
	var prevPayload []byte
	prevConhecido := false
	if oldPacked, ok := s.locs.Get(id); ok {
		oseg, ooff, _ := rowindex.Unpack(oldPacked)
		if oseg < seg || (oseg == seg && ooff < off) {
			prevRec, buf, err := s.segR.ReadRecord(oseg, ooff, s.prevBuf)
			s.prevBuf = buf
			if err != nil {
				return fmt.Errorf("%w: replay da row %d: estado anterior ilegível: %w", ErrDatasetCorrupt, id, err)
			}
			prevPayload = prevRec.Payload
			prevConhecido = true
		}
	}
	return s.applyRecord(recordApply{
		rowID:       id,
		key:         rec.Key,
		seq:         rec.Sequence,
		flags:       rec.Flags,
		payload:     rec.Payload,
		prevPayload: prevPayload,
		hadPrevious: prevConhecido,
		countsAsNew: st.contaComoNova(id),
		seg:         seg,
		off:         off,
		recLen:      encoding.RecordHeaderSize + len(rec.Key) + len(rec.Payload),
		mode:        applyReplay,
	})
}
