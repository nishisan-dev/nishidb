package storage

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// reapendaSeVivo re-apenda o record de (seg, off) no segmento corrente se, e
// somente se, a location da row ainda o nomear. Devolve se copiou e quantos
// bytes o record ocupou no destino — é por esse número que a varredura do §54
// mede o trabalho feito.
//
// A verificação e o append acontecem na MESMA seção crítica de s.wmu, e isso
// é a corretude inteira desta tarefa (decisão D3 do §53-55): entre ler a
// location e apendar, a ingestão pode ter gravado um update. Fora do lock, a
// cópia do payload ANTIGO seria publicada DEPOIS do novo e o dataset voltaria
// no tempo — sem erro, sem log, e com o CRC batendo, de modo que nem a
// recovery distinguiria o estrago de uma escrita legítima. É por isto que a
// compaction não chama Put: o Put resolve a key pela primary e apenda o que
// lhe deram, sem nunca perguntar se o record de ORIGEM ainda é o corrente.
// Quem prova isto é o TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica,
// e não o -race: mover a leitura da location para fora do lock não produz
// acesso de memória não sincronizado nenhum (locs.Get é seguro para leitor
// concorrente), então o detector não vê a violação de ATOMICIDADE — ele só
// perturba o escalonamento.
//
// Liveness aqui é "a location nomeia este record" (§55), não "a row é
// visível": record TOMBSTONED também é copiado (decisão D2), porque a
// location aponta para ele e não copiá-lo a deixaria pendurada num arquivo que
// a aposentadoria do D5 vai remover — o Open seguinte acharia location sem
// arquivo e recusaria o dataset (store_recovery.go:88). Recuperar tombstone
// exige limpar location e primary, que é trabalho do TTL (M6). Fixado pelo
// TestCopyForwardCopiaORecordTombstonadoSemRessuscitarARow, porque "não copia
// o que já está morto" é a otimização mais plausível que existe aqui.
//
// DIVERGÊNCIA conhecida e deliberada em relação ao Put: o Put honra
// s.opts.SyncEveryPut chamando syncLocked ao fim da seção crítica
// (store.go:858), esta função não. Fsyncar por record copiado transformaria
// a varredura do §54 num fsync por record, que é o custo que ela existe para
// evitar. Hoje isso não perde nada: o record de ORIGEM continua em disco até
// a aposentadoria da Task 6, que só acontece depois do Checkpoint — e é o
// Checkpoint que sincroniza os segmentos. Quem mexer na ordem daquela tarefa
// (aposentar segmento sem o Checkpoint antes) transforma esta assimetria em
// perda de dado, e é por isso que ela está escrita aqui.
func (s *Store) reapendaSeVivo(seg, off uint32, rec types.Record) (copiado bool, n int, err error) {
	if err := s.entra(); err != nil {
		return false, 0, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return false, 0, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	packed, had := s.locs.Get(rec.RowID)
	if !had {
		// Row sem location: o record de origem é resíduo de uma janela de
		// crash (§10.2), e copiá-lo faria a compaction RESSUSCITAR o que o
		// commit point nunca publicou.
		//
		// O ramo é DEFENSIVO, não indispensável: packed==0 desempacota para
		// segmento 0, nenhum segmentID válido é 0 (rowindex.go:33 entra em
		// panic com ele) e a comparação abaixo recusaria do mesmo jeito.
		// Ele fica porque pendurar a recusa do resíduo num detalhe do
		// empacotamento é caro de ler e frágil de manter. O comportamento —
		// resíduo não vira dado — está fixado pelo
		// TestCopyForwardRecusaResiduoSemLocationEmVezDeRessuscitarOEstado.
		return false, 0, nil
	}
	curSeg, curOff, flags := rowindex.Unpack(packed)
	if curSeg != seg || curOff != off {
		return false, 0, nil // a row já migrou sozinha; copiar seria regredir
	}
	// As flags vêm da LOCATION, não do record. A justificativa anterior aqui
	// ("reabrir uma row tombstoned cuja marcação só existe no índice")
	// descrevia um caminho que NÃO existe: locs.Publish e locs.SetDirect só
	// são chamados de dentro do applyRecord (store.go:994 e :998), que deriva
	// a flag do próprio record — nenhum código tombstona só o índice.
	//
	// O que a escolha de fato decide, hoje, é UM efeito: bits de flag fora do
	// FlagTombstone não sobrevivem à cópia, porque a location guarda só esse
	// bit. É deliberado — a compaction é o único caminho de escrita que não
	// recebe flags de um chamador, e reescrever no segmento um bit que
	// nenhuma location sustenta seria a compaction inventando estado. Quando
	// o TTL do M6 abrir o caminho de tombstonar pelo índice, é esta escolha
	// que impede a cópia de ressuscitar a row. Fixada pelo
	// TestCopyForwardGravaAsFlagsDaLocationENaoAsDoRecord: sem ele, trocar
	// `flags` por `rec.Flags` deixava a suíte inteira verde.
	buf, err := encoding.BuildRecord(s.compactBuf[:0], rec.RowID, rec.Sequence, flags, rec.Key, rec.Payload)
	s.compactBuf = buf
	if err != nil {
		return false, 0, err
	}
	novoSeg, novoOff, err := s.segW.Append(buf)
	if err != nil {
		return false, 0, s.failWriter(err)
	}
	s.curSegID.Store(novoSeg)
	// prevPayload é o PRÓPRIO payload, e hadPrevious é true, porque o "estado
	// anterior" desta row é literalmente o record que se está copiando: é o
	// que faz o applyRecord descontar do segmento de ORIGEM exatamente o
	// tamanho que acabou de somar no destino. Sem isso o copy-forward inflaria
	// a liveness a cada cópia e o segmento candidato nunca chegaria ao tally
	// zerado do D4. Os bitmaps ficam sem trabalho pela mesma razão — o SET
	// religa bits já ligados e o CLEAR não acha diferença entre os dois
	// payloads iguais.
	if err := s.applyRecord(recordApply{
		rowID:       rec.RowID,
		key:         rec.Key,
		seq:         rec.Sequence,
		flags:       flags,
		payload:     rec.Payload,
		prevPayload: rec.Payload,
		hadPrevious: true,
		countsAsNew: false,
		seg:         novoSeg,
		off:         novoOff,
		recLen:      len(buf),
	}); err != nil {
		return false, 0, s.failWriter(err)
	}
	return true, len(buf), nil
}
