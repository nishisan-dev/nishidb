package storage

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
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

// compactaSegmento varre seg record a record e copia para frente todo record
// que a location ainda nomeia (§55). Streaming puro: um record por vez, com o
// buffer do ScanFrom na leitura e o compactBuf do writer na escrita — nada
// proporcional ao TAMANHO do segmento entra em memória, que é o que o §54
// exige e o que separa esta varredura de um "carrega e reescreve".
//
// Devolve quantos records foram copiados, quantos bytes a varredura LEU da
// origem e quantos ESCREVEU no destino. Os três são diferentes por
// construção: `lidos` conta o arquivo inteiro, `escritos` só a parte viva, e
// a distância entre eles é o disco que a aposentadoria da Task 6 vai devolver.
//
// Terminada a varredura COMPLETA — sem cancelamento, sem erro e tendo
// alcançado o fim do arquivo —, nenhuma location aponta para seg, e é por
// isso que o tally dele é ZERADO aqui em vez de deduzido dos decrementos do
// applyRecord. O tally é limite superior por construção (débitos 38 e 45): o
// record órfão que o replay do tail deixa não é nomeado por location nenhuma,
// então nenhum decremento o encontra e o candidato jamais chegaria a zero por
// subtração, por mais completa que a varredura fosse. A varredura é a única
// leitura EXATA do segmento que existe no produto — é literalmente o que o
// godoc de checkpoint.SegmentInfo antecipa ao dizer que só uma varredura
// reconstrói o tally (manifest.go:73) —, e esta é a linha que fecha os dois
// débitos. Fixado pelo TestVarreduraCompletaZeraOTallyMesmoComResiduoOrfao,
// cuja asserção do meio mede o resíduo em que os decrementos param.
//
// A seleção do candidato NÃO mora aqui: quem decide qual segmento vale a pena
// e nunca elege o corrente (decisão D6) é a Task 7. Esta função obedece.
func (s *Store) compactaSegmento(ctx context.Context, seg uint32) (copiados uint64, lidos, escritos int64, err error) {
	fimValido, err := s.segR.ScanFrom(seg, segment.HeaderSize, func(off uint32, rec types.Record) error {
		// O cancelamento é a PRIMEIRA coisa do callback, e sai pelo erro que
		// o ScanFrom propaga: é assim que a goroutine da Task 7 desiste no
		// Close sem que ninguém precise esperar um segmento inteiro terminar
		// (decisão D7). Uma varredura interrompida não viu todos os records,
		// logo não tem autoridade sobre nenhum — o tally NÃO é zerado por
		// este caminho.
		if err := ctx.Err(); err != nil {
			return err
		}
		lidos += int64(encoding.RecordHeaderSize + len(rec.Key) + len(rec.Payload))
		copiou, n, err := s.reapendaSeVivo(seg, off, rec)
		if err != nil {
			return err
		}
		if copiou {
			copiados++
			escritos += int64(n)
		}
		return nil
	})
	if err != nil {
		return copiados, lidos, escritos, err
	}

	// O ScanFrom para no primeiro record inválido SEM erro, porque para o
	// tail scan da recovery (§52) isso é o caminho NORMAL — é assim que o EOF
	// chega até lá. Num segmento SELADO não é: ele não cresce desde a
	// rotação, então prefixo válido menor que o arquivo é corrupção real, e
	// os records depois dela continuam sendo nomeados por locations que a
	// varredura nunca visitou.
	//
	// Zerar o tally aqui daria à varredura uma autoridade que ela não tem: um
	// instrumento que devolve resultado sem ter medido. O estrago só
	// aterrissaria na Task 6 — o arquivo aposentado e removido com locations
	// vivas apontando para dentro dele, e o Open seguinte recusando o dataset
	// (store_recovery.go) —, de modo que a compaction transformaria a
	// corrupção de UM record na perda do segmento inteiro. É a mesma
	// aferição, e o mesmo veredito, do `validEnd != ref.SizeBytes` do
	// recoverFrom.
	tamanho, err := s.tamanhoDoSegmento(seg)
	if err != nil {
		return copiados, lidos, escritos, err
	}
	if fimValido != tamanho {
		return copiados, lidos, escritos, fmt.Errorf(
			"%w: a varredura do segmento %d parou em %d de %d bytes; o tally não é zerado por varredura incompleta (§53-55, D4)",
			ErrDatasetCorrupt, seg, fimValido, tamanho)
	}

	if err := s.entra(); err != nil {
		return copiados, lidos, escritos, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if t := s.segTally[seg]; t != nil {
		// NORMATIVO (invariante 4 da Fase B): o espelho acompanha o mapa na
		// MESMA seção crítica. Zerar só o mapa deixa o liveBytesAll à frente
		// dele, e o conferaEspelhos() do próximo checkpoint acusa divergência
		// a cada compaction — com razão, porque nesse intervalo o /v1/stats e
		// a nishidb_garbage_bytes publicaram um vivo que o dataset não tem.
		//
		// O espelho não é RECALCULADO da soma aqui (isso exigiria varrer
		// todos os segmentos sob o wmu, que é justamente a varredura que a
		// Fase A′ tirou do Stats): o que se desconta é exatamente o que este
		// segmento ainda reivindicava. A subtração em complemento de dois é a
		// forma que o applyRecord já usa (store.go:948) — Go não tem Sub
		// atômico para Uint64, e ^(x-1) devolve 0 para x==0, então tally já
		// vazio não vira underflow.
		s.liveBytesAll.Add(^(t.liveBytes - 1))
		t.liveRecords = 0
		t.liveBytes = 0
	}
	return copiados, lidos, escritos, nil
}

// tamanhoDoSegmento devolve o tamanho em disco do arquivo de seg pelo mesmo
// ListSegments que o checkpoint usa para reconciliar o segSize
// (store_checkpoint.go:116).
//
// É consulta ao DIRETÓRIO, e não ao mapa segSize do writer, porque o mapa só
// se move no checkpoint: um segmento selado depois do último checkpoint ainda
// não tem entrada nele, e responder "não sei o tamanho" logo para o segmento
// que a varredura acabou de ler seria trocar a medida por uma coincidência de
// agenda. Um readdir por segmento compactado é ruído ao lado das milhares de
// leituras da varredura que acabou de rodar.
func (s *Store) tamanhoDoSegmento(seg uint32) (int64, error) {
	refs, err := segment.ListSegments(filepath.Join(s.opts.Dir, "data"))
	if err != nil {
		return 0, err
	}
	for _, ref := range refs {
		if ref.ID == seg {
			return ref.SizeBytes, nil
		}
	}
	return 0, fmt.Errorf("%w: o segmento %d saiu do diretório durante a própria compaction", ErrDatasetCorrupt, seg)
}
