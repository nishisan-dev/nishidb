package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// sufixoAposentado marca o segmento cujos records vivos já migraram para
// frente. O nome deixa de ser canônico DE PROPÓSITO: o ListSegments ignora
// não-canônicos (segment.go:252), então o próximo manifest já não o lista sem
// que um único byte tenha saído do disco. É isso que fecha a janela entre o
// unlink e o manifest, em que um segmento LISTADO e AUSENTE vira
// ErrDatasetCorrupt no Open seguinte (store_recovery.go:88) — a decisão D5
// mantém os bytes em disco até o manifest concordar em não precisar deles.
const sufixoAposentado = ".retired"

// nomeCanonicoDoSegmento repete o formato que o segment.ListSegments
// reconhece (segment.go:249). A repetição é o preço de a aposentadoria ser
// ciclo de vida de ARQUIVO, e não de record: o pacote segment não expõe o
// caminho de um segmento, e inventar aqui um segundo formato "quase igual"
// seria pior que repetir o mesmo.
//
// Ela não fica solta. Padding ou grafia divergentes fazem o os.Rename da
// aposentadoria sair com ENOENT — não existe caminho em que a divergência
// remova o arquivo errado —, e é a aposentadoria bem-sucedida do
// TestAposentadoriaPodaOSegmentoDosMapasNaMesmaSecaoCritica que afirma o
// acordo com o nome que o writer de fato criou.
const nomeCanonicoDoSegmento = "segment-%08d.dat"

func caminhoCanonicoDoSegmento(dataDir string, seg uint32) string {
	return filepath.Join(dataDir, fmt.Sprintf(nomeCanonicoDoSegmento, seg))
}

// idDoNomeAposentado devolve o segmento que um nome aposentado descreve, e
// recusa tudo o mais. A recusa por igualdade EXATA é a mesma do ListSegments
// (segment.go:254) e pelo mesmo motivo: o Sscanf aceita padding divergente,
// de modo que dois nomes diferentes responderiam o mesmo id — e aqui o id
// decide entre RESTAURAR o arquivo e REMOVÊ-LO.
func idDoNomeAposentado(nome string) (uint32, bool) {
	base, temSufixo := strings.CutSuffix(nome, sufixoAposentado)
	if !temSufixo {
		return 0, false
	}
	var id uint32
	if _, err := fmt.Sscanf(base, nomeCanonicoDoSegmento, &id); err != nil || id == 0 {
		return 0, false
	}
	if base != fmt.Sprintf(nomeCanonicoDoSegmento, id) {
		return 0, false
	}
	return id, true
}

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
// construção: `lidos` conta os RECORDS, `escritos` só a parte viva, e a
// distância entre eles é o disco que a aposentadoria da Task 6 vai devolver.
//
// `lidos` NÃO é o arquivo inteiro: a soma começa em segment.HeaderSize e
// acumula RecordHeaderSize + key + payload, logo vale exatamente o tamanho do
// arquivo MENOS o header de 64 bytes do segmento. A precisão importa porque a
// Task 8 publica estes números como série de compaction, e viés constante em
// métrica é pior que métrica ausente — no gráfico ele não se distingue de
// medida. Fixado pelo TestVarreduraContaComoLidoOArquivoInteiroMenosOHeader,
// cujo oráculo é o arquivo em disco e não uma segunda soma dos mesmos
// records.
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

	// O entra() é a MESMA recusa do reapendaSeVivo aplicada ao único trecho da
	// compaction que roda fora dela. Sem ele, uma varredura que não teve
	// record nenhum para copiar devolveria sucesso — nil — para um dataset já
	// FECHADO, e é sobre esse nil que a aposentadoria da Task 6 decide
	// renomear e depois remover o arquivo. Erro, não panic, pelo motivo de
	// sempre: a ingestão tem política para erro (§101) e não tem para panic.
	//
	// O ramo é DEFENSIVO, não indispensável, e o registro é honesto: pela
	// decisão D7 o Close cancela e AGUARDA a goroutine de compaction antes de
	// tomar o vida.Lock(), então a janela "Close aterrissando ENTRE o fim da
	// varredura e o zeramento" não existe em produção — e a suíte não a
	// alcança sem um seam que esta tarefa não pede. O que está fixado é o
	// dataset fechado ANTES da chamada, pelo
	// TestZeramentoDoTallyRecusaDepoisDoCloseSemRecordParaCopiar; sem esse
	// teste, apagar estas quatro linhas deixava a suíte inteira verde.
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

// aposentaSegmento tira do dataset o segmento que a varredura esvaziou. É o
// primeiro código do projeto que APAGA arquivo, e a ordem dos quatro passos é
// a decisão D5 inteira — cada um deles existe para que um crash no passo
// seguinte deixe um estado que o Open saiba reparar.
//
//  1. Checkpoint      as republicações da varredura viram duráveis. Até o
//     ApplyOverlay elas vivem no overlay em memória, e é este
//     Checkpoint que as leva ao mmap e ao manifest.
//  2. rename          o segmento sai do ListSegments sem sair do disco. O
//     manifest vigente ainda o nomeia — é a janela em que um
//     crash exige o reparo do reparaAposentados, e é o preço
//     de NÃO destruir os bytes antes de o manifest concordar.
//  3. Checkpoint      o manifest é reconstruído a partir do ListSegments, que
//     já não enxerga o arquivo. Daqui em diante nenhum estado
//     durável referencia o segmento.
//  4. unlink          só agora os bytes saem, e o que um crash pode deixar é
//     um arquivo que ninguém referencia — resíduo, não perda.
//
// Inverter 3 e 4 é a troca que perde dado: o unlink com o manifest ainda
// nomeando o segmento produz "listado e ausente", que o Open trata como
// ErrDatasetCorrupt (store_recovery.go:88) — o dataset inteiro recusado por
// causa de um arquivo que já não tinha nada vivo.
//
// A seleção do candidato NÃO mora aqui: quem decide qual segmento vale a pena
// e nunca elege o corrente (decisão D6) é a Task 7. Esta função obedece, e
// pressupõe que a varredura completa do compactaSegmento já rodou — é ela que
// dá autoridade para afirmar que nenhuma location aponta para dentro deste
// arquivo.
func (s *Store) aposentaSegmento(seg uint32) error {
	dataDir := filepath.Join(s.opts.Dir, "data")
	canonico := caminhoCanonicoDoSegmento(dataDir, seg)
	aposentado := canonico + sufixoAposentado

	if err := s.Checkpoint(); err != nil {
		return err
	}
	s.passoDaAposentadoria("checkpointDasLocations")

	if err := os.Rename(canonico, aposentado); err != nil {
		return fmt.Errorf("storage: aposentar o segmento %d: %w", seg, err)
	}
	// O fsync do diretório vem ANTES do checkpoint que deixa de listar o
	// segmento, e não junto do unlink lá embaixo, porque é ele que torna a
	// ordem dos passos verdadeira em DISCO e não apenas no programa: sem ele,
	// o manifest novo pode sobreviver a um crash que a entrada de diretório
	// não sobreviveu, e o dataset reabriria com o arquivo canônico de volta e
	// nenhum manifest o nomeando — os mesmos bytes que esta função existe para
	// devolver, presos até que alguém eleja o segmento outra vez.
	if err := fsyncDir(dataDir); err != nil {
		return err
	}
	if err := s.podaSegmentoAposentado(seg); err != nil {
		return err
	}
	s.passoDaAposentadoria("rename")

	if err := s.Checkpoint(); err != nil {
		return err
	}
	s.passoDaAposentadoria("checkpointDoManifest")

	if err := os.Remove(aposentado); err != nil {
		return fmt.Errorf("storage: remover o segmento aposentado %d: %w", seg, err)
	}
	if err := fsyncDir(dataDir); err != nil {
		return err
	}
	s.passoDaAposentadoria("unlink")
	return nil
}

// podaSegmentoAposentado tira o segmento dos dois mapas de contabilidade e
// ajusta os espelhos na MESMA seção crítica — invariante 4 da Fase B.
//
// A reconciliação do checkpoint se cura sozinha logo adiante (ela reconstrói
// o segSize do ListSegments e poda o tally do que sobrou), e é por isso que a
// poda não muda o estado FINAL de nada. O que ela decide é a janela entre o
// rename e esse checkpoint: sem ela, o /v1/stats e a nishidb_segments_total
// publicam nesse intervalo um segmento que já não está no diretório e bytes
// que o filesystem já não cobra — mentira curta, mas exatamente do tipo que o
// §92 existe para não contar, e numa série cujo propósito é mostrar a
// compaction acontecendo.
//
// Nada é RECALCULADO aqui: o que se desconta é exatamente o que este segmento
// reivindicava, pelo mesmo motivo do zeramento do tally — recalcular exigiria
// varrer todos os segmentos sob o wmu, que é a varredura que a Fase A′ tirou
// do Stats. A subtração em complemento de dois é a forma que o applyRecord já
// usa (store.go:948): Go não tem Sub atômico para inteiro sem sinal, e
// ^(x-1) devolve 0 para x==0, então tally já vazio não vira underflow.
func (s *Store) podaSegmentoAposentado(seg uint32) error {
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if t := s.segTally[seg]; t != nil {
		s.liveBytesAll.Add(^(t.liveBytes - 1))
		delete(s.segTally, seg)
		// segCount espelha a QUANTIDADE de entradas do tally (é o que o
		// agregadosDoTally conta), não o conjunto de arquivos: a baixa
		// acompanha a entrada que acabou de sair, e só ela.
		s.segCount.Add(^uint32(0))
	}
	if n, tinha := s.segSize[seg]; tinha {
		s.diskBytesAll.Add(-n)
		delete(s.segSize, seg)
	}
	return nil
}

func (s *Store) passoDaAposentadoria(passo string) {
	if s.apTrace != nil {
		s.apTrace(passo)
	}
}
