package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

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

// nameCanonicoOfSegment repete o formato que o segment.ListSegments
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
const nameCanonicoOfSegment = "segment-%08d.dat"

func pathCanonicoOfSegment(dataDir string, sec uint32) string {
	return filepath.Join(dataDir, fmt.Sprintf(nameCanonicoOfSegment, sec))
}

// retiredNameID devolve o segmento que um nome aposentado descreve, e
// recusa tudo o mais. A recusa por igualdade EXATA é a mesma do ListSegments
// (segment.go:254) e pelo mesmo motivo: o Sscanf aceita padding divergente,
// de modo que dois nomes diferentes responderiam o mesmo id — e aqui o id
// decide entre RESTAURAR o arquivo e REMOVÊ-LO.
//
// As duas recusas são INDEPENDENTES, e cada uma tem o seu teste nominal
// porque uma não cobre a outra:
//
//   - igualdade exata — "segment-1.dat.retired" responderia o id 1 pelo
//     Sscanf. Com o segmento 1 listado no manifest e o canônico ausente, o
//     reparo instalaria esse arquivo NO NOME que o manifest nomeia. É o único
//     caminho desta fase que escreve lixo por cima de um segmento, e quem o
//     fixa é o TestRetiredComPaddingDivergenteNaoEhInstaladoSobreONomeCanonico.
//   - id != 0 — "segment-00000000.dat.retired" tem o padding canônico, então
//     a igualdade acima CONCORDA com ele. Segmento 0 não existe (rowindex.go:33
//     entra em pânico ao empacotar location com segmentID 0, e o writer começa
//     no 1), logo nenhum manifest o lista e a classificação seria sempre "não
//     listado" — que é o ramo que REMOVE. Fixado pelo
//     TestRetiredDoSegmentoZeroNaoEhClassificadoPeloReparo.
//
// O registro de por que isto está escrito: até a revisão desta tarefa nenhum
// teste sustentava qualquer uma das duas, e apagar cada uma deixava
// `go test ./... -count=1` INTEIRO verde.
func retiredNameID(name string) (uint32, bool) {
	base, hasSufixo := strings.CutSuffix(name, sufixoAposentado)
	if !hasSufixo {
		return 0, false
	}
	var id uint32
	if _, err := fmt.Sscanf(base, nameCanonicoOfSegment, &id); err != nil || id == 0 {
		return 0, false
	}
	if base != fmt.Sprintf(nameCanonicoOfSegment, id) {
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
func (s *Store) reapendaSeVivo(sec, off uint32, rec types.Record) (copiado bool, n int, err error) {
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
	curSec, curOff, flags := rowindex.Unpack(packed)
	if curSec != sec || curOff != off {
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
	// DESCOBERTA OPORTUNISTA DA EXPIRAÇÃO.
	//
	// A varredura já leu este record e já está dentro da seção crítica: saber
	// se ele venceu custa uma carga de 8 bytes em mmap. Copiar o payload
	// inteiro de uma entidade que não vai mais aparecer em consulta nenhuma
	// seria trabalho jogado fora — e, pior, trabalho REPETIDO a cada passada,
	// porque a location continuaria nomeando o record grande.
	//
	// Então em vez de copiar, grava-se a lápide DIRETO no destino: um passo
	// em vez de dois (o coletor marcaria e a compaction copiaria a marca
	// depois), e o payload grande morre aqui.
	//
	// Isto amplia a responsabilidade da compaction, que até então só movia
	// bytes de lugar. A fronteira que ela continua NÃO cruzando é a de
	// inventar estado: o prazo é uma regra determinística sobre um dado que
	// já está no record, e o resultado é o MESMO que o coletor produziria —
	// não é a compaction decidindo, é ela aplicando antes.
	//
	// O ponto cego continua existindo e é por isso que o coletor não some: um
	// segmento cheio de entidades abandonadas nunca fica abaixo da razão de
	// liveness, então nunca é candidato, então esta descoberta nunca o
	// alcança. Aqui se pega o que passar pela frente; a passada do coletor
	// pega o resto.
	payload, flagsSaida := rec.Payload, flags
	expirou := false
	if s.writes != nil && flags&types.FlagTombstone == 0 && s.prazoVencido(rec.RowID, s.relogioOfExpirationOportunista()) {
		lapide, err := s.buildLapide(rec.Payload)
		if err != nil {
			return false, 0, err
		}
		payload, flagsSaida, expirou = lapide, flags|types.FlagTombstone, true
	}

	buf, err := encoding.BuildRecord(s.compactBuf[:0], rec.RowID, rec.Sequence, flagsSaida, rec.Key, payload)
	s.compactBuf = buf
	if err != nil {
		return false, 0, err
	}
	newSeg, newOff, err := s.secW.Append(buf)
	if err != nil {
		return false, 0, s.failWriter(err)
	}
	if err := s.aposAppend(newSeg); err != nil {
		return false, 0, s.failWriter(err)
	}
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
		flags:       flagsSaida,
		payload:     payload,
		prevPayload: rec.Payload,
		hadPrevious: true,
		countsAsNew: false,
		sec:         newSeg,
		off:         newOff,
		recLen:      len(buf),
	}); err != nil {
		return false, 0, s.failWriter(err)
	}
	if expirou {
		s.expiredCount.Add(1)
	}
	return true, len(buf), nil
}

// compactaSegment varre seg record a record e copia para frente todo record
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
func (s *Store) compactaSegment(ctx context.Context, sec uint32) (copiados uint64, lidos, escritos int64, err error) {
	// Os dois contadores do §92 sobem no RETORNO, inclusive quando ele é de
	// erro, e o defer é o que garante isso por construção em vez de por
	// disciplina de quem editar a função depois.
	//
	// Contar também a varredura que FRACASSOU não é detalhe: o que estas duas
	// séries medem é o I/O que a compaction consumiu, e uma varredura
	// cancelada no meio já leu o prefixo e já copiou para frente o que achou
	// vivo. Zerar esse trabalho deixaria o /metrics parado exatamente na
	// passada que morreu — que é a passada sobre a qual alguém está
	// perguntando. O tally, esse sim, não é zerado por varredura incompleta
	// (D4): são autoridades diferentes, e a distinção está logo abaixo.
	defer func() {
		s.compBytesLidos.Add(lidos)
		s.compBytesEscritos.Add(escritos)
	}()
	endValido, err := s.secR.ScanFrom(sec, segment.HeaderSize, func(off uint32, rec types.Record) error {
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
		copiou, n, err := s.reapendaSeVivo(sec, off, rec)
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
	size, err := s.sizeOfSegment(sec)
	if err != nil {
		return copiados, lidos, escritos, err
	}
	if endValido != size {
		return copiados, lidos, escritos, fmt.Errorf(
			"%w: a varredura do segmento %d parou em %d de %d bytes; o tally não é zerado por varredura incompleta (§53-55, D4)",
			ErrDatasetCorrupt, sec, endValido, size)
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
	if t := s.secTally[sec]; t != nil {
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

// sizeOfSegment devolve o tamanho em disco do arquivo de seg pelo mesmo
// ListSegments que o checkpoint usa para reconciliar o secSize
// (store_checkpoint.go:116).
//
// É consulta ao DIRETÓRIO, e não ao mapa secSize do writer, porque o mapa só
// se move no checkpoint: um segmento selado depois do último checkpoint ainda
// não tem entrada nele, e responder "não sei o tamanho" logo para o segmento
// que a varredura acabou de ler seria trocar a medida por uma coincidência de
// schedule. Um readdir por segmento compactado é ruído ao lado das milhares de
// leituras da varredura que acabou de rodar.
func (s *Store) sizeOfSegment(sec uint32) (int64, error) {
	refs, err := segment.ListSegments(filepath.Join(s.opts.Dir, "data"))
	if err != nil {
		return 0, err
	}
	for _, ref := range refs {
		if ref.ID == sec {
			return ref.SizeBytes, nil
		}
	}
	return 0, fmt.Errorf("%w: o segmento %d saiu do diretório durante a própria compaction", ErrDatasetCorrupt, sec)
}

// aposentaSegment tira do dataset o segmento que a varredura esvaziou. É o
// primeiro código do projeto que APAGA arquivo, e a ordem dos quatro passos é
// a decisão D5 inteira — cada um deles existe para que um crash no passo
// seguinte deixe um estado que o Open saiba reparar.
//
//  1. Checkpoint      as republicações da varredura viram duráveis. Até o
//     ApplyOverlay elas vivem no overlay em memória
//     (rowindex.go:171), e é este Checkpoint que as leva ao
//     mmap e ao manifest. Quem afere este passo é o
//     TestAposentadoriaTornaAsLocationsDuraveisAntesDoRename,
//     e ele lê o ARQUIVO de locations por um segundo handle,
//     não o Store: enquanto o segmento ainda tem o nome
//     canônico, nenhuma das rows que a varredura moveu pode
//     continuar presa a ele no disco. É preciso dizer o que
//     este passo NÃO é: no desenho de hoje ele não é o que
//     salva um crash — o passo 3 aplica o mesmo overlay, e
//     os bytes de origem ficam sob o `.retired` durante a
//     janela inteira, de modo que o reparo do Open devolve o
//     dataset com ou sem ele. O que ele garante é que a
//     ordem do D5 vale em DISCO desde o primeiro passo, e
//     não a partir do segundo: nenhuma janela desta função
//     tem location viva apontando para um arquivo que já
//     perdeu o nome pelo qual o manifest o nomeia.
//  2. rename          o segmento sai do ListSegments sem sair do disco. O
//     manifest vigente ainda o nomeia — é a janela em que um
//     crash exige o reparo do repairAposentados, e é o preço
//     de NÃO destruir os bytes antes de o manifest concordar.
//  3. Checkpoint      o manifest é reconstruído a partir do ListSegments, que
//     já não enxerga o arquivo. Daqui em diante nenhum estado
//     durável referencia o segmento.
//  4. unlink          só now os bytes saem, e o que um crash pode deixar é
//     um arquivo que ninguém referencia — resíduo, não perda.
//
// Inverter 3 e 4 é a troca que perde dado: o unlink com o manifest ainda
// nomeando o segmento produz "listado e ausente", que o Open trata como
// ErrDatasetCorrupt (store_recovery.go:88) — o dataset inteiro recusado por
// causa de um arquivo que já não tinha nada vivo.
//
// A seleção do candidato NÃO mora aqui: quem decide qual segmento vale a pena
// e nunca elege o corrente (decisão D6) é a Task 7. Esta função obedece, e
// pressupõe que a varredura completa do compactaSegment já rodou — é ela que
// dá autoridade para afirmar que nenhuma location aponta para dentro deste
// arquivo.
// closeODescritorEDelete é a SEÇÃO CRÍTICA da aposentadoria, e ela é só isto:
// fechar o descritor cacheado do segmento e apagar o arquivo, sob a barreira
// que impede uma leitura de acontecer no meio.
//
// Por que o descritor precisa ser fechado. O unlink do POSIX remove o NOME e
// nunca o INODE: enquanto qualquer descritor o mantiver aberto, os blocos
// seguem alocados. E o cache de fds do segment.Reader não tem despejo, então o
// descritor que a PRÓPRIA varredura desta compaction abriu (compactaSegment
// chama ScanFrom, que cacheia) ficaria aberto pela vida do processo. O efeito
// era um inode preso por segmento aposentado, e a compaction devolvendo disco
// ao listing do diretório sem devolver um byte ao dispositivo — invisível para
// dirBytes, para diskOfSegmentos e para o Stats.StorageBytes, que somam o
// listing, e invisível até para o log desta função, que anuncia bytes
// devolvidos. Provado pelo TestSegmentoAposentadoDevolveOsBlocosAoFilesystem.
//
// Por que a barreira precisa existir junto. Era exatamente aquele descritor
// vazado que fazia a leitura concorrente funcionar: com o inode aberto, o pread
// sobre um arquivo já removido devolve os bytes certos e com CRC válido.
// Fechá-lo sem barreira faz a leitura seguinte cair em os.ErrClosed (segmento
// cacheado) ou ENOENT (não cacheado), e as duas abortam a query. Corrigir o
// vazamento e pôr a barreira não são duas tarefas: são uma.
//
// Por que a seção é CURTA e não cobre os passos vizinhos. Os dois Checkpoint
// tomam wmu e podem levar dezenas de segundos; o podaSegmentAposentado também
// toma wmu. Qualquer um deles aqui dentro inverteria a ordem de lock que o
// godoc de aposMu fixa — wmu→aposMu de um lado, aposMu→wmu do outro — e isso é
// deadlock. O rename fica fora por outra razão: descritor já aberto sobrevive a
// ele, porque o rename move o nome e não o inode.
//
// NÃO há rótulo de apTrace aqui, e a ausência é deliberada: o apTrace existe
// para que os testes de crash parem ENTRE passos que mudam estado durável, e
// fechar um descritor não muda nenhum. Um crash entre o fechamento e o unlink
// deixa um `.retired` sem manifest, que é exatamente o estado que o
// repairAposentados já trata. Rótulo que nenhum teste precisa é mecanismo sem
// oráculo.
func (s *Store) closeODescritorEDelete(sec uint32, aposentado string) error {
	s.aposMu.Lock()
	defer s.aposMu.Unlock()
	if err := s.secR.CloseSegment(sec); err != nil {
		return err
	}
	if err := os.Remove(aposentado); err != nil {
		return fmt.Errorf("storage: remover o segmento aposentado %d: %w", sec, err)
	}
	return nil
}

func (s *Store) aposentaSegment(sec uint32) error {
	dataDir := filepath.Join(s.opts.Dir, "data")
	canonico := pathCanonicoOfSegment(dataDir, sec)
	aposentado := canonico + sufixoAposentado

	if err := s.Checkpoint(); err != nil {
		return err
	}
	s.passoOfRetirement("checkpointDasLocations")

	if err := os.Rename(canonico, aposentado); err != nil {
		return fmt.Errorf("storage: aposentar o segmento %d: %w", sec, err)
	}
	// O fsync do diretório vem ANTES do checkpoint que deixa de listar o
	// segmento, e não só junto do unlink lá embaixo, para que a ordem dos
	// passos valha na ENTRADA DE DIRETÓRIO e não apenas no programa: o
	// manifest do passo 3 é durável por conta própria (manifest.go:281
	// fsyncia o diretório dele), e sem este fsync ele poderia sobreviver a um
	// crash que o rename não sobreviveu — o dataset reabriria com o arquivo
	// canônico de volta e nenhum manifest o nomeando, ou seja os mesmos bytes
	// que esta função existe para devolver, presos até que alguém eleja o
	// segmento outra vez.
	//
	// O REGISTRO HONESTO, porque a revisão cobrou esta linha e ela não estava
	// no bloco da tarefa: o que a suíte prova é a CHAMADA e a POSIÇÃO dela —
	// o TestAposentadoriaDurabilizaODiretorioNoRenameENoUnlink exige um fsync
	// deste diretório num instante em que o disco já mostra o nome aposentado
	// e o manifest AINDA nomeia o segmento, e outro depois do unlink. A
	// reordenação que ele previne, essa não é alcançável pela suíte: exige
	// perda de energia com writeback pela metade, e nem o kill -9 a produz
	// (o SIGKILL não descarta page cache). É o mesmo tipo de registro dos
	// ramos defensivos do entra() e do arquivoExists() — a linha fica pelo
	// argumento, e o argumento fica escrito.
	if err := s.durabilizaODiretorio(dataDir); err != nil {
		return err
	}
	if err := s.podaSegmentAposentado(sec); err != nil {
		return err
	}
	s.passoOfRetirement("rename")

	if err := s.Checkpoint(); err != nil {
		return err
	}
	s.passoOfRetirement("checkpointDoManifest")

	if err := s.closeODescritorEDelete(sec, aposentado); err != nil {
		return err
	}
	// O índice do segmento sai no MESMO passo que o .dat, e é isso que faz
	// "purgar o segmento leva o índice junto" ser código em vez de promessa —
	// antes, nada removia as postings de um segmento aposentado, e cada órfã
	// alongava a cadeia de quem ainda estava vivo.
	//
	// Fora do closeODescritorEDelete de propósito: aquela seção crítica segura o
	// aposMu, e o mapa de índices selados tem lock próprio — aninhar os dois
	// criaria uma ordem que ninguém precisa. Depois dele, e não antes, porque a
	// janela sobrevivente é a inofensiva: dado sem índice ENUMERA, que é o
	// estado normal de qualquer segmento fora da janela de indexação.
	//
	// O segundo fsync do diretório, logo abaixo, cobre também esta remoção.
	s.removeIndexOfSegment(sec)
	// O segundo fsync é o `syncDir(dir)` do fim que o bloco da tarefa previa,
	// e fecha a mesma pergunta na outra ponta: sem ele a REMOÇÃO fica só na
	// entrada de diretório em cache, e o ciclo seguinte de compaction acharia
	// em disco um `.retired` que este já removeu. O Open o trata como órfão e
	// remove de novo, então não há perda — o que se paga é medir disco que já
	// foi devolvido, na única série que existe para mostrar a compaction
	// acontecendo (§92).
	if err := s.durabilizaODiretorio(dataDir); err != nil {
		return err
	}
	s.passoOfRetirement("unlink")
	return nil
}

// durabilizaODiretorio faz o fsync da entrada de diretório e só então avisa o
// seam de teste. A ordem é o ponto: o aviso é consequência da chamada, não uma
// linha ao lado dela, de modo que apagar o fsync apaga o aviso e o teste fica
// vermelho. Um rótulo emitido ao lado provaria apenas que alguém escreveu o
// rótulo — foi assim que o primeiro Checkpoint desta função passou por uma
// revisão inteira sem oráculo nenhum, e o remédio é não repetir a forma.
//
// Atende a aposentadoria E o reparo dela (store_recovery.go): as três mutações
// de entrada de diretório do ciclo de vida do §53-55 passam por aqui. Este
// helper é o que TORNA um fsync observável, então um fsync desta família
// escrito cru é, por construção, um fsync sem oráculo — foi assim que o do
// reparo atravessou a revisão anterior.
//
// O limite fica escrito: o que o teste alcança é a EXISTÊNCIA e a POSIÇÃO da
// chamada. Que o fsync de fato tenha levado a entrada de diretório ao meio
// físico, isso nenhum teste deste repositório afirma de nenhum fsync — o
// kernel não devolve observável para isso e o SIGKILL não descarta page
// cache. Quem trocar o corpo desta função por um return nil não fica
// vermelho; fica com uma função chamada "durabiliza" que não durabiliza, e é
// a leitura que pega, não a suíte.
func (s *Store) durabilizaODiretorio(dir string) error {
	if err := fsyncDir(dir); err != nil {
		return err
	}
	if s.apDirSync != nil {
		s.apDirSync(dir)
	}
	return nil
}

// podaSegmentAposentado tira o segmento dos dois mapas de contabilidade e
// ajusta os espelhos na MESMA seção crítica — invariante 4 da Fase B.
//
// A reconciliação do checkpoint se cura sozinha logo adiante (ela reconstrói
// o secSize do ListSegments e poda o tally do que sobrou), e é por isso que a
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
func (s *Store) podaSegmentAposentado(sec uint32) error {
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if t := s.secTally[sec]; t != nil {
		s.liveBytesAll.Add(^(t.liveBytes - 1))
		delete(s.secTally, sec)
		// secCount espelha a QUANTIDADE de entradas do tally (é o que o
		// agregadosOfTally conta), não o conjunto de arquivos: a baixa
		// acompanha a entrada que acabou de sair, e só ela.
		s.secCount.Add(^uint32(0))
	}
	if n, tinha := s.secSize[sec]; tinha {
		s.diskBytesAll.Add(-n)
		delete(s.secSize, sec)
	}
	return nil
}

func (s *Store) passoOfRetirement(passo string) {
	if s.apTrace != nil {
		s.apTrace(passo)
	}
}

// intervaloEntrePassadas é o tempo que a goroutine dorme entre duas eleições
// (§53). Não é chave de configuração porque o §78 não tem uma: o operador
// declara SE a compaction roda e a partir de que liveness, e o resto é
// decisão do produto.
//
// O número sai da assimetria entre o custo de uma passada VAZIA e o de chegar
// tarde. Passada vazia é uma iteração do secTally sob o wmu — dezenas de
// entradas num disco de 50-100 GB com segmentos de 512 MiB (§85), microssegundos
// —, enquanto chegar tarde custa o disco do §86: a 500 ev/s são 30-43 GB/dia
// escritos contra 5,6-8 GB vivos, ou seja meio gigabyte a cada vinte minutos
// de atraso. Dormir pouco é barato; dormir muito é o defeito que este
// milestone existe para fechar.
const intervaloEntrePassadas = 5 * time.Second

// candidatos elege os segmentos cujo liveBytes/sizeBytes fica abaixo do
// minimumLiveRatio (§53).
//
// Lê o tally como LIMITE SUPERIOR, que é a direção segura registrada no débito
// 38: liveness superestimada apenas ADIA a eleição de um segmento que já era
// candidato, enquanto subestimá-la faria a compaction descartar dado vivo.
//
// O segmento CORRENTE nunca entra (decisão D6) — compactá-lo é copiar dentro
// de si mesmo, com a varredura perseguindo os records que ela própria acabou
// de apendar e a aposentadoria removendo o arquivo em que o writer está no
// meio de um Append.
//
// DIVERGÊNCIA deliberada do esboço da tarefa, e ela é a diferença entre a D6
// valer e não valer: o curSecID é lido DENTRO da seção crítica. Lido antes do
// Lock, o valor descreve um instante em que ninguém segurava o wmu — e quem
// espera esse lock quase sempre espera um CHECKPOINT, que o segura do início
// ao fim. É esse mesmo checkpoint que instala no secSize o tamanho do segmento
// recém-rotacionado, ou seja a janela entre a leitura e o Lock é exatamente
// aquela em que o novo corrente ganha tamanho conhecido e vira elegível: a
// seleção acordaria com o corrente ANTIGO na mão e elegeria o segmento em que
// o writer está apendando. Sob o wmu o writer não rotaciona, então ali o
// espelho é exato — e este é o campo que nada reconcilia (invariante 4 da
// Fase B). Fixado pelo
// TestCandidatosLeOSegmentoCorrenteDEPOISDeEntrarNaSecaoCritica.
//
// Segmento sem entrada no secSize é PULADO, e isso também é adiamento e não
// omissão: o secSize só se move no checkpoint, então um segmento selado depois
// do último checkpoint ainda não tem tamanho medido, e eleger sem tamanho
// seria dividir a liveness por um número inventado.
func (s *Store) candidatos(ratio float64) []uint32 {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	current := s.curSecID.Load()
	var out []uint32
	for id, t := range s.secTally {
		if id == current {
			continue
		}
		tam, ok := s.secSize[id]
		if !ok || tam <= 0 {
			continue
		}
		// Segmento com versão retida não é candidato: uma consulta aberta
		// ainda alcança bytes que moram nele, e aposentá-lo os levaria junto.
		// A recusa é na ELEIÇÃO, e não na aposentadoria, porque varrer o
		// segmento inteiro para descobrir no fim que ele não pode sair seria
		// pagar o I/O da passada por nada.
		//
		// O pin sai sozinho quando a última consulta que o segurava fecha, e é
		// por isso que ele não precisa de prazo nem de desempate: o teto de
		// memória do journal é que limita quanto tempo isso pode durar.
		if s.vers.Pinned(id) {
			continue
		}
		if float64(t.liveBytes)/float64(tam) < ratio {
			out = append(out, id)
		}
	}
	// Determinismo: a mesma eleição produz a mesma ordem. Iteração de mapa Go
	// é aleatória de propósito, e sem isto duas passadas do mesmo estado
	// aposentariam em ordens diferentes — o operador que lê a sequência no log
	// não distinguiria mudança de estado de troca de ordem.
	slices.Sort(out)
	return out
}

// StartCompaction sobe a goroutine que elege, varre e aposenta segmentos por
// conta própria (§53-55). É o que separa "a compaction funciona" de "a
// compaction resolve": até aqui todo ciclo dependia de alguém chamá-lo.
//
// Chamar duas vezes não sobe duas goroutines. Duas varreduras concorrentes
// disputariam o wmu record a record e poderiam eleger o mesmo segmento, com a
// segunda aposentando um arquivo que a primeira ainda está varrendo.
func (s *Store) StartCompaction(ctx context.Context, ratio float64) {
	if s.EhHistory() {
		// Sem update não há garbage, e a compaction existe para recuperar
		// versões superadas. Aqui o disco volta por descarte de segmento
		// inteiro, que é o motor de retenção. Recusar dentro do Inicia — e não
		// no chamador — é o que permite ao servidor ligá-la sobre a lista
		// inteira de datasets sem perguntar o modo de cada um.
		return
	}
	s.compMu.Lock()
	defer s.compMu.Unlock()
	if s.compCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.compCancel = cancel
	// O medidor do §92 sobe AQUI, e não dentro da goroutine: quem chamou tem
	// direito de ver o estado que acabou de instalar, e um Stats() lido entre
	// a chamada e o primeiro escalonamento não pode responder que a compaction
	// não subiu.
	s.compActive.Store(true)
	s.compInVoo.Add(1)
	go func() {
		defer s.compInVoo.Done()
		// E desce quando o LAÇO acaba, seja qual for o motivo — o
		// cancelamento do Close ou a falha sticky do writer, que é a que
		// importa: ali ninguém pediu para parar, o disco volta a crescer e a
		// única coisa que o operador tem para observar é esta série (o
		// compaction.parou fala com quem já foi ler o log).
		//
		// O defer é declarado DEPOIS do Done e por isso roda ANTES dele: o
		// ForCompaction devolve o controle já com o medidor em 0, e não com
		// uma janela em que a goroutine acabou e a série ainda diz 1.
		defer s.compActive.Store(false)
		s.runCompaction(ctx, ratio)
	}()
}

// ForCompaction cancela a goroutine e ESPERA por ela. As duas coisas, e nesta
// ordem, são a decisão D7: o cancelamento sozinho devolveria o controle com a
// varredura ainda apendando no segmento corrente e a aposentadoria ainda
// renomeando arquivo.
//
// É idempotente e pode ser chamada sem nunca ter havido compaction — é o que
// permite ao Close chamá-la sem perguntar se a config a ligou.
//
// Marca compEncerrando ANTES de esperar (revisão da B5, round 2 —
// Important): a partir daqui NENHUMA passada nova — do laço ou manual —
// consegue reivindicar a vaga em tentaClaimarPass, então compInVoo só
// pode DIMINUIR entre este ponto e o Wait() retornar, nunca AUMENTAR de
// novo depois. compEncerrando NUNCA volta a false: a única chamadora real
// desta função é Close(), que também não desfaz o que faz — reabrir a vaga
// depois do Wait() moveria a mesma janela para logo depois dele, onde
// Close() já está fechando secR/secW (ver o godoc de CompactaUmaVez).
func (s *Store) ForCompaction() {
	s.compMu.Lock()
	defer s.compMu.Unlock()
	if s.compCancel != nil {
		s.compCancel()
		s.compCancel = nil
	}
	s.compPassMu.Lock()
	s.compEncerrando = true
	s.compPassMu.Unlock()

	s.compInVoo.Wait()
}

// runCompaction é o laço perpétuo: uma passada, um cochilo, outra passada.
//
// A primeira passada é IMEDIATA, e não depois do primeiro cochilo. Um dataset
// reaberto depois de horas de ingestão já vem com o garbage acumulado, e fazê
// -lo esperar o intervalo é adiar o único trabalho que devolve disco.
//
// Cada rodada DISPUTA a vaga do single-flight fino (tentaClaimarPass)
// antes de rodar — e PULA a rodada, sem erro nem log, se uma chamada MANUAL
// (CompactaUmaVez, Task B5) já a tiver. É a metade "laço em voo + manual"
// da exclusão hermética: o laço nunca sobe uma segunda varredura por cima de
// uma passada manual, ele só espera o próprio cochilo e tenta de novo.
//
// Para na PRIMEIRA falha, e para de vez. Não é impaciência: o erro que chega
// aqui ou é o modo falho sticky do writer — que só sai com Close e reabertura
// (§14.14) —, ou é a corrupção que o compactaSegment recusa a varrer, e nos
// dois casos a passada seguinte reencontraria o mesmo estado e o mesmo erro.
// Um laço que insistisse transformaria uma falha em enxurrada de log e em
// varredura perpétua sobre um dataset que já não pode ser compactado.
func (s *Store) runCompaction(ctx context.Context, ratio float64) {
	for {
		if s.tentaClaimarPass(false) {
			if err := s.runOnePassClaimada(ctx, ratio); err != nil {
				if ctx.Err() != nil {
					return // cancelamento não é falha: é o Close ou o SIGTERM
				}
				s.opts.Log.Error("compaction.parou",
					"a compaction parou e NÃO volta sozinha; o disco recomeça a crescer sem nada devolvê-lo (§53-55, §86)",
					"erro", err.Error())
				return
			}
		}
		// Sem `else` com log: uma rodada pulada por causa de uma chamada
		// manual em voo não é anomalia — é a exclusão funcionando — e o
		// cochilo de 5s abaixo já é folga de sobra para a chamada manual
		// (que responde em vez de manter conexão aberta) ter liberado a vaga
		// bem antes da próxima tentativa.
		select {
		case <-ctx.Done():
			return
		case <-time.After(intervaloEntrePassadas):
		}
	}
}

// runOnePassClaimada roda a passada já reivindicada por
// tentaClaimarPass(false) e libera a vaga via DEFER, não chamada
// síncrona (revisão da B5, Minor — consistência com o release da chamada
// manual, que já era via defer): uma passada que entrasse em pânico (não
// deveria, mas defensivamente) deixaria compPassInVoo travado em true
// para sempre sem o defer, e nenhuma passada — automática ou manual —
// conseguiria correr de novo neste Store. Função própria, e não um literal
// inline em runCompaction, porque `defer` dentro do `for` de uma função de
// vida longa só dispararia quando a função INTEIRA retornasse, não a cada
// volta do laço.
func (s *Store) runOnePassClaimada(ctx context.Context, ratio float64) error {
	defer s.releasePass(false)
	_, err := s.passOfCompaction(ctx, ratio)
	return err
}

// tentaClaimarPass reivindica o direito de rodar UMA passada now.
// manual escolhe qual das duas flags do single-flight fino
// (compPassInVoo para o laço, compManualInVoo para CompactaUmaVez) checar
// e setar; devolve false se a OUTRA já estiver em voo, OU se compEncerrando
// já tiver sido setado por ForCompaction — Close em curso, e nenhuma
// passada nova pode começar depois disso (godoc do campo, store.go).
//
// Para a chamada MANUAL, compInVoo.Add(1) acontece AQUI DENTRO, na MESMA
// seção crítica que decide o claim — nunca como um passo separado depois
// (revisão da B5, round 2 — Important). Um Add(1) que rodasse DEPOIS do
// claim abriria uma janela em que o claim já vale (compManualInVoo==true)
// mas o WaitGroup ainda não sabe disso: ForCompaction podia enxergar o
// contador em zero e devolver o controle a Close() ANTES de esta chamada
// sequer ter começado a tocar secR/secW — violação do contrato do
// WaitGroup ("um Add positivo que parte de zero deve happen-before o Wait
// correspondente") e, na prática, Close() fechando arquivo debaixo de uma
// passada que ainda ia rodar. Para o LAÇO (manual=false), compInVoo já é
// contado uma vez só, em StartCompaction, pela vida inteira da goroutine —
// um Add(1) aqui contaria em dobro.
//
// Lock BLOQUEANTE em compPassMu é seguro aqui — ao contrário de compMu
// (godoc do campo, store.go), compPassMu NUNCA é mantido além do tempo de
// checar as flags e parear o Add/Done: nem tentaClaimarPass nem
// releasePass seguram o mutex durante a passada em si (que pode levar
// minutos), então não há goroutine nenhuma que fique presa nele por tempo
// que importe, e portanto nenhum cenário em que ForCompaction (via
// compInVoo.Wait()) esteja esperando por um goroutine bloqueado aqui.
func (s *Store) tentaClaimarPass(manual bool) bool {
	s.compPassMu.Lock()
	defer s.compPassMu.Unlock()
	if s.compEncerrando || s.compPassInVoo || s.compManualInVoo {
		return false
	}
	if manual {
		s.compManualInVoo = true
		s.compInVoo.Add(1)
	} else {
		s.compPassInVoo = true
	}
	return true
}

// releasePass devolve a vaga que tentaClaimarPass(manual) reivindicou
// — e, para a chamada manual, o Done() PAREADO com o Add(1) de lá, na MESMA
// seção crítica, pela mesma razão de atomicidade do godoc acima.
func (s *Store) releasePass(manual bool) {
	s.compPassMu.Lock()
	if manual {
		s.compInVoo.Done()
		s.compManualInVoo = false
	} else {
		s.compPassInVoo = false
	}
	s.compPassMu.Unlock()
}

// ResultCompactacao é o agregado de UMA passada — quantos segmentos ela
// aposentou e quanto disco moveu. O laço automático (runCompaction) ignora
// o valor e só olha o erro; CompactaUmaVez (Task B5, POST /v1/compact) o
// devolve ao operador. Zeros em toda a struct é resultado LEGÍTIMO, não
// ausência de resposta: uma eleição sem candidato (dataset já compactado, ou
// ratio que nada alcança) é o mesmo "idempotente por efeito" do Checkpoint —
// chamar de novo sobre um estado já compactado custa uma eleição vazia e não
// muda nada.
type ResultCompactacao struct {
	SegmentosAposentados int
	BytesLidos           int64
	BytesEscritos        int64
}

// passOfCompaction varre e aposenta, em ordem, os candidatos de UMA
// eleição.
//
// A lista é fotografada no começo e não é relida durante a passada. O
// copy-forward empurra o segmento corrente para frente e SELA arquivos novos,
// então uma passada que relesse a eleição a cada volta perseguiria as próprias
// cópias sem nunca terminar — e nunca chegaria ao cochilo que devolve a CPU.
// O que a passada não alcançar, a próxima alcança.
//
// É privada do PACOTE, não do laço: runCompaction é uma chamadora, e
// CompactaUmaVez (mais abaixo, Task B5) é a outra — as duas disputam o
// single-flight fino de compPassMu (tentaClaimarPass) ANTES de chegar
// aqui, então quando esta função roda já não há a quem ela possa disputar
// wmu record a record.
func (s *Store) passOfCompaction(ctx context.Context, ratio float64) (ResultCompactacao, error) {
	var out ResultCompactacao
	for _, sec := range s.candidatos(ratio) {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		copiados, lidos, escritos, err := s.compactaSegment(ctx, sec)
		if err != nil {
			return out, fmt.Errorf("compactar o segmento %d: %w", sec, err)
		}
		if err := s.aposentaSegment(sec); err != nil {
			return out, fmt.Errorf("aposentar o segmento %d: %w", sec, err)
		}
		out.SegmentosAposentados++
		out.BytesLidos += lidos
		out.BytesEscritos += escritos
		// O log deixou de ser o único lugar em que estes números aparecem: as
		// séries do §92 os publicam desde a Task 8, e as duas leituras não se
		// substituem. A série responde "quanto disco a compaction moveu neste
		// dataset, por segundo", que é o que o painel do §86 pergunta e o que
		// um alerta consegue observar; a linha responde "QUAL segmento
		// devolveu quanto", que é o que se lê depois do alerta, e que nenhuma
		// série carrega sem um label por segmento — cardinalidade que o §92
		// não pede e que o registry não tem por que pagar.
		//
		// Eles não são decorativos em nenhuma das duas: `lidos - escritos` é o
		// disco que este segmento devolveu ao filesystem, e é a resposta à
		// pergunta do §86.
		s.opts.Log.Info("compaction.segmento_aposentado",
			"segmento varrido, esvaziado e removido",
			"segmento", sec, "records_copiados", copiados,
			"bytes_lidos", lidos, "bytes_escritos", escritos,
			"bytes_devolvidos", lidos-escritos)
	}
	return out, nil
}

// CompactaUmaVez executa UMA passada de compaction sob demanda — o caso de
// uso do operador que desligou o laço automático (storage.compaction.enabled:
// false) e quer devolver disco na mão (Task B5, POST /v1/compact), E TAMBÉM
// o caso — corrigido pela revisão da B5, achado Critical — do operador que
// deixou o laço LIGADO (o default: storage.compaction.enabled ausente ⇒
// true) e quer forçar uma passada fora do intervalo dele: com o laço
// configurado mas OCIOSO entre duas passadas, esta chamada funciona e
// devolve trabalho de verdade.
//
// Disputa o MESMO single-flight fino de runCompaction — tentaClaimarPass
// — nunca um segundo, e NUNCA compCancel: compCancel fica não-nil da
// primeira à última hora do laço (dormindo ou varrendo), e checá-lo
// devolveria ErrCompactionBusy o tempo todo na config default, tornando esta
// rota morta no caso majoritário — o bug que a revisão achou. "Em voo" quer
// dizer que HÁ uma varredura de fato acontecendo NESTE INSTANTE (o laço
// dentro de passOfCompaction, ou outra chamada manual) — nunca "existe um
// laço configurado". Em qualquer um dos dois casos de exclusão, devolve
// ErrCompactionBusy NA HORA — não enfileira e não espera, porque quem chama
// (a rota HTTP) tem de responder ao cliente sem segurar a conexão por uma
// passada que pode durar minutos.
//
// A exclusão é HERMÉTICA NOS DOIS SENTIDOS (godoc de compPassMu,
// store.go): com esta chamada em voo, a tentativa do laço de rodar sua
// PRÓXIMA rodada falha e ele PULA (runCompaction) — nunca uma segunda
// varredura concorrente; com o laço em voo, esta chamada falha aqui.
//
// ctx cancelado no meio interrompe a passada com segurança: o copy-forward é
// escrita normal sob o wmu (reapendaSeVivo), e o candidato que a passada não
// alcançar fica para a PRÓXIMA — automática ou manual. É o mesmo argumento de
// segurança que o cancelamento do Close já usa (decisão D7), só que aqui quem
// cancela é o cliente HTTP desistindo, não o encerramento do processo.
//
// # Por que a janela entre ForCompaction retornar e Close tomar vida.Lock() é segura
//
// (Important 2 da revisão da B5; mecanismo corrigido no round 2 — ver
// Important abaixo.) Close() chama ForCompaction() ANTES de vida.Lock() —
// não depois —, então existe uma janela, por menor que seja, em que
// compCancel já é nil e vida ainda não está travada. O que impede uma
// chamada nova a CompactaUmaVez de entrar nessa janela e correr contra
// Close() fechando secR/secW NÃO é vida.RLock(): compactaSegment chama
// s.secR.ScanFrom SEM esse bracket (só reapendaSeVivo, por record, tem
// entra()/sai()) — são DUAS coisas juntas: (1) o PAREAMENTO Add/Done de
// compInVoo, atômico com o claim (tentaClaimarPass/releasePass, e não
// mais dois passos soltos aqui — essa separação era exatamente o Important
// que a revisão achou no round 2, ver abaixo); e (2) compEncerrando, que
// ForCompaction marca ANTES do Wait() e que faz tentaClaimarPass recusar
// qualquer reivindicação nova a partir dali. Entre as duas: nenhuma passada
// pode COMEÇAR depois que ForCompaction decidiu esperar, e nenhuma passada
// que já tinha COMEÇADO escapa de ser contada por compInVoo antes de tocar
// secR/secW. É esse par — não vida.RLock() — que garante que nenhuma
// varredura ainda está lendo/escrevendo segmento quando Close() chega a
// fechar os arquivos.
//
// O QUE REABRIRIA o risco: qualquer mudança que separasse de novo o Add(1)
// do claim (por exemplo, um "compacta em background e responde 202" futuro
// que reivindicasse a vaga num lugar e chamasse Add noutro), que resetasse
// compEncerrando para false em algum ponto (não faz — é permanente, uma vez
// que ForCompaction é chamada), ou que fizesse Close() reordenar
// ForCompaction() para DEPOIS de tocar secR/secW. Enquanto claim e Add
// continuarem ATÔMICOS sob compPassMu, e compEncerrando continuar
// permanente, a janela é segura; ela não é imposta pelo compilador — fica
// registrado aqui para a próxima mudança tropeçar no comentário.
//
// # Important do round 2 da revisão: o Add(1) tinha descolado do claim
//
// Uma primeira versão desta função fazia `tentaClaimarPass` devolver só
// um bool, e chamava `compInVoo.Add(1)` NA LINHA SEGUINTE, fora do lock de
// compPassMu. Isso abria exatamente a janela que o parágrafo acima
// fecha: entre o claim valer (compManualInVoo==true) e o Add rodar, um
// ForCompaction concorrente podia enxergar compInVoo em ZERO — a janela
// não durava mais que algumas instruções de máquina, mas o contrato do
// WaitGroup não tem uma cláusula de "só um pouquinho", e o efeito prático,
// se acontecesse, seria Close() liberado para fechar secR/secW enquanto
// esta função ainda ia CHAMAR passOfCompaction. A correção: Add(1)
// (chamada manual) e Done() (no release) moraram para DENTRO de
// tentaClaimarPass/releasePass, na MESMA seção crítica que decide o
// claim — nunca mais dois passos.
func (s *Store) CompactaUmaVez(ctx context.Context, ratio float64) (ResultCompactacao, error) {
	if !s.tentaClaimarPass(true) {
		return ResultCompactacao{}, ErrCompactionBusy
	}
	defer s.releasePass(true)

	return s.passOfCompaction(ctx, ratio)
}
