package replication

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

const (
	// segmentSuffix é a extensão dos arquivos de segmento. O nome é a primeira
	// sequence do segmento com 20 dígitos (o que ordena lexicograficamente).
	segmentSuffix    = ".seg"
	segmentNameDigit = 20

	// indexStride é o espaçamento do índice esparso: uma entrada a cada 1 MiB
	// de segmento, além do primeiro quadro de cada segmento. Com um teto de
	// 1 GiB de disco são ~1.024 entradas em heap.
	indexStride = 1 << 20

	// scanBufferBytes é o buffer de leitura sequencial da abertura.
	scanBufferBytes = 1 << 20
)

// Seams de I/O. Em produção são as chamadas diretas; os testes os trocam para
// registrar a ORDEM das operações de durabilidade (que nenhum teste em processo
// enxerga de outro jeito) e para injetar falha no meio de uma escrita. Nunca
// são trocados com goroutines do log em voo.
//
// São variáveis de pacote, compartilhadas por todos os logs do processo: o
// teste que as troca NÃO pode usar t.Parallel (nem rodar ao lado de outro que
// use um log), e deve restaurá-las no fim (ver seams em disklog_fault_test.go).
var (
	fdatasyncFn = fdatasync
	syncDirFn   = atomicfile.SyncDir
	writeAtFn   = func(f *os.File, p []byte, off int64) (int, error) { return f.WriteAt(p, off) }
	truncateFn  = func(f *os.File, size int64) error { return f.Truncate(size) }
)

var (
	// errLogClosed é uma operação num log já fechado.
	errLogClosed = errors.New("replication: disk log closed")
	// errLogUnbound é um append antes de bind: o cabeçalho de segmento precisa
	// da epoch, e o log vazio não a conhece.
	errLogUnbound = errors.New("replication: disk log not bound to an epoch")
	// errNotContiguous é um quadro cuja From não segue a To anterior. O feed
	// nunca publica com buraco invisível: quem cria a lacuna descarta o log
	// (wipe) e religa o piso (bind) antes do próximo quadro.
	errNotContiguous = errors.New("replication: frame does not continue the log")
	// errBadAppend é um quadro cujo conteúdo não confere com os argumentos
	// do append (bug do chamador, nunca dado de disco).
	errBadAppend = errors.New("replication: append arguments do not match the frame")
	// errRetentionUnlink: o quadro foi gravado, mas um segmento aposentado não
	// pôde ser apagado. É erro de LIMPEZA, não de gravação.
	errRetentionUnlink = errors.New("replication: retention could not unlink a segment")
	// errBindRegression é um bind, num log vazio e com a MESMA epoch, que
	// baixaria o piso ou a posição: reabriria sequences já entregues.
	errBindRegression = errors.New("replication: bind would lower the floor or position of the same epoch")
	// errCorruptLog é o log de segmentos incoerente no disco fora do que uma
	// queda explica: segmento selado com cabeçalho de quadro ruim, lacuna entre
	// segmentos, epoch misturada, nome que não bate com o cabeçalho. A cura é
	// removeSegments e um log novo (o Stream trata como queda).
	errCorruptLog = errors.New("replication: corrupt disk log")
	// errEpochMismatch é um bind com epoch diferente da dos segmentos em disco.
	errEpochMismatch = errors.New("replication: epoch differs from the one on disk")
	// errReleased é leitura por um handle já devolvido.
	errReleased = errors.New("replication: segment handle released")
	// errLogFailed envolve a falha de DURABILIDADE (fdatasync) que deixou o
	// log falho: todo append e sync devolvem esse erro até o wipe. Ao
	// contrário da falha de escrita, retentar não adianta; quem usa o log
	// (o gravador do feed) declara a lacuna e faz o wipe.
	errLogFailed = errors.New("replication: disk log failed a durability sync")
	// errTailMismatch é a cauda do log em desacordo com o `state`.
	errTailMismatch = errors.New("replication: log tail does not match the feed state")
)

// segmentInfo descreve um segmento aposentado pela retenção.
type segmentInfo struct {
	FirstSequence, LastTo uint64
	Bytes                 int64
	Path                  string
}

// indexEntry é uma entrada do índice esparso: covered é a última sequence
// JÁ coberta antes do quadro que começa em off (a To do quadro anterior, ou
// FirstSequence-1 no primeiro quadro do segmento). Ler a partir de off
// devolve, em ordem, todos os quadros com To > covered.
type indexEntry struct {
	covered uint64
	off     int64
}

// segment é um arquivo de segmento aberto.
//
// Os campos abaixo de "protegidos por mu" são lidos e escritos só sob o mu do
// diskLog. size é atômico porque o leitor (readAt) o consulta sem lock: ele é
// a fronteira de PUBLICAÇÃO do segmento, e só avança depois que o quadro
// inteiro foi escrito, de modo que nenhum leitor enxerga um quadro pela
// metade.
type segment struct {
	path     string
	firstSeq uint64
	startPos uint64
	epoch    [16]byte
	size     atomic.Int64

	f *os.File // leitura (e escrita, quando o segmento nasceu neste processo)
	// w é o handle de escrita. Nil num segmento reaberto até o primeiro
	// append: a abertura do log é SÓ LEITURA.
	w *os.File

	// protegidos por mu:
	lastTo  uint64
	endPos  uint64
	frames  int
	index   []indexEntry
	refs    int
	retired bool
	// tornBytes > 0 marca cauda SUJA: bytes além de size (cauda rasgada da
	// queda, ou resto de uma escrita que falhou e cujo truncamento também
	// falhou). O append trunca para size ANTES de qualquer outra coisa, inclusive
	// antes de decidir a rotação; enquanto for > 0, validateTail recusa.
	tornBytes int64

	closeOnce sync.Once
}

func (s *segment) closeFiles() {
	s.closeOnce.Do(func() {
		_ = s.f.Close()
		if s.w != nil && s.w != s.f {
			_ = s.w.Close()
		}
	})
}

// segmentHandle é um segmento arrendado a um leitor. Enquanto não for
// devolvido (release), o fd continua aberto mesmo que a retenção apague o
// arquivo: é o que deixa um leitor lento terminar de ler um segmento que o
// disco já esqueceu.
type segmentHandle struct {
	l        *diskLog
	s        *segment
	released atomic.Bool
}

// diskLog é o log de segmentos do feed.
//
// Concorrência: append, wipe, bind e close têm UM chamador por vez (o
// gravador do feed). locate, readAt, release, sync e as consultas podem rodar
// em qualquer goroutine, inclusive durante um append. Nenhum método segura mu
// durante I/O: o mu protege só os metadados em memória, e a escrita acontece
// fora dele (o gravador é único, então ninguém muda o segmento corrente por
// baixo dele).
type diskLog struct {
	dir          string
	segmentBytes int64
	maxBytes     int64

	mu       sync.Mutex
	segs     []*segment
	total    int64  // soma dos tamanhos dos segmentos, cabeçalhos incluídos
	floorSeq uint64 // maior piso conhecido; floor() considera também o 1º segmento
	endPos   uint64 // fim do último quadro (ou a base, num log vazio)
	epoch    [16]byte
	hasEp    bool // epoch conhecida: veio de segmento em disco ou de bind
	bound    bool
	orphans  []string // arquivos sem cabeçalho legível, apagados no 1º append/wipe
	dirGone  bool     // diretório inexistente na abertura; criado no 1º append
	err      error    // falha de DURABILIDADE (fdatasync) pegajosa: writer FALHO até wipe
	closed   bool
}

func segmentFileName(firstSeq uint64) string {
	s := strconv.FormatUint(firstSeq, 10)
	return strings.Repeat("0", segmentNameDigit-len(s)) + s + segmentSuffix
}

func parseSegmentName(name string) (uint64, bool) {
	if len(name) != segmentNameDigit+len(segmentSuffix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	n, err := strconv.ParseUint(name[:segmentNameDigit], 10, 64)
	if err != nil || segmentFileName(n) != name {
		return 0, false
	}
	return n, true
}

// peekFrame lê, sem decodificar entradas, os três campos que a continuidade do
// log precisa. Confere magic, tamanho e CRC; devolve o tamanho total do
// quadro.
func peekFrame(b []byte) (from, to, endPos uint64, size int, ok bool) {
	if len(b) < frameHeaderSize || [4]byte(b[0:4]) != frameMagic {
		return 0, 0, 0, 0, false
	}
	bodyLen := int64(binary.LittleEndian.Uint32(b[4:8]))
	if bodyLen < frameFixedBody || bodyLen > int64(len(b)-frameHeaderSize) {
		return 0, 0, 0, 0, false
	}
	body := b[frameHeaderSize : frameHeaderSize+int(bodyLen)]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(b[8:12]) {
		return 0, 0, 0, 0, false
	}
	return binary.LittleEndian.Uint64(body[0:8]), binary.LittleEndian.Uint64(body[8:16]),
		binary.LittleEndian.Uint64(body[24:32]), frameHeaderSize + int(bodyLen), true
}

// createDiskLog cria o diretório do feed (com fsync do pai de CADA nível
// criado, para a criação inteira sobreviver a uma queda) e abre o log nele.
func createDiskLog(dir string, segmentBytes, maxBytes int64) (*diskLog, error) {
	if err := ensureDir(dir); err != nil {
		return nil, err
	}
	return openDiskLog(dir, segmentBytes, maxBytes)
}

// dirSync guarda os níveis de diretório criados cujo pai ainda não teve um
// fsync bem-sucedido. Sem essa memória, a nova tentativa depois de um fsync
// falho acharia o diretório já criado, não veria nível nenhum a criar e
// voltaria sucesso sem nunca ter tornado a criação durável.
var dirSync struct {
	mu      sync.Mutex
	pending map[string]struct{}
}

// ensureDir cria dir e os ancestrais que faltam. Uma entrada de diretório só
// é durável depois do fsync do diretório que a contém, então cada nível criado
// tem o pai sincronizado (do mais alto para o mais baixo): sincronizar só o
// pai imediato deixaria os níveis de cima fora do fsync.
//
// Um nível criado fica PENDENTE até o fsync do pai dele dar certo: a chamada
// seguinte, mesmo achando o diretório já criado, sincroniza de novo todo nível
// pendente da cadeia de dir, e só então volta sucesso. Roda inteira sob
// dirSync.mu: uma chamada concorrente que achasse o diretório pronto não pode
// voltar antes do fsync da que o criou.
func ensureDir(dir string) error {
	dirSync.mu.Lock()
	defer dirSync.mu.Unlock()
	if dirSync.pending == nil {
		dirSync.pending = make(map[string]struct{})
	}
	dir = filepath.Clean(dir)
	for d := dir; ; d = filepath.Dir(d) {
		_, err := os.Stat(d)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		dirSync.pending[d] = struct{}{}
		if filepath.Dir(d) == d {
			break
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// A cadeia de dir, do mais baixo para o mais alto; o fsync vai na ordem
	// inversa.
	var chain []string
	for d := dir; ; d = filepath.Dir(d) {
		if _, ok := dirSync.pending[d]; ok {
			chain = append(chain, d)
		}
		if filepath.Dir(d) == d {
			break
		}
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if err := syncDirFn(filepath.Dir(chain[i])); err != nil {
			return err
		}
		delete(dirSync.pending, chain[i])
	}
	return nil
}

// removeSegments apaga todo arquivo de segmento (`*.seg`, inclusive sobras sem
// cabeçalho) de dir, sem abrir o log, e sincroniza o diretório. É a cura de um
// log que openDiskLog recusou com errCorruptLog: o Stream a trata como queda,
// chama removeSegments e cria um log novo. Diretório ausente não é erro. Não
// toca o `state` nem outros arquivos.
func removeSegments(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var firstErr error
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	if err := syncDirFn(dir); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// openDiskLog lê os segmentos de dir e reconstrói o índice esparso. É SÓ
// LEITURA: não cria o diretório (ausente = log vazio) e não grava, nem apaga,
// nem trunca nada. O que a queda deixou de lixo é lembrado e só é descartado
// no primeiro append ou no wipe.
//
// Custo da abertura, por classe de segmento:
//   - SELADOS (todos menos o último, já sincronizados na rotação): só os
//     cabeçalhos de quadro (magic, bodyLen) e os campos From/To/EndPos do
//     começo do corpo, para o índice e para a continuidade. O corpo não é
//     lido nem tem o CRC conferido: o CRC é conferido na leitura, por
//     decodeFrame, e quem lê trata errCorruptFrame como lacuna;
//   - ÚLTIMO: leitura completa, com o CRC de cada quadro, porque é onde uma
//     queda pode ter deixado um quadro pela metade.
//
// Classes de defeito, tratadas de forma diferente porque a causa é diferente:
//   - num segmento SELADO, cabeçalho de quadro ruim, lacuna entre segmentos ou
//     epoch misturada não são efeito de queda: devolve errCorruptLog (cure
//     com removeSegments);
//   - no ÚLTIMO arquivo, cabeçalho de segmento ilegível (uma queda pode deixar
//     zeros) é uma rotação interrompida: o arquivo vira órfão, sem erro;
//   - no ÚLTIMO segmento, bytes que não formam quadros válidos são a cauda
//     rasgada de uma gravação interrompida: o log abre com o prefixo válido, e
//     validateTail devolve erro enquanto houver cauda rasgada.
func openDiskLog(dir string, segmentBytes, maxBytes int64) (*diskLog, error) {
	if segmentBytes < segmentHeaderSize+frameHeaderSize || maxBytes < 1 {
		return nil, errors.New("replication: disk log needs positive segmentBytes and maxBytes")
	}
	l := &diskLog{dir: dir, segmentBytes: segmentBytes, maxBytes: maxBytes}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		l.dirGone = true
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		if _, ok := parseSegmentName(e.Name()); !ok {
			return nil, fmt.Errorf("%w: unexpected segment file name %q", errCorruptLog, e.Name())
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for i, name := range names {
		last := i == len(names)-1
		seg, err := l.scanSegment(filepath.Join(dir, name), name, last)
		if err != nil {
			l.closeAll()
			return nil, err
		}
		if seg == nil { // último arquivo sem cabeçalho: sobra de rotação interrompida
			l.orphans = append(l.orphans, filepath.Join(dir, name))
			continue
		}
		if n := len(l.segs); n > 0 {
			prev := l.segs[n-1]
			if seg.epoch != prev.epoch || seg.firstSeq != prev.lastTo+1 || seg.startPos != prev.endPos {
				seg.closeFiles()
				l.closeAll()
				return nil, fmt.Errorf("%w: segment %s does not continue %s", errCorruptLog, name, filepath.Base(prev.path))
			}
		}
		l.segs = append(l.segs, seg)
		l.total += seg.size.Load()
	}
	if n := len(l.segs); n > 0 {
		l.epoch, l.hasEp = l.segs[0].epoch, true
		l.endPos = l.segs[n-1].endPos
		l.floorSeq = l.segs[0].firstSeq - 1
	}
	return l, nil
}

func (l *diskLog) closeAll() {
	for _, s := range l.segs {
		s.closeFiles()
	}
}

// scanSegment abre um segmento e percorre os quadros (ver openDiskLog para o
// que cada classe de segmento paga). Devolve nil sem erro quando é o último
// arquivo e o cabeçalho de segmento não se sustenta (curto ou ilegível):
// rotação que caiu antes de gravá-lo por inteiro, sem dado nenhum nele.
func (l *diskLog) scanSegment(path, name string, last bool) (*segment, error) {
	wantFirst, _ := parseSegmentName(name)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = f.Close()
		}
	}()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := st.Size()
	var hdr segmentHeader
	var hdrErr error
	if fileSize < segmentHeaderSize {
		hdrErr = fmt.Errorf("segment %s has %d bytes, shorter than its header", name, fileSize)
	} else {
		var hb [segmentHeaderSize]byte
		if _, err := f.ReadAt(hb[:], 0); err != nil {
			return nil, err
		}
		hdr, hdrErr = decodeSegmentHeader(hb[:])
	}
	if hdrErr != nil {
		if last {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %w", errCorruptLog, hdrErr)
	}
	if hdr.FirstSequence != wantFirst {
		return nil, fmt.Errorf("%w: segment %s declares first sequence %d", errCorruptLog, name, hdr.FirstSequence)
	}
	seg := &segment{
		path: path, firstSeq: hdr.FirstSequence, startPos: hdr.StartPos, epoch: hdr.Epoch, f: f,
		lastTo: hdr.FirstSequence - 1, endPos: hdr.StartPos,
	}

	// head é o cabeçalho do quadro mais From, To, At e EndPos do corpo.
	const headBytes = frameHeaderSize + 32
	r := bufio.NewReaderSize(io.NewSectionReader(f, segmentHeaderSize, fileSize-segmentHeaderSize), scanBufferBytes)
	off := int64(segmentHeaderSize)
	var buf []byte
	var lastIndexOff int64 = -1
	for off < fileSize {
		var head [headBytes]byte
		if _, err := io.ReadFull(r, head[:]); err != nil {
			break
		}
		bodyLen := int64(binary.LittleEndian.Uint32(head[4:8]))
		if [4]byte(head[0:4]) != frameMagic || bodyLen < frameFixedBody || bodyLen > fileSize-off-frameHeaderSize {
			break
		}
		size := frameHeaderSize + bodyLen
		from := binary.LittleEndian.Uint64(head[12:20])
		to := binary.LittleEndian.Uint64(head[20:28])
		endPos := binary.LittleEndian.Uint64(head[36:44])
		if from != seg.lastTo+1 || to < from || endPos != seg.endPos+uint64(size) {
			break
		}
		if last {
			// Só o último segmento paga o corpo inteiro e o CRC.
			if cap(buf) < int(size) {
				buf = make([]byte, size)
			} else {
				buf = buf[:size]
			}
			copy(buf, head[:])
			if _, err := io.ReadFull(r, buf[headBytes:]); err != nil {
				break
			}
			if crc32.Checksum(buf[frameHeaderSize:], castagnoli) != binary.LittleEndian.Uint32(buf[8:12]) {
				break
			}
		}
		if lastIndexOff < 0 || off-lastIndexOff >= indexStride {
			seg.index = append(seg.index, indexEntry{covered: seg.lastTo, off: off})
			lastIndexOff = off
		}
		seg.lastTo, seg.endPos = to, endPos
		seg.frames++
		off += size
		if !last {
			// Pula o resto do corpo. Um salto grande vira novo ponto de leitura
			// em vez de ler e descartar o que o buffer não alcança.
			skip := bodyLen - 32
			if skip > scanBufferBytes {
				r.Reset(io.NewSectionReader(f, off, fileSize-off))
			} else if _, err := r.Discard(int(skip)); err != nil {
				break
			}
		}
	}
	if off < fileSize {
		if !last {
			return nil, fmt.Errorf("%w: sealed segment %s has an invalid frame at offset %d", errCorruptLog, name, off)
		}
		seg.tornBytes = fileSize - off
	}
	if !last && seg.frames == 0 {
		return nil, fmt.Errorf("%w: sealed segment %s has no frames", errCorruptLog, name)
	}
	seg.size.Store(off)
	keep = true
	return seg, nil
}

// bind liga o log à geração do feed: a epoch que os cabeçalhos dos novos
// segmentos vão levar, o piso e, num log VAZIO, a posição em bytes em que o
// próximo quadro começa.
//
// Um log sem segmento não tem como saber nada disso pelo disco: o piso e a
// posição do master ocioso (Sequence=0) ou de um feed recém-invalidado
// (wipe, com Floor == Sequence) vêm do `state`. Num log com segmentos a epoch
// é conferida contra a dos arquivos (errEpochMismatch) e o piso só sobe.
//
// Num log vazio, religar a MESMA epoch não pode baixar o piso nem a posição
// (errBindRegression): voltar atrás reabriria sequences já entregues com
// outro conteúdo. Uma epoch NOVA (geração nova) recomeça de onde disser.
func (l *diskLog) bind(epoch [16]byte, floor, endPos uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errLogClosed
	}
	if len(l.segs) > 0 {
		if epoch != l.segs[0].epoch {
			return errEpochMismatch
		}
		l.floorSeq = max(l.floorSeq, floor)
	} else {
		if l.hasEp && l.epoch == epoch && (floor < l.floorSeq || endPos < l.endPos) {
			return errBindRegression
		}
		l.floorSeq, l.endPos = floor, endPos
	}
	l.epoch, l.hasEp, l.bound = epoch, true, true
	return nil
}

// logEpoch devolve a epoch dos segmentos em disco, quando há algum.
func (l *diskLog) logEpoch() ([16]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segs) == 0 {
		return [16]byte{}, false
	}
	return l.segs[0].epoch, true
}

func (l *diskLog) floorLocked() uint64 {
	if len(l.segs) > 0 {
		return max(l.floorSeq, l.segs[0].firstSeq-1)
	}
	return l.floorSeq
}

// floor é o maior piso: a sequence a partir da qual (exclusive) o log ainda
// tem quadros. É o piso reaberto: max(state.Floor, FirstSequence(primeiro) - 1).
func (l *diskLog) floor() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.floorLocked()
}

// bytes é o total em disco, cabeçalhos de segmento incluídos.
func (l *diskLog) bytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.total
}

// segments é o número de segmentos.
func (l *diskLog) segments() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.segs)
}

// position devolve a última sequence e a posição em bytes do fim do último
// quadro (num log vazio, o piso e a base dada ao bind).
func (l *diskLog) position() (lastTo, endPos uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := len(l.segs); n > 0 {
		return l.segs[n-1].lastTo, l.endPos
	}
	return l.floorSeq, l.endPos
}

// append grava um quadro já codificado (appendFrame) cuja cobertura é
// [from, to] e cujo fim está na posição acumulada endPos.
//
// Contrato com o chamador:
//   - from continua exatamente o log (from == última To + 1; num log vazio,
//     from == piso + 1) e endPos == posição atual + len(frameBytes). Violar
//     devolve errNotContiguous/errBadAppend SEM gravar e SEM falhar o log;
//   - o conteúdo de frameBytes tem de conferir com from/to/endPos.
//
// Ordem das operações, e o porquê de cada uma:
//  1. cria o diretório (se a abertura não o achou) e apaga as sobras sem
//     cabeçalho;
//  2. trunca a cauda suja do segmento corrente (cauda rasgada da queda) ANTES
//     de decidir a rotação: se o quadro rotacionasse sem truncar, o lixo
//     ficaria no meio do log, num segmento que já nasceu selado;
//  3. a rotação acontece ANTES do quadro que passaria do teto do segmento,
//     nunca depois; um quadro maior que o teto sai sozinho num segmento. O
//     segmento que fecha é sincronizado (fdatasync) antes de o novo existir:
//     depois da rotação, sync() só enxerga o corrente, e uma cauda não
//     sincronizada do anterior ficaria fora de qualquer fsync futuro;
//  4. grava o quadro e só então o PUBLICA (avança o fim visível do segmento):
//     nenhum leitor enxerga um quadro pela metade.
//
// Depois de gravar, a retenção apaga os segmentos mais antigos enquanto
// bytes() > maxBytes E houver mais de um segmento (o corrente nunca é
// apagado); o piso passa a ser FirstSequence(próximo) - 1. removed lista o
// que a retenção aposentou. Um erro errRetentionUnlink significa que o
// quadro FOI gravado e só a limpeza falhou.
//
// Duas classes de falha de I/O, com tratamentos opostos:
//   - falha de ESCRITA (ENOSPC, EIO ao gravar), de criação de arquivo ou de
//     truncamento é RETENTÁVEL: o segmento volta ao tamanho publicado (um
//     segmento recém-criado é removido), o log NÃO fica falho e a mesma chamada
//     pode ser repetida quando o disco voltar. Se até o truncamento falhar, a
//     cauda fica marcada como suja e a próxima chamada a trunca primeiro;
//   - falha de DURABILIDADE (fdatasync) é PEGAJOSA até wipe, como o stickyErr
//     do Store: depois de um fsync que falhou o kernel pode ter descartado as
//     páginas sujas, e uma retentativa "bem-sucedida" mentiria. O erro vem
//     envolvido em errLogFailed.
//
// Se o log for fechado durante a gravação, o quadro não é publicado, o que
// foi escrito é desfeito e o erro é errLogClosed.
func (l *diskLog) append(frameBytes []byte, from, to, endPos uint64) (removed []segmentInfo, err error) {
	gotFrom, gotTo, gotEnd, size, ok := peekFrame(frameBytes)
	if !ok || size != len(frameBytes) || gotFrom != from || gotTo != to || gotEnd != endPos {
		return nil, errBadAppend
	}

	l.mu.Lock()
	switch {
	case l.closed:
		l.mu.Unlock()
		return nil, errLogClosed
	case l.err != nil:
		err = l.err
		l.mu.Unlock()
		return nil, err
	case !l.bound:
		l.mu.Unlock()
		return nil, errLogUnbound
	}
	var tail *segment
	wantFrom := l.floorSeq + 1
	if n := len(l.segs); n > 0 {
		tail = l.segs[n-1]
		wantFrom = tail.lastTo + 1
	}
	if from != wantFrom || to < from {
		l.mu.Unlock()
		return nil, errNotContiguous
	}
	if endPos != l.endPos+uint64(len(frameBytes)) {
		l.mu.Unlock()
		return nil, errBadAppend
	}
	rotate := tail == nil || (tail.frames > 0 && tail.size.Load()+int64(len(frameBytes)) > l.segmentBytes)
	orphans := slices.Clone(l.orphans)
	dirGone := l.dirGone
	dirty := tail != nil && tail.tornBytes > 0
	if tail != nil {
		// Referência do próprio append ao segmento em que ele escreve: um
		// close() no meio não fecha o fd debaixo da escrita (nem do undo).
		tail.refs++
	}
	l.mu.Unlock()
	if tail != nil {
		defer l.unref(tail)
	}

	if dirGone {
		if err := ensureDir(l.dir); err != nil {
			return nil, err
		}
		l.mu.Lock()
		l.dirGone = false
		l.mu.Unlock()
	}
	if len(orphans) > 0 {
		for _, p := range orphans {
			if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
		l.mu.Lock()
		l.orphans = nil
		l.mu.Unlock()
	}
	if dirty {
		if err := l.openWriter(tail); err != nil {
			return nil, err
		}
		if err := truncateFn(tail.w, tail.size.Load()); err != nil {
			return nil, err
		}
		l.mu.Lock()
		tail.tornBytes = 0
		l.mu.Unlock()
	}

	var seg *segment
	var frameOff int64
	created := false
	if rotate {
		if tail != nil {
			if err := fdatasyncFn(tail.f); err != nil {
				return nil, l.failDurability(err)
			}
		}
		seg, err = l.createSegment(from, endPos-uint64(len(frameBytes)))
		if err != nil {
			return nil, err
		}
		created = true
		frameOff = segmentHeaderSize
	} else {
		seg = tail
		frameOff = seg.size.Load()
		if err := l.openWriter(seg); err != nil {
			return nil, err
		}
	}
	// undo desfaz o que esta chamada escreveu e não publicou.
	undo := func() {
		if created {
			seg.closeFiles()
			_ = os.Remove(seg.path)
			return
		}
		if err := truncateFn(seg.w, frameOff); err != nil {
			l.mu.Lock()
			seg.tornBytes = max(seg.tornBytes, 1)
			l.mu.Unlock()
		}
	}
	if _, err := writeAtFn(seg.w, frameBytes, frameOff); err != nil {
		undo()
		return nil, err
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		undo()
		return nil, errLogClosed
	}
	if created {
		l.segs = append(l.segs, seg)
		l.total += segmentHeaderSize
	}
	if seg.frames == 0 || frameOff-seg.index[len(seg.index)-1].off >= indexStride {
		seg.index = append(seg.index, indexEntry{covered: seg.lastTo, off: frameOff})
	}
	seg.frames++
	seg.lastTo, seg.endPos = to, endPos
	seg.size.Store(frameOff + int64(len(frameBytes)))
	l.total += int64(len(frameBytes))
	l.endPos = endPos

	var victims []*segment
	var closable []*segment
	for l.total > l.maxBytes && len(l.segs) > 1 {
		v := l.segs[0]
		l.segs = slices.Delete(l.segs, 0, 1)
		l.total -= v.size.Load()
		v.retired = true
		victims = append(victims, v)
		if v.refs == 0 {
			closable = append(closable, v)
		}
	}
	l.mu.Unlock()

	var unlinkErr error
	for _, v := range victims {
		removed = append(removed, segmentInfo{FirstSequence: v.firstSeq, LastTo: v.lastTo, Bytes: v.size.Load(), Path: v.path})
		if err := os.Remove(v.path); err != nil && !errors.Is(err, os.ErrNotExist) && unlinkErr == nil {
			unlinkErr = fmt.Errorf("%w: %w", errRetentionUnlink, err)
		}
	}
	for _, v := range closable {
		v.closeFiles()
	}
	return removed, unlinkErr
}

// openWriter abre o handle de escrita de um segmento reaberto (a abertura do
// log é só leitura, então w nasce nil).
func (l *diskLog) openWriter(s *segment) error {
	if s.w != nil {
		return nil
	}
	w, err := os.OpenFile(s.path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	s.w = w
	return nil
}

// createSegment cria o arquivo de um segmento novo com o cabeçalho gravado.
// Falha de criação ou de escrita do cabeçalho remove o arquivo (a retentativa
// o cria de novo com O_EXCL).
func (l *diskLog) createSegment(firstSeq, startPos uint64) (*segment, error) {
	l.mu.Lock()
	epoch := l.epoch
	l.mu.Unlock()
	path := filepath.Join(l.dir, segmentFileName(firstSeq))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	hdr := appendSegmentHeader(make([]byte, 0, segmentHeaderSize), segmentHeader{Epoch: epoch, FirstSequence: firstSeq, StartPos: startPos})
	if _, err := writeAtFn(f, hdr, 0); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	s := &segment{path: path, firstSeq: firstSeq, startPos: startPos, epoch: epoch, f: f, w: f, lastTo: firstSeq - 1, endPos: startPos}
	s.size.Store(segmentHeaderSize)
	return s, nil
}

// failDurability torna o log FALHO depois de um fdatasync que falhou e
// devolve o erro pegajoso, envolvido em errLogFailed (o erro original segue
// alcançável por errors.Is).
func (l *diskLog) failDurability(err error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = fmt.Errorf("%w: %w", errLogFailed, err)
	return l.err
}

func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// locate encontra onde começar a ler para um cursor que já consumiu tudo até
// `after` (inclusive). Devolve o segmento arrendado (refs+1; devolva com
// release), o offset de leitura dentro dele e ok.
//
// É só busca em memória, sem syscall. Por causa do índice esparso o offset é
// CONSERVADOR: ler a partir dele devolve, em ordem, todos os quadros com
// To > after, possivelmente precedidos de quadros com To <= after (no máximo
// ~1 MiB deles), que o leitor descarta. Quando o cursor está no fim do log o
// offset é exato (o fim do segmento) e a leitura devolve EOF de imediato.
//
// ok=false quando nenhum segmento serve: o cursor está abaixo do piso, ou o
// log está vazio, ou o log foi fechado. Quem chama distingue os dois primeiros
// comparando com floor().
//
// O handle cobre UM segmento. io.EOF num handle quer dizer "fim deste
// segmento neste instante", NÃO "fim do log": o log pode ter rotacionado (o
// segmento seguinte existe) ou crescido depois do locate. Para continuar, o
// leitor devolve o handle e chama locate de novo com o último `To` que
// consumiu; nunca reutiliza um handle antigo esperando que ele enxergue o
// segmento novo.
func (l *diskLog) locate(after uint64) (*segmentHandle, int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || len(l.segs) == 0 || after < l.floorLocked() {
		return nil, 0, false
	}
	// Último segmento com firstSeq-1 <= after. Existe: after >= piso >= 1º firstSeq-1.
	i := sort.Search(len(l.segs), func(i int) bool { return l.segs[i].firstSeq-1 > after }) - 1
	s := l.segs[i]
	off := int64(segmentHeaderSize)
	if after >= s.lastTo && s.frames > 0 {
		off = s.size.Load()
	} else if j := sort.Search(len(s.index), func(j int) bool { return s.index[j].covered > after }) - 1; j >= 0 {
		off = s.index[j].off
	}
	s.refs++
	return &segmentHandle{l: l, s: s}, off, true
}

// readAt lê do segmento sem lock. Só enxerga bytes já PUBLICADOS: um quadro
// em gravação é invisível até estar inteiro. Segue a semântica de io.ReaderAt:
// devolve io.EOF quando alcança o fim publicado.
func (h *segmentHandle) readAt(p []byte, off int64) (int, error) {
	if h.released.Load() {
		return 0, errReleased
	}
	if off < 0 {
		return 0, errors.New("replication: negative segment offset")
	}
	size := h.s.size.Load()
	if off >= size {
		return 0, io.EOF
	}
	if int64(len(p)) > size-off {
		n, err := h.s.f.ReadAt(p[:size-off], off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return h.s.f.ReadAt(p, off)
}

// size é o fim publicado do segmento neste instante.
func (h *segmentHandle) size() int64 { return h.s.size.Load() }

// release devolve o arrendamento. O fd de um segmento já aposentado pela
// retenção só fecha quando o último leitor o devolve. Idempotente.
//
// Pode fazer syscall (close do fd): NÃO chame sob um lock que o gravador do
// feed ou o Publish também tomam (o do Stream). Chame depois de soltá-lo.
func (h *segmentHandle) release() {
	if !h.released.CompareAndSwap(false, true) {
		return
	}
	h.l.unref(h.s)
}

// unref devolve uma referência a s e fecha os fds se ele já foi aposentado e
// ninguém mais o lê.
func (l *diskLog) unref(s *segment) {
	l.mu.Lock()
	s.refs--
	closable := s.retired && s.refs == 0
	l.mu.Unlock()
	if closable {
		s.closeFiles()
	}
}

// sync torna duráveis os quadros gravados: fdatasync do segmento corrente (os
// anteriores foram sincronizados na rotação) e, DEPOIS, fsync do diretório
// (criações e remoções de segmento). Pode rodar durante um append.
//
// Falha de fdatasync é pegajosa (ver append): o log fica falho até wipe.
func (l *diskLog) sync() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errLogClosed
	}
	if l.err != nil {
		err := l.err
		l.mu.Unlock()
		return err
	}
	var cur *segment
	if n := len(l.segs); n > 0 {
		cur = l.segs[n-1]
		cur.refs++
	}
	gone := l.dirGone
	l.mu.Unlock()
	if cur != nil {
		err := fdatasyncFn(cur.f)
		l.unref(cur)
		if err != nil {
			return l.failDurability(err)
		}
	}
	if gone {
		return nil
	}
	return syncDirFn(l.dir)
}

// wipe apaga todos os segmentos (inclusive sobras sem cabeçalho) e zera o
// estado pegajoso de erro: é a cura do log falho e o passo de uma invalidação.
// O piso sobe para a última sequence apagada (nenhum cursor anterior pode ser
// servido), e a posição em bytes é mantida; quem recomeça uma geração religa
// ambos com bind. Um leitor que ainda segura um segmento continua lendo dele
// até o release: o arquivo some do diretório na hora, o fd não.
func (l *diskLog) wipe() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errLogClosed
	}
	var closable []*segment
	for _, s := range l.segs {
		s.retired = true
		l.floorSeq = max(l.floorSeq, s.lastTo)
		if s.refs == 0 {
			closable = append(closable, s)
		}
	}
	l.segs = nil
	l.total = 0
	l.orphans = nil
	l.err = nil
	gone := l.dirGone
	l.mu.Unlock()

	var firstErr error
	if !gone {
		firstErr = removeSegments(l.dir)
	}
	for _, s := range closable {
		s.closeFiles()
	}
	return firstErr
}

// validateTail confere a cauda do log contra o `state`: epoch é a da geração,
// wantTo a última sequence produzida e wantEndPos a posição em bytes do fim do
// último quadro. Devolve errTailMismatch se a epoch dos segmentos é outra, se
// o último quadro válido não é esse, ou se o último segmento termina numa
// cauda rasgada (bytes que não formam um quadro com CRC e continuidade
// certos). A leitura dos quadros do último segmento foi feita na abertura; aqui
// só se compara.
//
// Log vazio (diretório sem segmento) é válido: é o master ocioso
// (Sequence=0) e o feed logo depois de um wipe por invalidação (Floor ==
// Sequence). Nele não há quadro nenhum para conferir, então a cauda esperada é
// a base dada ao bind: passa só se epoch, wantEndPos e wantTo (== piso) forem
// os dessa base.
func (l *diskLog) validateTail(epoch [16]byte, wantTo, wantEndPos uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errLogClosed
	}
	if n := len(l.segs); n > 0 {
		tail := l.segs[n-1]
		switch {
		case l.segs[0].epoch != epoch:
			return fmt.Errorf("%w: log belongs to epoch %x, state says %x", errTailMismatch, l.segs[0].epoch, epoch)
		case tail.tornBytes > 0:
			return fmt.Errorf("%w: %d bytes of torn frame after offset %d of %s", errTailMismatch, tail.tornBytes, tail.size.Load(), filepath.Base(tail.path))
		case tail.lastTo != wantTo || l.endPos != wantEndPos:
			return fmt.Errorf("%w: log ends at sequence %d position %d, state says %d and %d", errTailMismatch, tail.lastTo, l.endPos, wantTo, wantEndPos)
		}
		return nil
	}
	if l.hasEp && l.epoch != epoch {
		return fmt.Errorf("%w: empty log is bound to epoch %x, state says %x", errTailMismatch, l.epoch, epoch)
	}
	if wantEndPos != l.endPos || wantTo != l.floorSeq {
		return fmt.Errorf("%w: empty log is at floor %d position %d, state says %d and %d", errTailMismatch, l.floorSeq, l.endPos, wantTo, wantEndPos)
	}
	return nil
}

// close fecha o log sem apagar nada. Os fds de segmentos arrendados fecham no
// último release. Depois dele toda operação devolve errLogClosed, e locate
// devolve !ok: um leitor que ainda precise do log não consegue arrendar mais
// nada. Por isso o Stream só fecha o log no fim da vida, depois de os leitores
// esgotarem o que tinham arrendado; um close no meio da operação faz o
// próximo locate de cada leitor parecer "abaixo do piso".
func (l *diskLog) close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	var closable []*segment
	for _, s := range l.segs {
		s.retired = true
		if s.refs == 0 {
			closable = append(closable, s)
		}
	}
	l.mu.Unlock()
	for _, s := range closable {
		s.closeFiles()
	}
	return nil
}
