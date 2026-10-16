package replication

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

var testEpoch = [16]byte{0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF}

// mkBatch monta um lote com UMA entrada por sequence de from a to, cada uma
// com payload de payload bytes preenchidos com o número da sequence. O quadro
// de n sequences tem 50 + n*(46+payload) bytes: com 1 sequence, 96+payload.
func mkBatch(from, to uint64, payload int) *Batch {
	b := &Batch{From: from, To: to, At: time.Unix(int64(to), 0), Reason: "r"}
	for s := from; s <= to; s++ {
		b.Entries = append(b.Entries, Entry{
			Sequence: s, Table: "t", Operation: Update, At: time.Unix(int64(s), 0),
			Record: types.Record{RowID: types.RowID(s), Key: []byte("k"), Payload: bytes.Repeat([]byte{byte(s)}, payload)},
		})
	}
	return b
}

// newTestLog cria um log em um diretório novo, ligado a testEpoch.
func newTestLog(t *testing.T, segmentBytes, maxBytes int64) (*diskLog, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "feed")
	l, err := createDiskLog(dir, segmentBytes, maxBytes)
	if err != nil {
		t.Fatalf("createDiskLog: %v", err)
	}
	t.Cleanup(func() { _ = l.close() })
	if err := l.bind(testEpoch, 0, 0); err != nil {
		t.Fatalf("bind: %v", err)
	}
	return l, dir
}

// appendSeqs grava o próximo quadro, de `seqs` sequences, e devolve o
// intervalo e o que a retenção aposentou.
func appendSeqs(t *testing.T, l *diskLog, seqs, payload int) (from, to uint64, removed []segmentInfo) {
	t.Helper()
	last, endPos := l.position()
	from, to = last+1, last+uint64(seqs)
	b := mkBatch(from, to, payload)
	size := uint64(frameSize(b))
	removed, err := l.append(mustAppendFrame(nil, b, endPos+size), from, to, endPos+size)
	if err != nil {
		t.Fatalf("append [%d,%d]: %v", from, to, err)
	}
	return from, to, removed
}

// readFrameAt lê o quadro que começa em off pelo handle, sem nunca pedir além
// do fim publicado. Devolve io.EOF SÓ quando off é o fim publicado. Leitura
// curta no meio de um quadro é erro, não EOF: tratá-la como fim mascararia um
// quadro publicado antes de estar inteiro.
func readFrameAt(h *segmentHandle, off int64) (frame, int64, error) {
	if off >= h.size() {
		return frame{}, off, io.EOF
	}
	var fh [frameHeaderSize]byte
	if n, err := h.readAt(fh[:], off); err != nil || n != len(fh) {
		return frame{}, off, errors.New("short read of a frame header at " + strconv.FormatInt(off, 10) + ": " + strconv.Itoa(n) + " bytes, err " + errString(err))
	}
	bodyLen := int64(fh[4]) | int64(fh[5])<<8 | int64(fh[6])<<16 | int64(fh[7])<<24
	buf := make([]byte, frameHeaderSize+bodyLen)
	if n, err := h.readAt(buf, off); err != nil || n != len(buf) {
		return frame{}, off, errors.New("short read of a frame body at " + strconv.FormatInt(off, 10) + ": " + strconv.Itoa(n) + " of " + strconv.Itoa(len(buf)) + " bytes, err " + errString(err))
	}
	f, n, err := decodeFrame(buf)
	if err != nil {
		return frame{}, off, err
	}
	return f, off + int64(n), nil
}

func errString(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

func readFrames(t *testing.T, h *segmentHandle, off int64) []frame {
	t.Helper()
	var out []frame
	for {
		f, next, err := readFrameAt(h, off)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("lendo quadro em %d: %v", off, err)
		}
		out = append(out, f)
		off = next
	}
}

func segmentFiles(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	out := map[string]int64{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = info.Size()
	}
	return out
}

// reopen fecha o log e abre o mesmo diretório de novo, como num reinício.
func reopen(t *testing.T, l *diskLog, dir string) *diskLog {
	t.Helper()
	_ = l.close()
	n, err := openDiskLog(dir, l.segmentBytes, l.maxBytes)
	if err != nil {
		t.Fatalf("openDiskLog: %v", err)
	}
	t.Cleanup(func() { _ = n.close() })
	return n
}

func TestDiskLogSegmentFileNameIsTheFirstSequenceWithTwentyDigits(t *testing.T) {
	for first, want := range map[uint64]string{
		1:                    "00000000000000000001.seg",
		5:                    "00000000000000000005.seg",
		1234567890123:        "00000001234567890123.seg",
		18446744073709551615: "18446744073709551615.seg",
	} {
		if got := segmentFileName(first); got != want {
			t.Fatalf("segmentFileName(%d) = %q, esperado %q", first, got, want)
		}
		if n, ok := parseSegmentName(want); !ok || n != first {
			t.Fatalf("parseSegmentName(%q) = %d, %v", want, n, ok)
		}
	}
	for _, bad := range []string{"1.seg", "0000000000000000001.seg", "000000000000000000001.seg", "0000000000000000000a.seg", "00000000000000000001.sec", "00000000000000000001.seg.tmp", "state"} {
		if _, ok := parseSegmentName(bad); ok {
			t.Fatalf("parseSegmentName(%q) aceitou nome inválido", bad)
		}
	}
}

// frame de 1000 bytes (96 + 904), cabeçalho de segmento de 64: quatro quadros
// ocupam 4064 e cabem num teto de 4096; o quinto passaria de 5000.
const frame1000 = 904

func TestDiskLogRotatesBeforeExceedingSegmentBytes(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 4; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	if l.segments() != 1 {
		t.Fatalf("segmentos = %d depois de 4 quadros que cabem, esperado 1", l.segments())
	}
	appendSeqs(t, l, 1, frame1000) // o quinto passaria do teto: rotaciona ANTES
	files := segmentFiles(t, dir)
	want := map[string]int64{"00000000000000000001.seg": 4064, "00000000000000000005.seg": 64 + 1000}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("arquivos = %v, esperado %v", files, want)
	}
	for name, size := range files {
		if size > 4096 {
			t.Fatalf("segmento %s tem %d bytes, acima do teto de 4096", name, size)
		}
	}

	// Um quadro maior que o teto sai sozinho num segmento...
	appendSeqs(t, l, 1, 6000-96)
	// ...e o seguinte rotaciona de novo, em vez de se juntar a ele.
	appendSeqs(t, l, 1, frame1000)
	files = segmentFiles(t, dir)
	want = map[string]int64{
		"00000000000000000001.seg": 4064,
		"00000000000000000005.seg": 64 + 1000,
		"00000000000000000006.seg": 64 + 6000,
		"00000000000000000007.seg": 64 + 1000,
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("arquivos = %v, esperado %v", files, want)
	}
	if l.segments() != 4 || l.bytes() != 4064+1064+6064+1064 {
		t.Fatalf("segments() = %d, bytes() = %d", l.segments(), l.bytes())
	}
}

func TestDiskLogRetentionDeletesOldestAndRaisesFloor(t *testing.T) {
	l, dir := newTestLog(t, 4096, 10000)
	for i := 0; i < 9; i++ { // 4 + 4 + 1 quadros: 4064 + 4064 + 1064 = 9192
		if _, _, removed := appendSeqs(t, l, 1, frame1000); len(removed) != 0 {
			t.Fatalf("retenção removeu cedo demais: %+v", removed)
		}
	}
	if l.bytes() != 9192 || l.floor() != 0 {
		t.Fatalf("bytes() = %d, floor() = %d", l.bytes(), l.floor())
	}
	_, _, removed := appendSeqs(t, l, 1, frame1000) // 10192 > 10000: o 1º segmento sai
	wantRemoved := []segmentInfo{{FirstSequence: 1, LastTo: 4, Bytes: 4064, Path: filepath.Join(dir, "00000000000000000001.seg")}}
	if !reflect.DeepEqual(removed, wantRemoved) {
		t.Fatalf("removed = %+v, esperado %+v", removed, wantRemoved)
	}
	if l.floor() != 4 {
		t.Fatalf("floor() = %d, esperado 4 (FirstSequence do próximo - 1)", l.floor())
	}
	if l.bytes() != 4064+2*1000+64 || l.segments() != 2 {
		t.Fatalf("bytes() = %d, segments() = %d", l.bytes(), l.segments())
	}
	files := segmentFiles(t, dir)
	if _, still := files["00000000000000000001.seg"]; still || len(files) != 2 {
		t.Fatalf("o arquivo do segmento aposentado deveria ter sumido: %v", files)
	}
	if _, _, ok := l.locate(3); ok {
		t.Fatalf("locate(3) abaixo do piso devolveu ok")
	}
	h, _, ok := l.locate(4)
	if !ok {
		t.Fatalf("locate(4) no piso deveria servir")
	}
	h.release()
}

func TestDiskLogRetentionKeepsTheCurrentSegmentEvenAboveTheCeiling(t *testing.T) {
	// Teto de 500 bytes: um único quadro de 1000 já o ultrapassa. O segmento
	// corrente nunca é apagado, senão o feed perderia o que acabou de gravar.
	l, _ := newTestLog(t, 4096, 500)
	_, _, removed := appendSeqs(t, l, 1, frame1000)
	if len(removed) != 0 || l.segments() != 1 || l.bytes() != 1064 || l.floor() != 0 {
		t.Fatalf("removed = %+v, segments() = %d, bytes() = %d, floor() = %d", removed, l.segments(), l.bytes(), l.floor())
	}
	h, off, ok := l.locate(0)
	if !ok {
		t.Fatalf("o quadro gravado deveria ser servível")
	}
	defer h.release()
	if fr := readFrames(t, h, off); len(fr) != 1 || fr[0].From != 1 {
		t.Fatalf("quadros = %+v", fr)
	}

	// Com dois segmentos, o antigo sai e fica exatamente um (o corrente).
	for i := 0; i < 4; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	if l.segments() != 1 {
		t.Fatalf("segments() = %d, esperado 1 (só o corrente sobrevive a um teto menor que um segmento)", l.segments())
	}
}

func TestDiskLogLocateFindsFrameAfterCursor(t *testing.T) {
	// 10 sequences por quadro, 1510 bytes cada: dois quadros por segmento.
	l, _ := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 5; i++ {
		appendSeqs(t, l, 10, 100)
	}
	// seg1: [1,10] [11,20]   seg2: [21,30] [31,40]   seg3: [41,50]
	if l.segments() != 3 {
		t.Fatalf("segments() = %d, esperado 3", l.segments())
	}

	cases := []struct {
		name         string
		after        uint64
		wantFirstSeq uint64 // do segmento devolvido
		wantOff      int64
		wantFrom     uint64 // From do primeiro quadro com To > after
		wantFrames   int    // quantos quadros restam a partir do offset
	}{
		{"cursor at the start", 0, 1, 64, 1, 2},
		{"cursor in the middle of a frame", 15, 1, 64, 11, 2},
		{"cursor at the last sequence of a frame", 10, 1, 64, 11, 2},
		{"cursor at the segment boundary", 20, 21, 64, 21, 2},
		{"cursor just before the segment boundary", 19, 1, 64, 11, 2},
		{"cursor in the middle of the last frame", 45, 41, 64, 41, 1},
		{"cursor at the log tail", 50, 41, 64 + 1510, 0, 0},
		{"cursor beyond the tail", 99, 41, 64 + 1510, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, off, ok := l.locate(c.after)
			if !ok {
				t.Fatalf("locate(%d) = !ok", c.after)
			}
			defer h.release()
			if h.s.firstSeq != c.wantFirstSeq || off != c.wantOff {
				t.Fatalf("locate(%d) = segmento %d offset %d, esperado %d e %d", c.after, h.s.firstSeq, off, c.wantFirstSeq, c.wantOff)
			}
			frames := readFrames(t, h, off)
			if len(frames) != c.wantFrames {
				t.Fatalf("quadros a partir do offset = %d, esperado %d", len(frames), c.wantFrames)
			}
			if c.wantFrames > 0 {
				// O primeiro quadro com To > after é o que contém after+1.
				for _, f := range frames {
					if f.To > c.after {
						if f.From != c.wantFrom {
							t.Fatalf("primeiro quadro após o cursor = [%d,%d], esperado From %d", f.From, f.To, c.wantFrom)
						}
						break
					}
				}
			}
		})
	}
}

func TestDiskLogLocateBelowTheFloorIsNotOk(t *testing.T) {
	l, _ := newTestLog(t, 4096, 1<<30)
	if _, _, ok := l.locate(0); ok {
		t.Fatalf("log vazio: locate deveria devolver !ok")
	}
	for i := 0; i < 3; i++ {
		appendSeqs(t, l, 10, 100)
	}
	// Um piso do `state` acima do primeiro segmento vale mais que ele.
	if err := l.bind(testEpoch, 15, 0); err != nil {
		t.Fatal(err)
	}
	if l.floor() != 15 {
		t.Fatalf("floor() = %d, esperado 15", l.floor())
	}
	if _, _, ok := l.locate(14); ok {
		t.Fatalf("locate(14) abaixo do piso 15 devolveu ok")
	}
	h, _, ok := l.locate(15)
	if !ok {
		t.Fatalf("locate(15) no piso deveria servir")
	}
	h.release()
}

func TestDiskLogLocateUsesTheSparseIndexExactly(t *testing.T) {
	// Quadros de exatamente 200.000 bytes (96 + 199.904), uma sequence cada.
	// Com o espaçamento de 1 MiB (1.048.576), as entradas caem nos quadros 0,
	// 6, 12 e 18 (offsets 64, 1.200.064, 2.400.064 e 3.600.064): o quadro 5
	// está a 1.000.000 bytes da entrada anterior (< 1.048.576) e o 6 a
	// 1.200.000. A entrada do quadro k tem covered = k.
	l, _ := newTestLog(t, 64<<20, 1<<30)
	for i := 0; i < 20; i++ {
		appendSeqs(t, l, 1, 199904)
	}
	cases := []struct {
		after   uint64
		wantOff int64
	}{
		{0, 64},
		{5, 64},              // a entrada do quadro 6 tem covered = 6 > 5
		{6, 1200064},         // covered = 6 <= 6: cai exatamente no quadro 6
		{11, 1200064},        // a entrada seguinte só cobre 12
		{12, 2400064},        // covered = 12 <= 12
		{17, 2400064},        // a entrada seguinte só cobre 18
		{18, 3600064},        // covered = 18 <= 18
		{19, 3600064},        // última entrada; o quadro 19 (sequence 20) vem depois
		{20, 64 + 20*200000}, // cursor no fim do log: offset exato do fim
	}
	for _, c := range cases {
		h, off, ok := l.locate(c.after)
		if !ok {
			t.Fatalf("locate(%d) = !ok", c.after)
		}
		if off != c.wantOff {
			t.Fatalf("locate(%d) offset = %d, esperado %d", c.after, off, c.wantOff)
		}
		// O que resta a ler contém o quadro de After+1 como primeiro com To > after.
		for _, f := range readFrames(t, h, off) {
			if f.To > c.after {
				if f.From != c.after+1 {
					t.Fatalf("locate(%d): primeiro quadro útil = %d, esperado %d", c.after, f.From, c.after+1)
				}
				break
			}
		}
		h.release()
	}
}

func TestDiskLogSparseIndexStaysBounded(t *testing.T) {
	// 200 MiB em quadros de 4 KiB, num teto de segmento de 64 MiB: três
	// segmentos cheios e um parcial. Sem o espaçamento (uma entrada por
	// quadro) seriam 51.200 entradas em heap.
	l, dir := newTestLog(t, 64<<20, 1<<40)
	frames := (200 << 20) / 4096
	for i := 0; i < frames; i++ {
		appendSeqs(t, l, 1, 4000)
	}
	count := func(l *diskLog) (n int) {
		for _, s := range l.segs {
			n += len(s.index)
		}
		return n
	}
	if got := count(l); got > 220 || got < 190 {
		t.Fatalf("índice com %d entradas para 200 MiB, esperado entre 190 e 220", got)
	}
	if l.segments() != 4 {
		t.Fatalf("segments() = %d, esperado 4", l.segments())
	}

	// A abertura reconstrói o MESMO índice a partir dos arquivos.
	before := make([][]indexEntry, len(l.segs))
	for i, s := range l.segs {
		before[i] = s.index
	}
	n := reopen(t, l, dir)
	if n.segments() != 4 {
		t.Fatalf("reaberto com %d segmentos", n.segments())
	}
	for i, s := range n.segs {
		if !reflect.DeepEqual(s.index, before[i]) {
			t.Fatalf("índice do segmento %d diverge depois da reabertura: %d entradas, antes %d", i, len(s.index), len(before[i]))
		}
	}

	// E o offset devolvido para um cursor no meio nunca obriga a varrer mais que
	// o espaçamento mais um quadro.
	lastTo, _ := n.position()
	for _, after := range []uint64{1000, 20000, 33333, lastTo - 7} {
		h, off, ok := n.locate(after)
		if !ok {
			t.Fatalf("locate(%d) = !ok", after)
		}
		target := int64(segmentHeaderSize) + int64(after-h.s.firstSeq+1)*4096 // início do quadro de after+1
		if off > target || target-off > indexStride {
			t.Fatalf("locate(%d): offset %d, alvo %d; varredura de %d bytes passa de 1 MiB", after, off, target, target-off)
		}
		h.release()
	}
}

func TestDiskLogLocateDoesNoFileSystemAccess(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 3; i++ {
		appendSeqs(t, l, 10, 100)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	// Com o diretório inteiro removido, qualquer open/stat falharia: locate
	// responde da memória, e o fd arrendado continua lendo o arquivo
	// desvinculado.
	h, off, ok := l.locate(5)
	if !ok {
		t.Fatalf("locate depende do sistema de arquivos")
	}
	defer h.release()
	if fr := readFrames(t, h, off); len(fr) != 2 || fr[0].From != 1 {
		t.Fatalf("quadros = %+v", fr)
	}
}

func TestDiskReaderSurvivesRetentionUnlink(t *testing.T) {
	l, dir := newTestLog(t, 4096, 6000)
	for i := 0; i < 4; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	h1, off1, ok1 := l.locate(0)
	h2, off2, ok2 := l.locate(0)
	if !ok1 || !ok2 || h1.s != h2.s {
		t.Fatalf("locate devolveu segmentos diferentes ou !ok")
	}
	seg1 := h1.s

	appendSeqs(t, l, 1, frame1000) // rotaciona: 4064 + 1064 = 5128, ainda cabe
	_, _, removed := appendSeqs(t, l, 1, frame1000)
	if len(removed) != 1 || removed[0].FirstSequence != 1 {
		t.Fatalf("a retenção deveria ter aposentado o 1º segmento: %+v", removed)
	}
	if _, still := segmentFiles(t, dir)["00000000000000000001.seg"]; still {
		t.Fatalf("o arquivo aposentado continua no diretório")
	}

	// O arquivo sumiu do diretório, mas o leitor ainda lê os 4 quadros.
	fr := readFrames(t, h1, off1)
	if len(fr) != 4 || fr[0].From != 1 || fr[3].To != 4 {
		t.Fatalf("o leitor perdeu o segmento aposentado: %+v", fr)
	}
	if _, err := seg1.f.Stat(); err != nil {
		t.Fatalf("o fd foi fechado com dois leitores ativos: %v", err)
	}

	// Devolver UM dos dois não fecha; o release duplicado não conta duas vezes.
	h1.release()
	h1.release()
	if _, err := seg1.f.Stat(); err != nil {
		t.Fatalf("o fd foi fechado com um leitor ainda ativo (release duplicado contou em dobro?): %v", err)
	}
	if fr := readFrames(t, h2, off2); len(fr) != 4 {
		t.Fatalf("o segundo leitor perdeu o segmento: %d quadros", len(fr))
	}
	h2.release()
	if _, err := seg1.f.Stat(); err == nil {
		t.Fatalf("o fd deveria fechar no release do último leitor")
	}
	if _, err := h2.readAt(make([]byte, 4), off2); !errors.Is(err, errReleased) {
		t.Fatalf("readAt depois do release: err = %v, esperado errReleased", err)
	}
	// O próximo cursor abaixo do piso não é servido: é o ErrResyncRequired do Stream.
	if _, _, ok := l.locate(3); ok {
		t.Fatalf("locate(3) abaixo do piso depois da retenção devolveu ok")
	}
}

// TestDiskLogClosesRetiredSegmentImmediatelyWithoutReaders: sem leitor, o fd
// do segmento aposentado fecha na própria retenção (não vaza até o fim do
// processo).
func TestDiskLogClosesRetiredSegmentImmediatelyWithoutReaders(t *testing.T) {
	l, _ := newTestLog(t, 4096, 6000)
	for i := 0; i < 4; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	seg1 := l.segs[0]
	appendSeqs(t, l, 1, frame1000)
	appendSeqs(t, l, 1, frame1000)
	if _, err := seg1.f.Stat(); err == nil {
		t.Fatalf("o fd do segmento aposentado sem leitores deveria estar fechado")
	}
}

// TestDiskLogConcurrentReadersDuringAppendRotationAndRetention: leitores
// concorrentes com append, rotação e retenção. O progresso NÃO depende de
// escalonamento nem de tempo:
//   - os leitores rápidos andam em degraus com o gravador: o gravador só
//     avança para o degrau seguinte quando todos eles alcançaram a `To` do
//     degrau (cedendo a CPU com Gosched, o que funciona até com GOMAXPROCS=1);
//   - um leitor atrasado fica parado até a retenção ter apagado o começo do
//     log, e então precisa ressincronizar pelo piso (locate abaixo do piso
//     devolve !ok) e alcançar a `To` final;
//   - todos rodam até a `To` final publicada, e não até um sinal de fim.
func TestDiskLogConcurrentReadersDuringAppendRotationAndRetention(t *testing.T) {
	const (
		total = 600
		step  = 5
		fast  = 3
	)
	l, _ := newTestLog(t, 4096, 9000)
	var wg sync.WaitGroup
	errc := make(chan error, fast+1)
	var framesRead, resyncs atomic.Int64
	progress := make([]atomic.Uint64, fast+1)
	lagGate := make(chan struct{})
	// stop e a abertura do portão garantem que NENHUMA goroutine fica presa se
	// o gravador sair cedo (erro de leitor ou t.Fatalf): sem isso um defeito
	// viraria um teste pendurado até o timeout de 10 min.
	var stop atomic.Bool
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(lagGate) }) }
	t.Cleanup(func() { stop.Store(true); openGate() })

	reader := func(id int, gate <-chan struct{}) {
		defer wg.Done()
		if gate != nil {
			<-gate
		}
		after := uint64(0)
		for after < total && !stop.Load() {
			h, off, ok := l.locate(after)
			if !ok {
				// Abaixo do piso (a retenção passou): ressincroniza.
				after = l.floor()
				resyncs.Add(1)
				progress[id].Store(after)
				runtime.Gosched()
				continue
			}
			for {
				f, next, err := readFrameAt(h, off)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					errc <- err
					h.release()
					return
				}
				framesRead.Add(1)
				if f.To > after {
					if f.From != after+1 {
						errc <- errors.New("lacuna: quadro [" + strconv.FormatUint(f.From, 10) + "," + strconv.FormatUint(f.To, 10) + "] depois do cursor " + strconv.FormatUint(after, 10))
						h.release()
						return
					}
					after = f.To
					progress[id].Store(after)
				}
				off = next
			}
			h.release()
			runtime.Gosched()
		}
	}
	for r := 0; r < fast; r++ {
		wg.Add(1)
		go reader(r, nil)
	}
	wg.Add(1)
	go reader(fast, lagGate)

	deadline := time.Now().Add(60 * time.Second) // só para um defeito não travar o teste
	removedTotal := 0
	for done := uint64(0); done < total; done += step {
		for i := 0; i < step; i++ { // um quadro por sequence: step quadros por degrau
			_, _, removed := appendSeqs(t, l, 1, 300)
			removedTotal += len(removed)
		}
		if done+step == total/2 {
			openGate() // a retenção já apagou o começo: o atrasado larga agora
		}
		// Espera os leitores rápidos alcançarem o degrau (ou um deles falhar).
		for r := 0; r < fast && len(errc) == 0; r++ {
			for progress[r].Load() < done+step && len(errc) == 0 {
				if time.Now().After(deadline) {
					t.Fatalf("leitor %d parado em %d; esperado %d", r, progress[r].Load(), done+step)
				}
				runtime.Gosched()
			}
		}
		if len(errc) > 0 {
			break
		}
	}
	if len(errc) > 0 {
		stop.Store(true)
	}
	openGate()
	wg.Wait()
	select {
	case err := <-errc:
		t.Fatalf("leitor concorrente: %v", err)
	default:
	}
	for r := range progress {
		if got := progress[r].Load(); got != total {
			t.Fatalf("leitor %d terminou em %d, esperado a To final %d", r, got, total)
		}
	}
	// A retenção rodou e o leitor atrasado teve de ressincronizar pelo piso.
	if removedTotal == 0 || l.floor() == 0 {
		t.Fatalf("a retenção deveria ter rodado: %d segmentos aposentados, piso %d", removedTotal, l.floor())
	}
	if resyncs.Load() == 0 {
		t.Fatalf("nenhum leitor ressincronizou pelo piso; o atrasado deveria ter cruzado a retenção")
	}
	if framesRead.Load() < total {
		t.Fatalf("os leitores leram só %d quadros", framesRead.Load())
	}
}

func TestDiskLogReadAtNeverExposesBytesBeyondThePublishedEnd(t *testing.T) {
	l, _ := newTestLog(t, 4096, 1<<30)
	appendSeqs(t, l, 1, frame1000)
	h, _, _ := l.locate(0)
	defer h.release()
	// Cauda de lixo além do fim publicado (o que um quadro em gravação, ou
	// uma cauda rasgada, deixa no arquivo): o leitor não a enxerga.
	if _, err := h.s.w.WriteAt(bytes.Repeat([]byte{0xEE}, 500), h.size()); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, err := h.readAt(buf, 0)
	if !errors.Is(err, io.EOF) || int64(n) != h.size() {
		t.Fatalf("readAt = %d, %v; esperado %d bytes e io.EOF", n, err, h.size())
	}
	if n, err := h.readAt(buf, h.size()); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("readAt no fim publicado = %d, %v", n, err)
	}
	if _, err := h.readAt(buf, -1); err == nil {
		t.Fatalf("offset negativo deveria falhar")
	}
}

func TestDiskLogValidateTailAcceptsIntactLogAndRejectsMismatches(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 6; i++ {
		appendSeqs(t, l, 3, 100)
	}
	lastTo, endPos := l.position()
	n := reopen(t, l, dir)
	if err := n.validateTail(testEpoch, lastTo, endPos); err != nil {
		t.Fatalf("log íntegro: %v", err)
	}
	if err := n.validateTail(testEpoch, lastTo-1, endPos); !errors.Is(err, errTailMismatch) {
		t.Fatalf("To errado: err = %v", err)
	}
	if err := n.validateTail(testEpoch, lastTo, endPos+1); !errors.Is(err, errTailMismatch) {
		t.Fatalf("EndPos errado: err = %v", err)
	}
	if err := n.validateTail(testEpoch, lastTo+3, endPos); !errors.Is(err, errTailMismatch) {
		t.Fatalf("To à frente: err = %v", err)
	}
}

func TestDiskLogValidateTailRejectsTornFrame(t *testing.T) {
	type damage struct {
		name   string
		mutate func(t *testing.T, path string)
	}
	truncate := func(n int64) func(t *testing.T, path string) {
		return func(t *testing.T, path string) {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(path, info.Size()-n); err != nil {
				t.Fatal(err)
			}
		}
	}
	appendRaw := func(raw []byte) func(t *testing.T, path string) {
		return func(t *testing.T, path string) {
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			if _, err := f.Write(raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	flipLastByte := func(t *testing.T, path string) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)-1] ^= 0xFF
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cases := []damage{
		{"last frame short by one byte", truncate(1)},
		{"last frame short by half", truncate(500)},
		{"only the frame header of the last frame survives", truncate(1000 - frameHeaderSize)},
		{"garbage after the last frame", appendRaw(bytes.Repeat([]byte{0xEE}, 300))},
		{"partial header after the last frame", appendRaw([]byte("NDFR\x01"))},
		{"flipped byte in the last frame", flipLastByte},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l, dir := newTestLog(t, 4096, 1<<30)
			for i := 0; i < 6; i++ {
				appendSeqs(t, l, 1, frame1000)
			}
			lastTo, endPos := l.position()
			_ = l.close()
			files := segmentFiles(t, dir)
			var tailName string
			for name := range files {
				if name > tailName {
					tailName = name
				}
			}
			c.mutate(t, filepath.Join(dir, tailName))

			n := reopen(t, l, dir)
			err := n.validateTail(testEpoch, lastTo, endPos)
			if !errors.Is(err, errTailMismatch) {
				t.Fatalf("validateTail = %v, esperado errTailMismatch", err)
			}
		})
	}
}

func TestDiskLogValidateTailRejectsTornTailEvenWhenTheStateMatchesTheValidPrefix(t *testing.T) {
	// Um state que descreve o prefixo VÁLIDO (To e EndPos do penúltimo quadro)
	// não pode autorizar a retomada: a cauda rasgada é prova de que o processo
	// caiu gravando, e o feed não retoma um log que só parece completo.
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 3; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	prevTo, prevEnd := uint64(2), uint64(2000)
	_ = l.close()
	path := filepath.Join(dir, "00000000000000000001.seg")
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-10); err != nil {
		t.Fatal(err)
	}
	n := reopen(t, l, dir)
	if lastTo, endPos := n.position(); lastTo != prevTo || endPos != prevEnd {
		t.Fatalf("o prefixo válido deveria terminar em %d/%d, termina em %d/%d", prevTo, prevEnd, lastTo, endPos)
	}
	if err := n.validateTail(testEpoch, prevTo, prevEnd); !errors.Is(err, errTailMismatch) {
		t.Fatalf("validateTail = %v, esperado errTailMismatch", err)
	}
}

func TestDiskLogAppendAfterTornTailDiscardsTheGarbage(t *testing.T) {
	l, dir := newTestLog(t, 1<<20, 1<<30)
	for i := 0; i < 2; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	_ = l.close()
	path := filepath.Join(dir, "00000000000000000001.seg")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{0xEE}, 1500)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	n := reopen(t, l, dir)
	if err := n.bind(testEpoch, 0, 0); err != nil {
		t.Fatal(err)
	}
	appendSeqs(t, n, 1, 100) // quadro menor que o lixo: o resto do lixo ficaria depois dele
	lastTo, endPos := n.position()
	if lastTo != 3 || endPos != 2000+196 {
		t.Fatalf("posição = %d/%d", lastTo, endPos)
	}
	again := reopen(t, n, dir)
	if err := again.validateTail(testEpoch, lastTo, endPos); err != nil {
		t.Fatalf("o lixo da cauda rasgada sobrou depois do append: %v", err)
	}
	info, _ := os.Stat(path)
	if info.Size() != 64+2000+196 {
		t.Fatalf("arquivo com %d bytes, esperado %d", info.Size(), 64+2000+196)
	}
}

func TestDiskLogValidateTailAcceptsEmptyLog(t *testing.T) {
	t.Run("idle master", func(t *testing.T) {
		// Sequence=0: nada foi produzido, não há segmento.
		l, _ := newTestLog(t, 4096, 1<<30)
		if err := l.validateTail(testEpoch, 0, 0); err != nil {
			t.Fatalf("log vazio de master ocioso: %v", err)
		}
		for name, c := range map[string][2]uint64{"position ahead": {0, 5}, "sequence ahead": {3, 0}} {
			if err := l.validateTail(testEpoch, c[0], c[1]); !errors.Is(err, errTailMismatch) {
				t.Fatalf("%s: err = %v, esperado errTailMismatch", name, err)
			}
		}
	})
	t.Run("after wipe by invalidation", func(t *testing.T) {
		// Floor == Sequence == 30 e EndPos preservado: o diretório não tem
		// quadro nenhum, e nem por isso o feed está inconsistente.
		l, dir := newTestLog(t, 4096, 1<<30)
		for i := 0; i < 3; i++ {
			appendSeqs(t, l, 10, 100)
		}
		lastTo, endPos := l.position()
		if err := l.wipe(); err != nil {
			t.Fatal(err)
		}
		if got := segmentFiles(t, dir); len(got) != 0 {
			t.Fatalf("wipe deixou %v", got)
		}
		if err := l.validateTail(testEpoch, lastTo, endPos); err != nil {
			t.Fatalf("log vazio depois do wipe: %v", err)
		}
		if err := l.validateTail(testEpoch, lastTo, endPos+1); !errors.Is(err, errTailMismatch) {
			t.Fatalf("EndPos errado num log vazio: err = %v", err)
		}
		if err := l.validateTail(testEpoch, lastTo-1, endPos); !errors.Is(err, errTailMismatch) {
			t.Fatalf("Sequence errada num log vazio: err = %v", err)
		}
	})
	t.Run("reopened empty directory", func(t *testing.T) {
		l, dir := newTestLog(t, 4096, 1<<30)
		n := reopen(t, l, dir)
		if err := n.bind(testEpoch, 30, 77000); err != nil {
			t.Fatal(err)
		}
		if err := n.validateTail(testEpoch, 30, 77000); err != nil {
			t.Fatalf("diretório vazio reaberto: %v", err)
		}
		// O log vazio só concorda com a base que recebeu: qualquer outro
		// valor do state (epoch, sequence ou posição) é divergência.
		other := testEpoch
		other[0] ^= 0xFF
		for name, c := range map[string]struct {
			epoch    [16]byte
			to, ends uint64
		}{
			"other epoch":    {other, 30, 77000},
			"sequence ahead": {testEpoch, 31, 77000},
			"sequence back":  {testEpoch, 29, 77000},
			"position ahead": {testEpoch, 30, 77001},
			"position back":  {testEpoch, 30, 76999},
		} {
			if err := n.validateTail(c.epoch, c.to, c.ends); !errors.Is(err, errTailMismatch) {
				t.Fatalf("%s: err = %v, esperado errTailMismatch", name, err)
			}
		}
	})
}

func TestOpenDiskLogIsReadOnlyAndDoesNotCreateTheDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "replication", "feed")
	l, err := openDiskLog(missing, 4096, 1<<30)
	if err != nil {
		t.Fatalf("openDiskLog de diretório ausente: %v", err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("openDiskLog criou o diretório: %v", err)
	}
	if l.segments() != 0 || l.bytes() != 0 || l.floor() != 0 {
		t.Fatalf("log ausente deveria ser vazio: %d segmentos, %d bytes, piso %d", l.segments(), l.bytes(), l.floor())
	}
	if err := l.bind(testEpoch, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := l.sync(); err != nil {
		t.Fatalf("sync de log sem diretório: %v", err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sync criou o diretório: %v", err)
	}
	// O primeiro append é quem o cria, e o log funciona normalmente.
	appendSeqs(t, l, 1, 100)
	if _, err := os.Stat(missing); err != nil {
		t.Fatalf("o primeiro append deveria criar o diretório: %v", err)
	}
	_ = l.close()

	// createDiskLog cria já na chamada.
	created := filepath.Join(t.TempDir(), "a", "feed")
	c, err := createDiskLog(created, 4096, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.close() }()
	if st, err := os.Stat(created); err != nil || !st.IsDir() {
		t.Fatalf("createDiskLog não criou o diretório: %v", err)
	}
}

func dirSnapshot(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = info.Size()
	}
	return out
}

func TestOpenDiskLogLeavesTornTailInPlaceUntilTheFirstAppend(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 5; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	_ = l.close()
	tail := filepath.Join(dir, "00000000000000000005.seg")
	f, err := os.OpenFile(tail, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("torn tail garbage"))
	_ = f.Close()

	before := dirSnapshot(t, dir)
	n, err := openDiskLog(dir, 4096, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.close() }()
	if after := dirSnapshot(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatalf("a abertura alterou o diretório:\nantes  %v\ndepois %v", before, after)
	}
	if err := n.bind(testEpoch, 0, 0); err != nil {
		t.Fatal(err)
	}
	appendSeqs(t, n, 1, frame1000)
	if got := dirSnapshot(t, dir)["00000000000000000005.seg"]; got != 64+2*1000 {
		t.Fatalf("depois do primeiro append o segmento tem %d bytes, esperado %d (cauda rasgada truncada)", got, 64+2*1000)
	}
}

func TestOpenDiskLogIgnoresHeaderlessLastFileAndDropsItOnFirstAppend(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 5; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	lastTo, endPos := l.position()
	_ = l.close()
	// Rotação que caiu depois de criar o arquivo e antes de gravar o cabeçalho.
	orphan := filepath.Join(dir, "00000000000000000006.seg")
	if err := os.WriteFile(orphan, []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}

	before := dirSnapshot(t, dir)
	n, err := openDiskLog(dir, 4096, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.close() }()
	if after := dirSnapshot(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatalf("a abertura alterou o diretório:\nantes  %v\ndepois %v", before, after)
	}
	if n.segments() != 2 {
		t.Fatalf("o arquivo sem cabeçalho não deveria contar como segmento: %d", n.segments())
	}
	// Nenhum dado se perdeu: o log termina onde terminava e a cauda confere.
	if err := n.validateTail(testEpoch, lastTo, endPos); err != nil {
		t.Fatalf("validateTail com sobra de rotação: %v", err)
	}
	if err := n.bind(testEpoch, 0, 0); err != nil {
		t.Fatal(err)
	}
	appendSeqs(t, n, 1, frame1000)
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a sobra sem cabeçalho deveria ter sido apagada no primeiro append: %v", err)
	}
}

// handmadeSegment grava um segmento à mão: cabeçalho e os quadros dados, na
// ordem, sem passar pelo diskLog. É como se fabrica um arquivo com quadros de
// CRC certo mas continuidade quebrada, que o append recusaria.
func handmadeSegment(t *testing.T, dir string, firstSeq uint64, frames ...[]byte) {
	t.Helper()
	handmadeSegmentAt(t, dir, firstSeq, 0, frames...)
}

func handmadeSegmentAt(t *testing.T, dir string, firstSeq, startPos uint64, frames ...[]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	b := appendSegmentHeader(nil, segmentHeader{Epoch: testEpoch, FirstSequence: firstSeq, StartPos: startPos})
	for _, f := range frames {
		b = append(b, f...)
	}
	if err := os.WriteFile(filepath.Join(dir, segmentFileName(firstSeq)), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDiskLogTreatsFramesThatBreakContinuityAsTornTail(t *testing.T) {
	one := func(from, to, endPos uint64) []byte { return mustAppendFrame(nil, mkBatch(from, to, 10), endPos) }
	size := uint64(frameSize(mkBatch(1, 1, 10)))
	size2 := uint64(frameSize(mkBatch(1, 2, 10)))
	cases := []struct {
		name       string
		frames     [][]byte
		wantFrames int // quadros coerentes do prefixo
	}{
		{"sequence hole between frames", [][]byte{one(1, 1, size), one(3, 3, 2*size)}, 1},
		{"sequence overlap between frames", [][]byte{one(1, 2, size2), one(2, 2, size2+size)}, 1},
		{"end position that skips bytes", [][]byte{one(1, 1, size), one(2, 2, 2*size+1)}, 1},
		{"end position that goes backwards", [][]byte{one(1, 1, size), one(2, 2, size)}, 1},
		{"first frame not at the header's first sequence", [][]byte{one(2, 2, size)}, 0},
		{"first end position not at the header's start position", [][]byte{one(1, 1, size+1)}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "feed")
			handmadeSegment(t, dir, 1, c.frames...)
			l, err := openDiskLog(dir, 1<<20, 1<<30)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.close() }()
			if got := l.segs[0].frames; got != c.wantFrames {
				t.Fatalf("quadros aceitos = %d, esperado %d", got, c.wantFrames)
			}
			// Só o prefixo coerente é aceito, e o resto é cauda rasgada: o
			// state que descreve esse prefixo NÃO autoriza a retomada.
			if err := l.validateTail(testEpoch, l.segs[0].lastTo, l.endPos); !errors.Is(err, errTailMismatch) {
				t.Fatalf("validateTail = %v, esperado errTailMismatch (cauda incoerente)", err)
			}
		})
	}
}

// TestOpenDiskLogRejectsSealedSegmentsWhoseChainIsConsistentButIncoherent: a
// continuidade entre segmentos (FirstSequence e StartPos) sozinha não basta.
// Os dois arquivos abaixo encadeiam perfeitamente com o prefixo válido do
// primeiro, e só a recusa explícita de lixo num segmento selado, ou do buraco
// de sequences com posições contíguas, os pega.
func TestOpenDiskLogRejectsSealedSegmentsWhoseChainIsConsistentButIncoherent(t *testing.T) {
	size := uint64(frameSize(mkBatch(1, 1, 10)))
	one := func(from, endPos uint64) []byte { return mustAppendFrame(nil, mkBatch(from, from, 10), endPos) }

	t.Run("garbage inside a sealed segment", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		handmadeSegmentAt(t, dir, 1, 0, one(1, size), bytes.Repeat([]byte{0xEE}, 40))
		handmadeSegmentAt(t, dir, 2, size, one(2, 2*size))
		if _, err := openDiskLog(dir, 1<<20, 1<<30); !errors.Is(err, errCorruptLog) {
			t.Fatalf("err = %v, esperado errCorruptLog", err)
		}
	})
	t.Run("sequence hole with contiguous positions", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		handmadeSegmentAt(t, dir, 1, 0, one(1, size))
		handmadeSegmentAt(t, dir, 5, size, one(5, 2*size))
		if _, err := openDiskLog(dir, 1<<20, 1<<30); !errors.Is(err, errCorruptLog) {
			t.Fatalf("err = %v, esperado errCorruptLog", err)
		}
	})
	t.Run("position hole with contiguous sequences", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		handmadeSegmentAt(t, dir, 1, 0, one(1, size))
		handmadeSegmentAt(t, dir, 2, size+7, one(2, 2*size+7))
		if _, err := openDiskLog(dir, 1<<20, 1<<30); !errors.Is(err, errCorruptLog) {
			t.Fatalf("err = %v, esperado errCorruptLog", err)
		}
	})
	t.Run("coherent chain opens", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		handmadeSegmentAt(t, dir, 1, 0, one(1, size))
		handmadeSegmentAt(t, dir, 2, size, one(2, 2*size))
		l, err := openDiskLog(dir, 1<<20, 1<<30)
		if err != nil {
			t.Fatalf("cadeia coerente recusada: %v", err)
		}
		defer func() { _ = l.close() }()
		if to, end := l.position(); to != 2 || end != 2*size || l.segments() != 2 {
			t.Fatalf("posição = %d/%d, segmentos = %d", to, end, l.segments())
		}
	})
}

func TestOpenDiskLogRejectsIncoherentSealedSegments(t *testing.T) {
	build := func(t *testing.T) (*diskLog, string) {
		l, dir := newTestLog(t, 4096, 1<<30)
		for i := 0; i < 12; i++ { // 3 segmentos: 1-4, 5-8, 9-12
			appendSeqs(t, l, 1, frame1000)
		}
		_ = l.close()
		return l, dir
	}
	seg := func(dir string, n int) string { return filepath.Join(dir, segmentFileName(uint64(n))) }

	t.Run("corrupt frame header in a sealed segment", func(t *testing.T) {
		l, dir := build(t)
		b, _ := os.ReadFile(seg(dir, 1))
		b[64] ^= 0xFF // magic do cabeçalho do primeiro quadro
		_ = os.WriteFile(seg(dir, 1), b, 0o644)
		requireCorruptThenHeal(t, dir, l)
	})
	t.Run("gap between segments", func(t *testing.T) {
		l, dir := build(t)
		if err := os.Remove(seg(dir, 5)); err != nil {
			t.Fatal(err)
		}
		requireCorruptThenHeal(t, dir, l)
	})
	t.Run("epoch changes between segments", func(t *testing.T) {
		l, dir := build(t)
		b, _ := os.ReadFile(seg(dir, 5))
		b[8] ^= 0xFF
		_ = os.WriteFile(seg(dir, 5), b, 0o644)
		requireCorruptThenHeal(t, dir, l)
	})
	t.Run("file name disagrees with the header", func(t *testing.T) {
		l, dir := build(t)
		if err := os.Rename(seg(dir, 9), seg(dir, 10)); err != nil {
			t.Fatal(err)
		}
		requireCorruptThenHeal(t, dir, l)
	})
	t.Run("sealed segment shorter than a header", func(t *testing.T) {
		l, dir := build(t)
		_ = os.Truncate(seg(dir, 5), 10)
		requireCorruptThenHeal(t, dir, l)
	})
	t.Run("unexpected .seg name", func(t *testing.T) {
		l, dir := build(t)
		_ = os.WriteFile(filepath.Join(dir, "stray.seg"), nil, 0o644)
		requireCorruptThenHeal(t, dir, l)
	})
}

func TestOpenDiskLogRebuildsFloorBytesAndPositionAfterRetention(t *testing.T) {
	l, dir := newTestLog(t, 4096, 10000)
	for i := 0; i < 14; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	lastTo, endPos := l.position()
	floor, bytes, segs := l.floor(), l.bytes(), l.segments()
	if floor == 0 {
		t.Fatalf("o cenário deveria ter exercido a retenção")
	}
	n := reopen(t, l, dir)
	var onDisk int64
	for _, sz := range segmentFiles(t, dir) {
		onDisk += sz
	}
	if n.floor() != floor || n.bytes() != bytes || n.bytes() != onDisk || n.segments() != segs {
		t.Fatalf("reaberto: floor %d bytes %d (em disco %d) segmentos %d; antes %d %d %d", n.floor(), n.bytes(), onDisk, n.segments(), floor, bytes, segs)
	}
	if to, end := n.position(); to != lastTo || end != endPos {
		t.Fatalf("posição reaberta = %d/%d, antes %d/%d", to, end, lastTo, endPos)
	}
	if ep, ok := n.logEpoch(); !ok || ep != testEpoch {
		t.Fatalf("epoch reaberta = %x, %v", ep, ok)
	}
	// Continua gravando no segmento corrente, sem rotação desnecessária.
	if err := n.bind(testEpoch, floor, 0); err != nil {
		t.Fatal(err)
	}
	appendSeqs(t, n, 1, frame1000)
	if to, _ := n.position(); to != lastTo+1 {
		t.Fatalf("depois do append a última sequence = %d, esperado %d", to, lastTo+1)
	}
	if err := n.bind([16]byte{9}, 0, 0); !errors.Is(err, errEpochMismatch) {
		t.Fatalf("bind com epoch diferente da do disco: err = %v", err)
	}
}

func TestDiskLogAppendRejectsFramesThatDoNotContinueTheLogWithoutFailingIt(t *testing.T) {
	l, _ := newTestLog(t, 4096, 1<<30)
	appendSeqs(t, l, 5, 100) // [1,5]
	lastTo, endPos := l.position()

	// frameFor monta o quadro [from,to] com o fim certo para a posição atual.
	frameFor := func(from, to uint64) ([]byte, uint64) {
		b := mkBatch(from, to, 100)
		end := endPos + uint64(frameSize(b))
		return mustAppendFrame(nil, b, end), end
	}
	good, goodEnd := frameFor(6, 8)

	cases := []struct {
		name string
		err  error
		call func() error
	}{
		{"hole in the sequences", errNotContiguous, func() error {
			buf, end := frameFor(7, 8)
			_, e := l.append(buf, 7, 8, end)
			return e
		}},
		{"overlapping sequences", errNotContiguous, func() error {
			buf, end := frameFor(5, 8)
			_, e := l.append(buf, 5, 8, end)
			return e
		}},
		{"wrong end position embedded in the frame", errBadAppend, func() error {
			b := mkBatch(6, 8, 100)
			end := endPos + uint64(frameSize(b)) + 1
			_, e := l.append(mustAppendFrame(nil, b, end), 6, 8, end)
			return e
		}},
		{"arguments disagree with the frame", errBadAppend, func() error { _, e := l.append(good, 6, 9, goodEnd); return e }},
		{"end position disagrees with the frame", errBadAppend, func() error { _, e := l.append(good, 6, 8, goodEnd+1); return e }},
		{"garbage instead of a frame", errBadAppend, func() error { _, e := l.append([]byte("not a frame at all"), 6, 8, goodEnd); return e }},
		{"trailing bytes after the frame", errBadAppend, func() error {
			_, e := l.append(append(append([]byte(nil), good...), 0), 6, 8, goodEnd)
			return e
		}},
	}
	for _, c := range cases {
		if err := c.call(); !errors.Is(err, c.err) {
			t.Fatalf("%s: err = %v, esperado %v", c.name, err, c.err)
		}
	}
	if to, end := l.position(); to != lastTo || end != endPos {
		t.Fatalf("append recusado mexeu na posição: %d/%d", to, end)
	}
	// Nada disso falhou o log: o quadro certo ainda entra.
	if _, err := l.append(good, 6, 8, goodEnd); err != nil {
		t.Fatalf("o quadro correto foi recusado depois dos recusados: %v", err)
	}
}

func TestDiskLogAppendRequiresBind(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	l, err := createDiskLog(dir, 4096, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.close() }()
	b := mkBatch(1, 1, 10)
	size := uint64(frameSize(b))
	if _, err := l.append(mustAppendFrame(nil, b, size), 1, 1, size); !errors.Is(err, errLogUnbound) {
		t.Fatalf("err = %v, esperado errLogUnbound", err)
	}
}

func TestDiskLogWipeRemovesSegmentsButKeepsTheFloorAndOtherFiles(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 3; i++ {
		appendSeqs(t, l, 10, 100)
	}
	if err := os.WriteFile(filepath.Join(dir, "state"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(dir, "00000000000000000099.seg"), []byte("orphan"), 0o644)
	h, off, _ := l.locate(0) // leitor com o segmento arrendado durante o wipe
	_, endPos := l.position()
	if err := l.wipe(); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "state" {
		t.Fatalf("depois do wipe só `state` deveria sobrar: %v", entries)
	}
	if l.segments() != 0 || l.bytes() != 0 || l.floor() != 30 {
		t.Fatalf("segments() = %d, bytes() = %d, floor() = %d", l.segments(), l.bytes(), l.floor())
	}
	if _, end := l.position(); end != endPos {
		t.Fatalf("o wipe perdeu a posição em bytes: %d, esperado %d", end, endPos)
	}
	if _, _, ok := l.locate(30); ok {
		t.Fatalf("log vazio não serve cursor nenhum")
	}
	// O leitor que já segurava o segmento termina de ler.
	if fr := readFrames(t, h, off); len(fr) != 2 {
		t.Fatalf("o leitor arrendado perdeu o segmento no wipe: %d quadros", len(fr))
	}
	h.release()
	// Depois do wipe a geração recomeça do piso.
	if err := l.bind(testEpoch, 30, endPos); err != nil {
		t.Fatal(err)
	}
	if from, to, _ := appendSeqs(t, l, 1, 100); from != 31 || to != 31 {
		t.Fatalf("próximo quadro = [%d,%d]", from, to)
	}
}

func TestDiskLogSyncAndCloseLifecycle(t *testing.T) {
	l, _ := newTestLog(t, 4096, 1<<30)
	appendSeqs(t, l, 1, 100)
	if err := l.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	h, off, _ := l.locate(0)
	if err := l.close(); err != nil {
		t.Fatal(err)
	}
	if err := l.close(); err != nil {
		t.Fatalf("close duplicado: %v", err)
	}
	// O leitor arrendado antes do close ainda lê; depois do release o fd fecha.
	if fr := readFrames(t, h, off); len(fr) != 1 {
		t.Fatalf("o leitor perdeu o segmento no close: %d quadros", len(fr))
	}
	seg := h.s
	h.release()
	if _, err := seg.f.Stat(); err == nil {
		t.Fatalf("o fd deveria fechar quando o último leitor devolve um log fechado")
	}
	b := mkBatch(2, 2, 10)
	if _, err := l.append(mustAppendFrame(nil, b, 300), 2, 2, 300); !errors.Is(err, errLogClosed) {
		t.Fatalf("append depois do close: err = %v", err)
	}
	if err := l.sync(); !errors.Is(err, errLogClosed) {
		t.Fatalf("sync depois do close: err = %v", err)
	}
	if _, _, ok := l.locate(0); ok {
		t.Fatalf("locate depois do close devolveu ok")
	}
}

func TestOpenDiskLogRejectsInvalidLimits(t *testing.T) {
	if _, err := openDiskLog(t.TempDir(), 10, 100); err == nil {
		t.Fatalf("segmentBytes menor que um cabeçalho deveria ser recusado")
	}
	if _, err := openDiskLog(t.TempDir(), 4096, 0); err == nil {
		t.Fatalf("maxBytes zero deveria ser recusado")
	}
}
