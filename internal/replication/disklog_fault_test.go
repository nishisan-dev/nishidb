package replication

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// seams guarda os seams de I/O do log e os restaura no fim do teste.
func seams(t *testing.T) {
	t.Helper()
	fd, sd, wa, tr := fdatasyncFn, syncDirFn, writeAtFn, truncateFn
	t.Cleanup(func() { fdatasyncFn, syncDirFn, writeAtFn, truncateFn = fd, sd, wa, tr })
}

// frameFor monta o quadro que o log aceitaria agora para as próximas `seqs`
// sequences, sem gravá-lo.
func frameFor(l *diskLog, seqs, payload int) (fb []byte, from, to, endPos uint64) {
	last, pos := l.position()
	from, to = last+1, last+uint64(seqs)
	b := mkBatch(from, to, payload)
	endPos = pos + uint64(frameSize(b))
	return mustAppendFrame(nil, b, endPos), from, to, endPos
}

var errInjected = errors.New("injected disk failure")

// failWritesWithGarbage troca o writeAtFn por um que, nas n primeiras
// chamadas que passam em `match`, grava LIXO maior que o pedido e falha, como
// um disco que escreve parte e devolve ENOSPC/EIO.
func failWritesWithGarbage(n int, match func(p []byte, off int64) bool) {
	realWrite := writeAtFn
	calls := 0
	writeAtFn = func(f *os.File, p []byte, off int64) (int, error) {
		if match(p, off) && calls < n {
			calls++
			_, _ = f.WriteAt(bytes.Repeat([]byte{0xEE}, len(p)+200), off)
			return 0, errInjected
		}
		return realWrite(f, p, off)
	}
}

func isFrameWrite(p []byte, off int64) bool {
	return off >= segmentHeaderSize && len(p) != segmentHeaderSize
}

// TestDiskLogWriteFailureIsRetryableWithoutWipe: o plano trata ENOSPC/EIO
// como disco lento, com retentativa e sem lacuna. Três falhas seguidas,
// cada uma deixando lixo maior que o quadro além do fim publicado; a quarta
// chamada, com o MESMO quadro, funciona, o log segue íntegro e nunca houve wipe.
func TestDiskLogWriteFailureIsRetryableWithoutWipe(t *testing.T) {
	seams(t)
	l, dir := newTestLog(t, 1<<20, 1<<30)
	appendSeqs(t, l, 1, 100) // 196 bytes
	fb, from, to, endPos := frameFor(l, 1, 100)
	lastTo, pos := l.position()
	failWritesWithGarbage(3, isFrameWrite)

	for i := 0; i < 3; i++ {
		if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errInjected) {
			t.Fatalf("tentativa %d: err = %v, esperado a falha injetada", i, err)
		}
		if gotTo, gotPos := l.position(); gotTo != lastTo || gotPos != pos || l.bytes() != 64+196 {
			t.Fatalf("tentativa %d mexeu no log: posição %d/%d, bytes %d", i, gotTo, gotPos, l.bytes())
		}
	}
	if _, err := l.append(fb, from, to, endPos); err != nil {
		t.Fatalf("a retentativa depois de 3 falhas deveria funcionar sem wipe: %v", err)
	}

	n := reopen(t, l, dir)
	if err := n.validateTail(testEpoch, to, endPos); err != nil {
		t.Fatalf("o lixo das escritas que falharam sobrou no arquivo: %v", err)
	}
	if got := segmentFiles(t, dir)["00000000000000000001.seg"]; got != 64+2*196 {
		t.Fatalf("segmento com %d bytes, esperado %d", got, 64+2*196)
	}
	h, off, ok := n.locate(0)
	if !ok {
		t.Fatal("locate(0) !ok")
	}
	defer h.release()
	fr := readFrames(t, h, off)
	if len(fr) != 2 || fr[1].From != 2 || !bytes.Equal(fr[1].Entries[0].Record.Payload, bytes.Repeat([]byte{2}, 100)) {
		t.Fatalf("conteúdo depois da retentativa: %+v", fr)
	}
}

// TestDiskLogWriteFailureOnRotationRemovesTheNewSegment: a escrita do quadro
// (ou do cabeçalho) de um segmento recém-criado falha: o arquivo some, o log
// fica como estava e a retentativa cria o segmento de novo.
func TestDiskLogWriteFailureOnRotationRemovesTheNewSegment(t *testing.T) {
	for name, match := range map[string]func(p []byte, off int64) bool{
		"frame write fails":  isFrameWrite,
		"header write fails": func(p []byte, off int64) bool { return off == 0 && len(p) == segmentHeaderSize },
	} {
		t.Run(name, func(t *testing.T) {
			seams(t)
			l, dir := newTestLog(t, 4096, 1<<30)
			for i := 0; i < 4; i++ {
				appendSeqs(t, l, 1, frame1000)
			}
			fb, from, to, endPos := frameFor(l, 1, frame1000)
			failWritesWithGarbage(1, match)

			if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errInjected) {
				t.Fatalf("err = %v, esperado a falha injetada", err)
			}
			if files := segmentFiles(t, dir); len(files) != 1 || l.segments() != 1 {
				t.Fatalf("o segmento novo deveria ter sido removido: arquivos %v, segments() = %d", files, l.segments())
			}
			if _, err := l.append(fb, from, to, endPos); err != nil {
				t.Fatalf("retentativa: %v", err)
			}
			if files := segmentFiles(t, dir); len(files) != 2 || files["00000000000000000005.seg"] != 64+1000 {
				t.Fatalf("arquivos depois da retentativa: %v", files)
			}
			n := reopen(t, l, dir)
			if err := n.validateTail(testEpoch, to, endPos); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestDiskLogTruncateFailureMarksTheTailDirtyAndTheNextAppendRetriesIt: se o
// desfazer da escrita também falha, o lixo fica no arquivo. A cauda passa a
// contar como suja (validateTail recusa) e a próxima chamada a trunca ANTES de
// gravar.
func TestDiskLogTruncateFailureMarksTheTailDirtyAndTheNextAppendRetriesIt(t *testing.T) {
	seams(t)
	l, dir := newTestLog(t, 1<<20, 1<<30)
	appendSeqs(t, l, 1, 100)
	fb, from, to, endPos := frameFor(l, 1, 100)
	lastTo, pos := l.position()
	failWritesWithGarbage(1, isFrameWrite)
	realTruncate := truncateFn
	failedTruncates := 0
	truncateFn = func(f *os.File, size int64) error {
		if failedTruncates == 0 {
			failedTruncates++
			return errInjected
		}
		return realTruncate(f, size)
	}

	if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v", err)
	}
	if err := l.validateTail(testEpoch, lastTo, pos); !errors.Is(err, errTailMismatch) {
		t.Fatalf("com lixo conhecido no arquivo validateTail deveria recusar: %v", err)
	}
	if _, err := l.append(fb, from, to, endPos); err != nil {
		t.Fatalf("a retentativa deveria truncar o lixo e gravar: %v", err)
	}
	n := reopen(t, l, dir)
	if err := n.validateTail(testEpoch, to, endPos); err != nil {
		t.Fatalf("sobrou lixo depois da retentativa: %v", err)
	}
	if got := segmentFiles(t, dir)["00000000000000000001.seg"]; got != 64+2*196 {
		t.Fatalf("segmento com %d bytes, esperado %d", got, 64+2*196)
	}
}

// TestDiskLogFsyncFailureIsStickyUntilWipe: ao contrário da escrita, a falha de
// fdatasync não é retentável (o kernel pode ter descartado as páginas sujas, e
// um segundo fsync "bem-sucedido" mentiria).
func TestDiskLogFsyncFailureIsStickyUntilWipe(t *testing.T) {
	failOnce := func() {
		realSync := fdatasyncFn
		failed := false
		fdatasyncFn = func(f *os.File) error {
			if !failed {
				failed = true
				return errInjected
			}
			return realSync(f)
		}
	}
	heal := func(t *testing.T, l *diskLog) {
		t.Helper()
		last, pos := l.position()
		if err := l.wipe(); err != nil {
			t.Fatal(err)
		}
		if err := l.bind(testEpoch, last, pos); err != nil {
			t.Fatal(err)
		}
		appendSeqs(t, l, 1, 100)
		if err := l.sync(); err != nil {
			t.Fatalf("depois do wipe o log deveria voltar a funcionar: %v", err)
		}
	}

	t.Run("sync", func(t *testing.T) {
		seams(t)
		l, _ := newTestLog(t, 1<<20, 1<<30)
		appendSeqs(t, l, 1, 100)
		failOnce()
		if err := l.sync(); !errors.Is(err, errInjected) {
			t.Fatalf("sync = %v", err)
		}
		// O disco "voltou": o log segue falho.
		if err := l.sync(); !errors.Is(err, errInjected) {
			t.Fatalf("segundo sync = %v; a falha de fsync tem de ser pegajosa", err)
		}
		fb, from, to, endPos := frameFor(l, 1, 100)
		if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errInjected) {
			t.Fatalf("append depois do fsync que falhou = %v; deveria seguir falho", err)
		}
		heal(t, l)
	})
	t.Run("rotation", func(t *testing.T) {
		seams(t)
		l, dir := newTestLog(t, 4096, 1<<30)
		for i := 0; i < 4; i++ {
			appendSeqs(t, l, 1, frame1000)
		}
		failOnce()
		fb, from, to, endPos := frameFor(l, 1, frame1000)
		if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errInjected) {
			t.Fatalf("append que rotaciona = %v", err)
		}
		if files := segmentFiles(t, dir); len(files) != 1 {
			t.Fatalf("o segmento novo não deveria existir: %v", files)
		}
		if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errInjected) {
			t.Fatalf("segundo append = %v; a falha de fsync da rotação tem de ser pegajosa", err)
		}
		heal(t, l)
	})
}

// TestDiskLogFirstAppendThatRotatesTruncatesTheTornTail: a cauda rasgada tem
// de sumir mesmo quando o primeiro append rotaciona. Do contrário o lixo fica
// no fim de um segmento que já nasceu selado, e a reabertura seguinte o recusa
// como corrupção.
func TestDiskLogFirstAppendThatRotatesTruncatesTheTornTail(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 4; i++ { // enche o segmento: 4064 de 4096
		appendSeqs(t, l, 1, frame1000)
	}
	_ = l.close()
	seg1 := filepath.Join(dir, "00000000000000000001.seg")
	f, err := os.OpenFile(seg1, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write(bytes.Repeat([]byte{0xEE}, 20))
	_ = f.Close()

	n := reopen(t, l, dir)
	if err := n.bind(testEpoch, 0, 0); err != nil {
		t.Fatal(err)
	}
	appendSeqs(t, n, 1, frame1000) // 4064 + 1000 > 4096: rotaciona
	if files := segmentFiles(t, dir); files["00000000000000000001.seg"] != 4064 || len(files) != 2 {
		t.Fatalf("o lixo ficou no segmento selado: %v", files)
	}
	again := reopen(t, n, dir)
	lastTo, endPos := again.position()
	if err := again.validateTail(testEpoch, lastTo, endPos); err != nil || lastTo != 5 {
		t.Fatalf("reabertura depois da rotação: lastTo %d, err %v", lastTo, err)
	}
}

func TestOpenDiskLogTreatsUnreadableHeaderOfTheLastFileAsAnOrphan(t *testing.T) {
	zero := make([]byte, segmentHeaderSize)
	badVersion := appendSegmentHeader(nil, segmentHeader{Epoch: testEpoch, FirstSequence: 6, StartPos: 5000})
	badVersion[4] = 9
	cases := map[string][]byte{
		"empty file":            {},
		"short header":          []byte("NDBF\x01"),
		"zeroed header":         zero,
		"garbage past a header": bytes.Repeat([]byte{0xEE}, 300),
		"unsupported version":   badVersion,
		"zeroed header and body": append(append([]byte(nil), zero...),
			bytes.Repeat([]byte{0}, 500)...),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			l, dir := newTestLog(t, 4096, 1<<30)
			for i := 0; i < 5; i++ {
				appendSeqs(t, l, 1, frame1000)
			}
			lastTo, endPos := l.position()
			_ = l.close()
			orphan := filepath.Join(dir, "00000000000000000006.seg")
			if err := os.WriteFile(orphan, content, 0o644); err != nil {
				t.Fatal(err)
			}

			n, err := openDiskLog(dir, 4096, 1<<30)
			if err != nil {
				t.Fatalf("cabeçalho ilegível no último arquivo não pode ser fatal: %v", err)
			}
			defer func() { _ = n.close() }()
			if n.segments() != 2 {
				t.Fatalf("segments() = %d, esperado 2", n.segments())
			}
			if err := n.validateTail(testEpoch, lastTo, endPos); err != nil {
				t.Fatalf("validateTail: %v", err)
			}
			if err := n.bind(testEpoch, 0, 0); err != nil {
				t.Fatal(err)
			}
			appendSeqs(t, n, 1, frame1000)
			if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a sobra deveria ter sido apagada no primeiro append: %v", err)
			}
		})
	}
	t.Run("only file", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, segmentFileName(1)), zero, 0o644); err != nil {
			t.Fatal(err)
		}
		n, err := openDiskLog(dir, 4096, 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = n.close() }()
		if err := n.bind(testEpoch, 0, 0); err != nil {
			t.Fatal(err)
		}
		if n.segments() != 0 {
			t.Fatalf("segments() = %d", n.segments())
		}
		if err := n.validateTail(testEpoch, 0, 0); err != nil {
			t.Fatal(err)
		}
		appendSeqs(t, n, 1, 10) // o nome da sobra é o mesmo do segmento novo
	})
}

// requireCorruptThenHeal confere que a abertura recusa o diretório com
// errCorruptLog e que removeSegments o cura: depois dele o log abre vazio, o
// `state` continua lá e o feed volta a gravar.
func requireCorruptThenHeal(t *testing.T, dir string, l *diskLog) {
	t.Helper()
	if _, err := openDiskLog(dir, l.segmentBytes, l.maxBytes); !errors.Is(err, errCorruptLog) {
		t.Fatalf("err = %v, esperado errCorruptLog", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := removeSegments(dir); err != nil {
		t.Fatalf("removeSegments: %v", err)
	}
	n, err := openDiskLog(dir, l.segmentBytes, l.maxBytes)
	if err != nil {
		t.Fatalf("o diretório curado deveria abrir: %v", err)
	}
	defer func() { _ = n.close() }()
	if n.segments() != 0 {
		t.Fatalf("segments() = %d depois de removeSegments", n.segments())
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "state")); string(got) != "keep" {
		t.Fatalf("removeSegments tocou o state: %q", got)
	}
	if err := n.bind(testEpoch, 0, 0); err != nil {
		t.Fatal(err)
	}
	appendSeqs(t, n, 1, 100)
}

func TestRemoveSegmentsIgnoresAMissingDirectoryAndOtherFiles(t *testing.T) {
	if err := removeSegments(filepath.Join(t.TempDir(), "nope")); err != nil {
		t.Fatalf("diretório ausente: %v", err)
	}
	dir := t.TempDir()
	for _, name := range []string{"00000000000000000001.seg", "stray.seg", "state", "other.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeSegments(dir); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	var left []string
	for _, e := range entries {
		left = append(left, e.Name())
	}
	if strings.Join(left, ",") != "other.txt,state" {
		t.Fatalf("sobraram %v", left)
	}
}

// TestDiskLogDoesNotPublishAFrameBeforeItIsWhole pausa o append NO MEIO da
// escrita do quadro (metade já no arquivo) e lê pelo handle: o leitor tem de
// ver só os quadros anteriores, sem erro. Se o fim publicado avançasse antes
// da escrita, o quadro de metade apareceria e a leitura estrita falharia.
func TestDiskLogDoesNotPublishAFrameBeforeItIsWhole(t *testing.T) {
	seams(t)
	l, _ := newTestLog(t, 1<<20, 1<<30)
	appendSeqs(t, l, 1, 100)
	appendSeqs(t, l, 1, 100)
	fb, from, to, endPos := frameFor(l, 1, 100)

	paused, resume := make(chan struct{}), make(chan struct{})
	// O Cleanup libera a goroutine do append mesmo se um t.Fatal sair antes do
	// close explícito: do contrário ela ficaria presa no <-resume.
	var resumeOnce sync.Once
	releaseWriter := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(releaseWriter)
	realWrite := writeAtFn
	writeAtFn = func(f *os.File, p []byte, off int64) (int, error) {
		if !isFrameWrite(p, off) {
			return realWrite(f, p, off)
		}
		half := len(p) / 2
		n, err := f.WriteAt(p[:half], off)
		if err != nil {
			return n, err
		}
		close(paused)
		<-resume
		m, err := f.WriteAt(p[half:], off+int64(half))
		return n + m, err
	}
	done := make(chan error, 1)
	go func() { _, err := l.append(fb, from, to, endPos); done <- err }()
	<-paused

	h, off, ok := l.locate(0)
	if !ok {
		t.Fatal("locate(0) !ok")
	}
	if fr := readFrames(t, h, off); len(fr) != 2 {
		t.Fatalf("durante a gravação o leitor viu %d quadros, esperado 2", len(fr))
	}
	if h.size() != 64+2*196 {
		t.Fatalf("fim publicado = %d durante a gravação, esperado %d", h.size(), 64+2*196)
	}
	h.release()
	// Um cursor no fim publicado também não enxerga o quadro em gravação.
	h2, off2, ok := l.locate(2)
	if !ok || len(readFrames(t, h2, off2)) != 0 {
		t.Fatalf("cursor no fim publicado deveria ler nada: ok %v", ok)
	}
	h2.release()

	releaseWriter()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h3, off3, _ := l.locate(0)
	defer h3.release()
	if fr := readFrames(t, h3, off3); len(fr) != 3 {
		t.Fatalf("depois da gravação: %d quadros, esperado 3", len(fr))
	}
}

// TestDiskLogRotationSyncsTheOldSegmentBeforeCreatingTheNext trava a ordem
// que o godoc afirma: a cauda do segmento que fecha é sincronizada ANTES de o
// segmento novo existir.
func TestDiskLogRotationSyncsTheOldSegmentBeforeCreatingTheNext(t *testing.T) {
	seams(t)
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 4; i++ {
		appendSeqs(t, l, 1, frame1000)
	}
	type call struct {
		name       string
		nextExists bool
	}
	var calls []call
	fdatasyncFn = func(f *os.File) error {
		_, err := os.Stat(filepath.Join(dir, segmentFileName(5)))
		calls = append(calls, call{filepath.Base(f.Name()), err == nil})
		return nil
	}
	appendSeqs(t, l, 1, frame1000) // rotaciona
	want := []call{{"00000000000000000001.seg", false}}
	if len(calls) != 1 || calls[0] != want[0] {
		t.Fatalf("fdatasync na rotação = %+v, esperado %+v", calls, want)
	}
	// Sem rotação, o append não sincroniza nada: quem sincroniza é o sync().
	calls = nil
	appendSeqs(t, l, 1, frame1000)
	if len(calls) != 0 {
		t.Fatalf("append sem rotação chamou fdatasync: %+v", calls)
	}
}

// TestDiskLogSyncFlushesTheCurrentSegmentThenTheDirectory: o sync() sincroniza
// o segmento corrente (só ele) e, depois, o diretório.
func TestDiskLogSyncFlushesTheCurrentSegmentThenTheDirectory(t *testing.T) {
	seams(t)
	l, dir := newTestLog(t, 4096, 1<<30)
	for i := 0; i < 5; i++ { // dois segmentos
		appendSeqs(t, l, 1, frame1000)
	}
	var events []string
	fdatasyncFn = func(f *os.File) error { events = append(events, "fdatasync "+filepath.Base(f.Name())); return nil }
	syncDirFn = func(d string) error { events = append(events, "syncdir "+d); return nil }
	if err := l.sync(); err != nil {
		t.Fatal(err)
	}
	want := []string{"fdatasync 00000000000000000005.seg", "syncdir " + dir}
	if strings.Join(events, "|") != strings.Join(want, "|") {
		t.Fatalf("eventos do sync = %q, esperado %q", events, want)
	}
}

// TestEnsureDirSyncsTheParentOfEveryCreatedLevel: criar a/b/feed sincroniza o
// pai de CADA nível criado, do mais alto para o mais baixo.
func TestEnsureDirSyncsTheParentOfEveryCreatedLevel(t *testing.T) {
	seams(t)
	var got []string
	syncDirFn = func(d string) error { got = append(got, d); return nil }
	base := t.TempDir()
	l, err := createDiskLog(filepath.Join(base, "a", "b", "feed"), 4096, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.close()
	want := []string{base, filepath.Join(base, "a"), filepath.Join(base, "a", "b")}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("fsyncs = %q, esperado %q", got, want)
	}
	// Diretório que já existe: nada a tornar durável.
	got = nil
	l, err = createDiskLog(filepath.Join(base, "a", "b", "feed"), 4096, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.close()
	if len(got) != 0 {
		t.Fatalf("diretório existente gerou fsyncs: %q", got)
	}
}

// TestOpenDiskLogDoesNotVerifySealedBodiesButReadsDo: a abertura confere o
// CRC só do último segmento; o corpo de um quadro de segmento selado é
// conferido na leitura, que devolve errCorruptFrame.
func TestOpenDiskLogDoesNotVerifySealedBodiesButReadsDo(t *testing.T) {
	build := func(t *testing.T) (*diskLog, string) {
		l, dir := newTestLog(t, 4096, 1<<30)
		for i := 0; i < 12; i++ { // seg 1-4, 5-8, 9-12
			appendSeqs(t, l, 1, frame1000)
		}
		_ = l.close()
		return l, dir
	}
	flip := func(t *testing.T, dir string, first uint64) {
		path := filepath.Join(dir, segmentFileName(first))
		b, _ := os.ReadFile(path)
		b[64+500] ^= 0xFF // no meio do payload do primeiro quadro
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("sealed segment", func(t *testing.T) {
		l, dir := build(t)
		flip(t, dir, 1)
		n, err := openDiskLog(dir, l.segmentBytes, l.maxBytes)
		if err != nil {
			t.Fatalf("a abertura não confere o corpo de segmento selado: %v", err)
		}
		defer func() { _ = n.close() }()
		if err := n.validateTail(testEpoch, 12, 12000); err != nil {
			t.Fatalf("validateTail: %v", err)
		}
		h, off, ok := n.locate(0)
		if !ok {
			t.Fatal("locate !ok")
		}
		defer h.release()
		if _, _, err := readFrameAt(h, off); !errors.Is(err, errCorruptFrame) {
			t.Fatalf("a leitura do quadro corrompido: err = %v, esperado errCorruptFrame", err)
		}
		if f, _, err := readFrameAt(h, off+1000); err != nil || f.From != 2 {
			t.Fatalf("o quadro seguinte deveria ler normalmente: %v %+v", err, f.From)
		}
	})
	t.Run("last segment", func(t *testing.T) {
		l, dir := build(t)
		flip(t, dir, 9)
		n, err := openDiskLog(dir, l.segmentBytes, l.maxBytes)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = n.close() }()
		if to, _ := n.position(); to != 8 {
			t.Fatalf("o último segmento deveria parar antes do quadro ruim: lastTo = %d", to)
		}
		if err := n.validateTail(testEpoch, 12, 12000); !errors.Is(err, errTailMismatch) {
			t.Fatalf("validateTail = %v, esperado errTailMismatch", err)
		}
	})
}

// TestOpenDiskLogRebuildsTheIndexAcrossFramesLargerThanTheScanBuffer exercita
// o salto de leitura da abertura de segmentos selados (quadros de 2 MiB, maiores
// que o buffer de varredura de 1 MiB).
func TestOpenDiskLogRebuildsTheIndexAcrossFramesLargerThanTheScanBuffer(t *testing.T) {
	l, dir := newTestLog(t, 5<<20, 1<<30)
	for i := 0; i < 5; i++ { // 2 quadros de ~2 MiB por segmento
		appendSeqs(t, l, 1, 2<<20)
	}
	if l.segments() != 3 {
		t.Fatalf("segments() = %d, esperado 3", l.segments())
	}
	type snap struct {
		first, lastTo, endPos uint64
		size                  int64
		frames                int
		index                 []indexEntry
	}
	take := func(l *diskLog) (out []snap) {
		for _, s := range l.segs {
			out = append(out, snap{s.firstSeq, s.lastTo, s.endPos, s.size.Load(), s.frames, append([]indexEntry(nil), s.index...)})
		}
		return out
	}
	before := take(l)
	n := reopen(t, l, dir)
	after := take(n)
	if len(after) != len(before) {
		t.Fatalf("segmentos reabertos = %d, antes %d", len(after), len(before))
	}
	for i := range before {
		b, a := before[i], after[i]
		if a.first != b.first || a.lastTo != b.lastTo || a.endPos != b.endPos || a.size != b.size || a.frames != b.frames || len(a.index) != len(b.index) {
			t.Fatalf("segmento %d diverge depois da reabertura: antes %+v, depois %+v", i, b, a)
		}
		for j := range b.index {
			if a.index[j] != b.index[j] {
				t.Fatalf("índice do segmento %d entrada %d: %+v, antes %+v", i, j, a.index[j], b.index[j])
			}
		}
	}
	// O primeiro quadro de ~2 MiB indexa em 64; o segundo está a >1 MiB: indexa também.
	if len(before[0].index) != 2 {
		t.Fatalf("índice do 1º segmento = %+v, esperado 2 entradas", before[0].index)
	}
}

func TestDiskLogValidateTailRejectsAnotherEpoch(t *testing.T) {
	l, dir := newTestLog(t, 4096, 1<<30)
	appendSeqs(t, l, 3, 100)
	lastTo, endPos := l.position()
	n := reopen(t, l, dir)
	other := testEpoch
	other[15] ^= 0x01
	if err := n.validateTail(other, lastTo, endPos); !errors.Is(err, errTailMismatch) {
		t.Fatalf("epoch diferente: err = %v, esperado errTailMismatch", err)
	}
	if err := n.validateTail(testEpoch, lastTo, endPos); err != nil {
		t.Fatalf("epoch certa: %v", err)
	}
}

func TestDiskLogBindRefusesToLowerFloorOrPositionOfTheSameEpoch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	l, err := createDiskLog(dir, 4096, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.close() }()
	if err := l.bind(testEpoch, 30, 5000); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string][2]uint64{"lower floor": {29, 5000}, "lower position": {30, 4999}, "both lower": {0, 0}} {
		if err := l.bind(testEpoch, c[0], c[1]); !errors.Is(err, errBindRegression) {
			t.Fatalf("%s: err = %v, esperado errBindRegression", name, err)
		}
	}
	if to, end := l.position(); to != 30 || end != 5000 {
		t.Fatalf("o bind recusado mexeu na base: %d/%d", to, end)
	}
	for _, c := range []struct {
		name       string
		floor, pos uint64
	}{{"same", 30, 5000}, {"higher", 31, 5001}} {
		if err := l.bind(testEpoch, c.floor, c.pos); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
	}
	// Uma epoch NOVA é outra geração: recomeça de onde disser.
	other := testEpoch
	other[0] ^= 0xFF
	if err := l.bind(other, 0, 0); err != nil {
		t.Fatalf("geração nova: %v", err)
	}
	if to, end := l.position(); to != 0 || end != 0 {
		t.Fatalf("geração nova deveria recomeçar de 0/0: %d/%d", to, end)
	}
}

// TestDiskLogCloseDuringAppendDoesNotPublishTheFrame: se o log fecha no meio
// da gravação, o quadro não é publicado e o que foi escrito é desfeito.
func TestDiskLogCloseDuringAppendDoesNotPublishTheFrame(t *testing.T) {
	closeDuringFrameWrite := func(l *diskLog) {
		realWrite := writeAtFn
		writeAtFn = func(f *os.File, p []byte, off int64) (int, error) {
			if isFrameWrite(p, off) {
				_ = l.close()
			}
			return realWrite(f, p, off)
		}
	}
	t.Run("new segment", func(t *testing.T) {
		seams(t)
		l, dir := newTestLog(t, 4096, 1<<30)
		for i := 0; i < 4; i++ {
			appendSeqs(t, l, 1, frame1000)
		}
		fb, from, to, endPos := frameFor(l, 1, frame1000)
		closeDuringFrameWrite(l)
		if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errLogClosed) {
			t.Fatalf("err = %v, esperado errLogClosed", err)
		}
		if files := segmentFiles(t, dir); len(files) != 1 || l.segments() != 1 {
			t.Fatalf("o segmento novo não publicado deveria ter sido removido: %v, segments() = %d", files, l.segments())
		}
	})
	t.Run("existing segment", func(t *testing.T) {
		seams(t)
		l, dir := newTestLog(t, 1<<20, 1<<30)
		appendSeqs(t, l, 1, 100)
		fb, from, to, endPos := frameFor(l, 1, 100)
		closeDuringFrameWrite(l)
		if _, err := l.append(fb, from, to, endPos); !errors.Is(err, errLogClosed) {
			t.Fatalf("err = %v, esperado errLogClosed", err)
		}
		// O quadro foi desfeito: a reabertura termina no quadro anterior.
		n, err := openDiskLog(dir, 1<<20, 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = n.close() }()
		if err := n.validateTail(testEpoch, 1, 196); err != nil {
			t.Fatalf("o quadro não publicado sobrou no arquivo: %v", err)
		}
	})
}
