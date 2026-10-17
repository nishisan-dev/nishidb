package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// testFP é a impressão dos Stores que os testes gravam no fechamento limpo.
var testFP = Fingerprint{Stores: []StorePosition{{Name: "events", Segment: 3, Offset: 4096, NextRowID: 10}}}

// diskOpts são opções com intervalo longo: só o Flush, os limiares e o Close
// fecham lotes.
func diskOpts() Options {
	return Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 3 << 20, MaxDiskBytes: testDiskBytes}
}

// openDir abre o feed em dir e o retoma com fp, sem limpeza automática.
func openDir(t *testing.T, dir string, o Options, fp Fingerprint) (*Stream, Origin) {
	t.Helper()
	s, err := Open(dir, o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	origin, err := s.Resume(fp)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	return s, origin
}

// closeClean fecha com a impressão e espera o fim da vida do feed.
func closeClean(t *testing.T, s *Stream, fp Fingerprint) {
	t.Helper()
	if err := s.Close(&fp, 5*time.Second); err != nil {
		t.Fatalf("Close limpo: %v", err)
	}
	s.Wait()
}

// readAll lê do feed todas as entradas depois de after até o produzido, com
// contexto curto, e devolve os lotes na ordem.
func readAll(t *testing.T, s *Stream, epoch string, after uint64) []*Batch {
	t.Helper()
	_, produced := s.Cursor()
	var out []*Batch
	for after < produced {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		b, release, err := s.Acquire(ctx, epoch, after)
		cancel()
		if err != nil {
			t.Fatalf("Acquire(%d): %v", after, err)
		}
		if b.From > after+1 || b.To <= after {
			release()
			t.Fatalf("lote [%d,%d] não continua o cursor %d", b.From, b.To, after)
		}
		out = append(out, b)
		after = b.To
		release()
	}
	return out
}

// payloadsBySequence junta as entradas dos lotes por sequence.
func payloadsBySequence(batches []*Batch) map[uint64]string {
	out := map[uint64]string{}
	for _, b := range batches {
		for _, e := range b.Entries {
			out[e.Sequence] = string(e.Record.Payload)
		}
	}
	return out
}

func TestFeedResumesSameEpochAfterCleanClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, origin := openDir(t, dir, diskOpts(), testFP)
	if origin.Kind != OriginFresh || origin.PreviousEpoch != "" || origin.ResumedSequence != 0 {
		t.Fatalf("primeira subida: origin=%+v, quer fresh", origin)
	}
	for i := 1; i <= 10; i++ {
		s.Publish("events", Insert, rec(uint64(i), "v"+strconv.Itoa(i)), time.Unix(int64(i), 0))
		if i%3 == 0 {
			s.Flush(time.Unix(int64(i), 0), "test")
		}
	}
	epoch, seq := s.Cursor()
	closeClean(t, s, testFP)
	// Depois do Close: ele fecha o intervalo ativo (a entrada 10), que avança
	// a posição em bytes.
	_, _, endPos := s.Position()

	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginClean || origin.ResumedSequence != seq || origin.PreviousEpoch != "" {
		t.Fatalf("reabertura limpa: origin=%+v, quer clean com ResumedSequence=%d", origin, seq)
	}
	if e, q := n.Cursor(); e != epoch || q != seq {
		t.Fatalf("cursor depois da retomada = %s/%d, quer %s/%d", e, q, epoch, seq)
	}
	if e, q, p := n.Position(); e != epoch || q != seq || p != endPos {
		t.Fatalf("posição depois da retomada = %s/%d/%d, quer %s/%d/%d", e, q, p, epoch, seq, endPos)
	}
	got := payloadsBySequence(readAll(t, n, epoch, 0))
	for i := 1; i <= 10; i++ {
		if got[uint64(i)] != "v"+strconv.Itoa(i) {
			t.Fatalf("sequence %d lida do disco = %q, quer v%d (lido: %v)", i, got[uint64(i)], i, got)
		}
	}
	if st := n.Status(); st.BufferedBytes != 0 {
		t.Fatalf("lotes lidos do disco entraram na conta do cache: bufferedBytes=%d", st.BufferedBytes)
	}
	if next := n.Publish("events", Update, rec(1, "w"), time.Unix(20, 0)); next != seq+1 {
		t.Fatalf("Publish depois da retomada = %d, quer %d", next, seq+1)
	}
	n.Flush(time.Unix(20, 0), "test")
	b := readAll(t, n, epoch, seq)
	if len(b) != 1 || b[0].From != seq+1 || b[0].EndPos <= endPos {
		t.Fatalf("lote depois da retomada: %+v, quer From=%d e EndPos > %d", b, seq+1, endPos)
	}
}

func TestFeedStartsNewEpochAfterUncleanClose(t *testing.T) {
	// A impressão vazia é a que um Close(nil) gravaria se gravasse a marca
	// limpa: a reabertura com ela só é queda se a marca não foi gravada.
	var noFP Fingerprint
	for _, tc := range []struct {
		name string
		// cleanFirst retoma uma geração limpa antes de parar: prova que o
		// Resume regrava o `state` sem a marca limpa antes de voltar.
		cleanFirst bool
		stop       func(*Stream)
	}{
		{"close_without_fingerprint", false, func(s *Stream) {
			if err := s.Close(nil, 5*time.Second); err != nil {
				t.Fatalf("Close(nil): %v", err)
			}
			s.Wait()
		}},
		{"process_died", false, func(s *Stream) { s.stopWriterWithoutState() }},
		{"process_died_after_clean_resume", true, func(s *Stream) { s.stopWriterWithoutState() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "feed")
			s, _ := openDir(t, dir, diskOpts(), noFP)
			s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
			s.Flush(time.Unix(1, 0), "test")
			s.waitWriterIdle(t)
			if tc.cleanFirst {
				closeClean(t, s, noFP)
				var origin Origin
				s, origin = openDir(t, dir, diskOpts(), noFP)
				if origin.Kind != OriginClean {
					t.Fatalf("preparo: origin=%+v, quer clean", origin)
				}
			}
			old, _ := s.Cursor()
			tc.stop(s)

			n, origin := openDir(t, dir, diskOpts(), noFP)
			t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
			if origin.Kind != OriginCrash || origin.PreviousEpoch != old || origin.ResumedSequence != 0 {
				t.Fatalf("origin=%+v, quer crash com PreviousEpoch=%s", origin, old)
			}
			epoch, seq := n.Cursor()
			if epoch == old || seq != 0 {
				t.Fatalf("depois da queda: epoch=%s seq=%d, quer epoch nova e seq 0", epoch, seq)
			}
			if st := n.Status(); st.DiskSegments != 0 {
				t.Fatalf("a queda não apagou o log: %d segmentos", st.DiskSegments)
			}
			if _, _, err := n.Acquire(context.Background(), old, 0); !errors.Is(err, ErrResyncRequired) {
				t.Fatalf("cursor da epoch velha: err=%v, quer resync", err)
			}
		})
	}
}

func TestFeedTreatsFingerprintMismatchAsCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	old, _ := s.Cursor()
	closeClean(t, s, testFP)
	if st, ok, err := readFeedState(dir); err != nil || !ok || !st.Clean {
		t.Fatalf("o fechamento limpo não gravou clean: %+v ok=%v err=%v", st, ok, err)
	}

	other := Fingerprint{Stores: []StorePosition{{Name: "events", Segment: 3, Offset: 4097, NextRowID: 10}}}
	n, origin := openDir(t, dir, diskOpts(), other)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginCrash || origin.PreviousEpoch != old {
		t.Fatalf("impressão divergente: origin=%+v, quer crash com PreviousEpoch=%s", origin, old)
	}
}

func TestFeedTreatsTornTailAsCrashEvenWhenClean(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	old, _ := s.Cursor()
	closeClean(t, s, testFP)
	var last string
	for name := range segmentFiles(t, dir) {
		last = max(last, name)
	}
	f, err := os.OpenFile(filepath.Join(dir, last), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("NDFR\x40\x00\x00\x00rasgado")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginCrash || origin.PreviousEpoch != old {
		t.Fatalf("cauda rasgada com clean=true: origin=%+v, quer crash", origin)
	}
}

func TestIdleFeedResumesCleanWithEmptyLog(t *testing.T) {
	t.Run("never_published", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		s, _ := openDir(t, dir, diskOpts(), testFP)
		epoch, _ := s.Cursor()
		closeClean(t, s, testFP)
		if files := segmentFiles(t, dir); len(files) != 0 {
			t.Fatalf("master ocioso com segmentos: %v", files)
		}
		n, origin := openDir(t, dir, diskOpts(), testFP)
		t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
		if e, q := n.Cursor(); origin.Kind != OriginClean || e != epoch || q != 0 {
			t.Fatalf("ocioso: origin=%+v cursor=%s/%d, quer clean %s/0", origin, e, q, epoch)
		}
	})
	t.Run("after_invalidation", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		s, _ := openDir(t, dir, diskOpts(), testFP)
		s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
		s.Flush(time.Unix(1, 0), "test")
		s.waitWriterIdle(t)
		s.Invalidate()
		s.waitWriterIdle(t)
		epoch, seq := s.Cursor()
		closeClean(t, s, testFP)
		n, origin := openDir(t, dir, diskOpts(), testFP)
		t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
		if e, q := n.Cursor(); origin.Kind != OriginClean || e != epoch || q != seq {
			t.Fatalf("depois da invalidação: origin=%+v cursor=%s/%d, quer clean %s/%d", origin, e, q, epoch, seq)
		}
		if st := n.Status(); st.AvailableAfter != seq {
			t.Fatalf("piso reaberto = %d, quer %d", st.AvailableAfter, seq)
		}
	})
}

// treeBytes lê todo arquivo sob dir (nome relativo → conteúdo); ausente vira
// mapa nil.
func treeBytes(t *testing.T, dir string) map[string]string {
	t.Helper()
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			out[rel+"/"] = ""
			return nil
		}
		b, err := os.ReadFile(p)
		out[rel] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sameTree(a, b map[string]string) bool {
	if (a == nil) != (b == nil) || len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func TestOpenIsReadOnlyUntilResume(t *testing.T) {
	t.Run("existing_clean_feed", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		s, _ := openDir(t, dir, diskOpts(), testFP)
		s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
		closeClean(t, s, testFP)
		before := treeBytes(t, dir)
		for _, fp := range []*Fingerprint{nil, &testFP} {
			n, err := Open(dir, diskOpts())
			if err != nil {
				t.Fatal(err)
			}
			n.Publish("events", Insert, rec(2, "b"), time.Unix(2, 0))
			n.Rotate("test")
			n.Invalidate()
			if err := n.Close(fp, time.Second); err != nil {
				t.Fatalf("Close antes do Resume: %v", err)
			}
			n.Wait()
			if after := treeBytes(t, dir); !sameTree(before, after) {
				t.Fatalf("Open+Close sem Resume mexeu no diretório:\nantes %v\ndepois %v", before, after)
			}
		}
	})
	t.Run("missing_directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "a", "feed")
		n, err := Open(dir, diskOpts())
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Close(&testFP, time.Second); err != nil {
			t.Fatal(err)
		}
		n.Wait()
		if _, err := os.Stat(filepath.Dir(dir)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Open+Close sem Resume criou o diretório: %v", err)
		}
	})
}

func TestOpenFailsOnRealIOErrorButNotOnCorruptLog(t *testing.T) {
	t.Run("corrupt_log_becomes_crash", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "feed")
		s, _ := openDir(t, dir, diskOpts(), testFP)
		s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
		old, _ := s.Cursor()
		closeClean(t, s, testFP)
		// Nome de segmento que a abertura recusa com errCorruptLog.
		if err := os.WriteFile(filepath.Join(dir, "lixo.seg"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		n, origin := openDir(t, dir, diskOpts(), testFP)
		t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
		if origin.Kind != OriginCrash || origin.PreviousEpoch != old {
			t.Fatalf("log incoerente: origin=%+v, quer crash", origin)
		}
		if files := segmentFiles(t, dir); len(files) != 0 {
			t.Fatalf("a queda não removeu os segmentos do log incoerente: %v", files)
		}
	})
	t.Run("unclean_state_does_not_read_segments", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root lê arquivo sem permissão")
		}
		dir := filepath.Join(t.TempDir(), "feed")
		s, _ := openDir(t, dir, diskOpts(), testFP)
		s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
		if err := s.Close(nil, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		s.Wait()
		// Sem a marca limpa o Resume apaga tudo: um segmento ilegível não
		// pode derrubar o Open.
		if err := os.Chmod(filepath.Join(dir, segmentFileName(1)), 0); err != nil {
			t.Fatal(err)
		}
		n, origin := openDir(t, dir, diskOpts(), testFP)
		t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
		if origin.Kind != OriginCrash {
			t.Fatalf("origin=%+v, quer crash", origin)
		}
	})
	t.Run("unreadable_state_fails_open", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root lê arquivo sem permissão")
		}
		dir := filepath.Join(t.TempDir(), "feed")
		s, _ := openDir(t, dir, diskOpts(), testFP)
		closeClean(t, s, testFP)
		if err := os.Chmod(filepath.Join(dir, stateFileName), 0); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir, diskOpts()); err == nil || !errors.Is(err, os.ErrPermission) {
			t.Fatalf("Open com state ilegível por permissão: err=%v, quer erro de permissão", err)
		}
	})
}

func TestLatePublishAfterCloseForcesCrashOnReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	logs := &lockedBuffer{}
	o := diskOpts()
	o.Logger = testLogger(logs)
	s, _ := openDir(t, dir, o, testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	old, _ := s.Cursor()
	if err := s.Close(&testFP, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if st, _, _ := readFeedState(dir); !st.Clean {
		t.Fatal("o Close limpo não gravou clean")
	}
	s.Publish("events", Update, rec(1, "tarde"), time.Unix(2, 0))
	s.Wait()
	if st, _, _ := readFeedState(dir); st.Clean {
		t.Fatal("o Publish tardio não derrubou a marca clean")
	}
	if !strings.Contains(logs.String(), `"replication.feed.publish_after_close"`) {
		t.Fatalf("sem ERROR do Publish tardio:\n%s", logs.String())
	}
	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginCrash || origin.PreviousEpoch != old {
		t.Fatalf("depois do Publish tardio: origin=%+v, quer crash", origin)
	}
}

func TestLatePublishDuringCloseNeverEndsClean(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	releaseLate := make(chan struct{})
	// O Publish acontece entre a drenagem do gravador e a gravação do state;
	// a goroutine do Publish tardio fica presa ANTES de gravar, para que o
	// teste enxergue o que o próprio Close deixou no disco.
	s.closeStateHook = func() { s.Publish("events", Update, rec(1, "tarde"), time.Unix(2, 0)) }
	s.lateStateHook = func() { <-releaseLate }
	err := s.Close(&testFP, 5*time.Second)
	if err == nil {
		t.Error("Close com Publish tardio no meio devolveu nil")
	}
	if st, _, _ := readFeedState(dir); st.Clean {
		t.Fatal("o Close terminou com clean=true no disco apesar do Publish tardio")
	}
	close(releaseLate)
	s.Wait()
	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginCrash {
		t.Fatalf("origin=%+v, quer crash", origin)
	}
}

func TestPublishBeforeResumeForcesCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	old, _ := s.Cursor()
	closeClean(t, s, testFP)

	logs := &lockedBuffer{}
	o := diskOpts()
	o.Logger = testLogger(logs)
	n, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if got := n.Publish("events", Update, rec(1, "cedo"), time.Unix(2, 0)); got != 0 {
		t.Fatalf("Publish antes do Resume registrou a sequence %d", got)
	}
	origin, err := n.Resume(testFP)
	if err != nil {
		t.Fatal(err)
	}
	if origin.Kind != OriginCrash || origin.PreviousEpoch != old {
		t.Fatalf("Publish antes do Resume: origin=%+v, quer crash com PreviousEpoch=%s", origin, old)
	}
	if !strings.Contains(logs.String(), `"replication.feed.publish_before_resume"`) {
		t.Fatalf("sem ERROR do Publish antes do Resume:\n%s", logs.String())
	}
}

// blockWriter troca o gancho de gravação por um que segura o gravador até o
// fim do teste. entered recebe um sinal por gravação que chegou ao gancho.
func blockWriter(t *testing.T, s *Stream) (entered <-chan struct{}, release func()) {
	t.Helper()
	in := make(chan struct{}, 1024)
	gate := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(gate) }) }
	s.mu.Lock()
	s.writeHook = func([]byte) error {
		select {
		case in <- struct{}{}:
		default:
		}
		<-gate
		return nil
	}
	s.mu.Unlock()
	// Registrado DEPOIS do Cleanup do openTestFeed: roda antes dele (LIFO),
	// senão o Close e o Wait esperariam um gravador preso para sempre.
	t.Cleanup(rel)
	return in, rel
}

func TestPublishNeverBlocksWhenTheWriterStalls(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 64 << 10, MaxBufferedBytes: 192 << 10}
	s := openTestFeed(t, o)
	blockWriter(t, s)
	payload := strings.Repeat("x", 1024)
	done := make(chan string, 1)
	go func() {
		for i := range 20000 {
			s.Publish("events", Insert, rec(uint64(i), payload), time.Unix(1, 0))
			if i%64 == 0 {
				st := s.Status()
				if q := s.queuedBytes(); st.BufferedBytes > o.MaxBufferedBytes || q > st.BufferedBytes {
					done <- "memória do feed fora do teto: buffered=" + strconv.FormatInt(st.BufferedBytes, 10) + " fila=" + strconv.FormatInt(q, 10)
					return
				}
			}
		}
		done <- ""
	}()
	select {
	case msg := <-done:
		if msg != "" {
			t.Fatal(msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a rajada de Publish não terminou em 10s com o gravador preso")
	}
	if st := s.Status(); st.Invalidations == 0 {
		t.Fatalf("o gravador preso não levou à lacuna: %+v", st)
	}
}

func TestRepeatedInvalidationsReleaseQueuedBatches(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 100000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 8 << 20}
	s := openTestFeed(t, o)
	blockWriter(t, s)
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse
	}
	base := heap()
	payload := strings.Repeat("y", 64<<10)
	row := uint64(0)
	for round := 1; round <= 5; round++ {
		for s.Status().Invalidations < uint64(round) {
			row++
			s.Publish("events", Insert, rec(row, payload), time.Unix(1, 0))
		}
		if h := heap(); h > base+2*uint64(o.MaxBufferedBytes) {
			t.Fatalf("rodada %d: HeapInuse=%d MiB, base %d MiB: a invalidação não soltou os lotes da fila", round, h>>20, base>>20)
		}
	}
}

func TestInvalidatedBatchIsNeverWrittenAfterInvalidation(t *testing.T) {
	t.Run("before_the_append", func(t *testing.T) {
		seams(t)
		var frameWrites atomic.Int32
		realWrite := writeAtFn
		writeAtFn = func(f *os.File, p []byte, off int64) (int, error) {
			if isFrameWrite(p, off) {
				frameWrites.Add(1)
			}
			return realWrite(f, p, off)
		}
		dir := filepath.Join(t.TempDir(), "feed")
		s := openFeedAt(t, dir, diskOpts())
		entered, release := blockWriter(t, s)
		s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
		s.Flush(time.Unix(1, 0), "test")
		<-entered
		s.Invalidate()
		release()
		s.waitWriterIdle(t)
		if n := frameWrites.Load(); n != 0 {
			t.Fatalf("o lote invalidado foi gravado %d vez(es) depois da invalidação", n)
		}
		if files := segmentFiles(t, dir); len(files) != 0 {
			t.Fatalf("sobrou segmento do lote invalidado: %v", files)
		}
	})
	t.Run("inside_the_write", func(t *testing.T) {
		seams(t)
		inWrite := make(chan struct{}, 1)
		gate := make(chan struct{})
		var blocked atomic.Bool
		realWrite := writeAtFn
		writeAtFn = func(f *os.File, p []byte, off int64) (int, error) {
			if isFrameWrite(p, off) && blocked.CompareAndSwap(false, true) {
				inWrite <- struct{}{}
				<-gate
			}
			return realWrite(f, p, off)
		}
		dir := filepath.Join(t.TempDir(), "feed")
		s := openFeedAt(t, dir, diskOpts())
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(gate) }) })
		s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
		s.Flush(time.Unix(1, 0), "test")
		<-inWrite
		s.Invalidate()
		once.Do(func() { close(gate) })
		s.waitWriterIdle(t)
		if files := segmentFiles(t, dir); len(files) != 0 {
			t.Fatalf("o lote que estava sendo gravado na invalidação sobreviveu no disco: %v", files)
		}
		if st := s.Status(); st.DiskSegments != 0 || st.DiskBytes != 0 {
			t.Fatalf("o log ficou com o lote invalidado: %+v", st)
		}
	})
}

func TestDiskWriteErrorRetriesAndRecovers(t *testing.T) {
	seams(t)
	dir := filepath.Join(t.TempDir(), "feed")
	logs := &lockedBuffer{}
	o := diskOpts()
	o.Logger = testLogger(logs)
	s, _ := openDir(t, dir, o, testFP)
	t.Cleanup(func() { _ = s.Close(nil, time.Second); s.Wait() })
	// Três escritas que gravam lixo além do fim e devolvem ENOSPC: o quadro
	// parcial tem de ser truncado antes de cada nova tentativa.
	realWrite := writeAtFn
	var failures atomic.Int32
	writeAtFn = func(f *os.File, p []byte, off int64) (int, error) {
		if isFrameWrite(p, off) && failures.Load() < 3 {
			failures.Add(1)
			_, _ = f.WriteAt(bytes.Repeat([]byte{0xEE}, len(p)/2), off)
			return len(p) / 2, syscall.ENOSPC
		}
		return realWrite(f, p, off)
	}
	for i := 1; i <= 5; i++ {
		s.Publish("events", Insert, rec(uint64(i), "v"+strconv.Itoa(i)), time.Unix(int64(i), 0))
	}
	s.Flush(time.Unix(5, 0), "test")
	s.waitWriterIdle(t)
	st := s.Status()
	if st.DiskErrors != 3 || st.Invalidations != 0 {
		t.Fatalf("depois de 3 ENOSPC: diskErrors=%d invalidations=%d, quer 3 e 0", st.DiskErrors, st.Invalidations)
	}
	if n := strings.Count(logs.String(), `"replication.feed.disk_error"`); n != 1 {
		t.Fatalf("WARN de erro de disco saiu %d vezes em menos de 5s, quer 1:\n%s", n, logs.String())
	}
	epoch, _ := s.Cursor()
	closeClean(t, s, testFP)
	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginClean {
		t.Fatalf("o lixo das escritas que falharam ficou no log: origin=%+v", origin)
	}
	got := payloadsBySequence(readAll(t, n, epoch, 0))
	for i := 1; i <= 5; i++ {
		if got[uint64(i)] != "v"+strconv.Itoa(i) {
			t.Fatalf("conteúdo depois da retentativa: %v", got)
		}
	}
}

// O backoff do gravador não encurta com lote novo, nem depois de uma mudança
// de geração: com o disco falhando e a ingestão seguindo, as retentativas
// ficam no ritmo de 100 ms, 200 ms, 400 ms...
func TestDiskRetryBackoffIgnoresNewBatches(t *testing.T) {
	seams(t)
	realWrite := writeAtFn
	var failing atomic.Bool
	failing.Store(true)
	writeAtFn = func(f *os.File, p []byte, off int64) (int, error) {
		if isFrameWrite(p, off) && failing.Load() {
			return 0, syscall.ENOSPC
		}
		return realWrite(f, p, off)
	}
	s := openTestFeed(t, diskOpts())
	t.Cleanup(func() { failing.Store(false) })
	publishFor := func(d time.Duration) {
		for end := time.Now().Add(d); time.Now().Before(end); {
			s.Publish("events", Insert, rec(1, "x"), time.Unix(1, 0))
			s.Flush(time.Unix(1, 0), "test")
			time.Sleep(5 * time.Millisecond)
		}
	}
	publishFor(350 * time.Millisecond) // tentativas em ~0, 100, 300 ms
	// A invalidação acorda o gravador (geração nova); depois dela o ritmo
	// recomeça, mas não dispara.
	s.Invalidate()
	publishFor(350 * time.Millisecond)
	if n := s.Status().DiskErrors; n > 10 {
		t.Fatalf("%d falhas de disco em 700ms: o backoff encurtou com lote novo ou depois do aviso de geração", n)
	}
}

// fillEvictedFeed publica n entradas (uma por lote, Interval 0) num feed com
// cache pequeno e espera o gravador: o grosso fica só no disco.
func fillEvictedFeed(t *testing.T, n int) (*Stream, Options) {
	t.Helper()
	o := Options{Interval: 0, MaxMessages: 100, MaxBatchBytes: 16 << 10, MaxBufferedBytes: 48 << 10}
	s := openTestFeed(t, o)
	for i := 1; i <= n; i++ {
		s.Publish("events", Insert, rec(uint64(i), "p"+strconv.Itoa(i)), time.Unix(int64(i), 0))
		if i%50 == 0 {
			s.waitWriterIdle(t)
		}
	}
	s.waitWriterIdle(t)
	if st := s.Status(); st.Invalidations != 0 || st.EvictedBatches == 0 {
		t.Fatalf("preparo: %+v, quer despejo sem invalidação", st)
	}
	return s, o
}

func TestAcquireMergesConsecutiveDiskFrames(t *testing.T) {
	s, o := fillEvictedFeed(t, 1000)
	epoch, _ := s.Cursor()
	batches := readAll(t, s, epoch, 0)
	if len(batches) >= 500 {
		t.Fatalf("%d Acquires para 1000 entradas de um quadro cada: os quadros do disco não foram agrupados", len(batches))
	}
	if b := batches[0]; b.From != 1 || len(b.Entries) < 2 {
		t.Fatalf("primeiro lote do disco: [%d,%d] com %d entradas, quer From=1 e mais de uma", b.From, b.To, len(b.Entries))
	}
	var prevTo, prevEnd uint64
	for _, b := range batches {
		if b.From != prevTo+1 || b.Messages != int(b.To-b.From+1) || b.Messages > o.MaxMessages || b.bytes > o.MaxBatchBytes || b.EndPos <= prevEnd {
			t.Fatalf("lote [%d,%d] messages=%d bytes=%d endPos=%d depois de %d/%d", b.From, b.To, b.Messages, b.bytes, b.EndPos, prevTo, prevEnd)
		}
		prevTo, prevEnd = b.To, b.EndPos
	}
	got := payloadsBySequence(batches)
	for i := 1; i <= 1000; i++ {
		if got[uint64(i)] != "p"+strconv.Itoa(i) {
			t.Fatalf("sequence %d = %q", i, got[uint64(i)])
		}
	}
}

// Depois do Close os leitores continuam sendo servidos do disco até o Wait,
// que fecha o log; a partir dele o que estava só no disco é ErrClosed.
func TestReadersDrainFromDiskAfterCloseUntilWait(t *testing.T) {
	s, _ := fillEvictedFeed(t, 300)
	epoch, _ := s.Cursor()
	if err := s.Close(nil, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := payloadsBySequence(readAll(t, s, epoch, 0)); len(got) != 300 || got[1] != "p1" {
		t.Fatalf("drenagem do disco depois do Close: %d entradas, 1=%q", len(got), got[1])
	}
	s.Wait()
	if _, _, err := s.Acquire(context.Background(), epoch, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("cursor só no disco depois do Wait: err=%v, quer ErrClosed", err)
	}
}

func TestDiskReadDoesNotHoldPublishLock(t *testing.T) {
	s, _ := fillEvictedFeed(t, 300)
	epoch, _ := s.Cursor()
	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	s.mu.Lock()
	s.readHook = func() {
		once.Do(func() { close(entered) })
		<-gate
	}
	s.mu.Unlock()
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)
	read := make(chan error, 1)
	go func() {
		_, release, err := s.Acquire(context.Background(), epoch, 0)
		if err == nil {
			release()
		}
		read <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("o leitor não chegou à leitura do disco")
	}
	published := make(chan struct{})
	go func() {
		s.Publish("events", Insert, rec(9999, "z"), time.Unix(1, 0))
		close(published)
	}()
	select {
	case <-published:
	case <-time.After(2 * time.Second):
		openGate()
		t.Fatal("o Publish ficou bloqueado enquanto um leitor lia do disco")
	}
	openGate()
	if err := <-read; err != nil {
		t.Fatalf("leitura do disco: %v", err)
	}
}

func TestEvictingPersistedBatchDoesNotCreateGap(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 4 << 10, MaxBufferedBytes: 12 << 10}
	s := openTestFeed(t, o)
	for i := 1; i <= 200; i++ {
		s.Publish("events", Insert, rec(uint64(i), "e"+strconv.Itoa(i)), time.Unix(int64(i), 0))
		if i%5 == 0 {
			s.Flush(time.Unix(int64(i), 0), "test")
			s.waitWriterIdle(t)
		}
	}
	st := s.Status()
	if st.EvictedBatches == 0 || st.Invalidations != 0 || st.AvailableAfter != 0 {
		t.Fatalf("despejo de lote gravado: %+v, quer despejo, sem invalidação e piso 0", st)
	}
	epoch, _ := s.Cursor()
	got := payloadsBySequence(readAll(t, s, epoch, 0))
	if len(got) != 200 || got[1] != "e1" || got[200] != "e200" {
		t.Fatalf("conteúdo depois do despejo: %d entradas, 1=%q 200=%q", len(got), got[1], got[200])
	}
}

// O lote ainda não gravado nunca sai da memória: sem espaço, o feed declara
// a lacuna (piso = produzido) em vez de anunciar um intervalo que não serve.
func TestUnpersistedBatchIsNeverEvictedSilently(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 4 << 10, MaxBufferedBytes: 12 << 10}
	s := openTestFeed(t, o)
	blockWriter(t, s)
	for i := 1; i <= 60; i++ {
		s.Publish("events", Insert, rec(uint64(i), strings.Repeat("u", 200)), time.Unix(int64(i), 0))
		if i%5 == 0 {
			s.Flush(time.Unix(int64(i), 0), "test")
		}
	}
	st := s.Status()
	epoch, produced := s.Cursor()
	for after := st.AvailableAfter; after < produced; after++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		b, release, err := s.Acquire(ctx, epoch, after)
		cancel()
		if err != nil {
			t.Fatalf("cursor %d dentro da janela anunciada (%d,%d]: %v", after, st.AvailableAfter, produced, err)
		}
		if b.From > after+1 {
			t.Fatalf("cursor %d recebeu [%d,%d]: buraco invisível", after, b.From, b.To)
		}
		release()
	}
	if st.Invalidations == 0 {
		t.Fatalf("memória cheia de lotes não gravados sem lacuna declarada: %+v", st)
	}
}

func TestDiskReaderBatchIsBoundedPerReader(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 10000, MaxBatchBytes: 256 << 10, MaxBufferedBytes: 1 << 20, MaxDiskBytes: 64 << 20}
	s := openTestFeed(t, o)
	payload := strings.Repeat("d", 8<<10)
	big := strings.Repeat("B", 300<<10)
	var bigSeq uint64
	row := uint64(0)
	for s.Status().DiskBytes < 60<<20 || s.Status().AvailableAfter == 0 {
		if row > 20000 {
			t.Fatalf("o disco não encheu: %+v", s.Status())
		}
		row++
		p := payload
		if row == 5000 {
			p = big
		}
		seq := s.Publish("events", Insert, rec(row, p), time.Unix(1, 0))
		if row == 5000 {
			bigSeq = seq
		}
		if row%30 == 0 {
			s.waitWriterIdle(t)
		}
	}
	s.Flush(time.Unix(1, 0), "test")
	s.waitWriterIdle(t)
	st := s.Status()
	if st.Invalidations != 0 || st.DiskBytes > o.MaxDiskBytes {
		t.Fatalf("preparo: %+v", st)
	}
	if bigSeq <= st.AvailableAfter {
		t.Fatalf("a entrada grande (%d) já saiu pela retenção (piso %d)", bigSeq, st.AvailableAfter)
	}
	epoch, produced := s.Cursor()
	after := st.AvailableAfter
	sawBig := false
	for after < produced {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		b, release, err := s.Acquire(ctx, epoch, after)
		cancel()
		if err != nil {
			t.Fatalf("Acquire(%d): %v", after, err)
		}
		single := len(b.Entries) == 1 && b.Entries[0].Sequence == bigSeq
		sawBig = sawBig || single
		if b.bytes > o.MaxBatchBytes && !single {
			t.Fatalf("lote do leitor [%d,%d] com %d bytes acima de maxBatchBytes sem ser um quadro só", b.From, b.To, b.bytes)
		}
		if buf := s.Status().BufferedBytes; buf > o.MaxBufferedBytes {
			t.Fatalf("bufferedBytes=%d acima do teto durante a leitura do disco", buf)
		}
		after = b.To
		release()
	}
	if !sawBig {
		t.Fatal("o quadro maior que maxBatchBytes não saiu sozinho num lote")
	}
}

func TestRotateForcesResyncAndNewEpoch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	logs := &lockedBuffer{}
	o := diskOpts()
	o.Logger = testLogger(logs)
	s, _ := openDir(t, dir, o, testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	s.Flush(time.Unix(1, 0), "test")
	s.waitWriterIdle(t)
	old, seq := s.Cursor()
	parked := startBlockedAcquire(t, s, old, seq)

	s.Rotate("rewind")
	if r := awaitAcquire(t, parked); !errors.Is(r.err, ErrResyncRequired) {
		t.Fatalf("leitor estacionado na rotação: err=%v, quer resync", r.err)
	}
	epoch, seq := s.Cursor()
	if epoch == old || seq != 0 {
		t.Fatalf("depois do Rotate: %s/%d, quer epoch nova e seq 0", epoch, seq)
	}
	if got := s.Origin(); got != (Origin{Kind: OriginRotated, PreviousEpoch: old}) {
		t.Fatalf("Origin depois do Rotate = %+v", got)
	}
	if _, _, err := s.Acquire(context.Background(), old, 0); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("cursor da epoch velha: err=%v", err)
	}
	s.waitWriterIdle(t)
	if files := segmentFiles(t, dir); len(files) != 0 {
		t.Fatalf("o log da epoch velha sobreviveu à rotação: %v", files)
	}
	if st, _, _ := readFeedState(dir); epochString(st.Epoch) != epoch || st.Clean {
		t.Fatalf("o state não registra a epoch nova: %+v", st)
	}
	if !strings.Contains(logs.String(), `"replication.feed.rotated"`) {
		t.Fatalf("sem log da rotação:\n%s", logs.String())
	}
	s.Publish("events", Insert, rec(2, "b"), time.Unix(2, 0))
	s.Flush(time.Unix(2, 0), "test")
	if b := readAll(t, s, epoch, 0); len(b) != 1 || b[0].From != 1 || b[0].Epoch != epoch {
		t.Fatalf("primeiro lote da epoch nova: %+v", b)
	}
	closeClean(t, s, testFP)
	s.Rotate("tarde")
	if e, _ := s.Cursor(); e != epoch {
		t.Fatal("Rotate depois do Close mudou a epoch")
	}
	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if e, _ := n.Cursor(); origin.Kind != OriginClean || e != epoch {
		t.Fatalf("reabertura depois da rotação: origin=%+v epoch=%s, quer clean %s", origin, e, epoch)
	}
}

func TestCloseDeadlineSkipsCleanMark(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	_, release := blockWriter(t, s)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	start := time.Now()
	if err := s.Close(&testFP, 100*time.Millisecond); err == nil {
		t.Error("Close com o gravador preso devolveu nil")
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("Close esperou %v com prazo de 100ms", waited)
	}
	if st, _, _ := readFeedState(dir); st.Clean {
		t.Fatal("o Close gravou clean sem drenar o gravador")
	}
	release()
	s.Wait()
	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginCrash {
		t.Fatalf("origin=%+v, quer crash", origin)
	}
}

// Piso de memória: com maxBufferedBytes = 3 × maxBatchBytes, os três lotes que
// coexistem (o intervalo ativo, um na gravação e um arrendado já despejado)
// cabem, e o feed nunca invalida com o disco saudável.
func TestMinimumBufferedBytesNeverInvalidatesWithHealthyDisk(t *testing.T) {
	const batch = 64 << 10
	o := Options{Interval: time.Minute, MaxMessages: 100000, MaxBatchBytes: batch, MaxBufferedBytes: 3 * batch}
	s := openTestFeed(t, o)
	// O gravador só termina o lote k quando o teste deixa: enquanto isso o
	// intervalo k+1 enche e o leitor segura o lote k-1.
	writing := make(chan struct{}, 1)
	proceed := make(chan struct{})
	stop := make(chan struct{})
	s.mu.Lock()
	s.writeHook = func([]byte) error {
		writing <- struct{}{}
		select {
		case <-proceed:
		case <-stop:
		}
		return nil
	}
	s.mu.Unlock()
	t.Cleanup(func() { close(stop) })
	// entryBytes = 192 + len("events") + 0 + len(payload) = 1024: 64 por lote.
	payload := strings.Repeat("h", 1024-192-len("events"))
	perBatch := batch / 1024
	epoch, _ := s.Cursor()
	row := uint64(0)
	publish := func(n int) {
		for range n {
			row++
			s.Publish("events", Insert, rec(row, payload), time.Unix(1, 0))
		}
	}
	var leased func()
	var leasedTo uint64
	publish(perBatch) // o lote 1 fecha cheio e vai para a gravação
	for round := 0; round < 20; round++ {
		<-writing // o lote k está na gravação
		// O intervalo k+1 enche até fechar. A última entrada chega com o lote
		// k-1 arrendado, o k na gravação e o k+1 cheio: 3 × maxBatchBytes.
		publish(perBatch)
		if st := s.Status(); st.Invalidations != 0 {
			t.Fatalf("rodada %d: invalidou no pico (%+v)", round, st)
		}
		proceed <- struct{}{} // o lote k fica gravado; o k+1 entra na gravação
		for s.queuedLen() != 1 {
			time.Sleep(time.Millisecond)
		}
		if leased != nil {
			leased()
		}
		b, release, err := s.Acquire(context.Background(), epoch, leasedTo)
		if err != nil {
			t.Fatalf("rodada %d: Acquire(%d): %v", round, leasedTo, err)
		}
		leased, leasedTo = release, b.To
	}
	if leased != nil {
		leased()
	}
	if st := s.Status(); st.Invalidations != 0 {
		t.Fatalf("com o piso de 3× e disco saudável o feed invalidou: %+v", st)
	}
}

func TestCorruptSealedFrameForcesResync(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	payload := strings.Repeat("c", 4<<10)
	for i := 1; i <= 600; i++ {
		s.Publish("events", Insert, rec(uint64(i), payload), time.Unix(1, 0))
		if i%10 == 0 {
			s.Flush(time.Unix(1, 0), "test")
			s.waitWriterIdle(t)
		}
	}
	epoch, _ := s.Cursor()
	closeClean(t, s, testFP)
	files := segmentFiles(t, dir)
	if len(files) < 2 {
		t.Fatalf("preparo sem segmento selado: %v", files)
	}
	first := filepath.Join(dir, segmentFileName(1))
	f, err := os.OpenFile(first, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, segmentHeaderSize+frameHeaderSize+100); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginClean {
		t.Fatalf("a abertura conferiu o CRC do segmento selado: origin=%+v", origin)
	}
	if _, _, err := n.Acquire(context.Background(), epoch, 0); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("quadro corrompido num segmento selado: err=%v, quer resync", err)
	}
	st := n.Status()
	if st.Invalidations != 1 || st.AvailableAfter != st.Produced {
		t.Fatalf("a corrupção não virou lacuna: %+v", st)
	}
}

// O leitor que já arrendou um segmento termina de lê-lo mesmo que a retenção
// o apague no meio; o cursor seguinte, abaixo do piso, é resync.
func TestDiskReaderSurvivesRetentionOfItsSegment(t *testing.T) {
	// Cache pequeno: o cursor 0 só é servido pelo disco.
	s := openTestFeed(t, Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 64 << 10, MaxBufferedBytes: 192 << 10})
	payload := strings.Repeat("r", 4<<10)
	row := uint64(0)
	publishBatches := func(n int) {
		for i := 0; i < n; i++ {
			for range 10 {
				row++
				s.Publish("events", Insert, rec(row, payload), time.Unix(1, 0))
			}
			s.Flush(time.Unix(1, 0), "test")
			s.waitWriterIdle(t)
		}
	}
	publishBatches(30)
	epoch, _ := s.Cursor()
	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	s.mu.Lock()
	s.readHook = func() {
		once.Do(func() { close(entered) })
		<-gate
	}
	s.mu.Unlock()
	type result struct {
		b   *Batch
		err error
	}
	out := make(chan result, 1)
	go func() {
		b, release, err := s.Acquire(context.Background(), epoch, 0)
		if err == nil {
			release()
		}
		out <- result{b, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("o leitor não chegou à leitura do disco")
	}
	s.mu.Lock()
	s.readHook = nil
	s.mu.Unlock()
	publishBatches(250) // ~10 MiB: a retenção apaga o primeiro segmento
	if st := s.Status(); st.AvailableAfter == 0 {
		t.Fatalf("a retenção não apagou o segmento do leitor: %+v", st)
	}
	close(gate)
	r := <-out
	if r.err != nil || r.b.From != 1 || len(r.b.Entries) == 0 || string(r.b.Entries[0].Record.Payload) != payload {
		t.Fatalf("leitura do segmento apagado: err=%v", r.err)
	}
	if _, _, err := s.Acquire(context.Background(), epoch, r.b.To); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("cursor abaixo do piso depois da retenção: err=%v", err)
	}
}

// A retenção do DISCO é o que cria a lacuna: o cursor que ela apagou é resync,
// sem invalidação.
func TestAcquireRequiresResyncAfterRetentionGap(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 64 << 10, MaxBufferedBytes: 192 << 10}
	s := openTestFeed(t, o)
	payload := strings.Repeat("g", 4<<10)
	// ~10 MiB, acima dos 8 MiB do disco.
	for i := 1; i <= 2500; i++ {
		s.Publish("events", Insert, rec(uint64(i), payload), time.Unix(1, 0))
		if i%10 == 0 {
			s.Flush(time.Unix(1, 0), "test")
			s.waitWriterIdle(t)
		}
	}
	epoch, _ := s.Cursor()
	if st := s.Status(); st.AvailableAfter == 0 {
		t.Fatalf("10 MiB num disco de 8 MiB sem o piso subir: %+v", st)
	}
	if err := s.Check(epoch, 0); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("Check do cursor apagado pela retenção: err=%v, quer resync", err)
	}
	if _, _, err := s.Acquire(context.Background(), epoch, 0); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("cursor apagado pela retenção: err=%v, quer resync", err)
	}
	if st := s.Status(); st.Invalidations != 0 || st.DiskBytes > testDiskBytes {
		t.Fatalf("a retenção virou invalidação ou passou do teto: %+v", st)
	}
}

// O fechamento por bytes ANTES de acrescentar a entrada termina o lote na
// sequence anterior: o lote seguinte começa exatamente na entrada que não
// coube, e nenhuma entrada fica com sequence <= To de um lote que não a tem.
func TestBytesFlushClosesBatchAtPreviousSequence(t *testing.T) {
	// entryBytes = 192 + len("events") + len("pN") = 200: cabem duas por lote.
	s := openTestFeed(t, Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 500, MaxBufferedBytes: 1500})
	for i := 1; i <= 4; i++ {
		s.Publish("events", Insert, rec(uint64(i), "p"+strconv.Itoa(i)), time.Unix(int64(i), 0))
	}
	s.Flush(time.Unix(4, 0), "test")
	epoch, _ := s.Cursor()
	batches := readAll(t, s, epoch, 0)
	type span struct{ from, to, first, last uint64 }
	var got []span
	for _, b := range batches {
		got = append(got, span{b.From, b.To, b.Entries[0].Sequence, b.Entries[len(b.Entries)-1].Sequence})
	}
	want := []span{{1, 2, 1, 2}, {3, 4, 3, 4}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("lotes = %+v, quer %+v", got, want)
	}
}

func TestOptionsValidateEnforcesMemoryAndDiskFloors(t *testing.T) {
	ok := Options{Interval: 0, MaxMessages: 1, MaxBatchBytes: 4 << 20, MaxBufferedBytes: 12 << 20, MaxDiskBytes: 16 << 20}
	if err := ok.Validate(); err != nil {
		t.Fatalf("opções nos pisos exatos recusadas: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*Options)
	}{
		{"buffered_below_3x", func(o *Options) { o.MaxBufferedBytes = 12<<20 - 1 }},
		{"disk_below_4x", func(o *Options) { o.MaxDiskBytes = 16<<20 - 1 }},
		{"disk_below_8MiB", func(o *Options) { o.MaxBatchBytes = 1; o.MaxBufferedBytes = 3; o.MaxDiskBytes = 8<<20 - 1 }},
		{"disk_absent", func(o *Options) { o.MaxDiskBytes = 0 }},
	} {
		o := ok
		tc.edit(&o)
		if err := o.Validate(); err == nil {
			t.Errorf("%s: %+v aceito", tc.name, o)
		}
	}
	small := Options{MaxMessages: 1, MaxBatchBytes: 1, MaxBufferedBytes: 3, MaxDiskBytes: 8 << 20}
	if err := small.Validate(); err != nil {
		t.Fatalf("piso absoluto de 8 MiB recusado: %v", err)
	}
}

func TestDefaultOptionsAreTheConfigurationDefaults(t *testing.T) {
	want := Options{Interval: 5 * time.Second, MaxMessages: 10000, MaxBatchBytes: 8 << 20, MaxBufferedBytes: 64 << 20, MaxDiskBytes: 2 << 30}
	if got := DefaultOptions(); got != want {
		t.Fatalf("DefaultOptions() = %+v, quer %+v", got, want)
	}
	if err := want.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentBytesIsClampedFromMaxDiskBytes(t *testing.T) {
	for _, tc := range []struct{ disk, want int64 }{
		{8 << 20, 1 << 20},
		{80 << 20, 10 << 20},
		{512 << 20, 64 << 20},
		{2 << 30, 64 << 20},
	} {
		if got := segmentBytesFor(tc.disk); got != tc.want {
			t.Errorf("segmentBytesFor(%d) = %d, quer %d", tc.disk, got, tc.want)
		}
	}
}

// O zero de Options vale o legado (histórica publicada): o cursor não
// anuncia exclusão nenhuma, e a chave nem aparece no wire.
func TestStatusAnnouncesHistoryExclusion(t *testing.T) {
	for _, exclude := range []bool{false, true} {
		o := diskOpts()
		o.ExcludeHistory = exclude
		s := openTestFeed(t, o)
		st := s.Status()
		raw, err := json.Marshal(st)
		if err != nil {
			t.Fatal(err)
		}
		if st.HistoryExcluded != exclude || strings.Contains(string(raw), `"historyExcluded"`) != exclude {
			t.Fatalf("excludeHistory=%v: historyExcluded=%v json=%s", exclude, st.HistoryExcluded, raw)
		}
	}
}

// Um registro maior que o teto do quadro (tabela acima de 65.535 bytes) vira
// lacuna no gravador, nunca pânico nem log travado.
func TestFrameTooLargeBecomesGap(t *testing.T) {
	s := openTestFeed(t, diskOpts())
	s.Publish(strings.Repeat("t", 70000), Insert, types.Record{RowID: 1, Payload: []byte("x")}, time.Unix(1, 0))
	s.Flush(time.Unix(1, 0), "test")
	s.waitWriterIdle(t)
	st := s.Status()
	if st.Invalidations != 1 || st.AvailableAfter != st.Produced {
		t.Fatalf("quadro grande demais: %+v, quer uma invalidação", st)
	}
	s.Publish("events", Insert, rec(2, "y"), time.Unix(2, 0))
	s.Flush(time.Unix(2, 0), "test")
	s.waitWriterIdle(t)
	epoch, seq := s.Cursor()
	if b := readAll(t, s, epoch, seq-1); len(b) != 1 || string(b[0].Entries[0].Record.Payload) != "y" {
		t.Fatalf("o feed não seguiu depois da lacuna: %+v", b)
	}
}

// Um Publish que chega DURANTE o I/O do Resume derruba a retomada limpa já
// decidida: o Resume refaz a decisão como queda.
func TestPublishDuringResumeForcesCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	old, _ := s.Cursor()
	closeClean(t, s, testFP)

	logs := &lockedBuffer{}
	o := diskOpts()
	o.Logger = testLogger(logs)
	n, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	var once sync.Once
	decisions := 0
	n.resumeHook = func() {
		decisions++
		once.Do(func() { n.Publish("events", Update, rec(1, "concorrente"), time.Unix(2, 0)) })
	}
	origin, err := n.Resume(testFP)
	if err != nil {
		t.Fatal(err)
	}
	if origin.Kind != OriginCrash || origin.PreviousEpoch != old {
		t.Fatalf("Publish durante o Resume: origin=%+v, quer crash com PreviousEpoch=%s", origin, old)
	}
	if decisions != 2 {
		t.Fatalf("o Resume decidiu %d vez(es), quer 2 (a limpa e a refeita como queda)", decisions)
	}
	if e, _ := n.Cursor(); e == old {
		t.Fatal("a queda manteve a epoch velha")
	}
	if !strings.Contains(logs.String(), `"replication.feed.publish_before_resume"`) {
		t.Fatalf("sem ERROR do Publish durante o Resume:\n%s", logs.String())
	}
}

// O sync do Close que falha impede a marca limpa: o erro volta ao chamador,
// o `state` fica sem clean e a reabertura é queda.
func TestCloseSyncFailureSkipsCleanMark(t *testing.T) {
	seams(t)
	dir := filepath.Join(t.TempDir(), "feed")
	s, _ := openDir(t, dir, diskOpts(), testFP)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	s.Flush(time.Unix(1, 0), "test")
	s.waitWriterIdle(t)
	realSync := fdatasyncFn
	// Sem rotação, o único fdatasync daqui em diante é o do Close.
	fdatasyncFn = func(*os.File) error { return errInjected }
	err := s.Close(&testFP, 5*time.Second)
	fdatasyncFn = realSync
	s.Wait()
	if !errors.Is(err, errInjected) {
		t.Errorf("Close com sync falho devolveu %v, quer a falha injetada", err)
	}
	if st, _, _ := readFeedState(dir); st.Clean {
		t.Fatal("o Close gravou clean depois de um sync que falhou")
	}
	n, origin := openDir(t, dir, diskOpts(), testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if origin.Kind != OriginCrash {
		t.Fatalf("origin=%+v, quer crash", origin)
	}
}

// fdatasync que falha na rotação deixa o log falho: o feed declara a lacuna
// na hora (sem retentar para sempre), religa o log, volta a gravar e fecha
// limpo dentro do prazo.
func TestDurabilityFailureBecomesGapAndHeals(t *testing.T) {
	seams(t)
	dir := filepath.Join(t.TempDir(), "feed")
	logs := &lockedBuffer{}
	o := Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 128 << 10, MaxBufferedBytes: 384 << 10, MaxDiskBytes: testDiskBytes, Logger: testLogger(logs)}
	s, _ := openDir(t, dir, o, testFP)
	realSync := fdatasyncFn
	var syncs atomic.Int32
	fdatasyncFn = func(f *os.File) error {
		if syncs.Add(1) == 1 {
			return errInjected
		}
		return realSync(f)
	}
	payload := strings.Repeat("s", 4<<10)
	row := uint64(0)
	publishBatch := func() {
		for range 25 {
			row++
			s.Publish("events", Insert, rec(row, payload), time.Unix(1, 0))
		}
		s.Flush(time.Unix(1, 0), "test")
		s.waitWriterIdle(t)
	}
	// ~100 KiB por lote: a rotação do segmento de 1 MiB vem no 11º.
	for syncs.Load() == 0 {
		if row > 20000 {
			t.Fatal("o log não rotacionou")
		}
		publishBatch()
	}
	if st := s.Status(); st.Invalidations != 1 {
		t.Fatalf("falha de fdatasync na rotação: %+v, quer exatamente uma invalidação", st)
	}
	if !strings.Contains(logs.String(), `"cause":"durability"`) {
		t.Fatalf("sem WARN de lacuna por durabilidade:\n%s", logs.String())
	}
	for range 3 {
		publishBatch()
	}
	if st := s.Status(); st.DiskSegments == 0 || st.Invalidations != 1 {
		t.Fatalf("o log não voltou a gravar depois da lacuna: %+v", st)
	}
	epoch, seq := s.Cursor()
	if err := s.Close(&testFP, 2*time.Second); err != nil {
		t.Fatalf("Close depois da lacuna por durabilidade: %v", err)
	}
	s.Wait()
	n, origin := openDir(t, dir, o, testFP)
	t.Cleanup(func() { _ = n.Close(nil, time.Second); n.Wait() })
	if e, q := n.Cursor(); origin.Kind != OriginClean || e != epoch || q != seq {
		t.Fatalf("reabertura: origin=%+v cursor=%s/%d, quer clean %s/%d", origin, e, q, epoch, seq)
	}
}

// Com o cache maior que o disco, o cache vai mais para trás que o piso do
// disco: a janela anunciada começa no cache, e o Acquire o consulta ANTES do
// piso.
func TestCacheReachingBehindTheDiskFloorIsServed(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 100000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 24 << 20, MaxDiskBytes: testDiskBytes}
	s := openTestFeed(t, o)
	payload := strings.Repeat("c", 64<<10)
	for i := 1; i <= 190; i++ { // ~12 MiB: passa do disco, cabe no cache
		s.Publish("events", Insert, rec(uint64(i), payload), time.Unix(1, 0))
	}
	s.Flush(time.Unix(1, 0), "test")
	s.waitWriterIdle(t)
	s.mu.Lock()
	diskFloor := s.floor
	s.mu.Unlock()
	if diskFloor == 0 {
		t.Fatal("preparo: a retenção do disco não subiu o piso")
	}
	epoch, _ := s.Cursor()
	if st := s.Status(); st.AvailableAfter != 0 || st.EvictedBatches != 0 {
		t.Fatalf("janela anunciada com o cache cobrindo tudo: %+v, quer availableAfter 0 (piso do disco %d)", st, diskFloor)
	}
	if err := s.Check(epoch, 0); err != nil {
		t.Fatalf("Check(0) com o cache cobrindo: %v", err)
	}
	b, release, err := s.Acquire(context.Background(), epoch, 0)
	if err != nil {
		t.Fatalf("Acquire(0) abaixo do piso do disco mas no cache: %v", err)
	}
	defer release()
	if b.From != 1 {
		t.Fatalf("lote [%d,%d], quer From=1", b.From, b.To)
	}
}

// Um bodyLen adulterado no meio do segmento é recusado SEM alocar: o leitor
// confere o tamanho contra o fim publicado antes de crescer o buffer.
func TestCorruptFrameLengthIsRejectedWithoutAllocating(t *testing.T) {
	s, _ := fillEvictedFeed(t, 300)
	epoch, _ := s.Cursor()
	f, err := os.OpenFile(filepath.Join(s.dir, segmentFileName(1)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 256 MiB de corpo declarados no primeiro quadro.
	if _, err := f.WriteAt([]byte{0x00, 0x00, 0x00, 0x10}, segmentHeaderSize+4); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, err = s.Acquire(context.Background(), epoch, 0)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("quadro com tamanho adulterado: err=%v, quer resync", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 64<<20 {
		t.Fatalf("a leitura alocou %d MiB por um tamanho de quadro adulterado", grew>>20)
	}
	if st := s.Status(); st.Invalidations != 1 {
		t.Fatalf("o quadro adulterado não virou lacuna: %+v", st)
	}
}

// O segundo Close devolve o MESMO erro do primeiro, em vez de nil.
func TestSecondCloseReturnsTheFirstError(t *testing.T) {
	s := openTestFeed(t, diskOpts())
	_, release := blockWriter(t, s)
	s.Publish("events", Insert, rec(1, "a"), time.Unix(1, 0))
	first := s.Close(&testFP, 50*time.Millisecond)
	if !errors.Is(first, errCloseDeadline) {
		t.Fatalf("primeiro Close = %v, quer errCloseDeadline", first)
	}
	if second := s.Close(&testFP, 5*time.Second); second != first {
		t.Fatalf("segundo Close = %v, quer o mesmo erro do primeiro (%v)", second, first)
	}
	release()
}

// O buffer de codificação do gravador que cresceu para um quadro gigante é
// solto depois do lote: ele não fica retido no heap além de 2 × maxBatchBytes.
func TestWriterReleasesOversizedEncodeBuffer(t *testing.T) {
	o := Options{Interval: time.Minute, MaxMessages: 1000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 24 << 20, MaxDiskBytes: 64 << 20}
	s := openTestFeed(t, o)
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse
	}
	base := heap()
	s.Publish("events", Insert, rec(1, strings.Repeat("G", 16<<20)), time.Unix(1, 0))
	s.Flush(time.Unix(1, 0), "test")
	s.waitWriterIdle(t)
	// Tira o lote do cache: o que sobrar no heap é do gravador.
	s.Invalidate()
	s.waitWriterIdle(t)
	if grew := int64(heap()) - int64(base); grew > 8<<20 {
		t.Fatalf("o heap ficou %d MiB acima da base depois de gravar um quadro de 16 MiB: o buffer do gravador não foi solto", grew>>20)
	}
}
