package replication

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// testDiskBytes é o teto de disco que os testes usam quando não dizem outro:
// o piso de Options.Validate.
const testDiskBytes = 8 << 20

// withDisk completa as opções de teste com o teto de disco mínimo quando o
// teste não o declara.
func withDisk(o Options) Options {
	if o.MaxDiskBytes == 0 {
		o.MaxDiskBytes = testDiskBytes
	}
	return o
}

// openFeedAt abre e retoma um feed em dir, com impressão vazia, e o fecha no
// fim do teste (Close sem marca limpa e Wait).
func openFeedAt(t *testing.T, dir string, o Options) *Stream {
	t.Helper()
	s, err := Open(dir, withDisk(o))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Resume(Fingerprint{}); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close(nil, 5*time.Second)
		s.Wait()
	})
	return s
}

// openTestFeed abre um feed num diretório novo.
func openTestFeed(t *testing.T, o Options) *Stream {
	t.Helper()
	return openFeedAt(t, filepath.Join(t.TempDir(), "feed"), o)
}

// stopWriterWithoutState simula o processo morto: para o gravador e fecha o
// log sem tocar no `state` (que fica como o Resume o deixou, sem a marca
// limpa). Depois dele o feed está fechado, como se não existisse mais.
func (s *Stream) stopWriterWithoutState() {
	s.mu.Lock()
	s.closed = true
	s.writerAbort = true
	s.kickWriterLocked()
	s.notifyLocked()
	done, disk := s.writerDone, s.disk
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	s.mu.Lock()
	s.logClosed = true
	s.mu.Unlock()
	if disk != nil {
		_ = disk.close()
	}
}

// waitWriterIdle espera o gravador esvaziar a fila e ligar o log à geração
// corrente (wipe e bind de uma invalidação ou rotação incluídos).
func (s *Stream) waitWriterIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s.mu.Lock()
		idle := len(s.queue) == 0 && s.writerGen == s.gen
		s.mu.Unlock()
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("o gravador não ficou ocioso em 10s")
		}
		time.Sleep(time.Millisecond)
	}
}

// queuedBytes é a memória dos lotes ainda não gravados.
func (s *Stream) queuedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, b := range s.queue {
		n += b.bytes
	}
	return n
}

// lockedBuffer é um io.Writer seguro para o logger de teste.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testLogger devolve um logger que grava tudo em buf.
func testLogger(buf *lockedBuffer) *logging.Logger {
	return logging.New(buf, logging.FormatJSON, slog.LevelDebug)
}

// queuedLen é o número de lotes ainda não gravados.
func (s *Stream) queuedLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}
