package replicationkafka

import (
	"errors"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
)

// Com dois destinos no mesmo dataset, o rewind do segundo não rotaciona de
// novo um feed que o primeiro já rotacionou: a segunda rotação derrubaria o
// reset que o primeiro já publicou na geração nova.
func TestRewindRotatesOnlyTheConferredGeneration(t *testing.T) {
	feed, err := replication.Open(t.TempDir(), replication.Options{
		MaxMessages: 10, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 4 << 20, MaxDiskBytes: 8 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Resume(replication.Fingerprint{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = feed.Close(nil, 5*time.Second)
		feed.Wait()
	})
	p := &Publisher{feed: feed}
	conferred, _ := feed.Cursor()
	if err := p.rotate(conferred); err != nil {
		t.Fatal(err)
	}
	first, _ := feed.Cursor()
	if first == conferred {
		t.Fatal("o rewind não rotacionou a geração conferida")
	}
	// O outro destino já rotacionou: a rotação conta como feita, sem outra.
	if err := p.rotate(conferred); err != nil {
		t.Fatalf("rewind de uma geração já rotacionada por outro destino: %v, quer nil", err)
	}
	if again, _ := feed.Cursor(); again != first {
		t.Fatalf("o segundo rewind da mesma geração rotacionou de novo: %s → %s", first, again)
	}
	// Feed fechado na geração conferida: o rewind não pode sair.
	_ = feed.Close(nil, 5*time.Second)
	if err := p.rotate(first); !errors.Is(err, replication.ErrClosed) {
		t.Fatalf("rewind com o feed fechado: %v, quer ErrClosed", err)
	}
}
