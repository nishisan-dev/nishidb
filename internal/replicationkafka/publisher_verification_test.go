package replicationkafka_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
)

// O publicador sobre um feed retomado limpo registra a pendência de
// conferência JÁ no New, antes do Start: o servidor sobe o HTTP entre os
// dois, e é a pendência que impede o follower de retomar o tail antes da
// conferência. Com a leitura do topo do tópico falhando, ela continua; com a
// leitura de volta, a conferência termina, o tail retoma e a pendência sai.
func TestCleanResumeHoldsTheFeedUntilTheRewindCheckEnds(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	var failing atomic.Bool
	failing.Store(true)
	h.o.publisher = topicReadFails(&failing)
	h.open()
	if h.feed.Origin().Kind != replication.OriginClean {
		t.Fatalf("pré-condição: o feed reabriu com a origem %v, quer a limpa", h.feed.Origin().Kind)
	}
	if !h.feed.VerificationPending() {
		t.Fatal("o publicador montado sobre o feed limpo não registrou a pendência antes do Start")
	}
	h.pub.Start()
	waitFor(t, "duas leituras falhas", func() bool { return h.pub.Stats().Errors["topic_read"] >= 2 })
	if !h.feed.VerificationPending() {
		t.Fatal("a pendência saiu com a conferência ainda falhando")
	}
	failing.Store(false)
	expectResume(t, h)
	if h.feed.VerificationPending() {
		t.Fatal("o tail retomou e a pendência continua")
	}
}

// No rewind, a pendência só sai DEPOIS da troca de geração: quem volta a ser
// servido já encontra a epoch nova, e nunca a do backup restaurado.
func TestRewindReleasesTheHoldOnlyWithTheNewGeneration(t *testing.T) {
	h, old, _ := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	var failing atomic.Bool
	failing.Store(true)
	// A epoch do feed NO INSTANTE da soltura, e não depois dela: a rotação
	// logo em seguida esconderia a ordem trocada.
	var atRelease atomic.Value
	h.o.publisher = func(o *replicationkafka.Options) {
		topicReadFails(&failing)(o)
		replicationkafka.WithVerificationReleaseHook(o, func() {
			epoch, _ := h.feed.Cursor()
			atRelease.Store(epoch)
		})
	}
	h.open()
	h.pub.Start()
	waitFor(t, "leitura falhando", func() bool { return h.pub.Stats().Errors["topic_read"] >= 1 })
	if !h.feed.VerificationPending() {
		t.Fatal("o feed restaurado deixou de anunciar a conferência pendente com a leitura falhando")
	}
	failing.Store(false)
	waitFor(t, "pendência solta", func() bool { return !h.feed.VerificationPending() })
	if epoch, _ := atRelease.Load().(string); epoch == old || epoch == "" {
		t.Fatalf("a pendência saiu com o feed na epoch %q (a restaurada é %s)", epoch, old)
	}
	assertRewind(t, h, old)
}

// Só a retomada limpa tem o que conferir: o feed novo e o que nasceu de queda
// já abrem uma geração nova, e o publicador sobre eles não registra pendência
// nenhuma — o HTTP serve os followers desde a subida.
func TestFreshAndCrashFeedsRegisterNoVerificationHold(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3})
	if h.feed.Origin().Kind != replication.OriginFresh || h.feed.VerificationPending() {
		t.Fatalf("feed novo: origem %v, pendente %v; quer fresh sem pendência", h.feed.Origin().Kind, h.feed.VerificationPending())
	}
	h.pub.Start()
	h.readUntil("snapshot da subida", snapshotEnds(3, 1))
	h.stop(false)
	h.open()
	if h.feed.Origin().Kind != replication.OriginCrash || h.feed.VerificationPending() {
		t.Fatalf("feed depois da queda: origem %v, pendente %v; quer crash sem pendência", h.feed.Origin().Kind, h.feed.VerificationPending())
	}
}

// O publicador que nunca chegou ao Start (a subida abortada depois do New)
// solta a pendência no Close: sem isso, nada mais a soltaria.
func TestCloseReleasesTheHoldOfAPublisherNeverStarted(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	h.open()
	if !h.feed.VerificationPending() {
		t.Fatal("pré-condição: o feed limpo sem pendência registrada")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = h.pub.Close(ctx)
	if h.feed.VerificationPending() {
		t.Fatal("o Close do publicador sem Start não soltou a pendência")
	}
}

// A subida que desiste com o feed fechado no meio da conferência (o
// encerramento chegou enquanto a leitura falhava) solta a pendência ao sair,
// antes de qualquer Close do publicador.
func TestStartupAbandonedOnClosedFeedReleasesTheHold(t *testing.T) {
	h, _, _ := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	var failing atomic.Bool
	failing.Store(true)
	h.o.publisher = topicReadFails(&failing)
	h.open()
	h.pub.Start()
	waitFor(t, "leitura falhando", func() bool { return h.pub.Stats().Errors["topic_read"] >= 1 })
	if err := h.feed.Close(&replication.Fingerprint{}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	failing.Store(false)
	waitFor(t, "publicador parado sem decidir", func() bool { return h.pub.Stats().State == replicationkafka.StateStopped })
	if h.feed.VerificationPending() {
		t.Fatal("a subida abandonada deixou a pendência registrada")
	}
}
