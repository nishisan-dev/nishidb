package replicationkafka_test

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// hookProducer chama hook antes de entregar cada registro ao kgo.
type hookProducer struct {
	replicationkafka.Producer
	hook func(*kgo.Record)
}

func (p *hookProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	p.hook(r)
	p.Producer.Produce(ctx, r, promise)
}

// A lacuna no MEIO do snapshot (o feed perde o cursor do reset entre dois
// blocos) aborta o snapshot, e o aborto é visível: snapshot_gap contado,
// LastError e o ERROR de livelock com o dimensionamento. O snapshot seguinte
// (motivo gap) conclui.
func TestSnapshotAbortedByGapIsCountedAndLogged(t *testing.T) {
	var h *harness
	var once sync.Once
	var late []byte
	h = newHarness(t, harnessOpts{partitions: 3,
		publisher: func(o *replicationkafka.Options) {
			o.ChunkRows = 5
			replicationkafka.WithProducerWrapper(o, func(p replicationkafka.Producer) replicationkafka.Producer {
				return &hookProducer{Producer: p, hook: func(r *kgo.Record) {
					// Primeiro registro de linha do snapshot: o bloco está saindo,
					// e a conferência do feed no fim dele vai achar a lacuna.
					if !strings.Contains(string(r.Value), `"op":"snapshot"`) {
						return
					}
					once.Do(func() {
						h.upsertPayload("late", late)
						h.feed.Invalidate()
					})
				}}
			})
		}})
	late = h.alarm("late", 1)
	for i := range 40 {
		h.seed("row-"+strconv.Itoa(i), int64(i))
	}
	h.pub.Start()
	waitFor(t, "snapshot gap concluído", func() bool { return h.pub.Stats().Snapshots["gap"] == 1 })
	s := h.pub.Stats()
	if s.Errors["snapshot_gap"] != 1 || s.Snapshots["startup"] != 0 || !strings.Contains(s.LastError, "RESYNC_REQUIRED") {
		t.Errorf("aborto por lacuna sem rastro nos stats: %+v", s)
	}
	logs := h.logs.String()
	if !strings.Contains(logs, `"replication.kafka.livelock"`) || !strings.Contains(logs, `"stage":"snapshot"`) ||
		!strings.Contains(logs, "maxBufferedBytes") || !strings.Contains(logs, "snapshot_duration") {
		t.Errorf("sem o ERROR de livelock do snapshot abortado:\n%s", logs)
	}
}

// Par negativo do TestLivelockAfterSnapshotIsLogged: depois de um tail
// ocioso por várias conferências, uma lacuna gera o snapshot gap, mas NÃO o
// ERROR de livelock — o snapshot já não é recente.
func TestIdleTailThenGapIsNotLivelock(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, feed: manualFeed,
		publisher: func(o *replicationkafka.Options) { o.PartitionCheckInterval = 20 * time.Millisecond }})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	waitFor(t, "três conferências com o tail ocioso", func() bool {
		return h.countLog("replication.kafka.partitions.checked") >= 3
	})
	h.upsert("x", 1)
	h.feed.Invalidate()
	waitFor(t, "snapshot gap", func() bool { return h.pub.Stats().Snapshots["gap"] == 1 })
	if logs := h.logs.String(); strings.Contains(logs, `"replication.kafka.livelock"`) {
		t.Errorf("ERROR de livelock falso depois de tail ocioso:\n%s", logs)
	}
}

// Um snapshot de centenas de linhas em blocos de 7 põe dois blocos em voo o
// tempo todo: TODAS as linhas chegam, com key e valor certos, na partição do
// murmur2, e o count de cada partição confere.
func TestSnapshotWithManySmallChunksPublishesEveryRow(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3,
		publisher: func(o *replicationkafka.Options) { o.ChunkRows = 7 }})
	const rows = 600
	for i := range rows {
		h.seed("row-"+strconv.Itoa(i), int64(i))
	}
	h.pub.Start()
	all := h.readUntil("snapshot", snapshotEnds(3, 1))
	got := make(map[string]string)
	for p, msgs := range byPartition(all) {
		count := 0
		for _, m := range msgs {
			switch m.op() {
			case "snapshot":
				count++
				if m.partition != partitionOf(m.key, 3) || m.str("key") != m.key {
					t.Errorf("linha %q na partição %d com key do envelope %q", m.key, m.partition, m.str("key"))
				}
				if _, dup := got[m.key]; dup {
					t.Errorf("linha %q publicada duas vezes", m.key)
				}
				rec := m.record()
				id, _ := rec["identifier"].(string)
				sev, _ := rec["sev"].(json.Number)
				got[m.key] = id + "/" + sev.String()
			case "snapshot_end":
				if m.num("count") != uint64(count) {
					t.Errorf("partição %d: snapshot_end count=%d, publicadas %d", p, m.num("count"), count)
				}
			}
		}
	}
	for i := range rows {
		key := "row-" + strconv.Itoa(i)
		if want := key + "/" + strconv.Itoa(i); got[key] != want {
			t.Errorf("linha %q = %q, quer %q", key, got[key], want)
		}
	}
	if len(got) != rows {
		t.Errorf("snapshot com %d linhas, quer %d", len(got), rows)
	}
}

// Uma linha maior que maxMessageBytes no SNAPSHOT bloqueia antes de o bloco
// sair: depois do reset, nenhuma linha e nenhum snapshot_end.
func TestOversizeSnapshotRowBlocksBeforeSnapshotEnd(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3,
		target: func(k *config.KafkaTargetSpec) { k.MaxMessageBytes = 1024 }})
	h.seed("a", 1)
	h.baseSrc.upsert("b", payloadOf(t, h.base, namedValue{"identifier", str("b")},
		namedValue{"persistent", doc(`{"blob":"` + strings.Repeat("x", 2000) + `"}`)}))
	h.seed("c", 3)
	h.pub.Start()
	waitFor(t, "estado blocked", func() bool { return h.pub.Stats().State == replicationkafka.StateBlocked })
	all := h.readAll()
	for _, m := range all {
		if m.op() != "reset" {
			t.Errorf("registro %q (key %q) publicado depois do bloqueio no snapshot", m.op(), m.key)
		}
	}
	if len(all) != 3 {
		t.Errorf("quer só os 3 resets no tópico, lidos %d:\n%s", len(all), dump(all))
	}
	if s := h.pub.Stats(); s.Errors["message_too_large"] != 1 || s.Snapshots["startup"] != 0 {
		t.Errorf("stats do bloqueio no snapshot: %+v", s)
	}
}

// O broker que responde MESSAGE_TOO_LARGE (o max.message.bytes do tópico
// menor que o do destino) deixa o publicador em retrying, contando
// message_too_large; corrigido o tópico, a entrega segue sem reinício.
func TestBrokerMessageTooLargeIsRetriedAndCounted(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, feed: manualFeed,
		publisher: func(o *replicationkafka.Options) {
			replicationkafka.WithKgoOpts(o, kgo.MetadataMinAge(50*time.Millisecond))
		}})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	var failing atomic.Bool
	failing.Store(true)
	h.cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		h.cluster.KeepControl()
		if !failing.Load() {
			return nil, nil, false
		}
		return produceError(req.(*kmsg.ProduceRequest), kerr.MessageTooLarge.Code), nil, true
	})
	seq := h.upsert("k", 1)
	h.feed.Flush(time.Now(), "test")
	waitFor(t, "retrying com message_too_large", func() bool {
		s := h.pub.Stats()
		return s.State == replicationkafka.StateRetrying && s.Errors["message_too_large"] >= 1
	})
	failing.Store(false)
	waitFor(t, "entrega depois de corrigido o tópico", func() bool { return h.pub.Stats().Cursor == seq })
	if s := h.pub.Stats(); s.Errors["publish_error"] != 0 {
		t.Errorf("MESSAGE_TOO_LARGE classificado como publish_error: %+v", s)
	}
}

// O lease é solto logo depois do render: com o broker recusando o lote por
// mais de um ciclo de retentativa, o feed despeja o lote (já gravado no
// disco) e libera a memória dele, sem invalidar o intervalo; o cursor só
// avança com o ack.
func TestLeaseReleasedWhileDeliveryRetries(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3,
		feed:   replication.Options{Interval: 0, MaxMessages: 100, MaxBatchBytes: 2048, MaxBufferedBytes: 6144},
		target: func(k *config.KafkaTargetSpec) { k.Timeout = "1s" },
		publisher: func(o *replicationkafka.Options) {
			replicationkafka.WithKgoOpts(o, kgo.MetadataMinAge(50*time.Millisecond))
		}})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	var failing atomic.Bool
	failing.Store(true)
	var rejected atomic.Int32
	h.cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		h.cluster.KeepControl()
		if !failing.Load() {
			return nil, nil, false
		}
		rejected.Add(1)
		return produceError(req.(*kmsg.ProduceRequest), kerr.NotEnoughReplicas.Code), nil, true
	})
	// Um lote que ocupa quase todo o teto do feed: segurado pelo lease, ele
	// não deixaria espaço para a escrita seguinte.
	big := payloadOf(t, h.base, namedValue{"identifier", str("big")},
		namedValue{"persistent", doc(`{"blob":"` + strings.Repeat("x", 5796) + `"}`)})
	bigSeq := h.upsertPayload("big", big)
	waitFor(t, "um ciclo de retentativa", func() bool {
		s := h.pub.Stats()
		return s.State == replicationkafka.StateRetrying && rejected.Load() >= 2
	})
	// Só lote gravado sai da memória: sem esperar o gravador, a escrita
	// seguinte acharia o lote ainda na fila e invalidaria por outro motivo.
	waitFor(t, "lote gravado no disco", func() bool { return h.feed.Status().DiskBytes > 0 })
	for i := range 4 {
		h.upsert("after-"+strconv.Itoa(i), int64(i))
	}
	st := h.feed.Status()
	if st.Invalidations != 0 || st.BufferedBytes >= int64(len(big)) {
		t.Fatalf("o feed ainda segura o lote em retentativa: %+v", st)
	}
	if s := h.pub.Stats(); s.Cursor >= bigSeq {
		t.Fatalf("cursor %d avançou sobre o lote sem ack (seq %d)", s.Cursor, bigSeq)
	}
	failing.Store(false)
	waitFor(t, "lote entregue", func() bool { return h.pub.Stats().Cursor >= bigSeq })
}
