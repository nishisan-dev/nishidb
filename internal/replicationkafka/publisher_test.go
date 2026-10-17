package replicationkafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// manualFeed é o feed que só fecha lote no Flush (o intervalo não roda sem
// Start): o teste escolhe o que vai junto num lote.
var manualFeed = replication.Options{Interval: time.Second, MaxMessages: 1000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 8 << 20, MaxDiskBytes: 8 << 20}

// A subida publica, em CADA partição: o reset (startup, com o cursor do
// feed), as linhas do snapshot que caem nela e o snapshot_end com a contagem
// DELA. O retain da histórica também vai a todas as partições.
func TestStartsWithResetSnapshotEndOnEveryPartition(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, history: true})
	for i := range 30 {
		h.upsert("alarm-"+strconv.Itoa(i), int64(i))
	}
	for i := range 5 {
		h.historyRow("alarm-"+strconv.Itoa(i), int64(i))
	}
	epoch, cursor0 := h.feed.Cursor()
	h.pub.Start()
	all := h.readUntil("snapshot de subida", snapshotEnds(3, 1))
	parts := byPartition(all)
	total := 0
	for p := range int32(3) {
		msgs := parts[p]
		first := msgs[0]
		if first.op() != "reset" || first.str("reason") != "startup" || first.num("cursor") != cursor0 ||
			first.str("epoch") != epoch || first.key != string(replicationkafka.ControlKey) || first.header != "reset" {
			t.Fatalf("partição %d: o primeiro registro deveria ser o reset startup cursor=%d: %v key=%q header=%q",
				p, cursor0, first.env, first.key, first.header)
		}
		snapshots, retains := 0, 0
		var end *msg
		for i := 1; i < len(msgs); i++ {
			m := msgs[i]
			switch m.op() {
			case "snapshot":
				snapshots++
				if m.num("sequence") != cursor0 {
					t.Errorf("partição %d: snapshot com sequence %d, quer o cursor do reset %d", p, m.num("sequence"), cursor0)
				}
				if got := partitionOf(m.key, 3); got != p {
					t.Errorf("key %q na partição %d, o murmur2 manda para %d", m.key, p, got)
				}
				if m.str("table") == goldenHistory && m.key != m.str("key") {
					t.Errorf("linha da histórica com key Kafka %q e key do envelope %q", m.key, m.str("key"))
				}
			case "retain":
				retains++
				if m.key != string(replicationkafka.ControlKey) || m.num("floor") != 0 {
					t.Errorf("partição %d: retain com key %q floor %d", p, m.key, m.num("floor"))
				}
			case "snapshot_end":
				end = &msgs[i]
			default:
				t.Errorf("partição %d: op %q inesperado no snapshot", p, m.op())
			}
			if end != nil {
				break
			}
		}
		if end == nil {
			t.Fatalf("partição %d sem snapshot_end", p)
		}
		if end.num("count") != uint64(snapshots) || end.num("cursor") != cursor0 || end.key != string(replicationkafka.ControlKey) {
			t.Errorf("partição %d: snapshot_end count=%d cursor=%d key=%q, quer count=%d cursor=%d",
				p, end.num("count"), end.num("cursor"), end.key, snapshots, cursor0)
		}
		if retains != 1 {
			t.Errorf("partição %d: %d retains no snapshot, quer 1", p, retains)
		}
		total += snapshots
	}
	if total != 35 {
		t.Errorf("snapshot publicou %d linhas, quer 35 (30 do base e 5 da histórica)", total)
	}
	s := h.pub.Stats()
	if s.Snapshots["startup"] != 1 || s.Cursor != cursor0 || s.Epoch != epoch || s.State != replicationkafka.StateFollowing {
		t.Errorf("stats depois do snapshot: %+v", s)
	}
}

// No tail, todas as versões de uma key vão para a MESMA partição, na ordem
// da sequence; a linha da histórica tem o rowID em decimal como key.
func TestEntityKeyAndHistoryRowIDKeysKeepPerKeyOrder(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, history: true})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	var last uint64
	var histIDs []types.RowID
	for i := range 5 {
		h.upsert("A", int64(i))
		h.upsert("B", int64(10+i))
		id, seq := h.historyRow("A", int64(i))
		histIDs = append(histIDs, id)
		last = seq
	}
	all := h.readUntil("tail completo", hasSequence(last))
	for _, key := range []string{"A", "B"} {
		var got []msg
		for _, m := range all {
			if m.op() != "reset" && m.op() != "snapshot_end" && m.str("table") == "events" && m.key == key {
				got = append(got, m)
			}
		}
		if len(got) != 5 {
			t.Fatalf("key %q: %d registros, quer 5:\n%s", key, len(got), dump(all))
		}
		for i, m := range got {
			if m.partition != got[0].partition {
				t.Errorf("key %q espalhada: partições %d e %d", key, got[0].partition, m.partition)
			}
			if i > 0 && (m.offset <= got[i-1].offset || m.num("sequence") <= got[i-1].num("sequence")) {
				t.Errorf("key %q fora de ordem: %v depois de %v", key, m.env, got[i-1].env)
			}
			if m.str("key") != key {
				t.Errorf("key do envelope %q, quer %q", m.str("key"), key)
			}
		}
		if got[0].op() != "insert" || got[4].op() != "update" {
			t.Errorf("key %q: ops %q ... %q, quer insert ... update", key, got[0].op(), got[4].op())
		}
	}
	for _, id := range histIDs {
		want := strconv.FormatUint(uint64(id), 10)
		found := false
		for _, m := range all {
			if m.str("table") == goldenHistory && m.op() == "insert" && m.key == want {
				found = true
				if m.str("key") != want || m.partition != partitionOf(want, 3) {
					t.Errorf("linha %s da histórica: key do envelope %q, partição %d", want, m.str("key"), m.partition)
				}
			}
		}
		if !found {
			t.Errorf("linha %s da histórica não publicada com a key em decimal:\n%s", want, dump(all))
		}
	}
}

// Com o broker parado, o feed perde o intervalo do publicador: o despejo da
// memória não basta (o lote despejado continua no disco), então o volume
// publicado passa do teto do DISCO e a retenção apaga o segmento do cursor
// do publicador. Ao voltar, o publicador publica um reset gap na MESMA
// epoch, com cursor maior, e um snapshot completo — nunca salta o intervalo
// perdido.
func TestResyncAfterEvictionPublishesNewReset(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3,
		feed: replication.Options{Interval: 0, MaxMessages: 100, MaxBatchBytes: 4096, MaxBufferedBytes: 256 << 10, MaxDiskBytes: 8 << 20}})
	for i := range 3 {
		h.seed("seed-"+strconv.Itoa(i), 1)
	}
	h.pub.Start()
	h.readUntil("snapshot de subida", snapshotEnds(3, 1))
	epoch, cursor0 := h.feed.Cursor()

	release := make(chan struct{})
	var blocking atomic.Bool
	blocking.Store(true)
	var held atomic.Int32
	h.cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		h.cluster.KeepControl()
		if blocking.Load() {
			held.Add(1)
			h.cluster.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	first := h.upsert("first", 1)
	waitFor(t, "produce retido no broker", func() bool { return held.Load() > 0 })
	var last uint64
	for i := range 200 {
		last = h.upsert("evict-"+strconv.Itoa(i), int64(i))
	}
	// ~9,6 MiB numa key só: passa dos 8 MiB do disco, e a retenção (ou, com o
	// gravador atrás, a memória cheia de lotes não gravados) leva o cursor do
	// publicador para baixo do piso.
	bulk := payloadOf(t, h.base, namedValue{"identifier", str("bulk")},
		namedValue{"persistent", doc(`{"blob":"` + strings.Repeat("b", 64<<10) + `"}`)})
	for range 150 {
		last = h.upsertPayload("bulk", bulk)
	}
	waitFor(t, "piso do feed acima do lote do publicador", func() bool { return h.feed.Status().AvailableAfter > first })
	blocking.Store(false)
	close(release)

	all := h.readUntil("reset gap e snapshot", snapshotEnds(3, 2))
	parts := byPartition(all)
	for p := range int32(3) {
		var reset *msg
		var rows []msg
		for i, m := range parts[p] {
			if m.op() == "reset" && m.str("reason") == "gap" {
				reset = &parts[p][i]
				rows = nil
				continue
			}
			if reset == nil {
				continue
			}
			if m.op() == "snapshot_end" {
				if m.num("count") != uint64(len(rows)) || m.num("cursor") != reset.num("cursor") {
					t.Errorf("partição %d: snapshot_end count=%d cursor=%d, quer %d %d", p, m.num("count"), m.num("cursor"), len(rows), reset.num("cursor"))
				}
				break
			}
			if m.op() == "snapshot" {
				rows = append(rows, m)
			}
		}
		if reset == nil {
			t.Fatalf("partição %d sem reset gap:\n%s", p, dump(parts[p]))
		}
		if reset.str("epoch") != epoch || reset.num("cursor") <= cursor0 || reset.num("cursor") != last {
			t.Errorf("partição %d: reset gap epoch=%s cursor=%d, quer epoch=%s e cursor %d (> %d)",
				p, reset.str("epoch"), reset.num("cursor"), epoch, last, cursor0)
		}
	}
	// O snapshot pós-lacuna cobre TODAS as keys vivas, inclusive as que só
	// passaram pelo intervalo perdido.
	seen := make(map[string]bool)
	for _, m := range all {
		if m.op() == "snapshot" && m.num("sequence") == last {
			seen[m.key] = true
		}
	}
	if len(seen) != 3+1+200+1 {
		t.Errorf("snapshot pós-lacuna com %d keys, quer 205", len(seen))
	}
	if s := h.pub.Stats(); s.Snapshots["gap"] != 1 {
		t.Errorf("snapshots por lacuna = %d, quer 1: %+v", s.Snapshots["gap"], s)
	}
}

// Com uma partição recusando o Produce, o cursor não passa do lote — nem
// com as outras partições confirmadas — e, quando o broker se recupera,
// nada do lote falta no tópico.
func TestCursorAdvancesOnlyAfterAllAcks(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, brokers: 3, feed: manualFeed,
		target: func(k *config.KafkaTargetSpec) { k.Timeout = "1s" },
		// O kgo só reavalia o prazo do registro depois de uma resposta, e a
		// resposta de erro dispara um refresh de metadado que espera o
		// MetadataMinAge (5 s por padrão).
		publisher: func(o *replicationkafka.Options) {
			replicationkafka.WithKgoOpts(o, kgo.MetadataMinAge(50*time.Millisecond))
		}})
	for p := range int32(3) {
		if err := h.cluster.MoveTopicPartition(pubTopic, p, p); err != nil {
			t.Fatal(err)
		}
	}
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	before := h.pub.Stats().Cursor

	var failing atomic.Bool
	failing.Store(true)
	var rejected atomic.Int32
	h.cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		h.cluster.KeepControl()
		pr := req.(*kmsg.ProduceRequest)
		if !failing.Load() || !hasPartition(pr, 0) {
			return nil, nil, false
		}
		rejected.Add(1)
		return produceError(pr, kerr.NotEnoughReplicas.Code), nil, true
	})
	k0, k1, k2 := keyIn("p0-", 0, 3), keyIn("p1-", 1, 3), keyIn("p2-", 2, 3)
	h.upsert(k0, 1)
	s1 := h.upsert(k1, 1)
	s2 := h.upsert(k2, 1)
	to := h.feed.Flush(time.Now(), "test")

	h.readUntil("partições sãs confirmadas", func(all []msg) bool { return hasSequence(s1)(all) && hasSequence(s2)(all) })
	waitFor(t, "publicador retentando a partição 0", func() bool {
		s := h.pub.Stats()
		return rejected.Load() >= 2 && s.State == replicationkafka.StateRetrying && s.Errors["timeout"]+s.Errors["unavailable"] > 0
	})
	if s := h.pub.Stats(); s.Cursor != before {
		t.Fatalf("cursor avançou para %d com a partição 0 sem ack (lote até %d, cursor anterior %d)", s.Cursor, to, before)
	}
	failing.Store(false)
	waitFor(t, "cursor no fim do lote", func() bool { return h.pub.Stats().Cursor == to })
	all := h.readAll()
	for _, key := range []string{k0, k1, k2} {
		found := false
		for _, m := range all {
			if m.key == key && m.op() == "insert" {
				found = true
			}
		}
		if !found {
			t.Errorf("key %q do lote ausente do tópico:\n%s", key, dump(all))
		}
	}
}

func hasPartition(pr *kmsg.ProduceRequest, part int32) bool {
	for _, tp := range pr.Topics {
		for _, p := range tp.Partitions {
			if p.Partition == part {
				return true
			}
		}
	}
	return false
}

func produceError(pr *kmsg.ProduceRequest, code int16) *kmsg.ProduceResponse {
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(pr.GetVersion())
	for _, tp := range pr.Topics {
		tr := kmsg.NewProduceResponseTopic()
		tr.Topic = tp.Topic
		tr.TopicID = tp.TopicID
		for _, p := range tp.Partitions {
			prp := kmsg.NewProduceResponseTopicPartition()
			prp.Partition = p.Partition
			prp.ErrorCode = code
			tr.Partitions = append(tr.Partitions, prp)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

// faultyProducer falha, na primeira tentativa, os registros escolhidos por
// fail, sem entregá-los ao broker; o resto segue para o kgo real.
type faultyProducer struct {
	replicationkafka.Producer
	mu     sync.Mutex
	fail   func(*kgo.Record) bool
	failed int
}

func (f *faultyProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	hit := f.fail != nil && f.fail(r)
	if hit {
		f.failed++
	}
	f.mu.Unlock()
	if hit {
		promise(r, kerr.NotEnoughReplicas)
		return
	}
	f.Producer.Produce(ctx, r, promise)
}

// Update e delete da mesma key no mesmo lote: o update falha e o delete
// (registro seguinte da MESMA partição) tem ack. A reprodução recomeça no
// update e inclui o delete, então o ÚLTIMO registro da key na partição é o
// delete, com a maior sequence.
func TestRetriedSuffixKeepsPerKeySequenceMonotonic(t *testing.T) {
	var faulty *faultyProducer
	h := newHarness(t, harnessOpts{partitions: 3, feed: manualFeed,
		publisher: func(o *replicationkafka.Options) {
			replicationkafka.WithProducerWrapper(o, func(p replicationkafka.Producer) replicationkafka.Producer {
				faulty = &faultyProducer{Producer: p}
				return faulty
			})
		}})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	ins := h.upsert("X", 1)
	h.feed.Flush(time.Now(), "test")
	h.readUntil("insert de X", hasSequence(ins))

	faulty.mu.Lock()
	faulty.fail = func(r *kgo.Record) bool {
		if string(r.Key) != "X" || !strings.Contains(string(r.Value), `"op":"update"`) {
			return false
		}
		faulty.fail = nil
		return true
	}
	faulty.mu.Unlock()
	upd := h.upsert("X", 2)
	del := h.remove("X")
	h.feed.Flush(time.Now(), "test")
	waitFor(t, "cursor depois do delete", func() bool { return h.pub.Stats().Cursor == del })

	var xs []msg
	for _, m := range h.readAll() {
		if m.key == "X" {
			xs = append(xs, m)
		}
	}
	if faulty.failed != 1 {
		t.Fatalf("a falha injetada rodou %d vezes, quer 1", faulty.failed)
	}
	lastX := xs[len(xs)-1]
	if lastX.op() != "delete" || lastX.num("sequence") != del {
		t.Fatalf("último registro de X é %q seq %d, quer delete seq %d (update seq %d):\n%s",
			lastX.op(), lastX.num("sequence"), del, upd, dump(xs))
	}
	var maxSeq uint64
	for _, m := range xs {
		maxSeq = max(maxSeq, m.num("sequence"))
	}
	if lastX.num("sequence") != maxSeq {
		t.Errorf("o último registro de X não tem a maior sequence (%d < %d)", lastX.num("sequence"), maxSeq)
	}
}

// Um registro acima de maxMessageBytes bloqueia o publicador ANTES de
// produzir qualquer registro do lote: nem ele nem os seguintes saem, e o
// cursor fica no último lote entregue.
func TestOversizeBlocksWithoutSkipping(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, feed: manualFeed,
		target: func(k *config.KafkaTargetSpec) { k.MaxMessageBytes = 1024 }})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	small1 := h.upsert("small-1", 1)
	h.feed.Flush(time.Now(), "test")
	h.readUntil("small-1", hasSequence(small1))
	waitFor(t, "cursor em small-1", func() bool { return h.pub.Stats().Cursor == small1 })

	big := payloadOf(t, h.base, namedValue{"identifier", str("big")},
		namedValue{"persistent", doc(`{"blob":"` + strings.Repeat("x", 2000) + `"}`)})
	h.upsertPayload("big", big)
	h.upsert("small-2", 2)
	h.feed.Flush(time.Now(), "test")
	waitFor(t, "estado blocked", func() bool { return h.pub.Stats().State == replicationkafka.StateBlocked })
	h.upsert("small-3", 3)
	h.feed.Flush(time.Now(), "test")

	s := h.pub.Stats()
	if s.Cursor != small1 || s.Errors["message_too_large"] != 1 || !strings.Contains(s.LastError, "maxMessageBytes") {
		t.Errorf("stats do bloqueio: %+v", s)
	}
	for _, m := range h.readAll() {
		if m.key == "big" || m.key == "small-2" || m.key == "small-3" {
			t.Errorf("registro %q publicado com o publicador bloqueado: %v", m.key, m.env)
		}
	}
	logs := h.logs.String()
	if !strings.Contains(logs, `"replication.kafka.blocked"`) || !strings.Contains(logs, "maxMessageBytes") || !strings.Contains(logs, `"key":"big"`) {
		t.Errorf("ERROR do bloqueio sem a key, o limite ou a remediação:\n%s", logs)
	}
}

// Fechar o feed e depois o publicador entrega a cauda inteira: tudo até o
// cursor do feed está no tópico.
func TestShutdownDrainsTail(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	for i := range 50 {
		h.upsert("tail-"+strconv.Itoa(i), int64(i))
	}
	// As 50 mudanças ainda estão no intervalo aberto: é o Close do feed que
	// fecha o lote, como no encerramento do servidor.
	_ = h.feed.Close(nil, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.pub.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, produced := h.feed.Cursor()
	got := make(map[uint64]bool)
	for _, m := range h.readAll() {
		if m.op() == "insert" {
			got[m.num("sequence")] = true
		}
	}
	for seq := uint64(1); seq <= produced; seq++ {
		if !got[seq] {
			t.Errorf("sequence %d (de %d) não chegou ao tópico no encerramento", seq, produced)
		}
	}
	if s := h.pub.Stats(); s.State != replicationkafka.StateStopped || s.Cursor != produced {
		t.Errorf("stats depois do encerramento: %+v", s)
	}
}

// countingProducer conta os registros entregues ao kgo.
type countingProducer struct {
	replicationkafka.Producer
	produced atomic.Int64
}

func (c *countingProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	c.produced.Add(1)
	c.Producer.Produce(ctx, r, promise)
}

// Com o broker fora e registros pendurados no produtor (o prazo de entrega é
// de 5 min: nenhum deles falha sozinho durante o teste), o Close respeita o
// prazo do contexto.
func TestShutdownBoundedWithUnreachableBroker(t *testing.T) {
	counting := &countingProducer{}
	h := newHarness(t, harnessOpts{partitions: 3, feed: manualFeed,
		target: func(k *config.KafkaTargetSpec) { k.Timeout = "5m" },
		publisher: func(o *replicationkafka.Options) {
			replicationkafka.WithProducerWrapper(o, func(p replicationkafka.Producer) replicationkafka.Producer {
				counting.Producer = p
				return counting
			})
		}})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	snapshotRecords := counting.produced.Load()
	h.cluster.Close()
	for i := range 20 {
		h.upsert("stuck-"+strconv.Itoa(i), int64(i))
	}
	h.feed.Flush(time.Now(), "test")
	waitFor(t, "lote entregue ao produtor", func() bool { return counting.produced.Load() == snapshotRecords+20 })
	_ = h.feed.Close(nil, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	err := h.pub.Close(ctx)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close com a cauda pendurada devolveu %v, quer DeadlineExceeded", err)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("Close levou %s com prazo de 2s", elapsed)
	}
}

// storeYAML é o dataset do teste de ponta a ponta com o storage real: TTL
// por escrita, que injeta o `_writtenAt` (coluna de sistema, fora do
// record).
const storeYAML = `
version: 1
name: events
schemaName: telecom
materialization:
  key:
    field: identifier
  mode: merge
  ttl:
    - expireAfterWrite: 1h
schema:
  include: ["*"]
  unknownFields: include
  fields:
    identifier:
      type: string
    severity:
      type: int64
`

// A expiração pelo coletor do storage REAL publica o delete com o último
// estado vivo, e o envelope leva esse estado no record: do Put ao tópico,
// sem feed montado à mão.
func TestDeleteEnvelopeCarriesLastStateEndToEnd(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, pubTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	feed, err := replication.Open(t.TempDir(), manualFeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Resume(replication.Fingerprint{}); err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC).UnixMilli())
	st, err := storage.Open(storage.Options{
		Dir: t.TempDir(), SegmentSize: 1 << 20, Dataset: "events",
		ActiveTTL: true, FallbackDuration: time.Hour,
		WriteInstantFieldName: schema.WriteInstantFieldName, TTLFieldName: schema.TTLFieldName,
		OnReplicationChange: func(table string, op replication.Operation, rec types.Record, at time.Time) {
			feed.Publish(table, op, rec, at)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.UsaRelogio(func() time.Time { return time.UnixMilli(clock.Load()) })
	spec, err := schema.Parse([]byte(storeYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	base, err := spec.Compile(st.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	renderer, err := replicationkafka.NewRenderer("events", "", base, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, cluster: c, addrs: c.ListenAddrs(), feed: feed, base: base, logs: &lockedBuffer{}}
	h.pub, err = replicationkafka.New(replicationkafka.Options{
		Dataset: "events", Feed: feed, Renderer: renderer,
		Target: config.KafkaTargetSpec{Name: "alarms_cdc", BootstrapServers: h.addrs, Topic: pubTopic, Timeout: "5s"},
		Tables: []replication.SnapshotTable{{Name: "events", Source: st}},
		Logger: logging.New(h.logs, logging.FormatJSON, slog.LevelDebug),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.close)

	alarm := func(severity int64) []byte {
		return payloadOf(t, base, namedValue{"identifier", str("A")}, namedValue{"severity", i64(severity)},
			namedValue{schema.WriteInstantFieldName, ts(clock.Load())})
	}
	id, _, err := st.Put([]byte("A"), 1, 0, alarm(5), nil)
	if err != nil {
		t.Fatal(err)
	}
	h.pub.Start()
	h.readUntil("snapshot com A", snapshotEnds(3, 1))
	if _, _, err := st.Put([]byte("A"), 2, 0, alarm(9), nil); err != nil {
		t.Fatal(err)
	}
	clock.Add((2 * time.Hour).Milliseconds())
	if expired, err := st.Expira(id); err != nil || !expired {
		t.Fatalf("Expira: expirou=%v err=%v", expired, err)
	}
	_, del := feed.Cursor()
	feed.Flush(time.Now(), "test")
	all := h.readUntil("delete por expiração", hasSequence(del))
	var got []string
	for _, m := range all {
		if m.key != "A" {
			continue
		}
		got = append(got, m.op())
		if m.op() != "delete" {
			continue
		}
		rec := m.record()
		if m.header != "delete" || m.num("sequence") != del || len(rec) != 2 || rec["identifier"] != "A" || rec["severity"] != json.Number("9") {
			t.Fatalf("delete sem o último estado vivo (severity 9, sem colunas de sistema): header=%q %v", m.header, m.env)
		}
	}
	if strings.Join(got, ",") != "snapshot,update,delete" {
		t.Fatalf("ops da key A = %v, quer snapshot,update,delete:\n%s", got, dump(all))
	}
}

// O tópico que ganha partição força um reset partitions_changed em TODAS as
// partições, a nova inclusive, e o snapshot seguinte distribui as keys pelo
// número novo.
func TestPartitionIncreaseForcesReset(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3,
		publisher: func(o *replicationkafka.Options) {
			o.PartitionCheckInterval = 50 * time.Millisecond
			replicationkafka.WithKgoOpts(o, kgo.MetadataMinAge(50*time.Millisecond))
		}})
	for i := range 20 {
		h.seed("alarm-"+strconv.Itoa(i), int64(i))
	}
	h.pub.Start()
	h.readUntil("snapshot de subida", snapshotEnds(3, 1))
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	_, err = kadm.NewClient(cl).CreatePartitions(context.Background(), 1, pubTopic)
	cl.Close()
	if err != nil {
		t.Fatal(err)
	}
	all := h.readUntil("reset partitions_changed", func(all []msg) bool {
		per := make(map[int32]bool)
		for _, m := range all {
			if m.op() == "snapshot_end" {
				per[m.partition] = true
			}
		}
		return len(per) == 4 && snapshotEnds(3, 2)(all)
	})
	parts := byPartition(all)
	rows := 0
	for p := range int32(4) {
		var reset bool
		for _, m := range parts[p] {
			if m.op() == "reset" && m.str("reason") == "partitions_changed" {
				reset = true
				continue
			}
			if reset && m.op() == "snapshot" {
				rows++
				if want := partitionOf(m.key, 4); want != p {
					t.Errorf("key %q na partição %d; com 4 partições o murmur2 manda para %d", m.key, p, want)
				}
			}
		}
		if !reset {
			t.Errorf("partição %d sem reset partitions_changed:\n%s", p, dump(parts[p]))
		}
	}
	if rows != 20 {
		t.Errorf("snapshot pós-mudança com %d linhas, quer 20", rows)
	}
	if s := h.pub.Stats(); s.Snapshots["partitions_changed"] != 1 {
		t.Errorf("snapshots partitions_changed = %d: %+v", s.Snapshots["partitions_changed"], s)
	}
	// Depois do reset, o tail usa o número NOVO: uma key que só existe na
	// partição 3 chega nela, e as conferências seguintes não disparam outro
	// reset (o número fixado foi atualizado).
	k := keyIn("tail-", 3, 4)
	seq := h.upsert(k, 1)
	for _, m := range h.readUntil("mudança do tail", hasSequence(seq)) {
		if m.num("sequence") == seq && !m.isControl() && m.partition != 3 {
			t.Errorf("mudança da key %q na partição %d, quer 3 (murmur2 entre 4)", k, m.partition)
		}
	}
	checks := h.countLog("replication.kafka.partitions.checked")
	waitFor(t, "três conferências depois da mudança", func() bool {
		return h.countLog("replication.kafka.partitions.checked") >= checks+3
	})
	if s := h.pub.Stats(); s.Snapshots["partitions_changed"] != 1 {
		t.Errorf("conferências depois do reset dispararam outro snapshot: partitions_changed=%d", s.Snapshots["partitions_changed"])
	}
}

// Uma Entry sem envelope (Schema) é bug: o publicador vai para failed, mas
// solta o lease — o feed segue despejando lotes sem perder intervalo.
func TestRenderErrorFailsPublisherWithoutBlockingFeed(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3,
		feed: replication.Options{Interval: 0, MaxMessages: 100, MaxBatchBytes: 2048, MaxBufferedBytes: 6144}})
	h.pub.Start()
	h.readUntil("snapshot vazio", snapshotEnds(3, 1))
	// Um lote grande: segurado pelo lease, ele sozinho não deixaria espaço
	// para as escritas seguintes, e o feed invalidaria o intervalo.
	seq := h.feed.Publish("events", replication.Schema, types.Record{RowID: 99, Key: []byte("s"), Payload: make([]byte, 5896)}, time.Now())
	waitFor(t, "estado failed", func() bool { return h.pub.Stats().State == replicationkafka.StateFailed })
	// Só lote gravado sai da memória: sem esperar o gravador, a escrita
	// seguinte acharia o lote ainda na fila e invalidaria por outro motivo.
	waitFor(t, "lote gravado no disco", func() bool { return h.feed.Status().DiskBytes > 0 })
	s := h.pub.Stats()
	if s.Errors["render_error"] != 1 || !strings.Contains(s.LastError, "has no envelope") {
		t.Errorf("stats do failed: %+v", s)
	}
	if logs := h.logs.String(); !strings.Contains(logs, `"replication.kafka.failed"`) || !strings.Contains(logs, `"sequence":`+strconv.FormatUint(seq, 10)) {
		t.Errorf("ERROR do failed sem a sequence %d:\n%s", seq, logs)
	}
	for i := range 20 {
		h.upsert("after-"+strconv.Itoa(i), int64(i))
	}
	if st := h.feed.Status(); st.Invalidations != 0 {
		t.Fatalf("o feed invalidou %d vezes: o lease do publicador falho não foi solto (%+v)", st.Invalidations, st)
	}
}

// Toda série de Stats existe em zero antes da primeira entrega. As listas
// esperadas são LITERAIS: comparar Stats com SnapshotReasons()/ErrorReasons()
// só provaria que a lista concorda consigo mesma, e um motivo removido dela
// (snapshot_gap, que alarma o livelock) sumiria das séries sem ninguém ver.
func TestStatsSeriesExistAtZeroBeforeFirstDelivery(t *testing.T) {
	wantSnapshots := []string{"startup", "gap", "snapshot_failed", "partitions_changed"}
	wantErrors := []string{"timeout", "unavailable", "message_too_large", "publish_error", "render_error", "snapshot_gap"}
	if got := replicationkafka.SnapshotReasons(); !slices.Equal(got, wantSnapshots) {
		t.Errorf("SnapshotReasons() = %v, quer %v", got, wantSnapshots)
	}
	if got := replicationkafka.ErrorReasons(); !slices.Equal(got, wantErrors) {
		t.Errorf("ErrorReasons() = %v, quer %v", got, wantErrors)
	}
	h := newHarness(t, harnessOpts{addrs: []string{"127.0.0.1:1"}})
	s := h.pub.Stats()
	for _, r := range wantSnapshots {
		if v, ok := s.Snapshots[r]; !ok || v != 0 {
			t.Errorf("snapshots[%q] = %d presente=%v, quer 0 presente", r, v, ok)
		}
	}
	for _, r := range wantErrors {
		if v, ok := s.Errors[r]; !ok || v != 0 {
			t.Errorf("errors[%q] = %d presente=%v, quer 0 presente", r, v, ok)
		}
	}
	if len(s.Snapshots) != len(wantSnapshots) || len(s.Errors) != len(wantErrors) ||
		s.Delivered != 0 || s.Destination != "alarms_cdc" {
		t.Errorf("stats iniciais: %+v", s)
	}
}

// O lote que começa antes do cursor do reset tem mudanças que o snapshot já
// cobriu: elas não se repetem no tail.
func TestTailDiscardsEntriesAtOrBelowCursor(t *testing.T) {
	h := newHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	// A e B estão no feed, mas o intervalo ainda não fechou: o cursor do
	// snapshot os inclui, e o lote que os traz também traz C.
	h.upsert("A", 1)
	h.upsert("B", 1)
	h.pub.Start()
	h.readUntil("snapshot com A e B", snapshotEnds(3, 1))
	c := h.upsert("C", 1)
	h.feed.Flush(time.Now(), "test")
	h.readUntil("C", hasSequence(c))
	waitFor(t, "cursor em C", func() bool { return h.pub.Stats().Cursor == c })
	var tail []msg
	for _, m := range h.readAll() {
		if m.op() == "insert" || m.op() == "update" {
			tail = append(tail, m)
		}
	}
	if len(tail) != 1 || tail[0].key != "C" || tail[0].num("sequence") != c {
		t.Fatalf("o tail repetiu o que o snapshot cobriu:\n%s", dump(tail))
	}
}

// snapshot_end seguido do feed perdendo o cursor antes do primeiro Acquire:
// ERROR nomeando o dimensionamento, e um novo snapshot por lacuna.
func TestLivelockAfterSnapshotIsLogged(t *testing.T) {
	var h *harness
	var once sync.Once
	h = newHarness(t, harnessOpts{partitions: 3,
		publisher: func(o *replicationkafka.Options) {
			replicationkafka.WithAfterSnapshot(o, func() {
				once.Do(func() {
					h.upsert("late", 1)
					h.feed.Invalidate()
				})
			})
		}})
	h.pub.Start()
	h.readUntil("snapshot de lacuna", snapshotEnds(3, 2))
	waitFor(t, "snapshot gap contado", func() bool { return h.pub.Stats().Snapshots["gap"] == 1 })
	logs := h.logs.String()
	if !strings.Contains(logs, `"replication.kafka.livelock"`) || !strings.Contains(logs, "maxBufferedBytes") ||
		!strings.Contains(logs, `"level":"ERROR"`) || !strings.Contains(logs, "snapshot_duration") {
		t.Errorf("sem o ERROR de livelock com o dimensionamento:\n%s", logs)
	}
}
