package replicationkafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// pubTopic é o tópico dos testes do publicador.
const pubTopic = "fault.alarms.cdc"

// settleTimeout é o prazo das esperas por condição.
const settleTimeout = 20 * time.Second

// memRow é uma linha do memSource.
type memRow struct {
	key, payload []byte
}

// memSource é uma tabela em memória com o contrato de SnapshotSource que o
// exportador usa: rowIds crescentes, teto fixado na abertura. O teste
// escreve nela e no feed na ordem do storage (primeiro a linha visível,
// depois a publicação), e o publicador a lê da goroutine dele.
type memSource struct {
	mu    sync.Mutex
	rows  map[types.RowID]memRow
	byKey map[string]types.RowID
	next  types.RowID
	base  types.RowID
}

func newMemSource() *memSource {
	return &memSource{rows: make(map[types.RowID]memRow), byKey: make(map[string]types.RowID)}
}

// upsert grava a key e devolve o rowID e se ela nasceu agora.
func (m *memSource) upsert(key string, payload []byte) (types.RowID, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.byKey[key]
	if !ok {
		id = m.next
		m.next++
		m.byKey[key] = id
	}
	m.rows[id] = memRow{key: []byte(key), payload: bytes.Clone(payload)}
	return id, !ok
}

// appendRow grava uma linha sem key (a histórica).
func (m *memSource) appendRow(payload []byte) types.RowID {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.next
	m.next++
	m.rows[id] = memRow{payload: bytes.Clone(payload)}
	return id
}

// remove apaga a key e devolve a linha que saiu.
func (m *memSource) remove(key string) (types.RowID, memRow) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.byKey[key]
	row := m.rows[id]
	delete(m.rows, id)
	return id, row
}

func (m *memSource) OpenSnapshot() types.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return types.Snapshot{Instant: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC), LimitRowID: m.next, BaseRowID: m.base}
}

func (m *memSource) CloseSnapshot(types.Snapshot) {}

func (m *memSource) ScanLiveFrom(snap types.Snapshot, from, limit types.RowID) types.RowIDIterator {
	m.mu.Lock()
	defer m.mu.Unlock()
	it := &memIterator{}
	for id := max(from, snap.BaseRowID); id < min(limit, snap.LimitRowID); id++ {
		if _, ok := m.rows[id]; ok {
			it.ids = append(it.ids, id)
		}
	}
	return it
}

func (m *memSource) GetByRowID(_ types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rows[id]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	buf = append(append(buf[:0], r.key...), r.payload...)
	return types.Record{RowID: id, Key: buf[:len(r.key)], Payload: buf[len(r.key):]}, buf, true, nil
}

type memIterator struct {
	ids []types.RowID
	pos int
	cur types.RowID
}

func (it *memIterator) Next() bool {
	if it.pos >= len(it.ids) {
		return false
	}
	it.cur = it.ids[it.pos]
	it.pos++
	return true
}
func (it *memIterator) RowID() types.RowID { return it.cur }
func (it *memIterator) Err() error         { return nil }
func (it *memIterator) Close() error       { return nil }

// lockedBuffer é o destino dos logs, lido pelo teste enquanto o publicador
// escreve.
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

// harnessOpts ajusta o cenário.
type harnessOpts struct {
	partitions int32
	brokers    int
	feed       replication.Options
	history    bool
	target     func(*config.KafkaTargetSpec)
	publisher  func(*replicationkafka.Options)
	// addrs substitui o cluster (broker inalcançável).
	addrs []string
}

// harness é um dataset `events` (e a histórica, se pedida) com feed, fontes
// em memória, um cluster kfake e o publicador.
type harness struct {
	t          *testing.T
	cluster    *kfake.Cluster
	addrs      []string
	feed       *replication.Stream
	base, hist *schema.Compiled
	baseSrc    *memSource
	histSrc    *memSource
	pub        *replicationkafka.Publisher
	logs       *lockedBuffer
	closeOnce  sync.Once
}

func newHarness(t *testing.T, o harnessOpts) *harness {
	t.Helper()
	if o.partitions == 0 {
		o.partitions = 3
	}
	if o.brokers == 0 {
		o.brokers = 1
	}
	if o.feed == (replication.Options{}) {
		o.feed = replication.Options{Interval: 0, MaxMessages: 1000, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 8 << 20}
	}
	h := &harness{t: t, baseSrc: newMemSource(), histSrc: newMemSource(), logs: &lockedBuffer{}}
	h.addrs = o.addrs
	if h.addrs == nil {
		c, err := kfake.NewCluster(kfake.NumBrokers(o.brokers), kfake.SeedTopics(o.partitions, pubTopic))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		h.cluster = c
		h.addrs = c.ListenAddrs()
	}
	feed, err := replication.New(o.feed)
	if err != nil {
		t.Fatal(err)
	}
	h.feed = feed
	h.base, h.hist = compiledPair(t, goldenYAML)
	var renderer *replicationkafka.Renderer
	tables := []replication.SnapshotTable{{Name: "events", Source: h.baseSrc}}
	if o.history {
		renderer, err = replicationkafka.NewRenderer("events", goldenHistory, h.base, h.hist)
		tables = append(tables, replication.SnapshotTable{Name: goldenHistory, History: true, Source: h.histSrc})
	} else {
		renderer, err = replicationkafka.NewRenderer("events", "", h.base, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	target := config.KafkaTargetSpec{Name: "alarms_cdc", BootstrapServers: h.addrs, Topic: pubTopic, Timeout: "5s"}
	if o.target != nil {
		o.target(&target)
	}
	opts := replicationkafka.Options{
		Dataset:                "events",
		Target:                 target,
		Feed:                   feed,
		Tables:                 tables,
		Renderer:               renderer,
		Logger:                 logging.New(h.logs, logging.FormatJSON, slog.LevelDebug),
		MinBackoff:             20 * time.Millisecond,
		MaxBackoff:             200 * time.Millisecond,
		PartitionCheckInterval: time.Hour,
	}
	if o.publisher != nil {
		o.publisher(&opts)
	}
	pub, err := replicationkafka.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	h.pub = pub
	t.Cleanup(h.close)
	return h
}

// close segue a ordem do servidor: o feed antes, o publicador depois.
func (h *harness) close() {
	h.closeOnce.Do(func() {
		h.feed.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = h.pub.Close(ctx)
	})
}

// alarm é o payload de um alarme do events.
func (h *harness) alarm(key string, severity int64) []byte {
	return payloadOf(h.t, h.base, namedValue{"identifier", str(key)}, namedValue{"severity", i64(severity)})
}

// seed grava a key só na fonte (estado anterior ao feed).
func (h *harness) seed(key string, severity int64) {
	h.baseSrc.upsert(key, h.alarm(key, severity))
}

// upsert grava a key na fonte e publica no feed, como o storage faz.
func (h *harness) upsert(key string, severity int64) uint64 {
	return h.upsertPayload(key, h.alarm(key, severity))
}

func (h *harness) upsertPayload(key string, payload []byte) uint64 {
	id, created := h.baseSrc.upsert(key, payload)
	op := replication.Update
	if created {
		op = replication.Insert
	}
	return h.feed.Publish("events", op, types.Record{RowID: id, Key: []byte(key), Payload: payload}, time.Now())
}

// remove apaga a key e publica o delete com o último estado.
func (h *harness) remove(key string) uint64 {
	id, row := h.baseSrc.remove(key)
	return h.feed.Publish("events", replication.Delete, types.Record{RowID: id, Key: row.key, Payload: row.payload, Flags: types.FlagTombstone}, time.Now())
}

// historyRow grava uma linha da histórica e a publica.
func (h *harness) historyRow(key string, severity int64) (types.RowID, uint64) {
	p := payloadOf(h.t, h.hist, namedValue{"identifier", str(key)}, namedValue{"severity", i64(severity)})
	id := h.histSrc.appendRow(p)
	return id, h.feed.Publish(goldenHistory, replication.Insert, types.Record{RowID: id, Payload: p}, time.Now())
}

// msg é um registro lido do tópico, com o envelope decodificado.
type msg struct {
	partition int32
	offset    int64
	key       string
	header    string
	env       map[string]any
}

func (m msg) op() string { s, _ := m.env["op"].(string); return s }
func (m msg) str(k string) string {
	s, _ := m.env[k].(string)
	return s
}
func (m msg) num(k string) uint64 {
	n, ok := m.env[k].(json.Number)
	if !ok {
		return 0
	}
	v, _ := strconv.ParseUint(n.String(), 10, 64)
	return v
}
func (m msg) record() map[string]any { r, _ := m.env["record"].(map[string]any); return r }
func (m msg) isControl() bool {
	switch m.op() {
	case "reset", "snapshot_end", "retain":
		return true
	}
	return false
}

// readUntil consome o tópico desde o começo até done aceitar o que foi lido.
func (h *harness) readUntil(desc string, done func([]msg) bool) []msg {
	h.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...), kgo.ConsumeTopics(pubTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.MetadataMinAge(50*time.Millisecond),
		kgo.MetadataMaxAge(100*time.Millisecond), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		h.t.Fatal(err)
	}
	defer cl.Close()
	var all []msg
	deadline := time.Now().Add(settleTimeout)
	for {
		if done(all) {
			return all
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("%s: o tópico não chegou ao estado esperado em %s; lidos %d registros:\n%s\nlogs:\n%s",
				desc, settleTimeout, len(all), dump(all), h.logs.String())
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		fs := cl.PollFetches(ctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) {
			all = append(all, decode(h.t, r))
		})
	}
}

// readAll consome o tópico até a marca d'água de cada partição: é a leitura
// COMPLETA, para as asserções de ausência.
func (h *harness) readAll() []msg {
	h.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		h.t.Fatal(err)
	}
	ends, err := kadm.NewClient(cl).ListEndOffsets(context.Background(), pubTopic)
	cl.Close()
	if err != nil {
		h.t.Fatal(err)
	}
	want := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) { want[o.Partition] = o.Offset })
	return h.readUntil("leitura completa", func(all []msg) bool {
		got := make(map[int32]int64)
		for _, m := range all {
			got[m.partition] = m.offset + 1
		}
		for p, end := range want {
			if got[p] < end {
				return false
			}
		}
		return true
	})
}

func decode(t *testing.T, r *kgo.Record) msg {
	t.Helper()
	m := msg{partition: r.Partition, offset: r.Offset, key: string(r.Key)}
	for _, hd := range r.Headers {
		if hd.Key == replicationkafka.HeaderOp {
			m.header = string(hd.Value)
		}
	}
	d := json.NewDecoder(bytes.NewReader(r.Value))
	d.UseNumber()
	if err := d.Decode(&m.env); err != nil {
		t.Fatalf("envelope inválido na partição %d offset %d: %v\n%s", r.Partition, r.Offset, err, r.Value)
	}
	return m
}

func dump(all []msg) string {
	var b strings.Builder
	for _, m := range all {
		raw, _ := json.Marshal(m.env)
		b.WriteString("  p" + strconv.Itoa(int(m.partition)) + "@" + strconv.FormatInt(m.offset, 10) + " key=" + m.key + " " + string(raw) + "\n")
	}
	return b.String()
}

// byPartition agrupa na ordem do offset.
func byPartition(all []msg) map[int32][]msg {
	out := make(map[int32][]msg)
	for _, m := range all {
		out[m.partition] = append(out[m.partition], m)
	}
	for p := range out {
		sort.Slice(out[p], func(i, j int) bool { return out[p][i].offset < out[p][j].offset })
	}
	return out
}

// snapshotEnds diz se cada uma das n partições já tem pelo menos want
// snapshot_end.
func snapshotEnds(n int32, want int) func([]msg) bool {
	return func(all []msg) bool {
		per := make(map[int32]int)
		for _, m := range all {
			if m.op() == "snapshot_end" {
				per[m.partition]++
			}
		}
		for p := range n {
			if per[p] < want {
				return false
			}
		}
		return true
	}
}

// hasSequence diz se algum registro de dado com esta sequence foi lido.
func hasSequence(seq uint64) func([]msg) bool {
	return func(all []msg) bool {
		for _, m := range all {
			if !m.isControl() && m.num("sequence") == seq {
				return true
			}
		}
		return false
	}
}

// partitionOf é a partição que o murmur2 do Kafka dá à key entre n.
func partitionOf(key string, n int) int32 {
	return int32(kgo.StickyKeyPartitioner(nil).ForTopic("").Partition(&kgo.Record{Key: []byte(key)}, n))
}

// keyIn devolve a primeira key com o prefixo que cai na partição part entre n.
func keyIn(prefix string, part int32, n int) string {
	for i := 0; ; i++ {
		k := prefix + strconv.Itoa(i)
		if partitionOf(k, n) == part {
			return k
		}
	}
}

// countLog conta as linhas de log do evento.
func (h *harness) countLog(event string) int {
	return strings.Count(h.logs.String(), `"event":"`+event+`"`)
}

// waitFor espera cond com prazo.
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(settleTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: condição não atingida em %s", desc, settleTimeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
