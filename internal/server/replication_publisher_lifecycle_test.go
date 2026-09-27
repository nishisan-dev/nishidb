package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// lifecycleDataset é o master do replicatedDataset com o intervalo do feed
// longo: o lote fica ABERTO até o Close do encerramento, e só a drenagem o
// entrega. Os %s recebem o broker da origem e a lista de destinos.
const lifecycleDataset = `version: 1
name: events
schemaName: telecom
source:
  type: kafka
  kafka:
    enabled: true
    bootstrapServers:
      - %s
    topic: src
    consumer:
      groupId: replication-src
      autoOffsetReset: earliest
materialization:
  key:
    field: id
  mode: merge
history:
  expireAfterWrite: 7d
  maxBytes: 4GiB
  fields:
    id:
      index: true
schema:
  include: ["*"]
  fields:
    id:
      type: string
    status:
      type: string
replication:
  enabled: true
  flushInterval: %s
  targets: [%s]
`

// produceTo publica os valores no tópico, sem key.
func produceTo(t *testing.T, broker, topic string, values ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	recs := make([]*kgo.Record, 0, len(values))
	for _, v := range values {
		recs = append(recs, &kgo.Record{Topic: topic, Value: []byte(v)})
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// readReplicationTopic lê o tópico desde o início, só com os registros da
// epoch dada, até done aceitar o que foi lido ou o prazo vencer.
func readReplicationTopic(t *testing.T, broker, topic, epoch string, done func([]cdcRecord) bool) []cdcRecord {
	t.Helper()
	reader, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var got []cdcRecord
	for !done(got) {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("lendo %s: %v (lidos da epoch: %d)", topic, err, len(got))
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var env map[string]any
			if err := json.Unmarshal(r.Value, &env); err != nil {
				t.Fatalf("registro que não é JSON: %s", r.Value)
			}
			if env["epoch"] == epoch {
				got = append(got, cdcRecord{Partition: r.Partition, Key: string(r.Key), Envelope: env})
			}
		})
	}
	return got
}

// waitFollowing espera todos os publicadores do dataset em following, depois
// do snapshot da subida, e devolve a epoch do feed.
func waitFollowing(t *testing.T, srv *server.Server, name string, destinations int) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rep := replicationOf(t, srv, name)
		ready := rep != nil && len(rep.Delivery) == destinations
		for i := 0; ready && i < destinations; i++ {
			ready = rep.Delivery[i].State == "following" && rep.Delivery[i].Snapshots["startup"] == 1
		}
		if ready {
			return rep.Feed.Epoch
		}
		if time.Now().After(deadline) {
			t.Fatalf("os publicadores não chegaram a following: %+v", rep)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// tailOf filtra as linhas de dado do tail (insert/update) de uma tabela.
func tailOf(recs []cdcRecord, table string) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, r := range recs {
		if r.table() == table && (r.op() == "insert" || r.op() == "update") {
			out[r.Key] = r.record()
		}
	}
	return out
}

// A cauda que o feed ainda segura no lote aberto chega ao Kafka no
// encerramento: o Close do feed a fecha num lote, e o publicador a entrega
// dentro do próprio orçamento antes de parar.
func TestShutdownDeliversTheRetainedFeedTailToKafka(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(3, "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("events", fmt.Sprintf(lifecycleDataset, broker, "60s", "cdc"))
	appendKafkaTargets(t, a, broker, [2]string{"cdc", "cdc"})
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	stop := startWebhookServer(t, srv)
	epoch := waitFollowing(t, srv, "events", 1)
	produceTo(t, broker, "src", `{"id":"late","status":"retained"}`)
	waitIngestionAplicarPeloMenos(t, srv, "events", 1, 20*time.Second)
	// Com 60 s de intervalo, o evento está no lote aberto do feed: nada o
	// entregaria antes do encerramento.
	if rep := replicationOf(t, srv, "events"); rep.Feed.Flushes != 0 {
		t.Fatalf("o feed fechou lote antes do encerramento (%d); o teste não prova a drenagem", rep.Feed.Flushes)
	}
	stop()
	got := readReplicationTopic(t, broker, "cdc", epoch, func(r []cdcRecord) bool {
		return tailOf(r, "events")["late"] != nil
	})
	if want := map[string]any{"id": "late", "status": "retained"}; !reflect.DeepEqual(tailOf(got, "events")["late"], want) {
		t.Fatalf("a cauda chegou com outro record: %v", tailOf(got, "events")["late"])
	}
}

// Com o broker do destino fora no encerramento, a drenagem estoura o próprio
// orçamento: o encerramento termina perto dele, sem erro, e deixa o WARN que
// diz ao operador que a próxima subida refaz o snapshot.
func TestShutdownWarnsWhenThePublisherDrainOverrunsItsBudget(t *testing.T) {
	source, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	sourceBroker, targetBroker := source.ListenAddrs()[0], target.ListenAddrs()[0]
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("events", fmt.Sprintf(lifecycleDataset, sourceBroker, "60s", "cdc"))
	appendKafkaTargets(t, a, targetBroker, [2]string{"cdc", "cdc"})
	logs := &recordOfServer{}
	srv, err := server.Novo(a.load(), server.Options{Log: logging.New(logs, logging.FormatText, slog.LevelInfo)})
	if err != nil {
		t.Fatal(err)
	}
	const budget = 500 * time.Millisecond
	srv.SetPublisherCloseBudgetForTest(budget)
	stop := startWebhookServer(t, srv)
	waitFollowing(t, srv, "events", 1)
	produceTo(t, sourceBroker, "src", `{"id":"stuck","status":"pending"}`)
	waitIngestionAplicarPeloMenos(t, srv, "events", 1, 20*time.Second)
	target.Close()
	started := time.Now()
	stop() // o stop reprova o teste se o Run devolver erro
	elapsed := time.Since(started)
	if !strings.Contains(logs.String(), "replication.publisher.close_failed") {
		t.Fatalf("o estouro do orçamento não deixou o WARN replication.publisher.close_failed:\n%s", logs.String())
	}
	if elapsed < budget || elapsed > budget+5*time.Second {
		t.Errorf("o encerramento levou %v; com o broker fora, deveria ficar perto do orçamento de %v", elapsed, budget)
	}
}

// A recusa do destino vem ANTES do lock de qualquer dataset: com o flock do
// dataset tomado por outro processo, a subida ainda responde pelo destino, e
// não pelo lock.
func TestReplicationTargetRefusalComesBeforeTheDatasetLock(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", fmt.Sprintf(replicatedWithoutSource, "events", "events", "ghost"))
	cfg := a.load()
	st, err := storage.Open(storage.Options{Dir: cfg.DatasetRootDir("events"), Dataset: "events", SegmentSize: cfg.SegmentSize()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	srv, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		_ = srv.Close()
		t.Fatal("a subida foi aceita com o destino inexistente")
	}
	if errors.Is(err, storage.ErrDatasetLocked) || !errors.Is(err, config.ErrKafkaTarget) {
		t.Fatalf("a recusa veio do lock, e não do destino: %v", err)
	}
}

// Dois destinos do mesmo dataset recebem o feed inteiro, cada um no seu
// tópico e com o seu número de partições. Cada publicador tem o próprio
// Renderer: sob -race, um Renderer compartilhado entre as duas goroutines
// reprova aqui.
func TestTwoReplicationDestinationsReceiveTheSameFeed(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(3, "cdc"), kfake.SeedTopics(2, "mirror"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("events", fmt.Sprintf(lifecycleDataset, broker, "0s", "cdc, mirror"))
	appendKafkaTargets(t, a, broker, [2]string{"cdc", "cdc"}, [2]string{"mirror", "mirror"})
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	run(t, srv)
	epoch := waitFollowing(t, srv, "events", 2)
	const n = 30
	events := make([]string, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, fmt.Sprintf(`{"id":"k%02d","status":"s%d"}`, i, i))
	}
	produceTo(t, broker, "src", events...)
	complete := func(r []cdcRecord) bool {
		return len(tailOf(r, "events")) == n && len(tailOf(r, "events_history")) == n
	}
	cdc := readReplicationTopic(t, broker, "cdc", epoch, complete)
	mirror := readReplicationTopic(t, broker, "mirror", epoch, complete)
	for _, table := range []string{"events", "events_history"} {
		if !reflect.DeepEqual(tailOf(cdc, table), tailOf(mirror, table)) {
			t.Errorf("os dois destinos divergem na tabela %s:\ncdc=%v\nmirror=%v", table, tailOf(cdc, table), tailOf(mirror, table))
		}
	}
	if got := tailOf(cdc, "events")["k07"]; !reflect.DeepEqual(got, map[string]any{"id": "k07", "status": "s7"}) {
		t.Errorf("record de k07 = %v", got)
	}
}
