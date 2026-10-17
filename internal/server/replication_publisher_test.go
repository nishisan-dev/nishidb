package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// replicatedDataset é um master com tabela histórica, ingestão pelo tópico
// `src` e o feed publicado no destino `cdc`. O %s recebe o broker.
const replicatedDataset = `version: 1
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
  rawPayload: true
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
  flushInterval: 0s
  targets: [cdc]
`

// appendKafkaTargets acrescenta `kafkaTargets:` ao nishidb.yaml do ambiente,
// um destino por par nome → tópico, todos no mesmo broker.
func appendKafkaTargets(t *testing.T, a *env, broker string, targets ...[2]string) {
	t.Helper()
	body, err := os.ReadFile(a.cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	body = append(body, "\nkafkaTargets:\n"...)
	for _, k := range targets {
		body = append(body, fmt.Sprintf("  - name: %s\n    bootstrapServers: [%q]\n    topic: %s\n", k[0], broker, k[1])...)
	}
	if err := os.WriteFile(a.cfgPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

// cdcRecord é um registro do tópico da replicação, já decodificado.
type cdcRecord struct {
	Partition int32
	Key       string
	Header    string
	Envelope  map[string]any
}

func (r cdcRecord) op() string    { return r.Envelope["op"].(string) }
func (r cdcRecord) table() string { s, _ := r.Envelope["table"].(string); return s }
func (r cdcRecord) record() map[string]any {
	m, _ := r.Envelope["record"].(map[string]any)
	return m
}

// replicationOf devolve o bloco `replication` do /v1/stats do dataset.
func replicationOf(t *testing.T, srv *server.Server, name string) *protocol.ReplicationStats {
	t.Helper()
	for _, ds := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
		if ds.Name == name {
			return ds.Replication
		}
	}
	t.Fatalf("dataset %q ausente do /v1/stats", name)
	return nil
}

// O caminho inteiro pelo servidor real: linhas que já estavam no disco saem
// como snapshot, cercadas por `reset` e `snapshot_end` em TODAS as partições,
// e os eventos que a ingestão aplica depois saem como tail — base e
// histórica, com o `record` do estado corrente —, com o /v1/stats e o
// /metrics lendo o mesmo retrato.
func TestReplicationPublisherPublishesSnapshotAndTailThroughTheServer(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(3, "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("events", fmt.Sprintf(replicatedDataset, broker))
	appendKafkaTargets(t, a, broker, [2]string{"cdc", "cdc"})
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	produce := func(values ...string) {
		t.Helper()
		recs := make([]*kgo.Record, 0, len(values))
		for _, v := range values {
			recs = append(recs, &kgo.Record{Topic: "src", Value: []byte(v)})
		}
		if err := producer.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Linhas que existem ANTES da subida, ingeridas offline pelo `nishidb
	// ingest` (base e histórica): o feed nasce agora, e são as que só o
	// snapshot entrega. Uma primeira vida do servidor não serve mais para
	// isso: o encerramento limpo mantém a epoch, e o tópico teria as duas
	// vidas sob a mesma epoch.
	ingestOfflineAtKafkaStart(t, a, "events", `{"id":"k1","status":"open"}`, `{"id":"k2","status":"open"}`)

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// As séries nascem na construção, antes de qualquer entrega: o alarme
	// sobre erro precisa delas desde a subida.
	labels := `dataset="events",destination="cdc"`
	initial := srv.MetricsExpositionForTest()
	born := []string{
		metrics.MetReplicationKafkaDeliveredTotal + `{` + labels + `} 0`,
		metrics.MetReplicationFeedInvalidationsTotal + `{dataset="events"} 0`,
		// As do feed em disco e do checkpoint também: o feed recém-aberto
		// ainda não gravou segmento, e o destino ainda não gravou .ckpt.
		metrics.MetReplicationFeedDiskBytes + `{dataset="events"} 0`,
		metrics.MetReplicationFeedDiskSegments + `{dataset="events"} 0`,
		metrics.MetReplicationFeedDiskErrorsTotal + `{dataset="events"} 0`,
		metrics.MetReplicationKafkaLagBytes + `{` + labels + `} 0`,
		metrics.MetReplicationKafkaCheckpointSequence + `{` + labels + `} 0`,
	}
	// As listas fechadas são lidas do publicador, e não escritas aqui: um
	// motivo novo tem de nascer em zero sem que ninguém lembre deste teste.
	for _, reason := range replicationkafka.ErrorReasons() {
		born = append(born, metrics.MetReplicationKafkaErrorsTotal+`{`+labels+`,reason="`+reason+`"} 0`)
	}
	for _, reason := range replicationkafka.SnapshotReasons() {
		born = append(born, metrics.MetReplicationKafkaSnapshotsTotal+`{`+labels+`,reason="`+reason+`"} 0`)
	}
	for _, state := range replicationkafka.States() {
		value := "0"
		if state == replicationkafka.StateSnapshotting {
			value = "1"
		}
		born = append(born, metrics.MetReplicationKafkaState+`{`+labels+`,state="`+state+`"} `+value)
	}
	for _, want := range born {
		if !strings.Contains(initial, want) {
			t.Fatalf("série %q ausente antes da primeira entrega:\n%s", want, initial)
		}
	}
	run(t, srv)

	deadline := time.Now().Add(20 * time.Second)
	for {
		rep := replicationOf(t, srv, "events")
		if rep != nil && len(rep.Delivery) == 1 && rep.Delivery[0].State == "following" && rep.Delivery[0].Snapshots["startup"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("o publicador não chegou a following depois do snapshot: %+v", rep)
		}
		time.Sleep(20 * time.Millisecond)
	}

	produce(`{"id":"k1","status":"closed"}`, `{"id":"k3","status":"new"}`)

	epoch := replicationOf(t, srv, "events").Feed.Epoch
	reader, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("cdc"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var got []cdcRecord
	tail := func() (base, history int) {
		for _, r := range got {
			switch {
			case r.table() == "events" && (r.op() == "insert" || r.op() == "update"):
				base++
			case r.table() == "events_history" && r.op() == "insert":
				history++
			}
		}
		return base, history
	}
	for {
		if b, h := tail(); b >= 2 && h >= 2 {
			break
		}
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("lendo o tópico da replicação: %v (lidos: %d)", err, len(got))
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var env map[string]any
			if err := json.Unmarshal(r.Value, &env); err != nil {
				t.Fatalf("registro que não é JSON: %s", r.Value)
			}
			if env["epoch"] != epoch {
				return
			}
			var header string
			for _, h := range r.Headers {
				if h.Key == "nishidb-op" {
					header = string(h.Value)
				}
			}
			got = append(got, cdcRecord{Partition: r.Partition, Key: string(r.Key), Header: header, Envelope: env})
		})
	}

	// Por partição, na ordem do tópico: reset → linhas do snapshot →
	// snapshot_end com a contagem daquela partição → tail.
	byPartition := map[int32][]cdcRecord{}
	for _, r := range got {
		if r.Header != r.op() {
			t.Errorf("header nishidb-op %q difere do op %q do envelope", r.Header, r.op())
		}
		byPartition[r.Partition] = append(byPartition[r.Partition], r)
	}
	snapshot := map[string]map[string]any{}
	tailRecords := map[string]map[string]any{}
	historyIDs := map[string]bool{}
	for part := int32(0); part < 3; part++ {
		recs := byPartition[part]
		if len(recs) == 0 || recs[0].op() != "reset" || recs[0].Envelope["reason"] != "startup" {
			t.Fatalf("partição %d não começa com o reset da subida: %+v", part, recs)
		}
		end := -1
		rows := 0
		for i, r := range recs[1:] {
			switch r.op() {
			case "snapshot":
				if end >= 0 {
					t.Fatalf("partição %d: linha de snapshot depois do snapshot_end: %+v", part, r)
				}
				rows++
				if r.table() == "events" {
					if r.Key != r.Envelope["key"] {
						t.Errorf("key do registro Kafka %q difere da key do envelope %v", r.Key, r.Envelope["key"])
					}
					snapshot[r.Key] = r.record()
				}
			case "snapshot_end":
				if end >= 0 {
					t.Fatalf("partição %d: dois snapshot_end", part)
				}
				end = i + 1
				if count := r.Envelope["count"]; count != float64(rows) {
					t.Errorf("partição %d: snapshot_end.count = %v, e a partição recebeu %d linhas", part, count, rows)
				}
			case "insert", "update":
				if end < 0 {
					t.Fatalf("partição %d: tail antes do snapshot_end: %+v", part, r)
				}
				if r.table() == "events" {
					tailRecords[r.Key] = r.record()
				} else {
					// A coluna `_raw` só existe no Compiled da histórica: é
					// ela que prova que a histórica é renderizada pelo
					// schema dela, e não pelo do base.
					rec := r.record()
					raw, _ := rec["_raw"].(map[string]any)
					if raw == nil || raw["id"] != rec["id"] {
						t.Errorf("linha da histórica sem o _raw do evento: %v", rec)
					}
					historyIDs[rec["id"].(string)] = true
				}
			}
		}
		if end < 0 {
			t.Fatalf("partição %d sem snapshot_end", part)
		}
	}
	wantSnapshot := map[string]map[string]any{"k1": {"id": "k1", "status": "open"}, "k2": {"id": "k2", "status": "open"}}
	if !reflect.DeepEqual(snapshot, wantSnapshot) {
		t.Errorf("snapshot = %v, quero %v", snapshot, wantSnapshot)
	}
	wantTail := map[string]map[string]any{"k1": {"id": "k1", "status": "closed"}, "k3": {"id": "k3", "status": "new"}}
	if !reflect.DeepEqual(tailRecords, wantTail) {
		t.Errorf("tail = %v, quero %v", tailRecords, wantTail)
	}
	if !historyIDs["k1"] || !historyIDs["k3"] {
		t.Errorf("a histórica não publicou os dois eventos do tail: %v", historyIDs)
	}

	// /v1/stats e /metrics leem o MESMO retrato: parado o tail, os dois
	// convergem para o mesmo número.
	deadline = time.Now().Add(10 * time.Second)
	for {
		rep := replicationOf(t, srv, "events")
		d := rep.Delivery[0]
		exposition := srv.MetricsExpositionForTest()
		delivered := fmt.Sprintf("%s{%s} %d\n", metrics.MetReplicationKafkaDeliveredTotal, labels, d.Delivered)
		input := fmt.Sprintf("%s{dataset=\"events\"} %d\n", metrics.MetReplicationFeedInputEntriesTotal, rep.Feed.InputEntries)
		// O feed em disco e o checkpoint pelas duas portas: o tail gravou
		// segmento, e o número do /metrics é o do /v1/stats.
		disk := fmt.Sprintf("%s{dataset=\"events\"} %d\n", metrics.MetReplicationFeedDiskBytes, rep.Feed.DiskBytes)
		segments := fmt.Sprintf("%s{dataset=\"events\"} %d\n", metrics.MetReplicationFeedDiskSegments, rep.Feed.DiskSegments)
		checkpoint := fmt.Sprintf("%s{%s} %d\n", metrics.MetReplicationKafkaCheckpointSequence, labels, d.CheckpointCursor)
		lagBytes := fmt.Sprintf("%s{%s} %d\n", metrics.MetReplicationKafkaLagBytes, labels, d.LagBytes)
		if d.State == "following" && d.LagSequences == 0 && rep.Feed.InputEntries == 4 &&
			rep.Feed.DiskBytes > 0 && rep.Feed.DiskSegments > 0 && d.LagBytes == 0 &&
			strings.Contains(exposition, disk) && strings.Contains(exposition, segments) &&
			strings.Contains(exposition, checkpoint) && strings.Contains(exposition, lagBytes) &&
			strings.Contains(exposition, delivered) && strings.Contains(exposition, input) &&
			strings.Contains(exposition, metrics.MetReplicationKafkaState+`{`+labels+`,state="following"} 1`) {
			if d.Delivered == 0 || d.LastSuccessTimestamp == 0 || d.Epoch != epoch {
				t.Fatalf("retrato do publicador incoerente: %+v", d)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/v1/stats e /metrics não convergiram: %+v\n%s", rep, exposition)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startupError sobe o servidor com a configuração do ambiente e devolve o
// erro da recusa, que tem de ser do sentinela dos destinos Kafka (exit de
// uso) e trazer a mensagem PRÓPRIA da guarda.
func startupError(t *testing.T, a *env, want string) {
	t.Helper()
	srv, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		_ = srv.Close()
		t.Fatalf("a subida foi aceita; queria a recusa %q", want)
	}
	if !errors.Is(err, config.ErrKafkaTarget) {
		t.Fatalf("recusa %v não embrulha config.ErrKafkaTarget", err)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("recusa %q não traz a mensagem da guarda %q", err.Error(), want)
	}
}

const replicatedWithoutSource = `version: 1
name: %s
schemaName: schema_%s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
replication:
  enabled: true
  targets: [%s]
`

func TestStartupRefusesReplicationTargetMissingFromKafkaTargets(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", fmt.Sprintf(replicatedWithoutSource, "events", "events", "ghost"))
	appendKafkaTargets(t, a, "127.0.0.1:1", [2]string{"cdc", "cdc"})
	startupError(t, a, `dataset "events" replication target "ghost" is not declared in kafkaTargets`)
}

func TestStartupRefusesReplicationTargetSharedWithCounterEmit(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", fmt.Sprintf(replicatedWithoutSource, "events", "events", "shared"))
	a.writeDataset("servicos", strings.Replace(specServicesWithWindowMetricsAndEmit, "      maxBytes: 1MiB\n      keep: 2\n", "", 1)+"      target: shared\n")
	appendKafkaTargets(t, a, "127.0.0.1:1", [2]string{"shared", "shared"})
	startupError(t, a, `dataset "events" replication target "shared" is also the emit target of counter "autenticados_por_olt" in dataset "servicos"`)
}

func TestStartupRefusesReplicationTargetSharedByTwoDatasets(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("alpha", fmt.Sprintf(replicatedWithoutSource, "alpha", "alpha", "cdc"))
	a.writeDataset("beta", fmt.Sprintf(replicatedWithoutSource, "beta", "beta", "cdc"))
	appendKafkaTargets(t, a, "127.0.0.1:1", [2]string{"cdc", "cdc"})
	startupError(t, a, `replication target "cdc" is used by datasets "alpha" and "beta"`)
}

// appendRawConfig acrescenta YAML cru ao nishidb.yaml do ambiente.
func appendRawConfig(t *testing.T, a *env, yml string) {
	t.Helper()
	body, err := os.ReadFile(a.cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(a.cfgPath, append(body, yml...), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Outro NOME não esconde o mesmo tópico: o destino da replicação que aponta
// para o tópico de outro destino em uso, no mesmo cluster, é recusado — seja
// o outro a replicação de outro dataset, seja o emit.target de um contador. O
// cluster é a interseção dos bootstraps em minúsculas e sem espaço (o schema
// valida o endereço aparado, mas guarda o valor cru); o mesmo tópico noutro
// cluster é aceito.
func TestStartupRefusesReplicationTargetAliasingAnotherTargetTopic(t *testing.T) {
	t.Run("replication", func(t *testing.T) {
		a := newEnv(t)
		a.writeDataset("alpha", fmt.Sprintf(replicatedWithoutSource, "alpha", "alpha", "cdc_a"))
		a.writeDataset("beta", fmt.Sprintf(replicatedWithoutSource, "beta", "beta", "cdc_b"))
		appendRawConfig(t, a, "\nkafkaTargets:\n"+
			"  - name: cdc_a\n    bootstrapServers: [\"Broker-1:9092\"]\n    topic: shared\n"+
			"  - name: cdc_b\n    bootstrapServers: [\"broker-2:9092\", \" broker-1:9092\"]\n    topic: shared\n")
		startupError(t, a, `dataset "alpha" replication target "cdc_a" writes to topic "shared" on the same cluster as target "cdc_b", the replication target of dataset "beta"`)
	})
	t.Run("counter", func(t *testing.T) {
		a := newEnv(t)
		a.writeDataset("events", fmt.Sprintf(replicatedWithoutSource, "events", "events", "cdc"))
		a.writeDataset("servicos", strings.Replace(specServicesWithWindowMetricsAndEmit, "      maxBytes: 1MiB\n      keep: 2\n", "", 1)+"      target: samples\n")
		appendRawConfig(t, a, "\nkafkaTargets:\n"+
			"  - name: cdc\n    bootstrapServers: [\"127.0.0.1:1\"]\n    topic: shared\n"+
			"  - name: samples\n    bootstrapServers: [\"127.0.0.1:1\"]\n    topic: shared\n")
		startupError(t, a, `dataset "events" replication target "cdc" writes to topic "shared" on the same cluster as target "samples", the emit target of counter "autenticados_por_olt" in dataset "servicos"`)
	})
	t.Run("other cluster", func(t *testing.T) {
		a := newEnv(t)
		a.writeDataset("alpha", fmt.Sprintf(replicatedWithoutSource, "alpha", "alpha", "cdc_a"))
		a.writeDataset("beta", fmt.Sprintf(replicatedWithoutSource, "beta", "beta", "cdc_b"))
		appendRawConfig(t, a, "\nkafkaTargets:\n"+
			"  - name: cdc_a\n    bootstrapServers: [\"127.0.0.1:1\"]\n    topic: shared\n"+
			"  - name: cdc_b\n    bootstrapServers: [\"127.0.0.1:2\"]\n    topic: shared\n")
		srv, err := server.Novo(a.load(), server.Options{})
		if err != nil {
			t.Fatalf("o mesmo tópico em outro cluster foi recusado: %v", err)
		}
		_ = srv.Close()
	})
}

// O tópico de replicação que é a origem de algum dataset no mesmo cluster é
// recusado: o feed reingeriria o próprio envelope.
func TestStartupRefusesReplicationTargetThatIsAnIngestionSource(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", fmt.Sprintf(lifecycleDataset, "LOCALHOST:9092", "0s", "cdc"))
	appendRawConfig(t, a, "\nkafkaTargets:\n  - name: cdc\n    bootstrapServers: [\"localhost:9092\"]\n    topic: src\n")
	startupError(t, a, `dataset "events" replication target "cdc" writes to topic "src", which dataset "events" consumes on the same cluster`)
}

// O master sem destino tem o feed no /v1/stats, com a lista de entrega
// vazia (e não omitida); o dataset sem replicação não tem o bloco.
func TestReplicationStatsBlockFollowsTheFeed(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("plain", strings.ReplaceAll(specWithoutKafka, "%s", "plain"))
	a.writeDataset("master", strings.Replace(fmt.Sprintf(replicatedWithoutSource, "master", "master", "x"), "  targets: [x]\n", "", 1))
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	run(t, srv)
	if rep := replicationOf(t, srv, "plain"); rep != nil {
		t.Errorf("dataset sem replicação publicou o bloco: %+v", rep)
	}
	// No JSON CRU: o bloco nil decodifica igual a `"replication": null`, e só
	// a ausência da chave prova o omitempty que o contrato promete.
	var raw struct {
		Datasets []map[string]json.RawMessage `json:"datasets"`
	}
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/stats")), &raw); err != nil {
		t.Fatal(err)
	}
	for _, ds := range raw.Datasets {
		var name string
		if err := json.Unmarshal(ds["name"], &name); err != nil {
			t.Fatal(err)
		}
		_, has := ds["replication"]
		if name == "plain" && has {
			t.Errorf("a chave replication saiu no dataset sem feed: %s", ds["replication"])
		}
		if name == "master" && (!has || !strings.Contains(string(ds["replication"]), `"delivery": []`)) {
			t.Errorf("master sem destino: replication = %s, quero a lista delivery vazia", ds["replication"])
		}
	}
	rep := replicationOf(t, srv, "master")
	if rep == nil || rep.Feed.Epoch == "" || rep.Delivery == nil || len(rep.Delivery) != 0 {
		t.Fatalf("master sem destino: bloco = %+v", rep)
	}
	if !strings.Contains(srv.MetricsExpositionForTest(), metrics.MetReplicationFeedInputEntriesTotal+`{dataset="master"} 0`) {
		t.Error("o master sem destino não publica as séries do feed")
	}
}

// O encerramento solta o long-poll de um seguidor à espera de lote antes de o
// http.Shutdown esperar por ele: o handler espera sob o ctx do Run, e o
// cancelamento responde 410 na hora (o master está saindo; nunca 409, que
// mandaria o follower ao snapshot). O encerramento não gasta o prazo de 15 s
// do Shutdown, mesmo com o feed fechando só depois dele.
func TestShutdownReleasesFollowerLongPollBeforeHTTPShutdown(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("master", strings.Replace(fmt.Sprintf(replicatedWithoutSource, "master", "master", "x"), "  targets: [x]\n", "", 1))
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()
	var cursor struct {
		Epoch string `json:"epoch"`
	}
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/replication/cursor?dataset=master")), &cursor); err != nil || cursor.Epoch == "" {
		t.Fatalf("cursor ilegível: %v %+v", err, cursor)
	}
	status := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + srv.Endereco() + "/v1/replication/batch?dataset=master&after=0&epoch=" + cursor.Epoch)
		if err != nil {
			status <- 0
			return
		}
		_ = resp.Body.Close()
		status <- resp.StatusCode
	}()
	// O long-poll precisa estar dentro do handler quando o encerramento
	// começa; sem lote no feed, ele espera ali até o feed fechar.
	time.Sleep(300 * time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case err := <-end:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("o encerramento ficou preso no long-poll do seguidor: o handler não saiu pelo ctx do Run")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Errorf("o encerramento levou %v com um long-poll em voo", elapsed)
	}
	if got := <-status; got != http.StatusGone {
		t.Errorf("long-poll terminou com %d, quero 410 (master encerrando)", got)
	}
}
