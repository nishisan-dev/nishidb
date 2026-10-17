package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicRecord é um registro do tópico da replicação com a partição e o
// offset, de qualquer epoch.
type topicRecord struct {
	partition int32
	offset    int64
	env       map[string]any
}

func (r topicRecord) op() string { s, _ := r.env["op"].(string); return s }
func (r topicRecord) num(k string) uint64 {
	n, _ := r.env[k].(json.Number)
	v, _ := n.Int64()
	return uint64(v)
}

// readWholeTopic lê o tópico desde o início, de TODAS as epochs, até done
// aceitar o que foi lido.
func readWholeTopic(t *testing.T, broker, topic string, done func([]topicRecord) bool) []topicRecord {
	t.Helper()
	reader, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var got []topicRecord
	for !done(got) {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("lendo %s: %v (lidos: %d)", topic, err, len(got))
		}
		fetches.EachRecord(func(r *kgo.Record) {
			d := json.NewDecoder(bytes.NewReader(r.Value))
			d.UseNumber()
			var env map[string]any
			if err := d.Decode(&env); err != nil {
				t.Fatalf("registro que não é JSON: %s", r.Value)
			}
			got = append(got, topicRecord{partition: r.Partition, offset: r.Offset, env: env})
		})
	}
	return got
}

// opsPerPartition conta, por partição, os registros com o op.
func opsPerPartition(recs []topicRecord, op string) map[int32]int {
	out := map[int32]int{}
	for _, r := range recs {
		if r.op() == op {
			out[r.partition]++
		}
	}
	return out
}

// waitPublisherState espera o publicador único do dataset no estado dado.
func waitPublisherState(t *testing.T, srv *server.Server, name, state string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rep := replicationOf(t, srv, name)
		if rep != nil && len(rep.Delivery) == 1 && rep.Delivery[0].State == state {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("o publicador não chegou a %s: %+v", state, rep)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// cleanRestartScenario sobe o master, ingere first, espera o tail no tópico,
// encerra limpo, sobe de novo e ingere second. discover liga a descoberta de
// colunas (`unknownFields: include`). Devolve a epoch da primeira vida, o
// broker e o servidor da segunda.
func cleanRestartScenario(t *testing.T, discover bool, first, second []string) (string, string, *server.Server) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(3, "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]
	a := newEnvWithStorage(t, blockPadrao, "")
	spec := fmt.Sprintf(lifecycleDataset, broker, "0s", "cdc")
	if discover {
		spec = strings.Replace(spec, "  include: [\"*\"]\n", "  include: [\"*\"]\n  unknownFields: include\n", 1)
	}
	a.writeDataset("events", spec)
	appendKafkaTargets(t, a, broker, [2]string{"cdc", "cdc"})

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	stop := startWebhookServer(t, srv)
	epoch := waitFollowing(t, srv, "events", 1)
	produceTo(t, broker, "src", first...)
	readReplicationTopic(t, broker, "cdc", epoch, func(r []cdcRecord) bool {
		return len(tailOf(r, "events")) == len(first) && len(tailOf(r, "events_history")) == len(first)
	})
	stop()

	srv, err = server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	startWebhookServer(t, srv)
	waitPublisherState(t, srv, "events", "following")
	produceTo(t, broker, "src", second...)
	return epoch, broker, srv
}

// O reinício LIMPO do servidor atravessa o tópico sem snapshot: a vida nova
// publica `resume` em todas as partições, nenhum `reset`, e a numeração do
// tail continua a da vida anterior, sem buraco nem repetição.
func TestReplicationPublisherResumesAcrossCleanServerRestart(t *testing.T) {
	epoch, broker, srv := cleanRestartScenario(t, false,
		[]string{`{"id":"k1","status":"open"}`, `{"id":"k2","status":"open"}`},
		[]string{`{"id":"k3","status":"new"}`, `{"id":"k1","status":"closed"}`})
	const tailRecords = 8 // 4 eventos × (base + histórica)
	recs := readWholeTopic(t, broker, "cdc", func(r []topicRecord) bool {
		n := 0
		for _, x := range r {
			if x.op() == "insert" || x.op() == "update" {
				n++
			}
		}
		return n >= tailRecords && len(opsPerPartition(r, "resume")) == 3
	})
	for p := int32(0); p < 3; p++ {
		if n := opsPerPartition(recs, "reset")[p]; n != 1 {
			t.Errorf("partição %d com %d resets; o reinício limpo não publica reset", p, n)
		}
		if n := opsPerPartition(recs, "resume")[p]; n != 1 {
			t.Errorf("partição %d com %d resumes, quer 1", p, n)
		}
	}
	var seqs []uint64
	var resumeCursor uint64
	for _, r := range recs {
		if r.env["epoch"] != epoch {
			t.Fatalf("registro de outra epoch depois do reinício limpo: %v", r.env)
		}
		switch r.op() {
		case "insert", "update", "delete":
			// Toda mudança do tail consome uma sequence do feed. A linha do
			// snapshot e o retain dele levam o cursor do reset, e ficam de
			// fora; a retenção da histórica não roda no prazo do teste.
			seqs = append(seqs, r.num("sequence"))
		case "resume":
			resumeCursor = r.num("cursor")
		}
	}
	slices.Sort(seqs)
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("sequences do tail %v: não são contínuas a partir de 1", seqs)
		}
	}
	if resumeCursor != uint64(tailRecords/2) {
		t.Fatalf("resume com cursor %d, quer %d (a última sequence da primeira vida)", resumeCursor, tailRecords/2)
	}
	d := replicationOf(t, srv, "events").Delivery[0]
	for reason, n := range d.Snapshots {
		if n != 0 {
			t.Errorf("a vida nova contou snapshot %s=%d", reason, n)
		}
	}
	// O /v1/stats publica o ponto do .ckpt de onde a vida nova retomou: o
	// encerramento limpo gravou a última sequence da primeira vida.
	if d.CheckpointCursor < resumeCursor {
		t.Errorf("checkpointCursor = %d no /v1/stats, quero ao menos %d (o .ckpt gravado no encerramento)", d.CheckpointCursor, resumeCursor)
	}
}

// A impressão da renderização cobre só o DECLARADO: a coluna descoberta em
// runtime pela primeira vida (`include: ["*"]`) não muda o rótulo de coluna
// nenhuma, e a segunda vida retoma em vez de publicar config_changed.
func TestRenderHashIgnoresRuntimeDiscoveredColumns(t *testing.T) {
	_, broker, srv := cleanRestartScenario(t, true,
		[]string{`{"id":"k1","status":"open","discovered":"x"}`},
		[]string{`{"id":"k2","status":"open","discovered":"y"}`})
	recs := readWholeTopic(t, broker, "cdc", func(r []topicRecord) bool {
		// Pára também no segundo reset, para a falha dizer o motivo em vez
		// de vencer o prazo.
		return len(opsPerPartition(r, "resume")) == 3 || opsPerPartition(r, "reset")[0] >= 2
	})
	// A coluna nasceu de verdade na primeira vida: ela sai no record do
	// tail. Sem isso, o teste passaria com a descoberta desligada.
	discovered := false
	for _, r := range recs {
		if rec, _ := r.env["record"].(map[string]any); r.op() == "insert" && rec["discovered"] == "x" {
			discovered = true
		}
	}
	if !discovered {
		t.Fatalf("a coluna descoberta não chegou ao tópico; o teste não prova nada")
	}
	for _, r := range recs {
		if r.op() == "reset" && r.env["reason"] != "startup" {
			t.Fatalf("a coluna descoberta forçou um reset %v na segunda vida", r.env["reason"])
		}
	}
	if d := replicationOf(t, srv, "events").Delivery[0]; d.Snapshots["config_changed"] != 0 {
		t.Fatalf("config_changed = %d", d.Snapshots["config_changed"])
	}
}
