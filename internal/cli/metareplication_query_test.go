package cli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kfake"
)

// replicatedMasterCLI é um master com o feed publicado no destino `cdc`, sem
// fonte Kafka: as linhas entram pelo `nishidb ingest`, e o que importa aqui é
// o publicador.
const replicatedMasterCLI = `version: 1
name: events
schemaName: events
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
replication:
  enabled: true
  flushInterval: 0s
  targets: [cdc, fresh, torn]
`

// runReplicatedServerUntilFollowing sobe o servidor, espera os publicadores
// dos três destinos em following com o snapshot da subida gravado no .ckpt, e
// o encerra limpo. Devolve a epoch do feed.
func runReplicatedServerUntilFollowing(t *testing.T, a *envRemote) string {
	t.Helper()
	cfg, err := config.Load(a.config)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-end; err != nil {
			t.Errorf("Run: %v", err)
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(20 * time.Second)
	epoch := ""
	for epoch == "" {
		if time.Now().After(deadline) {
			t.Fatal("o publicador não chegou a following com o .ckpt gravado em 20 s")
		}
		time.Sleep(20 * time.Millisecond)
		resp, err := http.Get("http://" + a.endereco() + "/v1/stats")
		if err != nil {
			continue
		}
		var st protocol.Stats
		err = json.NewDecoder(resp.Body).Decode(&st)
		_ = resp.Body.Close()
		if err != nil {
			continue
		}
		for _, d := range st.Datasets {
			if d.Name != "events" || d.Replication == nil || len(d.Replication.Delivery) != 3 {
				continue
			}
			ready := true
			for _, dv := range d.Replication.Delivery {
				ready = ready && dv.State == "following" && dv.Snapshots["startup"] == 1
			}
			if ready {
				epoch = d.Replication.Feed.Epoch
			}
		}
	}
	stop()
	return epoch
}

// Com o servidor parado, a `_meta.replication` do `nishidb query --config`
// responde com o que está no disco: o .ckpt de cada destino (ponto confirmado
// e último snapshot) e o `state` do feed fechado limpo. O que só um
// publicador vivo mede — estado, cursor em memória, atrasos, cache — é NULL.
// Sem esta asserção, apagar o Register de cli/query.go (ou trocar os
// leitores por nil) não derruba teste nenhum de fiação.
//
// Os três destinos cobrem os três desfechos da leitura do .ckpt: `cdc` lido,
// `fresh` sem arquivo (apagado depois do encerramento, como o destino que
// nunca gravou) e `torn` ilegível (corrompido depois do encerramento). O
// destino declarado nunca some da tabela.
func TestQueryLocalReadsMetaReplicationFromTheCheckpoint(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "cdc-topic", "fresh-topic", "torn-topic"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	a := newEnvRemote(t, 100000)
	body, err := os.ReadFile(a.config)
	if err != nil {
		t.Fatal(err)
	}
	body = append(body, "kafkaTargets:\n"...)
	for _, name := range []string{"cdc", "fresh", "torn"} {
		body = append(body, fmt.Sprintf("  - name: %s\n    bootstrapServers: [%q]\n    topic: %s-topic\n", name, cluster.ListenAddrs()[0], name)...)
	}
	if err := os.WriteFile(a.config, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.dir, "datasets", "events.yaml"), []byte(replicatedMasterCLI), 0o644); err != nil {
		t.Fatal(err)
	}
	epoch := runReplicatedServerUntilFollowing(t, a)
	ckptDir := filepath.Join(a.dir, "dados", "events", "replication", "kafka")
	if err := os.Remove(filepath.Join(ckptDir, "fresh.ckpt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ckptDir, "torn.ckpt"), []byte("NDBKrasgado"), 0o644); err != nil {
		t.Fatal(err)
	}

	code := a.run("query", "--config", a.config, "--dataset", "events",
		"SELECT id, topic, state, epoch, cursor, checkpointCursor, produced, floor, feedCacheBytes, feedSegments, lagSequences, lagBytes, lastResetReason, lastSnapshotAt, lastSuccessAt, lastError FROM _meta.replication")
	if code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	lines := strings.FieldsFunc(a.stdout.String(), func(r rune) bool { return r == '\n' })
	if len(lines) != 3 {
		t.Fatalf("%d linhas, quero 3 (um por destino declarado):\n%s", len(lines), a.stdout.String())
	}
	rows := make([]map[string]any, len(lines))
	for i, l := range lines {
		if err := json.Unmarshal([]byte(l), &rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	row := rows[0]
	for col, want := range map[string]any{"id": "cdc", "topic": "cdc-topic", "epoch": epoch} {
		if row[col] != want {
			t.Errorf("%s = %v, quero %v", col, row[col], want)
		}
	}
	for _, col := range []string{"checkpointCursor", "produced", "floor", "feedSegments", "lastSnapshotAt"} {
		if row[col] == nil {
			t.Errorf("%s NULL offline: o disco o sabe (%v)", col, row)
		}
	}
	for _, col := range []string{"state", "cursor", "feedCacheBytes", "lagSequences", "lagBytes", "lastResetReason", "lastSuccessAt", "lastError"} {
		if row[col] != nil {
			t.Errorf("%s = %v offline; quero NULL, sem publicador não há o que medir", col, row[col])
		}
	}
	// `fresh`, sem .ckpt: o destino e o tópico saem, o que o .ckpt diria é
	// NULL, e não há erro. `torn`, ilegível: a linha sai com o erro.
	fresh, torn := rows[1], rows[2]
	if fresh["id"] != "fresh" || fresh["topic"] != "fresh-topic" || torn["id"] != "torn" || torn["topic"] != "torn-topic" {
		t.Fatalf("destinos fora de ordem ou sem o tópico da configuração: %v / %v", fresh, torn)
	}
	for _, col := range []string{"epoch", "checkpointCursor", "lastSnapshotAt", "lastError"} {
		if fresh[col] != nil {
			t.Errorf("fresh.%s = %v sem .ckpt; quero NULL", col, fresh[col])
		}
	}
	for _, col := range []string{"epoch", "checkpointCursor", "lastSnapshotAt"} {
		if torn[col] != nil {
			t.Errorf("torn.%s = %v com o .ckpt ilegível; quero NULL", col, torn[col])
		}
	}
	if msg, _ := torn["lastError"].(string); !strings.HasPrefix(msg, "checkpoint unreadable:") {
		t.Errorf("torn.lastError = %v; quero o erro de leitura do .ckpt", torn["lastError"])
	}
}
