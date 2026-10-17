package server_test

import (
	"testing"
	"time"
)

// A `_meta.replication` do servidor lê o publicador VIVO de cada destino e o
// feed do dataset pela rota de consulta: sem esta asserção, trocar o closure
// de Destinations por nil (a tabela vazia) ou o de Feed (as colunas do feed
// NULL) em server.go não derruba teste nenhum — o catálogo continuaria
// listando a tabela.
func TestMetaReplicationAnswersTheLivePublisherThroughTheServer(t *testing.T) {
	srv, _ := resyncMaster(t, blockPadrao)
	epoch := waitFollowing(t, srv, "events", 1)
	const sql = "SELECT id, topic, state, epoch, cursor, checkpointCursor, produced, feedCacheBytes, lagSequences, lagBytes, lastResetReason, lastSnapshotAt FROM _meta.replication"
	// O publicador entra em following ANTES de gravar o .ckpt do fim do
	// snapshot (o fsync não atrasa o /v1/stats), e é a gravação que publica
	// lastSnapshotAt: espera por ela.
	deadline := time.Now().Add(10 * time.Second)
	var rows []map[string]any
	for {
		rows = queryRowsInSchema(t, srv, "telecom", sql)
		if len(rows) != 1 || rows[0]["lastSnapshotAt"] != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(rows) != 1 {
		t.Fatalf("%d linhas, quero 1 (um destino): %v", len(rows), rows)
	}
	r := rows[0]
	for col, want := range map[string]any{"id": "cdc", "topic": "cdc", "state": "following", "epoch": epoch, "lastResetReason": "startup"} {
		if r[col] != want {
			t.Errorf("%s = %v, quero %v", col, r[col], want)
		}
	}
	// As medidas do publicador e do feed existem (não são NULL): o snapshot
	// da subida já terminou.
	// checkpointCursor também: o fim do snapshot gravou o .ckpt nesta vida.
	for _, col := range []string{"cursor", "checkpointCursor", "produced", "feedCacheBytes", "lagSequences", "lagBytes", "lastSnapshotAt"} {
		if r[col] == nil {
			t.Errorf("%s NULL com o publicador vivo: %v", col, r)
		}
	}
}
