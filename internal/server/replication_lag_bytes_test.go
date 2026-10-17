package server_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// O atraso em bytes do feed em disco sai no /v1/stats e no /metrics pelo MESMO
// retrato. Com o tópico do destino segurado no broker, o feed continua
// gravando e o cursor do publicador para: o atraso é diferente de zero e
// estável, e só assim um campo esquecido no replicationStats (que sairia zero
// nos dois lados, como no teste de subida) fica visível.
func TestReplicationStatsPublishesTheLagInBytes(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(3, "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("events", fmt.Sprintf(replicatedDataset, broker))
	appendKafkaTargets(t, a, broker, [2]string{"cdc", "cdc"})
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	startWebhookServer(t, srv)
	waitFollowing(t, srv, "events", 1)

	// Segura só o Produce do tópico da replicação: a ingestão do `src` segue.
	// Registrado DEPOIS do servidor, o Cleanup solta o broker ANTES do
	// encerramento, que drena a cauda.
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		for _, topic := range req.(*kmsg.ProduceRequest).Topics {
			if topic.Topic == "cdc" {
				cluster.SleepControl(func() { <-release })
				break
			}
		}
		return nil, nil, false
	})
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx,
		&kgo.Record{Topic: "src", Value: []byte(`{"id":"k1","status":"open"}`)},
		&kgo.Record{Topic: "src", Value: []byte(`{"id":"k2","status":"open"}`)},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	labels := `dataset="events",destination="cdc"`
	deadline := time.Now().Add(20 * time.Second)
	for {
		rep := replicationOf(t, srv, "events")
		d := rep.Delivery[0]
		exposition := srv.MetricsExpositionForTest()
		want := fmt.Sprintf("%s{%s} %d\n", metrics.MetReplicationKafkaLagBytes, labels, d.LagBytes)
		// Parada a entrega, o atraso só cresce até a ingestão terminar: os
		// dois lados convergem para o mesmo número diferente de zero.
		if d.LagSequences > 0 && d.LagBytes > 0 && rep.Feed.InputEntries == 4 && strings.Contains(exposition, want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lagBytes no /v1/stats e no /metrics não convergiram para um atraso: %+v\n%s", d, exposition)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
