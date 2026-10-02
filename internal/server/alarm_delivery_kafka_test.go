package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// K12: o destino Kafka de alarmes nasce com as séries em zero, sai no
// /v1/stats com `kind: kafka`, e o /metrics e o /v1/stats leem a MESMA
// estrutura. O alarme vem de uma transição real do runner.
func TestAlarmKafkaDeliveryPublishesSharedStatsBornAtZero(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "olt_alarms"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	a := newEnv(t)
	cfgBytes, err := os.ReadFile(a.cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfgBytes = append(cfgBytes, []byte(fmt.Sprintf("\nalarmDelivery:\n  kafka:\n    - name: olt_alarms\n      bootstrapServers: [%q]\n      topic: olt_alarms\n", cluster.ListenAddrs()[0]))...)
	if err := os.WriteFile(a.cfgPath, cfgBytes, 0600); err != nil {
		t.Fatal(err)
	}
	spec := strings.Replace(fmt.Sprintf(specServicesComAlarme, "servicos"), "        sustainedFor: 10s", "        sustainedFor: 10s\n        notify: [olt_alarms]", 1)
	a.writeDataset("servicos", spec)
	seedServices(t, a, "servicos", 1)
	at := time.Now().UTC()
	clock := newTestClock(at)
	srv, err := server.Novo(a.load(), server.Options{Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	labels := `{dataset="servicos",destination="olt_alarms"} 0`
	initial := srv.MetricsExpositionForTest()
	for _, name := range []string{metrics.MetAlarmDeliveryDeliveredTotal, metrics.MetAlarmDeliveryFailedAttemptsTotal, metrics.MetAlarmDeliveryDiskEvents, metrics.MetAlarmDeliveryAdmissionWaitSecondsTotal} {
		if !strings.Contains(initial, name+labels) {
			t.Fatalf("série do destino Kafka não nasceu em zero: %s\n%s", name, initial)
		}
	}
	stop := startWebhookServer(t, srv)
	defer stop()
	ready := waitDelivery(t, srv, func(s protocol.AlarmDeliveryStats) bool { return s.State == "running" })
	if ready.Kind != "kafka" || ready.Destination != "olt_alarms" {
		t.Fatalf("o /v1/stats não publicou o tipo do destino: %+v", ready)
	}
	for i := 0; i < 400 && !srv.AlarmsActive("servicos"); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if !srv.AlarmsActive("servicos") {
		t.Fatal("laço não subiu")
	}
	srv.StopSamplingAndAlarmsForTest("servicos")
	if err := srv.EvaluateAlarmsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	clock.set(at.Add(11 * time.Second))
	if err := srv.EvaluateAlarmsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics("olt_alarms"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) == 0 {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("o alarme não chegou ao tópico: %v", err)
		}
		records = fetches.Records()
	}
	var event map[string]any
	if err := json.Unmarshal(records[0].Value, &event); err != nil {
		t.Fatal(err)
	}
	var key []string
	if err := json.Unmarshal(records[0].Key, &key); err != nil || len(key) != 3 || key[0] != "servicos" || key[1] != "autenticados_por_olt" || key[2] != event["groupKey"] {
		t.Fatalf("chave fora do formato [dataset, counter, groupKey]: %s (%v)", records[0].Key, err)
	}
	if event["event"] != "alarm.firing" || event["dataset"] != "servicos" {
		t.Fatalf("corpo inesperado: %s", records[0].Value)
	}
	delivered := waitDelivery(t, srv, func(s protocol.AlarmDeliveryStats) bool { return s.Delivered == 1 && s.DiskEvents == 0 })
	if delivered.LastSuccessTimestamp == 0 || delivered.Kind != "kafka" {
		t.Fatalf("estatísticas incompletas: %+v", delivered)
	}
	samples := coletaSamples(t, srv)
	labelSet := map[string]string{metrics.LabelDataset: "servicos", metrics.LabelDestination: "olt_alarms"}
	confereIgual(t, samples, metrics.MetAlarmDeliveryDeliveredTotal, labelSet, int64(delivered.Delivered))
	confereIgual(t, samples, metrics.MetAlarmDeliveryDiskEvents, labelSet, int64(delivered.DiskEvents))
	confereIgual(t, samples, metrics.MetAlarmDeliveryActive, labelSet, 1)
}
