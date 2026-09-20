package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

const measureDataset = `version: 1
name: servicos
schemaName: telecom
materialization:
  key:
    field: customerId
  sequence:
    field: seq
schema:
  fields:
    customerId:
      type: string
    seq:
      type: int64
    olt:
      type: string
    authenticated:
      type: boolean
counters:
  - name: rates
    groupBy: [olt]
    where: authenticated = true
    total: false
    measures:
      - name: event_rate
        window: 1m
        unit: second
      - name: minute_rate
        window: 5m
        unit: minute
    metrics:
      - name: rates_24h
        type: window
        source: event_rate
        window: 24h
        aggregates: [avg, min, max]
    alarms:
      - name: burst
        when: event_rate > 0
        value: event_rate
        sustainedFor: 10s
    emit:
      every: 10s
`

func TestMeasuresReachSQLFileKafkaAndDecayWithoutNewEvents(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "rates"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("servicos", measureDataset+"      target: [file, rates]\n")
	config, err := os.ReadFile(a.cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	config = append(config, []byte(fmt.Sprintf("\nseriesDelivery:\n  kafka:\n    - name: rates\n      bootstrapServers: [%q]\n      topic: rates\n", cluster.ListenAddrs()[0]))...)
	if err := os.WriteFile(a.cfgPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	// Os patches preservam grupo e where, mas cada escrita aceita é ocorrência.
	events := []string{`{"customerId":"x","seq":1,"olt":"A","authenticated":true}`}
	for i := 2; i <= 30; i++ {
		events = append(events, fmt.Sprintf(`{"customerId":"x","seq":%d}`, i))
	}
	events = append(events, `{"customerId":"x","seq":30}`, `{"customerId":"x","seq":31,"authenticated":false}`, `{"customerId":"bad","seq":1,"authenticated":true}`)
	applyWithCounters(t, a, "servicos", events)
	clock := newTestClock(time.Now())
	srv := stoppedServer(t, a, clock)
	if err := srv.SampleCounterMetricsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	if err := srv.EmitOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	rows := engineRows(t, srv, "SELECT groupKey, total, event_rate, minute_rate, rates_24h_avg, rates_24h_min, rates_24h_max FROM _counters.rates")
	if len(rows) != 1 {
		t.Fatalf("linhas = %v", rows)
	}
	row := rows[0]
	for _, name := range []string{"event_rate", "rates_24h_avg", "rates_24h_min", "rates_24h_max"} {
		if row[name] != 0.5 {
			t.Fatalf("%s = %v, quero 0.5", name, row[name])
		}
	}
	if row["total"] != nil || math.Abs(row["minute_rate"].(float64)-6) > 1e-12 {
		t.Fatalf("total/unidade: %v", row)
	}
	meta := engineRows(t, srv, "SELECT total FROM _meta.counters")
	if len(meta) != 1 || meta[0]["total"] != nil {
		t.Fatalf("meta total: %v", meta)
	}
	if strings.Contains(srv.MetricsExpositionForTest(), metrics.MetCounterCountedRows+`{dataset="servicos",counter="rates"}`) {
		t.Fatal("total desativado publicado no Prometheus")
	}
	lines := seriesLines(t, a, "servicos", "rates")
	if len(lines) != 1 {
		t.Fatalf("file: %v", lines)
	}
	var file map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &file); err != nil {
		t.Fatal(err)
	}
	for col, want := range row {
		if file[col] != want {
			t.Fatalf("file %s = %v, SQL %v", col, file[col], want)
		}
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics("rates"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := reader.PollRecords(ctx, 1)
	if err := got.Err(); err != nil {
		t.Fatal(err)
	}
	records := got.Records()
	if len(records) != 1 {
		t.Fatalf("Kafka: %d registros", len(records))
	}
	var kafka map[string]any
	if err := json.Unmarshal(records[0].Value, &kafka); err != nil {
		t.Fatal(err)
	}
	file["dataset"] = "servicos"
	if !reflect.DeepEqual(file, kafka) {
		t.Fatalf("Kafka e file divergem: %v / %v", kafka, file)
	}
	if string(records[0].Key) != `["servicos","rates","A"]` {
		t.Fatalf("chave Kafka: %s", records[0].Key)
	}
	if err := srv.EvaluateAlarmsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	clock.set(clock.Now().Add(10 * time.Second))
	if err := srv.EvaluateAlarmsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	alarms := engineRows(t, srv, "SELECT firingValue, valueColumn FROM _counters.alarms")
	if len(alarms) != 1 || alarms[0]["firingValue"] != 0.5 || alarms[0]["valueColumn"] != "event_rate" {
		t.Fatalf("alarme não usou a taxa: %v", alarms)
	}
	clock.set(clock.Now().Add(61 * time.Second))
	if err := srv.SampleCounterMetricsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	rows = engineRows(t, srv, "SELECT event_rate, rates_24h_min, rates_24h_max FROM _counters.rates")
	if rows[0]["event_rate"] != float64(0) || rows[0]["rates_24h_min"] != float64(0) || rows[0]["rates_24h_max"] != 0.5 {
		t.Fatalf("janela sem eventos: %v", rows)
	}
	if err := srv.EvaluateAlarmsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	if alarms := engineRows(t, srv, "SELECT firingValue FROM _counters.alarms"); len(alarms) != 0 {
		t.Fatalf("alarme não resolveu ao zerar a taxa: %v", alarms)
	}
}

func TestMeasuresDoNotRequireMetricsOrEmission(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	spec := measureDataset[:strings.Index(measureDataset, "    metrics:")]
	a.writeDataset("servicos", spec)
	applyWithCounters(t, a, "servicos", []string{`{"customerId":"x","seq":1,"olt":"A","authenticated":true}`})
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close() }()
	rows := engineRows(t, srv, "SELECT total, event_rate FROM _counters.rates")
	if len(rows) != 1 || rows[0]["total"] != nil || rows[0]["event_rate"] != 1.0/60 {
		t.Fatalf("measure isolada: %v", rows)
	}
}
