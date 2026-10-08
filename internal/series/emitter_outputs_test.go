package series

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type capturedSample struct {
	key, body []byte
	at        time.Time
}
type capturingOutput struct{ samples []capturedSample }

func (o *capturingOutput) Publish(_ context.Context, key, body []byte, at time.Time) error {
	o.samples = append(o.samples, capturedSample{bytes.Clone(key), bytes.Clone(body), at})
	return nil
}

func TestOneReadingFeedsFileAndAllKafkaOutputs(t *testing.T) {
	f := newFakeCounter(t, group{"A", 1, 0, true, 0}, group{"B", 2, 7, true, 9}, group{"old", 3, 8, false, 0})
	a, b := &capturingOutput{}, &capturingOutput{}
	dir := t.TempDir()
	e, err := NewEmitter(Options{Dataset: "services", Counter: "by_olt", Columns: []string{"olt", "slot"}, Reader: f,
		Metrics: []Metric{{Name: "load", Aggregates: []string{"avg"}}}, Outputs: []Output{a, b}, Dir: dir, File: "samples.ndjson", MaxBytes: 1 << 20, Keep: 2, Every: time.Minute, Now: func() time.Time { return t0 }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if len(a.samples) != 2 || !reflect.DeepEqual(a.samples, b.samples) {
		t.Fatalf("fanout mismatch: %+v %+v", a, b)
	}
	rows := readRows(t, dir, "samples.ndjson")
	for i, sample := range a.samples {
		var row map[string]any
		if err := json.Unmarshal(sample.body, &row); err != nil {
			t.Fatal(err)
		}
		if row["dataset"] != "services" || !sample.at.Equal(t0) {
			t.Fatal(row)
		}
		delete(row, "dataset")
		if !reflect.DeepEqual(row, rows[i]) {
			t.Fatalf("file and Kafka values diverged: %v %v", row, rows[i])
		}
		if v, ok := row["load_avg"]; !ok || v != nil {
			t.Fatalf("missing null metric: %v", row)
		}
	}
	if string(a.samples[0].key) != `["services","by_olt","A",1]` {
		t.Fatalf("typed key: %s", a.samples[0].key)
	}
	if rows[0]["total"] != float64(0) {
		t.Fatal("zero group missing")
	}
	// `rows` vai nas DUAS saídas, arquivo e Kafka, com o valor dos membros e
	// não o do total (7 contra 9).
	var kafkaRow map[string]any
	if err := json.Unmarshal(a.samples[1].body, &kafkaRow); err != nil {
		t.Fatal(err)
	}
	if kafkaRow["rows"] != float64(9) || kafkaRow["total"] != float64(7) || rows[1]["rows"] != float64(9) {
		t.Fatalf("rows do grupo B: Kafka %v, arquivo %v; quero 9 nos dois (total 7)", kafkaRow, rows[1])
	}
	// Fail the file after the first sweep. Both Kafka outputs must continue.
	if err := e.w.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatalf("file failure stopped Kafka: %v", err)
	}
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if len(a.samples) != 6 || !e.fileFailed || e.Stats().Errors != 1 {
		t.Fatalf("file failure not isolated: %+v", e.Stats())
	}
}

func TestKafkaOnlyCreatesNoDirectoryAndKeyDoesNotUseDisplayText(t *testing.T) {
	f := newFakeCounter(t, group{"A|B", 1, 1, true, 0}, group{"A", 2, 2, true, 0})
	f.texts[0], f.texts[1] = []byte("same display"), []byte("same display")
	o := &capturingOutput{}
	dir := filepath.Join(t.TempDir(), "series")
	opts := Options{Dataset: "one", Counter: "same", Columns: []string{"olt", "slot"}, Reader: f, DisableFile: true, Outputs: []Output{o}, Every: time.Minute, Dir: dir}
	for _, dataset := range []string{"one", "two"} {
		opts.Dataset = dataset
		e, err := NewEmitter(opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.EmitOnce(context.Background(), t0); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if e.Stats().File {
			t.Fatal("Kafka-only counted as file")
		}
	}
	seen := make(map[string]bool)
	for _, s := range o.samples {
		if seen[string(s.key)] {
			t.Fatalf("key collision: %s", s.key)
		}
		seen[string(s.key)] = true
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Kafka created series directory: %v", err)
	}
}
