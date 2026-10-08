package server_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
)

func TestHistoryTimeMetricsMatchStatsWithoutIndexes(t *testing.T) {
	// This history has no field indexes; its temporal worker still runs.
	srv := serverWithHistory(t, `version: 1
name: eventos
schemaName: schema_eventos
materialization:
  key:
    field: id
history:
  maxBytes: 4GiB
schema:
  include: ["*"]
  fields:
    id:
      type: string
`)
	var timeStats *protocol.HistoryTimePruningStats
	for _, d := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
		if d.Name == "eventos" && d.History != nil {
			timeStats = d.History.TimePruning
		}
	}
	if timeStats == nil || !timeStats.RebuildRunning {
		t.Fatal("history without indexes has no temporal worker")
	}
	samples := coletaSamples(t, srv)
	for name, want := range map[string]float64{
		metrics.MetHistoryTimeSegmentsSkipped: float64(timeStats.SegmentsSkipped),
		metrics.MetHistoryTimeBoundsPending:   float64(timeStats.Pending),
		metrics.MetHistoryTimeBoundsActive:    1,
		metrics.MetHistoryTimeBoundsRebuilt:   float64(timeStats.Rebuilt),
	} {
		got, ok := achaSerie(samples, name, map[string]string{metrics.LabelDataset: "eventos_history"})
		if !ok || got.Value != want {
			t.Fatalf("%s: got=%+v present=%v want=%v", name, got, ok, want)
		}
		if _, ok := achaSerie(samples, name, map[string]string{metrics.LabelDataset: "eventos"}); ok {
			t.Fatalf("%s repeated under base dataset", name)
		}
	}
}
