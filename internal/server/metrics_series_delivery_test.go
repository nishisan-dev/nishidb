package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"testing"
)

func TestSeriesDeliveryMetricsUseSharedStats(t *testing.T) {
	s := protocol.SeriesDeliveryStats{Counter: "c", Destination: "dest", Delivered: 3, PendingRecords: 5, PendingBytes: 7, LastSuccessTimestamp: 11, Dropped: map[string]uint64{"timeout": 13, "buffer_full": 17}}
	want := map[string]float64{metrics.MetSeriesKafkaDeliveredTotal: 3, metrics.MetSeriesKafkaPendingRecords: 5, metrics.MetSeriesKafkaPendingBytes: 7, metrics.MetSeriesKafkaLastSuccessTimestamp: 11}
	for _, sample := range samplesOfSeriesDelivery("d", s) {
		if sample.Labels[0].Value != "d" || sample.Labels[1].Value != "c" || sample.Labels[2].Value != "dest" {
			t.Fatal(sample)
		}
		if sample.Name == metrics.MetSeriesKafkaDroppedTotal {
			if sample.Value != float64(s.Dropped[sample.Labels[3].Value]) {
				t.Fatal(sample)
			}
		} else {
			if v, ok := want[sample.Name]; !ok || v != sample.Value {
				t.Fatal(sample)
			}
			delete(want, sample.Name)
		}
	}
	if len(want) != 0 {
		t.Fatal(want)
	}
}
