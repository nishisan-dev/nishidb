package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/seriesdelivery"
)

func samplesOfSeriesDelivery(dataset string, s protocol.SeriesDeliveryStats) []metrics.Sample {
	labels := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}, {Name: metrics.LabelCounter, Value: s.Counter}, {Name: metrics.LabelDestination, Value: s.Destination}}
	out := make([]metrics.Sample, 0, 4+len(seriesdelivery.DropReasons()))
	add := func(name, help string, typ metrics.Type, value float64) {
		out = append(out, metrics.Sample{Family: name, Name: name, Help: help, Type: typ, Labels: labels, Value: value})
	}
	add(metrics.MetSeriesKafkaDeliveredTotal, "counter samples acknowledged by Kafka", metrics.TypeCounter, float64(s.Delivered))
	add(metrics.MetSeriesKafkaPendingRecords, "counter samples awaiting Kafka completion", metrics.TypeGauge, float64(s.PendingRecords))
	add(metrics.MetSeriesKafkaPendingBytes, "key and value bytes awaiting Kafka completion", metrics.TypeGauge, float64(s.PendingBytes))
	add(metrics.MetSeriesKafkaLastSuccessTimestamp, "last Kafka acknowledgement in epoch seconds", metrics.TypeGauge, s.LastSuccessTimestamp)
	for _, reason := range seriesdelivery.DropReasons() {
		out = append(out, metrics.Sample{Family: metrics.MetSeriesKafkaDroppedTotal, Name: metrics.MetSeriesKafkaDroppedTotal, Help: "counter samples discarded or not confirmed by Kafka", Type: metrics.TypeCounter,
			Labels: []metrics.Label{labels[0], labels[1], labels[2], {Name: metrics.LabelReason, Value: reason}}, Value: float64(s.Dropped[reason])})
	}
	return out
}
