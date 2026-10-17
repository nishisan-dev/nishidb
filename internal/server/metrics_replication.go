package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
)

// registerMetricsOfReplication publica as séries do feed de replicação e dos
// publicadores Kafka.
//
// Coletor PULL sobre replicationStats, a MESMA leitura do bloco `replication`
// do /v1/stats: um contador mantido aqui seria a segunda fonte do mesmo fato.
// Registrado depois de todos os datasets abertos, pela razão do coletor de
// dataset: antes, um scrape veria metade deles.
func registerMetricsOfReplication(reg *metrics.Registry, s *Server) {
	reg.Register(metrics.CollectorFunc(func() []metrics.Sample {
		var out []metrics.Sample
		for _, d := range s.datasets {
			if st := replicationStats(d); st != nil {
				out = append(out, samplesOfReplication(d.nome, st)...)
			}
		}
		return out
	}))
}

// samplesOfReplication converte o retrato de um dataset em séries. Toda
// série de motivo e de estado sai para TODOS os valores das listas fechadas
// do publicador, inclusive os que o mapa não trouxer: série que só aparece
// no primeiro erro não serve para alarme, porque ausência não se distingue de
// alvo fora do ar.
func samplesOfReplication(dataset string, st *protocol.ReplicationStats) []metrics.Sample {
	feedLabels := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}}
	f := st.Feed
	out := []metrics.Sample{
		{Family: metrics.MetReplicationFeedInputEntriesTotal, Name: metrics.MetReplicationFeedInputEntriesTotal,
			Help: "entries published to the replication feed by the writer", Type: metrics.TypeCounter,
			Labels: feedLabels, Value: float64(f.InputEntries)},
		{Family: metrics.MetReplicationFeedOutputEntriesTotal, Name: metrics.MetReplicationFeedOutputEntriesTotal,
			Help: "entries emitted in closed replication feed batches, after coalescing", Type: metrics.TypeCounter,
			Labels: feedLabels, Value: float64(f.OutputEntries)},
		{Family: metrics.MetReplicationFeedBufferedBytes, Name: metrics.MetReplicationFeedBufferedBytes,
			Help: "bytes retained by the replication feed", Type: metrics.TypeGauge,
			Labels: feedLabels, Value: float64(f.BufferedBytes)},
		{Family: metrics.MetReplicationFeedEvictedBatchesTotal, Name: metrics.MetReplicationFeedEvictedBatchesTotal,
			Help: "replication feed batches evicted by the memory ceiling", Type: metrics.TypeCounter,
			Labels: feedLabels, Value: float64(f.EvictedBatches)},
		{Family: metrics.MetReplicationFeedInvalidationsTotal, Name: metrics.MetReplicationFeedInvalidationsTotal,
			Help: "times the replication feed dropped its retained range and forced a resync", Type: metrics.TypeCounter,
			Labels: feedLabels, Value: float64(f.Invalidations)},
		{Family: metrics.MetReplicationFeedDiskBytes, Name: metrics.MetReplicationFeedDiskBytes,
			Help: "bytes of the replication feed segment log on disk, segment headers included", Type: metrics.TypeGauge,
			Labels: feedLabels, Value: float64(f.DiskBytes)},
		{Family: metrics.MetReplicationFeedDiskSegments, Name: metrics.MetReplicationFeedDiskSegments,
			Help: "segments of the replication feed log on disk", Type: metrics.TypeGauge,
			Labels: feedLabels, Value: float64(f.DiskSegments)},
		{Family: metrics.MetReplicationFeedDiskErrorsTotal, Name: metrics.MetReplicationFeedDiskErrorsTotal,
			Help: "failed disk operations of the replication feed, one per failed attempt", Type: metrics.TypeCounter,
			Labels: feedLabels, Value: float64(f.DiskErrors)},
	}
	for _, p := range st.Delivery {
		labels := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}, {Name: metrics.LabelDestination, Value: p.Destination}}
		out = append(out,
			metrics.Sample{Family: metrics.MetReplicationKafkaDeliveredTotal, Name: metrics.MetReplicationKafkaDeliveredTotal,
				Help: "replication records acknowledged by Kafka", Type: metrics.TypeCounter,
				Labels: labels, Value: float64(p.Delivered)},
			metrics.Sample{Family: metrics.MetReplicationKafkaLagSequences, Name: metrics.MetReplicationKafkaLagSequences,
				Help: "feed sequences produced beyond the publisher's acknowledged cursor", Type: metrics.TypeGauge,
				Labels: labels, Value: float64(p.LagSequences)},
			metrics.Sample{Family: metrics.MetReplicationKafkaLastSuccessTimestamp, Name: metrics.MetReplicationKafkaLastSuccessTimestamp,
				Help: "last complete replication delivery in epoch seconds; zero until the first", Type: metrics.TypeGauge,
				Labels: labels, Value: p.LastSuccessTimestamp},
			metrics.Sample{Family: metrics.MetReplicationKafkaLagBytes, Name: metrics.MetReplicationKafkaLagBytes,
				Help: "feed frame bytes on disk beyond the publisher's acknowledged cursor", Type: metrics.TypeGauge,
				Labels: labels, Value: float64(p.LagBytes)},
			metrics.Sample{Family: metrics.MetReplicationKafkaCheckpointSequence, Name: metrics.MetReplicationKafkaCheckpointSequence,
				Help: "feed sequence of the last destination checkpoint written; the next start resumes from it", Type: metrics.TypeGauge,
				Labels: labels, Value: float64(p.CheckpointCursor)},
		)
		for _, reason := range replicationkafka.SnapshotReasons() {
			out = append(out, metrics.Sample{Family: metrics.MetReplicationKafkaSnapshotsTotal, Name: metrics.MetReplicationKafkaSnapshotsTotal,
				Help: "replication snapshots completed on Kafka, by reason", Type: metrics.TypeCounter,
				Labels: withLabel(labels, metrics.LabelReason, reason), Value: float64(p.Snapshots[reason])})
		}
		for _, reason := range replicationkafka.ErrorReasons() {
			out = append(out, metrics.Sample{Family: metrics.MetReplicationKafkaErrorsTotal, Name: metrics.MetReplicationKafkaErrorsTotal,
				Help: "failed replication delivery attempts, by reason", Type: metrics.TypeCounter,
				Labels: withLabel(labels, metrics.LabelReason, reason), Value: float64(p.Errors[reason])})
		}
		for _, state := range replicationkafka.States() {
			value := 0.0
			if state == p.State {
				value = 1
			}
			out = append(out, metrics.Sample{Family: metrics.MetReplicationKafkaState, Name: metrics.MetReplicationKafkaState,
				Help: "1 on the current state of the replication publisher, 0 on the others", Type: metrics.TypeGauge,
				Labels: withLabel(labels, metrics.LabelPublisherState, state), Value: value})
		}
	}
	return out
}
