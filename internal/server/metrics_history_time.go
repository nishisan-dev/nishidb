package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

func samplesOfHistoryTime(name string, e storage.HistoryTimeStats) []metrics.Sample {
	labels := []metrics.Label{{Name: metrics.LabelDataset, Value: name}}
	active := float64(0)
	if e.Active {
		active = 1
	}
	return []metrics.Sample{
		{Family: metrics.MetHistoryTimeSegmentsSkipped, Name: metrics.MetHistoryTimeSegmentsSkipped, Help: "fragmentos fechados excluídos das fontes de consulta pelo intervalo de _writtenAt", Type: metrics.TypeCounter, Labels: labels, Value: float64(e.Skipped)},
		{Family: metrics.MetHistoryTimeBoundsPending, Name: metrics.MetHistoryTimeBoundsPending, Help: "fragmentos fechados sem limites temporais completos na última passada de reconstrução", Type: metrics.TypeGauge, Labels: labels, Value: float64(e.Pending)},
		{Family: metrics.MetHistoryTimeBoundsActive, Name: metrics.MetHistoryTimeBoundsActive, Help: "1 enquanto o trabalhador de reconstrução temporal está de pé", Type: metrics.TypeGauge, Labels: labels, Value: active},
		{Family: metrics.MetHistoryTimeBoundsRebuilt, Name: metrics.MetHistoryTimeBoundsRebuilt, Help: "metadados temporais de fragmentos publicados desde a abertura", Type: metrics.TypeCounter, Labels: labels, Value: float64(e.Rebuilt)},
	}
}
