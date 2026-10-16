package server

import (
	"github.com/nishisan-dev/nishidb/internal/alarmdelivery"
	"github.com/nishisan-dev/nishidb/internal/metrics"
)

func samplesOfAlarmDelivery(dataset string, s alarmdelivery.Stats) []metrics.Sample {
	labels := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}, {Name: metrics.LabelDestination, Value: s.Destination}}
	var out []metrics.Sample
	add := func(name, help string, typ metrics.Type, value float64) {
		out = append(out, metrics.Sample{Family: name, Name: name, Help: help, Type: typ, Labels: labels, Value: value})
	}
	add(metrics.MetAlarmDeliveryMemoryEvents, "eventos aguardando persistência", metrics.TypeGauge, float64(s.MemoryEvents))
	add(metrics.MetAlarmDeliveryMemoryBytes, "bytes em memória aguardando persistência", metrics.TypeGauge, float64(s.MemoryBytes))
	add(metrics.MetAlarmDeliveryDiskEvents, "eventos pendentes em disco", metrics.TypeGauge, float64(s.DiskEvents))
	add(metrics.MetAlarmDeliveryDiskBytes, "bytes dos arquivos pendentes em disco", metrics.TypeGauge, float64(s.DiskBytes))
	add(metrics.MetAlarmDeliveryOldestPendingSeconds, "idade do evento durável mais antigo, em segundos", metrics.TypeGauge, s.OldestPendingSeconds)
	add(metrics.MetAlarmDeliveryDeliveredTotal, "entregas confirmadas com remoção durável desde a subida", metrics.TypeCounter, float64(s.Delivered))
	add(metrics.MetAlarmDeliveryFailedAttemptsTotal, "tentativas de entrega malsucedidas (webhook ou Kafka) desde a subida", metrics.TypeCounter, float64(s.FailedAttempts))
	add(metrics.MetAlarmDeliveryPersistenceErrorsTotal, "falhas de persistência desde a subida", metrics.TypeCounter, float64(s.PersistenceErrors))
	add(metrics.MetAlarmDeliveryLastSuccessTimestamp, "última entrega confirmada, em segundos do epoch", metrics.TypeGauge, s.LastSuccessTimestamp)
	add(metrics.MetAlarmDeliveryAdmissionWaitSecondsTotal, "tempo acumulado de espera da varredura por vaga em memória desde a subida, em segundos", metrics.TypeCounter, s.AdmissionWaitSeconds)
	for _, state := range []struct{ name, value string }{
		{metrics.MetAlarmDeliveryActive, "running"}, {metrics.MetAlarmDeliveryFailed, "failed"}, {metrics.MetAlarmDeliveryPaused, "paused"},
	} {
		value := 0.0
		if s.State == state.value {
			value = 1
		}
		add(state.name, "1 enquanto o destino está neste estado", metrics.TypeGauge, value)
	}
	for _, reason := range alarmdelivery.DropReasons() {
		out = append(out, metrics.Sample{Family: metrics.MetAlarmDeliveryDroppedTotal, Name: metrics.MetAlarmDeliveryDroppedTotal, Help: "eventos descartados por motivo desde a subida", Type: metrics.TypeCounter,
			Labels: []metrics.Label{labels[0], labels[1], {Name: metrics.LabelReason, Value: reason}}, Value: float64(s.Dropped[reason])})
	}
	return out
}
