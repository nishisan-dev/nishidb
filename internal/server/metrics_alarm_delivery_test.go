package server

import (
	"github.com/nishisan-dev/nishidb/internal/alarmdelivery"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"testing"
)

// Valores distintos e não nulos detectam troca de fios e contadores congelados.
func TestDeliveryMetricsProjectDistinctValuesAndStates(t *testing.T) {
	s := alarmdelivery.Stats{Destination: "noc", State: "failed", MemoryEvents: 2, MemoryBytes: 3, DiskEvents: 5, DiskBytes: 7, OldestPendingSeconds: 11, Delivered: 13, FailedAttempts: 17, PersistenceErrors: 19, LastSuccessTimestamp: 23, AdmissionWaitSeconds: 31, Dropped: map[string]uint64{alarmdelivery.DropDiskFull: 29}}
	want := map[string]float64{
		metrics.MetAlarmDeliveryMemoryEvents: 2, metrics.MetAlarmDeliveryMemoryBytes: 3,
		metrics.MetAlarmDeliveryDiskEvents: 5, metrics.MetAlarmDeliveryDiskBytes: 7,
		metrics.MetAlarmDeliveryOldestPendingSeconds: 11, metrics.MetAlarmDeliveryDeliveredTotal: 13,
		metrics.MetAlarmDeliveryFailedAttemptsTotal: 17, metrics.MetAlarmDeliveryPersistenceErrorsTotal: 19,
		metrics.MetAlarmDeliveryLastSuccessTimestamp: 23, metrics.MetAlarmDeliveryActive: 0,
		metrics.MetAlarmDeliveryFailed: 1, metrics.MetAlarmDeliveryPaused: 0,
		metrics.MetAlarmDeliveryAdmissionWaitSecondsTotal: 31,
	}
	for _, sample := range samplesOfAlarmDelivery("d", s) {
		if sample.Name != sample.Family {
			t.Fatal("família divergente")
		}
		if sample.Name == metrics.MetAlarmDeliveryDroppedTotal {
			reason := sample.Labels[2].Value
			if sample.Value != float64(s.Dropped[reason]) {
				t.Fatal("descarte divergente")
			}
		} else if v, ok := want[sample.Name]; !ok || v != sample.Value {
			t.Fatalf("amostra incorreta: %+v", sample)
		} else {
			delete(want, sample.Name)
		}
	}
	if len(want) != 0 {
		t.Fatalf("séries ausentes: %v", want)
	}
	for _, state := range []string{"running", "paused", "ready", "closed"} {
		s.State = state
		for _, sample := range samplesOfAlarmDelivery("d", s) {
			switch sample.Name {
			case metrics.MetAlarmDeliveryActive:
				if (sample.Value == 1) != (state == "running") {
					t.Fatal("ativo divergente")
				}
			case metrics.MetAlarmDeliveryPaused:
				if (sample.Value == 1) != (state == "paused") {
					t.Fatal("pausado divergente")
				}
			case metrics.MetAlarmDeliveryFailed:
				if sample.Value != 0 {
					t.Fatal("falha divergente")
				}
			}
		}
	}
}
