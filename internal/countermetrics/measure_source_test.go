package countermetrics

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

type fakeMeasureCounter struct {
	*fakeCounter
	count int64
}

func (c fakeMeasureCounter) MeasureCount(uint32, int, int64) int64 { return c.count }

// O scope entra na descrição da grandeza (R4 do plano do escopo): trocá-lo
// zera a história e sai como counter_metric.source_changed; mantê-lo — com ou
// sem scope — restaura.
func TestMeasureSourceRestoresAndResetsOnlyWhenItsMeaningChanges(t *testing.T) {
	for _, change := range []string{"none", "unit", "window", "where", "groupBy", "total", "scope", "scopeKept"} {
		t.Run(change, func(t *testing.T) {
			h := newHarness(t)
			id := h.counter.add("A", 7)
			opts := h.options(false)
			rate := schema.CounterMeasureSpec{Name: "event_rate", Window: time.Minute, Unit: "second"}
			opts.Counters[0].Spec.Measures = []schema.CounterMeasureSpec{rate}
			opts.Counters[0].Reader = fakeMeasureCounter{h.counter, 1800}
			metric := &opts.Counters[0].Spec.Metrics[0]
			metric.Source, metric.Rate = rate.Name, &rate
			if change == "scopeKept" {
				opts.Counters[0].Spec.Scope = "eligible = true"
			}
			s := openWith(t, opts)
			sampleOnce(t, s)
			wantAggregate(t, mustMetric(t, s), id, 1, 1800, 1800, 1800)
			closeSampler(t, s)
			switch change {
			case "unit":
				rate.Unit = "minute"
			case "window":
				rate.Window = 5 * time.Minute
			case "where":
				opts.Counters[0].Spec.Where = "active = true"
			case "groupBy":
				opts.Counters[0].Spec.GroupBy = []string{"region"}
			case "total":
				metric.Source, metric.Rate = schema.MetricSourceTotal, nil
			case "scope":
				opts.Counters[0].Spec.Scope = "eligible = true"
			}
			changed := len(h.logs.events(t, "counter_metric.source_changed"))
			s = openWith(t, opts)
			m := mustMetric(t, s)
			resets := len(h.logs.events(t, "counter_metric.source_changed")) - changed
			if (change == "none" || change == "scopeKept") != (resets == 0) {
				t.Fatalf("%s: %d counter_metric.source_changed na reabertura", change, resets)
			}
			if change == "none" || change == "scopeKept" {
				wantAggregate(t, m, id, 1, 1800, 1800, 1800)
			} else {
				wantNoValue(t, m, id)
			}
			sampleOnce(t, s)
			if change == "total" {
				wantAggregate(t, m, id, 1, 7, 7, 7)
			}
		})
	}
}
