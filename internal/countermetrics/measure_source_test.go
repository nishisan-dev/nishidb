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

func TestMeasureSourceRestoresAndResetsOnlyWhenItsMeaningChanges(t *testing.T) {
	for _, change := range []string{"none", "unit", "window", "where", "groupBy", "total"} {
		t.Run(change, func(t *testing.T) {
			h := newHarness(t)
			id := h.counter.add("A", 7)
			opts := h.options(false)
			rate := schema.CounterMeasureSpec{Name: "event_rate", Window: time.Minute, Unit: "second"}
			opts.Counters[0].Spec.Measures = []schema.CounterMeasureSpec{rate}
			opts.Counters[0].Reader = fakeMeasureCounter{h.counter, 1800}
			metric := &opts.Counters[0].Spec.Metrics[0]
			metric.Source, metric.Rate = rate.Name, &rate
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
			}
			s = openWith(t, opts)
			m := mustMetric(t, s)
			if change == "none" {
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
