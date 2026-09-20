package schema

import (
	"strings"
	"testing"
)

const measureContract = `counters:
  - name: rates
    groupBy: [olt]
    total: false
    measures:
      - name: event_rate
        window: 1m
        unit: second
      - name: minute_rate
        window: 5m
        unit: minute
    metrics:
      - name: rate_history
        type: window
        source: event_rate
        window: 24h
        aggregates: [avg, min, max]
    emit:
      every: 10s
      keep: 100
`

func TestCounterMeasuresContractAndDefaults(t *testing.T) {
	for _, total := range []string{"false", "true", "omitted"} {
		t.Run(total, func(t *testing.T) {
			block := strings.Replace(measureContract, "total: false", "total: "+total, 1)
			if total == "omitted" {
				block = strings.Replace(measureContract, "    total: false\n", "", 1)
			}
			s, err := Parse([]byte(yamlWithCounters(block)), BuiltinDefaults)
			if err != nil {
				t.Fatal(err)
			}
			c := s.Counters[0]
			if c.DisableTotal != (total == "false") || len(c.Measures) != 2 || c.Metrics[0].Rate == nil || !s.RequiresWriteInstant() {
				t.Fatalf("contrato divergente: %+v", c)
			}
			if got := 1800 * c.Measures[0].Scale(); got != 30 {
				t.Fatalf("taxa = %v, quero 30", got)
			}
		})
	}
}

func TestCounterMeasureRefusals(t *testing.T) {
	cases := []struct{ name, old, new, hint string }{
		{"null total", "total: false", "total: null", "must be a boolean"},
		{"string total", "total: false", "total: \"false\"", "must be a boolean"},
		{"unit", "unit: second", "unit: millisecond", "unit"},
		{"window", "window: 1m", "window: 90s", "multiple of 1m"},
		{"unknown", "source: event_rate", "source: missing", "source"},
		{"disabled total", "source: event_rate", "source: total", "source"},
		{"duplicate", "name: minute_rate", "name: event_rate", "repeated"},
		{"fixed column", "name: minute_rate", "name: total", "collides"},
		{"group column", "name: minute_rate", "name: olt", "collides"},
		{"aggregate column", "name: minute_rate", "name: rate_history_min", "collides"},
		{"unknown property", "unit: second", "unit: second\n        type: rate", "type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(yamlWithCounters(strings.Replace(measureContract, tc.old, tc.new, 1))), BuiltinDefaults)
			if err == nil || !strings.Contains(err.Error(), tc.hint) {
				t.Fatalf("recusa = %v, quero %s", err, tc.hint)
			}
		})
	}
	_, err := Parse([]byte(yamlWithCounters("counters:\n  - name: empty\n    groupBy: [olt]\n    total: false\n")), BuiltinDefaults)
	if err == nil || !strings.Contains(err.Error(), "requires at least one measure") {
		t.Fatalf("contador vazio: %v", err)
	}
}
