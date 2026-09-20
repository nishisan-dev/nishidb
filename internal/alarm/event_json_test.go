package alarm

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

func TestJSONAndLogShareEventProjectionAndRedaction(t *testing.T) {
	for _, state := range []string{StateFiring, StateResolved} {
		t.Run(state, func(t *testing.T) {
			e := eventoDeExemplo()
			e.ID = "shared"
			e.State = state
			e.Columns = append(e.Columns, Column{Name: "token", Value: "private"})
			e.Metrics = []Column{{Name: "auth_avg", Value: "1.25"}, {Name: "password", Value: "private"}}
			e.Enrichment = []Enrichment{{Name: "data", Rows: json.RawMessage(`[{"id":"a"}]`)}, {Name: "missing", Note: "timeout"}}
			e.Restored = true
			e.DowntimeMs = 5000
			e.QualifyCondition = "auth_max > 0"
			if state == StateFiring {
				e.ValueColumn = "total"
				e.HasFiringValue = true
				e.FiringValue = 42
			} else {
				e.Reason = ReasonDisqualified
				e.Enrichment = nil
			}
			var buffer bytes.Buffer
			NewLogSink(logging.New(&buffer, logging.FormatJSON, slog.LevelDebug)).Emit(e)
			var fromLog, fromJSON map[string]any
			if err := json.Unmarshal(buffer.Bytes(), &fromLog); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &fromJSON); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"timestamp", "component", "level", "message"} {
				delete(fromLog, key)
			}
			if !reflect.DeepEqual(fromLog, fromJSON) {
				t.Fatalf("projeções divergentes:\nlog=%v\njson=%v", fromLog, fromJSON)
			}
			if bytes.Contains(raw, []byte("private")) {
				t.Fatal("valor sensível vazou")
			}
		})
	}
}
