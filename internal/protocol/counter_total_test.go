package protocol

import (
	"encoding/json"
	"testing"
)

func TestDisabledCounterTotalsAreNullOnAdministrativeResponses(t *testing.T) {
	stats := CounterStats{Name: "rates", Total: 123, TotalDisabled: true}
	b, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if value, present := got["total"]; !present || value != nil {
		t.Fatalf("total = %v, presente = %v", value, present)
	}
	var decoded CounterStats
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.TotalDisabled {
		t.Fatal("cliente perdeu total desativado")
	}
	audit := CounterAudit{TotalDisabled: true, Maintained: 123, Recomputed: 123, AggregateTotal: 123, AggregateRows: 7,
		Sample: []GroupDivergence{{TotalDisabled: true, Maintained: 123, RefsMaintained: 7}}}
	b, err = json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"maintained", "recomputed", "aggregateTotal"} {
		if value, present := got[name]; !present || value != nil {
			t.Fatalf("%s = %v, presente = %v", name, value, present)
		}
	}
	if got["aggregateRows"] != float64(7) {
		t.Fatalf("refs não preservados: %v", got)
	}
	var decodedAudit CounterAudit
	if err := json.Unmarshal(b, &decodedAudit); err != nil {
		t.Fatal(err)
	}
	if !decodedAudit.TotalDisabled || decodedAudit.AggregateRows != 7 {
		t.Fatalf("auditoria decodificada = %+v", decodedAudit)
	}
}
