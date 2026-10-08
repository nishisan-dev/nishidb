package alarm

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// TestMeasureAlarmFingerprintIsByteForByteTheSameWithoutScope é o golden
// LITERAL, escrito à mão, da assinatura de um alarme que cita uma medida num
// contador SEM scope. É o texto que o binário anterior ao scope gravava no
// alarms.state (medido igual em origin/main): se ele mudar, todo alarme sobre
// medida perde o cronômetro na troca de binário. O que o protege é o
// `omitempty` do campo Scope do contexto — e é ele que este teste fixa. Com
// scope, o campo aparece no fim.
func TestMeasureAlarmFingerprintIsByteForByteTheSameWithoutScope(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	def.Where = "authenticated = true"
	def.Measures = []counter.MeasureDefinition{{Name: "event_rate", Window: time.Minute, Unit: time.Second}}
	fingerprint := func(d counter.Definition) string {
		t.Helper()
		spec := alarmeDe("event_rate > 1")
		spec.Value = "event_rate"
		out, err := Compile("telecom", specWith(spec), []counter.Definition{d})
		if err != nil {
			t.Fatal(err)
		}
		return out[0].alarms[0].metricsFingerprint
	}
	const withoutScope = `event_rate:rate:60000000000:1000000000;context:{"GroupBy":["olt","slot"],"Where":"authenticated = true"}`
	if got := fingerprint(def); got != withoutScope {
		t.Fatalf("assinatura sem scope mudou:\n veio %s\nquero %s", got, withoutScope)
	}
	def.Scope = "eligible = true"
	const withScope = `event_rate:rate:60000000000:1000000000;context:{"GroupBy":["olt","slot"],"Where":"authenticated = true","Scope":"eligible = true"}`
	if got := fingerprint(def); got != withScope {
		t.Fatalf("assinatura com scope:\n veio %s\nquero %s", got, withScope)
	}
}
