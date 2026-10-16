package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// specWithAnchor é o contrato do service_state, mais a regra condicional da
// publicação antiga na frente, para conferir que ela sai como era.
func specWithAnchor() *schema.DatasetSpec {
	return &schema.DatasetSpec{
		Name: "d",
		Materialization: schema.MaterializationSpec{
			TTLRules: []schema.TTLRule{
				{When: "type = 2", Duration: 3 * time.Minute},
				{When: "lastAuthAt IS NULL", Duration: time.Hour},
				{Anchor: "lastAuthAt", Duration: 48 * time.Hour},
			},
		},
	}
}

// A regra ancorada publica a âncora e a duração sob `expireAfter`, e deixa
// `expireAfterWrite` VAZIO: 48h sob `expireAfterWrite` diria que a escrita
// renova o prazo, que é o que a âncora impede. As regras sem âncora saem byte
// a byte como saíam antes dos campos novos.
func TestMaterializationStatsPublishesAnchoredRuleUnderItsOwnKeys(t *testing.T) {
	snap := materializer.MetricsSnapshot{
		TTLRuleMatches:    []int64{4, 2, 7},
		TTLAnchorAbsent:   []int64{0, 0, 3},
		TTLAnchorInFuture: 5,
		TTLExpiredOnWrite: 6,
	}
	got := materializationStats(specWithAnchor(), snap)

	raw, err := json.Marshal(got.TTLRules)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"index":0,"when":"type = 2","expireAfterWrite":"3m0s","matched":4},` +
		`{"index":1,"when":"lastAuthAt IS NULL","expireAfterWrite":"1h0m0s","matched":2},` +
		`{"index":2,"expireAfterWrite":"","matched":7,"anchor":"lastAuthAt","expireAfter":"48h0m0s","anchorAbsent":3}]`
	if string(raw) != want {
		t.Errorf("ttlRules =\n%s\nquero\n%s", raw, want)
	}
	if got.TTLAnchorInFuture != 5 || got.TTLExpiredOnWrite != 6 {
		t.Errorf("inFuture=%d expiredOnWrite=%d, quero 5 e 6", got.TTLAnchorInFuture, got.TTLExpiredOnWrite)
	}
}

// A regra ancorada publica `anchorAbsent` mesmo em zero: série que só aparece
// quando há o que mostrar vira lacuna, e lacuna não se distingue de defeito.
func TestMaterializationStatsPublishesZeroAnchorAbsent(t *testing.T) {
	got := materializationStats(specWithAnchor(), materializer.MetricsSnapshot{})
	if got.TTLRules[2].AnchorAbsent == nil || *got.TTLRules[2].AnchorAbsent != 0 {
		t.Errorf("anchorAbsent da regra ancorada = %v, quero 0 presente", got.TTLRules[2].AnchorAbsent)
	}
}

// Cada série nova lê o SEU contador, e a de âncora ausente só existe para a
// regra ancorada. Os valores são distintos para que uma série lendo o
// contador vizinho não passe.
func TestMaterializationSamplesPublishAnchorSeries(t *testing.T) {
	snap := materializer.MetricsSnapshot{
		TTLRuleMatches:    []int64{4, 2, 7},
		TTLAnchorAbsent:   []int64{0, 0, 3},
		TTLAnchorInFuture: 5,
		TTLExpiredOnWrite: 6,
	}
	values := map[string]float64{}
	for _, a := range samplesOfMaterialization("d", specWithAnchor(), snap) {
		key := a.Name
		for _, l := range a.Labels {
			if l.Name == "rule" {
				key += "{rule=" + l.Value + "}"
			}
		}
		if _, dup := values[key]; dup {
			t.Errorf("série %s publicada duas vezes", key)
		}
		values[key] = a.Value
	}
	want := map[string]float64{
		metrics.MetTTLAnchorAbsent + "{rule=2}": 3,
		metrics.MetTTLAnchorInFuture:            5,
		metrics.MetTTLExpiredOnWrite:            6,
		metrics.MetTTLRuleMatched + "{rule=2}":  7,
	}
	for name, v := range want {
		if got, ok := values[name]; !ok || got != v {
			t.Errorf("%s = %v (presente=%v), quero %v", name, got, ok, v)
		}
	}
	// E nascem com zero: num snapshot vazio, as duas séries sem regra já
	// existem.
	empty := map[string]bool{}
	for _, a := range samplesOfMaterialization("d", specWithAnchor(), materializer.MetricsSnapshot{}) {
		empty[a.Name] = true
	}
	if !empty[metrics.MetTTLAnchorInFuture] || !empty[metrics.MetTTLExpiredOnWrite] || !empty[metrics.MetTTLAnchorAbsent] {
		t.Errorf("séries da âncora ausentes de um snapshot zerado: %v", empty)
	}
	for _, rule := range []string{"0", "1"} {
		if _, ok := values[metrics.MetTTLAnchorAbsent+"{rule="+rule+"}"]; ok {
			t.Errorf("série de âncora ausente publicada para a regra %s, que não tem âncora", rule)
		}
	}
}
