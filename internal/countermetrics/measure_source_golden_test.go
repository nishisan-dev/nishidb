package countermetrics

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// TestMeasureSourceDescriptionIsByteForByteTheSameWithoutScope é o golden
// LITERAL, escrito à mão, do `.source` de uma métrica de janela por taxa num
// contador SEM scope: o texto que o binário anterior ao scope gravava. Se ele
// mudar, toda métrica por taxa perde o histórico na troca de binário
// (counter_metric.source_changed). O que o protege é o `omitempty` do campo
// Scope da descrição. Com scope, o campo aparece no fim.
func TestMeasureSourceDescriptionIsByteForByteTheSameWithoutScope(t *testing.T) {
	for _, tc := range []struct{ scope, want string }{
		{"", `{"Rate":{"Name":"event_rate","Window":60000000000,"Unit":"second"},"GroupBy":["olt"],"Where":"authenticated = true"}`},
		{"eligible = true", `{"Rate":{"Name":"event_rate","Window":60000000000,"Unit":"second"},"GroupBy":["olt"],"Where":"authenticated = true","Scope":"eligible = true"}`},
	} {
		h := newHarness(t)
		h.counter.add("A", 7)
		opts := h.options(false)
		rate := schema.CounterMeasureSpec{Name: "event_rate", Window: time.Minute, Unit: "second"}
		spec := &opts.Counters[0].Spec
		spec.GroupBy, spec.Where, spec.Scope = []string{"olt"}, "authenticated = true", tc.scope
		spec.Measures = []schema.CounterMeasureSpec{rate}
		opts.Counters[0].Reader = fakeMeasureCounter{h.counter, 1800}
		spec.Metrics[0].Source, spec.Metrics[0].Rate = rate.Name, &rate
		s := openWith(t, opts)
		closeSampler(t, s)
		var sources []string
		if err := filepath.WalkDir(h.dir, func(path string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(path, ".source") {
				sources = append(sources, path)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if len(sources) != 1 {
			t.Fatalf("scope %q: %d arquivos .source, quero 1: %v", tc.scope, len(sources), sources)
		}
		got, err := os.ReadFile(sources[0])
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tc.want {
			t.Fatalf("scope %q: .source\n veio %s\nquero %s", tc.scope, got, tc.want)
		}
	}
}
