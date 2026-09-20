package schema

import (
	"reflect"
	"strings"
	"testing"
)

func TestEmitTargetSelection(t *testing.T) {
	for _, tc := range []struct {
		target string
		want   []string
		file   bool
	}{
		{"", []string{"file"}, true},
		{"file", []string{"file"}, true},
		{"metrics", []string{"metrics"}, false},
		{"[file, metrics, backup]", []string{"file", "metrics", "backup"}, true},
	} {
		body := "counters:\n  - name: by_olt\n    groupBy: [olt]\n    emit:\n      every: 300s\n"
		if tc.target != "" {
			body += "      target: " + tc.target + "\n"
		}
		spec, err := Parse([]byte(yamlWithCounters(body)), BuiltinDefaults)
		if err != nil {
			t.Fatal(err)
		}
		e := spec.Counters[0].Emit
		if !reflect.DeepEqual(e.Targets, tc.want) || e.HasFile() != tc.file {
			t.Fatalf("%s: %+v", tc.target, e)
		}
		if !tc.file && (e.File != "" || e.MaxBytes != 0 || e.Keep != 0) {
			t.Fatalf("Kafka inherited file settings: %+v", e)
		}
	}
}

func TestEmitTargetsRejectInvalidAndIneffectiveConfiguration(t *testing.T) {
	base := "counters:\n  - name: by_olt\n    groupBy: [olt]\n    emit:\n      every: 30s\n"
	for _, tail := range []string{
		"target: []", "target: null", "target: ''", "target: 12", "target: [true]", "target: [metrics, metrics]", "target: [../bad]", "target: {name: metrics}",
		"target: metrics\n      file: ''", "target: metrics\n      maxBytes: 1MiB", "target: metrics\n      keep: 1",
	} {
		if _, err := Parse([]byte(yamlWithCounters(base+"      "+tail+"\n")), BuiltinDefaults); err == nil {
			t.Errorf("accepted %s", tail)
		}
	}
}

func TestKafkaPayloadNamesOnlyReservedForKafkaOutputs(t *testing.T) {
	for _, col := range []string{"dataset", "counter", "at"} {
		body := strings.ReplaceAll(yamlWithCounters("counters:\n  - name: by_olt\n    groupBy: [olt]\n    emit:\n      every: 30s\n"), "olt", col)
		if _, err := Parse([]byte(body), BuiltinDefaults); err != nil {
			t.Fatalf("legacy file rejected: %v", err)
		}
		if _, err := Parse([]byte(body+"      target: metrics\n"), BuiltinDefaults); err == nil || !strings.Contains(err.Error(), "reserved Kafka field") {
			t.Fatalf("collision %s: %v", col, err)
		}
	}
}
