package schema

import (
	"reflect"
	"strings"
	"testing"
)

func TestAlarmNotifyParsingAndDuplicateRefusal(t *testing.T) {
	base := yamlWithCounters(`
counters:
  - name: by_olt
    groupBy: [olt]
    alarms:
      - name: down
        when: total = 0
        sustainedFor: 10s
        notify: [noc, backup]
`)
	spec, err := Parse([]byte(base), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec.Counters[0].Alarms[0].Notify, []string{"noc", "backup"}) {
		t.Fatal("notify não chegou à spec")
	}
	_, err = Parse([]byte(strings.Replace(base, "[noc, backup]", "[noc, noc]", 1)), BuiltinDefaults)
	if err == nil || !strings.Contains(err.Error(), "invalid or duplicate destination") {
		t.Fatalf("guarda de duplicata ausente: %v", err)
	}
}
