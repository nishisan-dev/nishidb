package schema

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// replicationYAML monta o dataset base com o bloco `replication:` dado,
// linha a linha, já indentado sob a chave.
func replicationYAML(lines ...string) string {
	return baseYAML + "replication:\n  " + strings.Join(lines, "\n  ") + "\n"
}

// parseReplicationRefusal exige a recusa com ErrInvalidSpec e com a mensagem
// PRÓPRIA da guarda em teste — outra guarda recusando a mesma entrada não
// pode deixar o teste verde.
func parseReplicationRefusal(t *testing.T, yml, wantSubstr string) {
	t.Helper()
	_, err := Parse([]byte(yml), BuiltinDefaults)
	if err == nil {
		t.Fatalf("esperava recusa contendo %q; o YAML foi aceito:\n%s", wantSubstr, yml)
	}
	if !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("erro %v não embrulha ErrInvalidSpec", err)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("erro %q não contém a mensagem da guarda %q", err.Error(), wantSubstr)
	}
}

func TestReplicationTargetsAreKeptInFileOrderOnEnabledMaster(t *testing.T) {
	s := parseOK(t, replicationYAML("enabled: true", "targets: [metrics, backup_dc2]"))
	if want := []string{"metrics", "backup_dc2"}; !reflect.DeepEqual(s.Replication.Targets, want) {
		t.Fatalf("Targets = %#v, quero %#v", s.Replication.Targets, want)
	}
	explicitMaster := parseOK(t, replicationYAML("enabled: true", "role: master", "targets: [metrics]"))
	if want := []string{"metrics"}; !reflect.DeepEqual(explicitMaster.Replication.Targets, want) {
		t.Fatalf("Targets com role master explícito = %#v, quero %#v", explicitMaster.Replication.Targets, want)
	}
}

func TestReplicationTargetsAbsentLeavesTheListNil(t *testing.T) {
	for _, yml := range []string{
		baseYAML,
		replicationYAML("enabled: true"),
	} {
		s := parseOK(t, yml)
		if s.Replication.Targets != nil {
			t.Fatalf("Targets = %#v sem a chave, quero nil:\n%s", s.Replication.Targets, yml)
		}
	}
}

func TestReplicationTargetsRejectsExplicitEmptyList(t *testing.T) {
	parseReplicationRefusal(t, replicationYAML("enabled: true", "targets: []"),
		"replication.targets must not be empty")
}

func TestReplicationTargetsRejectsValueThatIsNotAList(t *testing.T) {
	// Nulo e escalar não viram "sem destino" nem "um destino": a chave é
	// plural e só aceita lista.
	for _, value := range []string{"", "~", "metrics", "{name: metrics}"} {
		parseReplicationRefusal(t, replicationYAML("enabled: true", "targets: "+value),
			"replication.targets must be a list of Kafka target names")
	}
}

func TestReplicationTargetsRejectsItemThatIsNotAString(t *testing.T) {
	// O item nulo é o caso que o []string do yaml.v3 descartaria em
	// silêncio — `[metrics, ~]` viraria `[metrics]` sem aviso.
	for _, tc := range []struct{ list, want string }{
		{"[metrics, ~]", "replication.targets[1] must be a Kafka target name string, got !!null"},
		{"[1]", "replication.targets[0] must be a Kafka target name string, got !!int"},
		{"[true]", "replication.targets[0] must be a Kafka target name string, got !!bool"},
		{"[[metrics]]", "replication.targets[0] must be a Kafka target name string, got !!seq"},
	} {
		parseReplicationRefusal(t, replicationYAML("enabled: true", "targets: "+tc.list), tc.want)
	}
	// String explícita que só tem dígitos é nome válido: a recusa acima é do
	// tipo do item, não do conteúdo.
	s := parseOK(t, replicationYAML("enabled: true", `targets: ["1"]`))
	if want := []string{"1"}; !reflect.DeepEqual(s.Replication.Targets, want) {
		t.Fatalf("Targets = %#v, quero %#v", s.Replication.Targets, want)
	}
}

func TestReplicationTargetsFollowYAMLAliases(t *testing.T) {
	s := parseOK(t, baseYAML+"x-targets: &kt [metrics]\nreplication:\n  enabled: true\n  targets: *kt\n")
	if want := []string{"metrics"}; !reflect.DeepEqual(s.Replication.Targets, want) {
		t.Fatalf("Targets por alias = %#v, quero %#v", s.Replication.Targets, want)
	}
	// Alias no ITEM, não só na lista inteira: sem resolvê-lo o item seria um
	// AliasNode e cairia na recusa de "não é string".
	s = parseOK(t, baseYAML+"x-name: &n metrics\nreplication:\n  enabled: true\n  targets: [*n, backup]\n")
	if want := []string{"metrics", "backup"}; !reflect.DeepEqual(s.Replication.Targets, want) {
		t.Fatalf("Targets com alias por item = %#v, quero %#v", s.Replication.Targets, want)
	}
	// E o alias resolvido é julgado pelo que aponta: mapa continua recusado.
	parseReplicationRefusal(t, baseYAML+"x-m: &m {a: 1}\nreplication:\n  enabled: true\n  targets: [*m]\n",
		"replication.targets[0] must be a Kafka target name string, got !!map")
}

func TestReplicationRejectsSingularTargetKey(t *testing.T) {
	for _, value := range []string{"[metrics]", "metrics", "~"} {
		parseReplicationRefusal(t, replicationYAML("enabled: true", "target: "+value),
			"replication.target is not a key; use replication.targets (a list of Kafka target names)")
	}
}

func TestReplicationTargetsRejectsFeedNotEnabled(t *testing.T) {
	for _, yml := range []string{
		replicationYAML("targets: [metrics]"),
		replicationYAML("enabled: false", "targets: [metrics]"),
	} {
		parseReplicationRefusal(t, yml, "replication.targets requires replication.enabled: true")
	}
}

func TestReplicationTargetsRejectsFollowerRole(t *testing.T) {
	parseReplicationRefusal(t,
		replicationYAML("enabled: true", "role: follower", "masterURL: https://master.example", "targets: [metrics]"),
		`replication.targets is only valid for role master (role is "follower")`)
}

func TestReplicationTargetsRejectsFileTarget(t *testing.T) {
	parseReplicationRefusal(t, replicationYAML("enabled: true", "targets: [metrics, file]"),
		`replication.targets: "file" is not a Kafka target`)
}

func TestReplicationTargetsRejectsInvalidName(t *testing.T) {
	for _, tc := range []struct{ item, name string }{
		{"Metrics", "Metrics"},
		{"backup-dc2", "backup-dc2"},
		{`""`, ""},
		{strings.Repeat("a", 65), strings.Repeat("a", 65)},
	} {
		parseReplicationRefusal(t, replicationYAML("enabled: true", "targets: ["+tc.item+"]"),
			`replication.targets: invalid Kafka target name "`+tc.name+`"`)
	}
	// O teto de 64 caracteres é inclusivo, como em ValidDeliveryName.
	longest := strings.Repeat("a", 64)
	s := parseOK(t, replicationYAML("enabled: true", "targets: ["+longest+"]"))
	if len(s.Replication.Targets) != 1 || s.Replication.Targets[0] != longest {
		t.Fatalf("nome de 64 caracteres recusado ou perdido: %#v", s.Replication.Targets)
	}
}

func TestReplicationTargetsRejectsDuplicateName(t *testing.T) {
	parseReplicationRefusal(t, replicationYAML("enabled: true", "targets: [metrics, backup, metrics]"),
		`replication.targets: duplicate Kafka target "metrics"`)
}
