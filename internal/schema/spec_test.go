package schema

import (
	"strings"
	"testing"
	"time"
)

// YAML mínimo válido usado como base; testes de erro mutam trechos.
const baseYAML = `
version: 1
name: radius_sessions
materialization:
  key:
    source: payload
    field: id
    type: string
  mode: merge
  sequence:
    field: _SEQ
    type: int64
    strategy: greater
  ttl:
    field: EventExpireDate
    type: timestamp
schema:
  include:
    - "*"
  exclude:
    - debug
  unknownFields: include
  fields:
    id:
      type: string
      alias: service_id
    _SEQ:
      type: int64
    Acct-Status-Type:
      type: string
      alias: status
      index: bitmap
    NAS-Identifier:
      type: string
      alias: nas
      index: hash
    Framed-IP-Address:
      type: ipv4
      alias: ip
      index: hash
    Event-Timestamp:
      type: timestamp
    EventExpireDate:
      type: timestamp
      alias: expiration
    Acct-Input-Octets:
      type: int64
    ratio:
      type: double
    extra:
      type: auto
query:
  maxRows: 50000
  timeout: 10s
`

func parseOK(t *testing.T, yml string) *DatasetSpec {
	t.Helper()
	s, err := Parse([]byte(yml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return s
}

func parseErr(t *testing.T, yml, wantSubstr string) {
	t.Helper()
	_, err := Parse([]byte(yml))
	if err == nil {
		t.Fatalf("esperava erro contendo %q", wantSubstr)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("erro %q não contém %q", err.Error(), wantSubstr)
	}
}

func TestParseBaseYAML(t *testing.T) {
	s := parseOK(t, baseYAML)
	if s.Name != "radius_sessions" {
		t.Errorf("Name = %q", s.Name)
	}
	if s.Materialization.Key.Field != "id" {
		t.Errorf("Key.Field = %q", s.Materialization.Key.Field)
	}
	if s.Materialization.Mode != "merge" {
		t.Errorf("Mode = %q", s.Materialization.Mode)
	}
	if s.Materialization.Sequence == nil || s.Materialization.Sequence.Field != "_SEQ" {
		t.Errorf("Sequence = %+v", s.Materialization.Sequence)
	}
	if s.Schema.UnknownFields != "include" {
		t.Errorf("UnknownFields = %q", s.Schema.UnknownFields)
	}
	if got := s.Schema.Fields["Acct-Status-Type"]; got.Alias != "status" || got.Index != "bitmap" {
		t.Errorf("Acct-Status-Type = %+v", got)
	}
	if s.Query.MaxRows != 50000 || s.Query.Timeout != 10*time.Second {
		t.Errorf("Query = %+v", s.Query)
	}
}

func TestParseDefaults(t *testing.T) {
	s := parseOK(t, `
version: 1
name: minimal
materialization:
  key:
    field: id
    type: string
schema:
  fields:
    id:
      type: string
`)
	if s.Materialization.Mode != "merge" {
		t.Errorf("Mode default = %q, want merge", s.Materialization.Mode)
	}
	if s.Schema.UnknownFields != "ignore" {
		t.Errorf("UnknownFields default = %q, want ignore", s.Schema.UnknownFields)
	}
	if s.Materialization.Key.Source != "payload" {
		t.Errorf("Key.Source default = %q, want payload", s.Materialization.Key.Source)
	}
	if s.Query.MaxRows != 100000 {
		t.Errorf("MaxRows default = %d, want 100000", s.Query.MaxRows)
	}
	if s.Query.Timeout != 30*time.Second {
		t.Errorf("Timeout default = %v, want 30s", s.Query.Timeout)
	}
	// Sem include e sem "*": include implícito = campos declarados.
	if len(s.Schema.Include) != 1 || s.Schema.Include[0] != "id" {
		t.Errorf("Include implícito = %v, want [id]", s.Schema.Include)
	}
}

func TestParseValidationErrors(t *testing.T) {
	repl := func(old, new string) string {
		if !strings.Contains(baseYAML, old) {
			t.Fatalf("baseYAML não contém %q", old)
		}
		return strings.Replace(baseYAML, old, new, 1)
	}
	cases := []struct {
		name string
		yml  string
		want string
	}{
		{"yaml invalido", "{{not yaml", "yaml"},
		{"version errada", repl("version: 1", "version: 2"), "version"},
		{"nome vazio", repl("name: radius_sessions", "name: \"\""), "name"},
		{"nome invalido", repl("name: radius_sessions", "name: Radius-Sessions"), "name"},
		{"key ausente", repl("    field: id\n", ""), "key.field"},
		{"key tipo nao-string", repl("    field: id\n    type: string", "    field: id\n    type: int64"), "key.type"},
		{"mode invalido", repl("mode: merge", "mode: upsert"), "mode"},
		{"sequence tipo invalido", repl("    type: int64\n    strategy: greater", "    type: string\n    strategy: greater"), "sequence.type"},
		{"strategy invalida", repl("strategy: greater", "strategy: lower"), "sequence.strategy"},
		// Sequence/ttl referenciados devem estar DECLARADOS em fields
		// incondicionalmente (como a key): sob include explícito, um campo
		// não declarado compilaria como auto/TypeNull e quebraria o greater
		// do materializer (sequence) e a expiração (ttl).
		{"sequence nao declarada", "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n  sequence:\n    field: _SEQ\nschema:\n  include: [id, _SEQ]\n  fields:\n    id:\n      type: string\n", "sequence"},
		{"ttl nao declarada", "version: 1\nname: t\nmaterialization:\n  key:\n    field: id\n  ttl:\n    field: exp\nschema:\n  include: [id, exp]\n  fields:\n    id:\n      type: string\n", "ttl"},
		{"sequence declarada com outro tipo", repl("    _SEQ:\n      type: int64", "    _SEQ:\n      type: string"), "_SEQ"},
		{"ttl tipo invalido", repl("  ttl:\n    field: EventExpireDate\n    type: timestamp", "  ttl:\n    field: EventExpireDate\n    type: string"), "ttl.type"},
		{"ttl declarado com outro tipo", repl("    EventExpireDate:\n      type: timestamp", "    EventExpireDate:\n      type: string"), "EventExpireDate"},
		{"unknownFields invalido", repl("unknownFields: include", "unknownFields: drop"), "unknownFields"},
		{"tipo de campo invalido", repl("      type: double", "      type: float"), "float"},
		{"index invalido", repl("index: bitmap", "index: btree"), "btree"},
		{"bitmap em double", repl("    ratio:\n      type: double", "    ratio:\n      type: double\n      index: bitmap"), "ratio"},
		{"hash em json", repl("    extra:\n      type: auto", "    extra:\n      type: json\n      index: hash"), "extra"},
		{"index na key", repl("      alias: service_id", "      alias: service_id\n      index: hash"), "key"},
		{"alias duplicado", repl("      alias: nas", "      alias: status"), "status"},
		{"alias colide com nome", repl("      alias: nas", "      alias: _SEQ"), "_SEQ"},
		{"exclude sem wildcard", strings.Replace(repl("    - \"*\"", "    - id"), "  exclude:\n    - debug\n", "  exclude:\n    - debug\n", 1), "exclude"},
		{"wildcard com outros", repl("    - \"*\"", "    - \"*\"\n    - id"), "*"},
		{"maxRows negativo", repl("maxRows: 50000", "maxRows: -1"), "maxRows"},
		{"timeout invalido", repl("timeout: 10s", "timeout: depressa"), "timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parseErr(t, tc.yml, tc.want)
		})
	}
}

func TestParseSourceKafkaIsAcceptedAndIgnored(t *testing.T) {
	// A seção source é validada só sintaticamente no M1 (Kafka é M2).
	s := parseOK(t, strings.Replace(baseYAML, "materialization:", `source:
  type: kafka
  kafka:
    topic: radius-accounting
materialization:`, 1))
	if s.Source.Type != "kafka" {
		t.Errorf("Source.Type = %q", s.Source.Type)
	}
}
