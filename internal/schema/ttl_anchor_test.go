package schema

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// anchorSpecYAML monta uma spec com a lista de regras dada e um schema que
// tem um campo de cada classe que a âncora precisa distinguir.
func anchorSpecYAML(rules string) string {
	return "version: 1\nname: t\nschemaName: telecom\nmaterialization:\n  key:\n    field: id\n" + rules +
		"schema:\n  include: [\"*\"]\n  unknownFields: include\n  exclude: [dropped]\n  fields:\n" +
		"    id:\n      type: string\n" +
		"    lastAuthAt:\n      type: timestamp\n" +
		"    seenAt:\n      type: datetime\n      format: \"yyyy-MM-dd HH:mm:ss\"\n" +
		"    hits:\n      type: int64\n" +
		"    loose:\n      type: auto\n" +
		"    dropped:\n      type: timestamp\n" +
		"    firstSeenAt:\n      frozenFrom: \"@writeTime\"\n" +
		"    firstAuthAt:\n      frozenFrom: lastAuthAt\n" +
		"    firstLoose:\n      frozenFrom: loose\n" +
		"    doc:\n      type: json\n      extract:\n" +
		"        - path: since\n          as: docSince\n          type: timestamp\n" +
		"        - path: count\n          as: docCount\n          type: int64\n"
}

// anchorRule é a lista com UMA regra ancorada no campo dado, seguida da
// regra de fundo.
func anchorRule(field string) string {
	return "  ttl:\n    - anchor: " + field + "\n      expireAfter: 48h\n    - expireAfterWrite: 1h\n"
}

func parseAnchorSpec(t *testing.T, rules string) (*DatasetSpec, error) {
	t.Helper()
	return Parse([]byte(anchorSpecYAML(rules)), BuiltinDefaults)
}

func compileAnchorSpec(t *testing.T, rules string) (*Compiled, error) {
	t.Helper()
	s, err := parseAnchorSpec(t, rules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return s.Compile(newFakeDict())
}

// Cada recusa de forma assevera a MENSAGEM PRÓPRIA da sua guarda: quando
// outra guarda também recusaria a entrada, o vermelho viria de graça e o
// teste não provaria a guarda que diz provar.
func TestTTLAnchorShapeRefusalsNameTheirOwnRule(t *testing.T) {
	cases := []struct {
		name, rules, want string
	}{
		{"anchor com expireAfterWrite",
			"  ttl:\n    - anchor: lastAuthAt\n      expireAfterWrite: 48h\n", "cannot be combined with anchor"},
		{"expireAfter sem anchor",
			"  ttl:\n    - when: \"hits > 1\"\n      expireAfter: 48h\n", "expireAfter is counted from an anchor field and requires anchor"},
		{"anchor sem duração",
			"  ttl:\n    - anchor: lastAuthAt\n", "anchor requires expireAfter"},
		{"anchor com never",
			"  ttl:\n    - anchor: lastAuthAt\n      expireAfter: never\n", "makes the anchor irrelevant"},
		{"anchor abaixo da resolução",
			"  ttl:\n    - anchor: lastAuthAt\n      expireAfter: 30s\n", "expireAfter 30s is below the collector resolution"},
		{"anchor com zero",
			"  ttl:\n    - anchor: lastAuthAt\n      expireAfter: 0\n", "expireAfter \"0\" declares no deadline"},
		{"fundo fora da última posição continua recusado",
			"  ttl:\n    - expireAfterWrite: 1h\n    - anchor: lastAuthAt\n      expireAfter: 48h\n", "a fallback rule (no when and no anchor) must be the last one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseAnchorSpec(t, tc.rules)
			if err == nil {
				t.Fatal("a spec foi aceita")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("a mensagem precisa ser a da guarda própria (%q): %v", tc.want, err)
			}
		})
	}
}

// O campo da âncora é validado no COMPILE, contra o schema resolvido: cada
// classe de campo inválida tem a sua mensagem.
func TestTTLAnchorFieldRefusalsNameTheirOwnRule(t *testing.T) {
	cases := []struct {
		name, field, want string
	}{
		{"não temporal", "hits", `"hits" has type int64; an anchor must be timestamp or datetime`},
		{"derivada não temporal", "docCount", `"docCount" has type int64; an anchor must be timestamp or datetime`},
		{"auto", "loose", `"loose" is auto-typed`},
		{"congelada de origem auto", "firstLoose", `"firstLoose" is auto-typed`},
		{"inexistente", "nope", `"nope" is not a field of the dataset`},
		{"excluída", "dropped", `"dropped" is excluded from the dataset`},
		{"sistema _writtenAt", WriteInstantFieldName, `"` + WriteInstantFieldName + `" is a system field`},
		{"sistema _expiresIn", TTLFieldName, `"` + TTLFieldName + `" is a system field`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileAnchorSpec(t, anchorRule(tc.field))
			if err == nil {
				t.Fatalf("âncora %q foi aceita", tc.field)
			}
			if !strings.Contains(err.Error(), "materialization.ttl[0].anchor "+tc.want) {
				t.Errorf("a mensagem precisa ser a da guarda própria (%q): %v", tc.want, err)
			}
		})
	}
}

// A recusa de `auto` olha a DECLARAÇÃO, nunca o tipo já observado: um auto
// cujo dicionário já fixou timestamp continua recusado. Aceitá-lo faria a
// validade da regra depender de qual evento chegou primeiro.
func TestTTLAnchorRefusesAutoEvenWhenObservedTemporal(t *testing.T) {
	s, err := parseAnchorSpec(t, anchorRule("loose"))
	if err != nil {
		t.Fatal(err)
	}
	dict := newFakeDict()
	id, _ := dict.LookupOrCreate([]byte("loose"))
	if err := dict.SetTypeHint(id, types.TypeTimestamp); err != nil {
		t.Fatal(err)
	}
	_, err = s.Compile(dict)
	if err == nil {
		t.Fatal("auto com tipo observado timestamp foi aceito como âncora")
	}
	if want := `materialization.ttl[0].anchor "loose" is auto-typed`; !strings.Contains(err.Error(), want) {
		t.Errorf("a mensagem precisa ser a da guarda de auto (%q): %v", want, err)
	}
}

// Os campos temporais aceitos: declarado timestamp e datetime, congelada de
// relógio (timestamp no compile), congelada de campo temporal (tipo herdado) e
// coluna derivada tipada timestamp.
func TestTTLAnchorAcceptsTemporalFields(t *testing.T) {
	for _, field := range []string{"lastAuthAt", "seenAt", "firstSeenAt", "firstAuthAt", "docSince"} {
		t.Run(field, func(t *testing.T) {
			c, err := compileAnchorSpec(t, anchorRule(field))
			if err != nil {
				t.Fatalf("âncora %q recusada: %v", field, err)
			}
			if c.TTLRules[0].Anchor != field || c.TTLRules[0].Duration != 48*time.Hour {
				t.Errorf("regra = %+v", c.TTLRules[0])
			}
		})
	}
}

// A regra-âncora na última posição NÃO é fundo: a linha legada (sem
// `_expiresIn`) não pode ganhar o prazo de uma âncora que nunca foi avaliada
// para ela, e um dataset sem fundo publica que não tem.
func TestTTLAnchorRuleInLastPositionIsNotTheFallback(t *testing.T) {
	s, err := parseAnchorSpec(t, "  ttl:\n    - when: \"lastAuthAt IS NULL\"\n      expireAfterWrite: 1h\n"+
		"    - anchor: lastAuthAt\n      expireAfter: 48h\n")
	if err != nil {
		t.Fatal(err)
	}
	if s.Materialization.FallbackDuration != 0 {
		t.Errorf("duração de fundo = %v, quero 0: a última regra é ancorada", s.Materialization.FallbackDuration)
	}
	if HasFallback(s.Materialization.TTLRules) {
		t.Error("HasFallback = true para uma lista que termina em regra ancorada")
	}
	if s.Materialization.TTLRules[1].IsFallback() {
		t.Error("a regra ancorada se declarou de fundo")
	}
}

// Fundo `never` é fundo: a duração é zero, e por isso "tem fundo" não pode
// ser inferido de FallbackDuration > 0.
func TestHasFallbackSeesNeverFallback(t *testing.T) {
	s, err := parseAnchorSpec(t, "  ttl:\n    - anchor: lastAuthAt\n      expireAfter: 48h\n    - expireAfterWrite: never\n")
	if err != nil {
		t.Fatal(err)
	}
	if !HasFallback(s.Materialization.TTLRules) {
		t.Error("HasFallback = false com fundo `never`")
	}
	if s.Materialization.FallbackDuration != 0 {
		t.Errorf("duração de fundo = %v, quero 0 (never)", s.Materialization.FallbackDuration)
	}
}

// `when` e `anchor` na mesma regra: as duas condições, e a descrição do log
// mostra as duas e a chave da duração.
func TestDescribeRulesShowsAnchorAndWhen(t *testing.T) {
	s, err := parseAnchorSpec(t, "  ttl:\n    - when: \"hits > 1\"\n      anchor: lastAuthAt\n      expireAfter: 48h\n"+
		"    - anchor: firstSeenAt\n      expireAfter: 7d\n    - expireAfterWrite: 1h\n")
	if err != nil {
		t.Fatal(err)
	}
	got := DescribeRules(s.Materialization.TTLRules)
	for _, snippet := range []string{
		`[0] when="hits > 1" anchor=lastAuthAt expireAfter=48h0m0s`,
		`[1] anchor=firstSeenAt expireAfter=168h0m0s`,
		`[2] fallback 1h0m0s`,
	} {
		if !strings.Contains(got, snippet) {
			t.Errorf("descrição %q não contém %q", got, snippet)
		}
	}
}
