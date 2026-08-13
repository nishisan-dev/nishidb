package materializer

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const patchYAML = `
version: 1
name: radius_sessions
materialization:
  key:
    field: id
  sequence:
    field: _SEQ
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
    Framed-IP-Address:
      type: ipv4
      alias: ip
      index: hash
    Event-Timestamp:
      type: timestamp
    Acct-Input-Octets:
      type: int64
    extra:
      type: auto
`

func compileYAML(t *testing.T, yml string) *schema.Compiled {
	t.Helper()
	s, err := schema.Parse([]byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func parsePatch(t *testing.T, c *schema.Compiled, event string) *PatchBuffer {
	t.Helper()
	var b PatchBuffer
	if err := b.Parse([]byte(event), c); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return &b
}

func entryByName(t *testing.T, b *PatchBuffer, name string) patchEntry {
	t.Helper()
	for _, e := range b.entries {
		if e.field.Name == name {
			return e
		}
	}
	t.Fatalf("entry %q não encontrada", name)
	return patchEntry{}
}

func TestParseFullEvent(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, `{
		"id": "CMP-001",
		"_SEQ": 101,
		"Acct-Status-Type": "Start",
		"Framed-IP-Address": "10.0.0.1",
		"Event-Timestamp": "2026-08-11T22:28:10.000Z",
		"Acct-Input-Octets": 66952
	}`)
	if string(b.key) != "CMP-001" {
		t.Errorf("key = %q", b.key)
	}
	if !b.hasSeq || b.seq != 101 {
		t.Errorf("seq = %d, hasSeq = %v", b.seq, b.hasSeq)
	}
	if len(b.entries) != 6 {
		t.Fatalf("entries = %d, want 6", len(b.entries))
	}
	for i := 1; i < len(b.entries); i++ {
		if b.entries[i].field.ID <= b.entries[i-1].field.ID {
			t.Fatalf("entries fora de ordem por FieldID")
		}
	}
	st := entryByName(t, b, "Acct-Status-Type")
	if st.val.Type != types.TypeString || string(st.val.Bytes) != "Start" {
		t.Errorf("status = %+v", st.val)
	}
	ip := entryByName(t, b, "Framed-IP-Address")
	if ip.val.Type != types.TypeIPv4 || len(ip.val.Bytes) != 4 || ip.val.Bytes[0] != 10 {
		t.Errorf("ip = %+v", ip.val)
	}
	seq := entryByName(t, b, "_SEQ")
	if seq.val.I64 != 101 {
		t.Errorf("_SEQ entry = %+v", seq.val)
	}
}

func TestParseWildcardUnknownInclude(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, `{"id":"A","_SEQ":1,"X-Vendor":"custom"}`)
	e := entryByName(t, b, "X-Vendor")
	if !e.field.IsAuto || e.val.Type != types.TypeString {
		t.Errorf("unknown = %+v", e)
	}
	// Segundo evento reusa o campo cacheado (mesmo ID).
	b2 := parsePatch(t, c, `{"id":"B","_SEQ":2,"X-Vendor":"other"}`)
	if entryByName(t, b2, "X-Vendor").field.ID != e.field.ID {
		t.Error("campo unknown deveria ser cacheado com o mesmo ID")
	}
}

func TestParseExclusion(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, `{"id":"A","_SEQ":1,"debug":{"big":"blob"}}`)
	for _, e := range b.entries {
		if e.field.Name == "debug" {
			t.Fatal("campo excluído não deveria gerar entry")
		}
	}
	if len(b.entries) != 2 {
		t.Errorf("entries = %d, want 2 (id e _SEQ)", len(b.entries))
	}
}

// A exclusão vale para o nome E para o alias do campo excluído: o alias era
// a porta pela qual o campo descartado voltava a materializar.
func TestParseExclusionByNameAndAlias(t *testing.T) {
	yml := replace(t, patchYAML, "  exclude:\n    - debug\n", "  exclude:\n    - debug\n    - Acct-Status-Type\n")
	c := compileYAML(t, yml)
	b := parsePatch(t, c, `{"id":"A","_SEQ":1,"Acct-Status-Type":"Start","status":"Stop","debug":{"big":"blob"}}`)
	for _, e := range b.entries {
		if e.field.Name == "Acct-Status-Type" || e.field.Name == "status" {
			t.Fatalf("campo excluído materializou: %+v", e.field)
		}
	}
	if len(b.entries) != 2 {
		t.Errorf("entries = %d, want 2 (id e _SEQ)", len(b.entries))
	}
}

func TestParseUnknownIgnore(t *testing.T) {
	yml := patchYAML
	yml = replace(t, yml, "unknownFields: include", "unknownFields: ignore")
	c := compileYAML(t, yml)
	// O valor do campo ignorado (objeto aninhado) precisa ser consumido
	// corretamente para não corromper o parse dos campos seguintes.
	b := parsePatch(t, c, `{"id":"A","X-Skip":{"nested":[1,2]},"_SEQ":1}`)
	if len(b.entries) != 2 {
		t.Errorf("entries = %d, want 2", len(b.entries))
	}
}

func TestParseUnknownReject(t *testing.T) {
	yml := replace(t, patchYAML, "unknownFields: include", "unknownFields: reject")
	c := compileYAML(t, yml)
	var b PatchBuffer
	err := b.Parse([]byte(`{"id":"A","_SEQ":1,"X-Bad":"v"}`), c)
	if !errors.Is(err, schema.ErrUnknownField) {
		t.Errorf("err = %v, want ErrUnknownField", err)
	}
}

func TestParseKeyMissing(t *testing.T) {
	c := compileYAML(t, patchYAML)
	var b PatchBuffer
	err := b.Parse([]byte(`{"_SEQ":1,"Acct-Status-Type":"Start"}`), c)
	if !errors.Is(err, ErrInvalidKey) {
		t.Errorf("err = %v, want ErrInvalidKey", err)
	}
}

func TestParseKeyNull(t *testing.T) {
	c := compileYAML(t, patchYAML)
	var b PatchBuffer
	err := b.Parse([]byte(`{"id":null,"_SEQ":1}`), c)
	if !errors.Is(err, ErrInvalidKey) {
		t.Errorf("err = %v, want ErrInvalidKey", err)
	}
}

func TestParseSequenceMissing(t *testing.T) {
	c := compileYAML(t, patchYAML)
	var b PatchBuffer
	err := b.Parse([]byte(`{"id":"A"}`), c)
	if !errors.Is(err, ErrInvalidSequence) {
		t.Errorf("err = %v, want ErrInvalidSequence", err)
	}
}

func TestParseSequenceNull(t *testing.T) {
	c := compileYAML(t, patchYAML)
	var b PatchBuffer
	err := b.Parse([]byte(`{"id":"A","_SEQ":null}`), c)
	if !errors.Is(err, ErrInvalidSequence) {
		t.Errorf("err = %v, want ErrInvalidSequence", err)
	}
}

// A mensagem precisa dizer a verdade: "ausente" e "nulo" são diagnósticos
// diferentes, e key vazia (rejeitada também pelo Store real) é um terceiro
// caso que antes se escondia atrás de "ausente ou nula".
func TestParseKeyAndSequenceErrorsSayTheCause(t *testing.T) {
	c := compileYAML(t, patchYAML)
	cases := []struct {
		event string
		sent  error
		field string
		cause string
	}{
		{`{"_SEQ":1}`, ErrInvalidKey, "id", "ausente"},
		{`{"id":null,"_SEQ":1}`, ErrInvalidKey, "id", "nulo"},
		{`{"id":"","_SEQ":1}`, ErrInvalidKey, "id", "vazio"},
		{`{"id":"A"}`, ErrInvalidSequence, "_SEQ", "ausente"},
		{`{"id":"A","_SEQ":null}`, ErrInvalidSequence, "_SEQ", "nulo"},
	}
	for _, tc := range cases {
		var b PatchBuffer
		err := b.Parse([]byte(tc.event), c)
		if !errors.Is(err, tc.sent) {
			t.Errorf("%s: err = %v, want %v", tc.event, err, tc.sent)
			continue
		}
		if !contains(err.Error(), tc.field) || !contains(err.Error(), tc.cause) {
			t.Errorf("%s: err = %q, deveria citar %q e %q", tc.event, err, tc.field, tc.cause)
		}
	}
}

// Exceção documentada ao last-wins: key, sequence e conversão são resolvidas
// durante a tokenização, antes do dedup, então a PRIMEIRA ocorrência
// inválida aborta o evento mesmo havendo uma válida logo à frente.
func TestParseAbortsOnFirstInvalidOccurrence(t *testing.T) {
	c := compileYAML(t, patchYAML)
	cases := []struct {
		event string
		sent  error
	}{
		{`{"id":null,"id":"A","_SEQ":1}`, ErrInvalidKey},
		{`{"id":"","id":"A","_SEQ":1}`, ErrInvalidKey},
		{`{"id":"A","id":null,"_SEQ":1}`, ErrInvalidKey},
		{`{"id":"A","_SEQ":null,"_SEQ":2}`, ErrInvalidSequence},
		{`{"id":"A","_SEQ":1,"Acct-Input-Octets":"abc","Acct-Input-Octets":10}`, ErrTypeConversion},
	}
	for _, tc := range cases {
		var b PatchBuffer
		if err := b.Parse([]byte(tc.event), c); !errors.Is(err, tc.sent) {
			t.Errorf("%s: err = %v, want %v", tc.event, err, tc.sent)
		}
	}
}

func TestParseNoSequenceConfigured(t *testing.T) {
	yml := replace(t, patchYAML, "  sequence:\n    field: _SEQ\n", "")
	c := compileYAML(t, yml)
	b := parsePatch(t, c, `{"id":"A"}`)
	if b.hasSeq {
		t.Error("hasSeq deveria ser false sem sequence configurada")
	}
}

func TestParseExplicitNull(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, `{"id":"A","_SEQ":1,"Framed-IP-Address":null}`)
	ip := entryByName(t, b, "Framed-IP-Address")
	if !ip.isNull || ip.val.Type != types.TypeNull {
		t.Errorf("null explícito = %+v", ip)
	}
}

func TestParseMongoExtendedJSON(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, `{
		"id": "A",
		"_SEQ": {"$numberLong": "7455409733"},
		"Event-Timestamp": {"$date": "2026-08-11T22:28:10.000Z"},
		"extra": {"$date": {"$numberLong": "1786399690000"}}
	}`)
	if b.seq != 7455409733 {
		t.Errorf("seq via $numberLong = %d", b.seq)
	}
	ts := entryByName(t, b, "Event-Timestamp")
	if ts.val.Type != types.TypeTimestamp {
		t.Errorf("Event-Timestamp = %+v", ts.val)
	}
	// Campo auto com $date canonical infere timestamp.
	extra := entryByName(t, b, "extra")
	if extra.val.Type != types.TypeTimestamp || extra.val.I64 != 1786399690000 {
		t.Errorf("extra = %+v", extra.val)
	}
}

func TestParseDuplicateKeyLastWins(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, `{"id":"A","_SEQ":1,"Acct-Input-Octets":100,"Acct-Input-Octets":200}`)
	count := 0
	for _, e := range b.entries {
		if e.field.Name == "Acct-Input-Octets" {
			count++
			if e.val.I64 != 200 {
				t.Errorf("last-wins: I64 = %d, want 200", e.val.I64)
			}
		}
	}
	if count != 1 {
		t.Errorf("entries duplicadas: %d", count)
	}
}

// Alias no evento também resolve (nome OU alias apontam pro mesmo campo).
func TestParseAliasInEvent(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, `{"id":"A","_SEQ":1,"status":"Stop"}`)
	e := entryByName(t, b, "Acct-Status-Type")
	if string(e.val.Bytes) != "Stop" {
		t.Errorf("alias: %+v", e.val)
	}
}

// O design §2.5 lista DOIS geradores de duplicata: chave JSON repetida e
// alias/nome apontando para o mesmo campo. O segundo só é exercitado quando
// os dois aparecem no mesmo evento — e o last-wins tem que valer na ordem do
// evento, não na ordem nome-antes-de-alias.
func TestParseAliasAndNameDedup(t *testing.T) {
	c := compileYAML(t, patchYAML)
	cases := []struct{ event, want string }{
		{`{"id":"A","_SEQ":1,"Acct-Status-Type":"Start","status":"Stop"}`, "Stop"},
		{`{"id":"A","_SEQ":1,"status":"Stop","Acct-Status-Type":"Start"}`, "Start"},
	}
	for _, tc := range cases {
		b := parsePatch(t, c, tc.event)
		count := 0
		for _, e := range b.entries {
			if e.field.Name == "Acct-Status-Type" {
				count++
				if string(e.val.Bytes) != tc.want {
					t.Errorf("%s: valor = %q, want %q", tc.event, e.val.Bytes, tc.want)
				}
			}
		}
		if count != 1 {
			t.Errorf("%s: %d entries para o mesmo campo", tc.event, count)
		}
	}
}

// A key é uma janela sobre o arena, capturada no meio do evento. Um campo
// grande depois dela força o append do arena a realocar: a janela continua
// apontando para o array anterior, que segue vivo e íntegro justamente
// porque b.key o referencia.
func TestParseKeySurvivesArenaGrowth(t *testing.T) {
	c := compileYAML(t, patchYAML)
	blob := make([]byte, 8192)
	for i := range blob {
		blob[i] = 'x'
	}
	event := append([]byte(`{"id":"CMP-001","_SEQ":1,"blob":"`), blob...)
	event = append(event, `"}`...)

	var b PatchBuffer
	if err := b.Parse(event, c); err != nil {
		t.Fatal(err)
	}
	if string(b.key) != "CMP-001" {
		t.Errorf("key corrompida pelo crescimento do arena: %q", b.key)
	}
	if e := entryByName(t, &b, "blob"); len(e.val.Bytes) != len(blob) {
		t.Errorf("blob = %d bytes, want %d", len(e.val.Bytes), len(blob))
	}
}

func TestParseTypeConversionError(t *testing.T) {
	c := compileYAML(t, patchYAML)
	var b PatchBuffer
	err := b.Parse([]byte(`{"id":"A","_SEQ":1,"Acct-Status-Type":123}`), c)
	if !errors.Is(err, ErrTypeConversion) {
		t.Fatalf("err = %v, want ErrTypeConversion", err)
	}
	if !contains(err.Error(), "Acct-Status-Type") {
		t.Errorf("erro deveria citar o campo: %v", err)
	}
	// O tipo alvo aparece pelo nome, não pela tag numérica do wire.
	if !contains(err.Error(), "string") {
		t.Errorf("erro deveria citar o tipo alvo pelo nome: %v", err)
	}
}

func TestParseInvalidJSON(t *testing.T) {
	c := compileYAML(t, patchYAML)
	var b PatchBuffer
	err := b.Parse([]byte(`{"id":`), c)
	if !errors.Is(err, jsonp.ErrInvalidJSON) {
		t.Errorf("err = %v, want ErrInvalidJSON", err)
	}
}

// Tudo que o PatchBuffer retém é cópia: mutar o evento após o Parse não
// pode afetar entries nem key.
func TestParseCopiesFromEvent(t *testing.T) {
	c := compileYAML(t, patchYAML)
	event := []byte(`{"id":"CMP-001","_SEQ":1,"Acct-Status-Type":"Start"}`)
	var b PatchBuffer
	if err := b.Parse(event, c); err != nil {
		t.Fatal(err)
	}
	for i := range event {
		event[i] = 'X'
	}
	if string(b.key) != "CMP-001" {
		t.Errorf("key aliasa o evento: %q", b.key)
	}
	if st := entryByName(t, &b, "Acct-Status-Type"); string(st.val.Bytes) != "Start" {
		t.Errorf("entry aliasa o evento: %q", st.val.Bytes)
	}
}

// O evento do benchmark só cumpre seu papel se os escapes forem escapes de
// verdade: este teste decodifica o que o gate de 0 allocs atravessa a cada
// iteração e falha se alguém trocar a sequência por UTF-8 cru.
func TestBenchEventEscapesDecode(t *testing.T) {
	c := compileYAML(t, patchYAML)
	b := parsePatch(t, c, string(benchRadiusEvent))
	if got := string(entryByName(t, b, "User-Name").val.Bytes); got != "joão@isp.net.br 😀" {
		t.Errorf("User-Name = %q (escapes \\u e surrogate pair não decodificaram)", got)
	}
	if got := string(entryByName(t, b, "Class").val.Bytes); got != "srv=hsi&prof=200M \"tag\"\nfim" {
		t.Errorf("Class = %q", got)
	}
	if got := string(entryByName(t, b, "NAS-Port-Id").val.Bytes); got != "ethernet1/1/1:100.200" {
		t.Errorf("NAS-Port-Id = %q", got)
	}
}

func TestParseReuse(t *testing.T) {
	c := compileYAML(t, patchYAML)
	var b PatchBuffer
	if err := b.Parse([]byte(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Start"}`), c); err != nil {
		t.Fatal(err)
	}
	if err := b.Parse([]byte(`{"id":"B","_SEQ":2}`), c); err != nil {
		t.Fatal(err)
	}
	if string(b.key) != "B" || b.seq != 2 || len(b.entries) != 2 {
		t.Errorf("reuso sujo: key=%q seq=%d entries=%d", b.key, b.seq, len(b.entries))
	}
}

func replace(t *testing.T, s, old, new string) string {
	t.Helper()
	if !contains(s, old) {
		t.Fatalf("%q não contém %q", s, old)
	}
	out := ""
	idx := indexOf(s, old)
	out = s[:idx] + new + s[idx+len(old):]
	return out
}

func contains(s, sub string) bool { return indexOf(s, sub) >= 0 }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
