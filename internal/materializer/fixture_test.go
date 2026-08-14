package materializer

import (
	"os"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const (
	fixtureEvent = "../../testdata/radius_event_sample.json"
	fixtureSpec  = "../../testdata/radius.materialization.yaml"
)

// Converte a amostra REAL de evento RADIUS (testdata/radius_event_sample.json,
// registrada no plano M1) campo a campo com os tipos que o YAML de produção
// declara. É o teste obrigatório do ajuste pós-amostra: Connect-Info chega
// como string "1000000000" num campo int64 e os timestamps usam offset
// +00:00 (não Z). Timestamps são conferidos contra time.Parse (oráculo
// independente do parser próprio).
func TestConvertRadiusFixture(t *testing.T) {
	data, err := os.ReadFile(fixtureEvent)
	if err != nil {
		t.Fatal(err)
	}

	rfcMillis := func(s string) int64 {
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatalf("oráculo: %v", err)
		}
		return ts.UnixMilli()
	}

	declared := map[string]types.FieldType{
		"Connect-Info":           types.TypeInt64, // string "1000000000" na amostra
		"_SEQ":                   types.TypeInt64,
		"Acct-Output-Octets":     types.TypeInt64, // 2613239581 > MaxInt32
		"Event-Timestamp":        types.TypeTimestamp,
		"EventExpireDate":        types.TypeTimestamp,
		"LastSessionStartedDate": types.TypeTimestamp, // fração .219
		"Framed-IP-Address":      types.TypeIPv4,
		"Framed-IPv6-Prefix":     types.TypeIPv6Prefix,
		"Acct-Status-Type":       types.TypeString,
	}
	wantI64 := map[string]int64{
		"Connect-Info":           1000000000,
		"_SEQ":                   7479960553,
		"Acct-Output-Octets":     2613239581,
		"Event-Timestamp":        rfcMillis("2026-08-12T21:00:08.000+00:00"),
		"EventExpireDate":        rfcMillis("2026-08-19T03:00:00.000+00:00"),
		"LastSessionStartedDate": rfcMillis("2026-08-07T09:00:08.219+00:00"),
	}

	var p jsonp.Parser
	p.Reset(data)
	var arena []byte
	seen := map[string]bool{}
	for {
		f, ok, err := p.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		target, isDeclared := declared[string(f.Key)]
		if !isDeclared {
			continue
		}
		name := string(f.Key)
		v, newArena, err := Convert(f, target, arena)
		if err != nil {
			t.Fatalf("campo %q: %v", name, err)
		}
		arena = newArena
		seen[name] = true

		if want, hasI64 := wantI64[name]; hasI64 && v.I64 != want {
			t.Errorf("campo %q: I64 = %d, want %d", name, v.I64, want)
		}
		switch name {
		case "Framed-IP-Address": // "191.31.31.162"
			if len(v.Bytes) != 4 || v.Bytes[0] != 191 || v.Bytes[1] != 31 || v.Bytes[2] != 31 || v.Bytes[3] != 162 {
				t.Errorf("Framed-IP-Address = %v", v.Bytes)
			}
		case "Framed-IPv6-Prefix": // "2804:1b2:d102:b1d::/64"
			if len(v.Bytes) != 17 || v.Bytes[16] != 64 || v.Bytes[0] != 0x28 || v.Bytes[1] != 0x04 {
				t.Errorf("Framed-IPv6-Prefix = %v", v.Bytes)
			}
		case "Acct-Status-Type":
			if string(v.Bytes) != "Alive" {
				t.Errorf("Acct-Status-Type = %q", v.Bytes)
			}
		}
	}
	for name := range declared {
		if !seen[name] {
			t.Errorf("campo %q não encontrado na fixture", name)
		}
	}
}

// Amostra REAL de evento parseada pelo caminho COMPLETO do PatchBuffer com o
// YAML de produção da spec §12 — tokenizer, resolução de schema (alias,
// wildcard, unknownFields include), conversão para o arena, captura de
// key/sequence, sort e dedup. O teste acima cobre Convert campo a campo com
// um mapa local de tipos; este prova que a spec inteira casa com o evento
// inteiro, que é o que a ingestão vai fazer.
func TestParseRadiusFixtureWithSpecYAML(t *testing.T) {
	spec, err := schema.Load(fixtureSpec, schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(fixtureEvent)
	if err != nil {
		t.Fatal(err)
	}

	var b PatchBuffer
	if err := b.Parse(data, c); err != nil {
		t.Fatalf("Parse da amostra real: %v", err)
	}

	// A amostra tem 43 campos e nenhum deles é excluído ou duplicado: o
	// evento inteiro materializa, sem perda no dedup.
	total, keyPos, seqPos := fixtureFieldPositions(t, data)
	if total != 43 {
		t.Fatalf("a amostra mudou: %d campos", total)
	}
	if len(b.entries) != total {
		t.Errorf("entries = %d, want %d", len(b.entries), total)
	}
	for i := 1; i < len(b.entries); i++ {
		if b.entries[i].field.ID <= b.entries[i-1].field.ID {
			t.Fatalf("entries fora de ordem estrita por FieldID na posição %d", i)
		}
	}

	// key e sequence aparecem no MEIO do objeto (24ª e 37ª posições), não no
	// começo: a captura não pode depender de ordem.
	if keyPos != 24 || seqPos != 37 {
		t.Errorf("posições na amostra: id=%d _SEQ=%d (esperado 24 e 37)", keyPos, seqPos)
	}
	if string(b.key) != "BLM-816P7LRL2Y-013" {
		t.Errorf("key = %q", b.key)
	}
	if !b.hasSeq || b.seq != 7479960553 {
		t.Errorf("seq = %d, hasSeq = %v", b.seq, b.hasSeq)
	}

	// Coerção C1: Connect-Info chega como string "1000000000" num campo
	// declarado int64.
	ci := entryByName(t, &b, "Connect-Info")
	if ci.val.Type != types.TypeInt64 || ci.val.I64 != 1000000000 {
		t.Errorf("Connect-Info = %+v", ci.val)
	}

	// Timestamps com offset +00:00 (não Z), conferidos contra time.Parse.
	for name, raw := range map[string]string{
		"Event-Timestamp":        "2026-08-12T21:00:08.000+00:00",
		"EventExpireDate":        "2026-08-19T03:00:00.000+00:00",
		"LastSessionStartedDate": "2026-08-07T09:00:08.219+00:00",
	} {
		want, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			t.Fatalf("oráculo %q: %v", raw, err)
		}
		e := entryByName(t, &b, name)
		if e.val.Type != types.TypeTimestamp || e.val.I64 != want.UnixMilli() {
			t.Errorf("%s = %+v, want millis %d", name, e.val, want.UnixMilli())
		}
	}

	// Endereços: ipv4 em 4 bytes, prefixo ipv6 em 17 (16 + tamanho).
	if ip := entryByName(t, &b, "Framed-IP-Address"); ip.val.Type != types.TypeIPv4 || len(ip.val.Bytes) != 4 {
		t.Errorf("Framed-IP-Address = %+v", ip.val)
	}
	if p6 := entryByName(t, &b, "Framed-IPv6-Prefix"); p6.val.Type != types.TypeIPv6Prefix || len(p6.val.Bytes) != 17 || p6.val.Bytes[16] != 64 {
		t.Errorf("Framed-IPv6-Prefix = %+v", p6.val)
	}

	// Campo da amostra que o YAML não declara resolve via unknownFields
	// include, como auto.
	rs := entryByName(t, &b, "RemoteServerAddr")
	if !rs.field.IsAuto || rs.val.Type != types.TypeString {
		t.Errorf("RemoteServerAddr = %+v (%+v)", rs.field, rs.val)
	}

	// Alias declarado no YAML resolve para o mesmo campo compilado.
	if byAlias, ok := c.FieldByName([]byte("connect_info")); !ok || byAlias != ci.field {
		t.Error("alias connect_info deveria resolver para o campo Connect-Info")
	}
}

// fixtureFieldPositions conta os campos do evento e devolve as posições
// (1-based) de id e _SEQ na ordem em que aparecem no JSON.
func fixtureFieldPositions(t *testing.T, data []byte) (total, keyPos, seqPos int) {
	t.Helper()
	var p jsonp.Parser
	p.Reset(data)
	for {
		f, ok, err := p.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return total, keyPos, seqPos
		}
		total++
		switch string(f.Key) {
		case "id":
			keyPos = total
		case "_SEQ":
			seqPos = total
		}
	}
}
