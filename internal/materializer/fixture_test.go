package materializer

import (
	"os"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Converte a amostra REAL de evento RADIUS (testdata/radius_event_sample.json,
// registrada no plano M1) campo a campo com os tipos que o YAML de produção
// declara. É o teste obrigatório do ajuste pós-amostra: Connect-Info chega
// como string "1000000000" num campo int64 e os timestamps usam offset
// +00:00 (não Z). Timestamps são conferidos contra time.Parse (oráculo
// independente do parser próprio).
func TestConvertRadiusFixture(t *testing.T) {
	data, err := os.ReadFile("../../testdata/radius_event_sample.json")
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
