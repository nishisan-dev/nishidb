package materializer

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestConvertTimestampRFC3339(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string // RFC3339Nano de referência, parseado pela stdlib
	}{
		{"utc com millis", "2026-08-11T22:28:10.000Z", "2026-08-11T22:28:10Z"},
		{"utc sem fracao", "2026-08-11T22:28:10Z", "2026-08-11T22:28:10Z"},
		{"millis nao zero", "2026-08-11T22:28:10.123Z", "2026-08-11T22:28:10.123Z"},
		{"micros truncados p/ millis", "2026-08-11T22:28:10.123999Z", "2026-08-11T22:28:10.123Z"},
		{"nanos truncados p/ millis", "2026-08-11T22:28:10.123999999Z", "2026-08-11T22:28:10.123Z"},
		{"um digito de fracao", "2026-08-11T22:28:10.5Z", "2026-08-11T22:28:10.5Z"},
		{"offset negativo", "2026-08-11T19:28:10-03:00", "2026-08-11T22:28:10Z"},
		{"offset positivo", "2026-08-12T03:28:10+05:00", "2026-08-11T22:28:10Z"},
		{"offset com fracao", "2026-08-11T19:28:10.250-03:00", "2026-08-11T22:28:10.25Z"},
		{"t z minusculos", "2026-08-11t22:28:10z", "2026-08-11T22:28:10Z"},
		{"epoch", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z"},
		{"pre epoch", "1969-12-31T23:59:59Z", "1969-12-31T23:59:59Z"},
		{"bissexto", "2024-02-29T12:00:00Z", "2024-02-29T12:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := time.Parse(time.RFC3339Nano, tc.want)
			if err != nil {
				t.Fatalf("referência inválida: %v", err)
			}
			v := convOK(t, str(tc.in), types.TypeTimestamp)
			if v.I64 != ref.UnixMilli() {
				t.Errorf("Convert(%q) = %d, want %d (%s)", tc.in, v.I64, ref.UnixMilli(), tc.want)
			}
		})
	}
}

func TestConvertTimestampEpochMillisNumber(t *testing.T) {
	v := convOK(t, num("1755037690000"), types.TypeTimestamp)
	if v.I64 != 1755037690000 {
		t.Errorf("I64 = %d", v.I64)
	}
	v = convOK(t, num("-1000"), types.TypeTimestamp)
	if v.I64 != -1000 {
		t.Errorf("pré-epoch: I64 = %d", v.I64)
	}
	convErr(t, num("1.5"), types.TypeTimestamp)
}

func TestConvertTimestampExtDate(t *testing.T) {
	// Relaxed: {"$date":"iso"} chega como KindString+ExtDate.
	f := jsonp.Field{Kind: jsonp.KindString, Ext: jsonp.ExtDate, Raw: []byte("2026-08-11T22:28:10.000Z")}
	ref, _ := time.Parse(time.RFC3339, "2026-08-11T22:28:10Z")
	if v := convOK(t, f, types.TypeTimestamp); v.I64 != ref.UnixMilli() {
		t.Errorf("relaxed: I64 = %d, want %d", v.I64, ref.UnixMilli())
	}
	// Canonical: {"$date":{"$numberLong":"ms"}} chega como KindNumber+ExtDate.
	f = jsonp.Field{Kind: jsonp.KindNumber, Ext: jsonp.ExtDate, Raw: []byte("1786399690000")}
	if v := convOK(t, f, types.TypeTimestamp); v.I64 != 1786399690000 {
		t.Errorf("canonical: I64 = %d", v.I64)
	}
}

func TestConvertTimestampInvalid(t *testing.T) {
	cases := []string{
		"",
		"2026-08-11",                // só data
		"22:28:10Z",                 // só hora
		"2026-08-11 22:28:10Z",      // espaço em vez de T
		"2026-13-01T00:00:00Z",      // mês 13
		"2026-00-01T00:00:00Z",      // mês 0
		"2026-02-30T00:00:00Z",      // dia inexistente
		"2025-02-29T00:00:00Z",      // 29/02 em ano não bissexto
		"2026-08-11T24:00:00Z",      // hora 24
		"2026-08-11T22:60:10Z",      // minuto 60
		"2026-08-11T22:28:61Z",      // segundo 61
		"2026-08-11T22:28:10",       // sem timezone
		"2026-08-11T22:28:10.Z",     // fração vazia
		"2026-08-11T22:28:10.12e3Z", // fração não-dígito
		"2026-08-11T22:28:10+3:00",  // offset mal formado
		"2026-08-11T22:28:10+25:00", // offset fora de range
		"2026-08-11T22:28:10+03:60", // minuto de offset inválido
		"26-08-11T22:28:10Z",        // ano curto
		"2026-8-11T22:28:10Z",       // mês sem zero à esquerda
		"garbage",
		"2026-08-11T22:28:10Zextra", // lixo após timezone
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			convErr(t, str(in), types.TypeTimestamp)
		})
	}
	convErr(t, boolean(true), types.TypeTimestamp)
	convErr(t, obj(`{}`), types.TypeTimestamp)
}
