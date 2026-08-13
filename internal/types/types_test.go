package types

import "testing"

// O nome tem que ser o mesmo que o operador escreveu no YAML: é ele que
// aparece nas mensagens de erro de conversão.
func TestFieldTypeString(t *testing.T) {
	cases := map[FieldType]string{
		TypeNull:       "null",
		TypeInt64:      "int64",
		TypeDouble:     "double",
		TypeBool:       "boolean",
		TypeTimestamp:  "timestamp",
		TypeIPv4:       "ipv4",
		TypeIPv6:       "ipv6",
		TypeIPv4Prefix: "ipv4_prefix",
		TypeIPv6Prefix: "ipv6_prefix",
		TypeString:     "string",
		TypeBytes:      "bytes",
		TypeJSON:       "json",
	}
	for ft, want := range cases {
		if got := ft.String(); got != want {
			t.Errorf("FieldType(%#x).String() = %q, want %q", uint8(ft), got, want)
		}
	}
	if got := FieldType(0x7F).String(); got != "FieldType(0x7f)" {
		t.Errorf("tag desconhecida = %q", got)
	}
}
