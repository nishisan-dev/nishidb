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
		TypeDateTime:   "datetime",
		TypeTime:       "time",
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

// CarriesI64 é a fonte única da distinção "valor mora em I64 ou em Bytes".
// Este teste existe para que uma tag nova apareça aqui como falha em vez de
// silenciosamente cair no ramo dos bytes, onde nil == nil aprova valores
// diferentes.
func TestCarregaI64CobreTodasAsTagsNomeadas(t *testing.T) {
	i64 := map[FieldType]bool{
		TypeInt64:     true,
		TypeDouble:    true,
		TypeBool:      true,
		TypeTimestamp: true,
		TypeDateTime:  true,
		TypeTime:      true,
	}
	for tag := 0; tag < 0x20; tag++ {
		ft := FieldType(tag)
		if ft == TypeNull || tag >= len(fieldTypeNames) || fieldTypeNames[tag] == "" {
			continue // reservada ou sem nome: não é tipo de campo
		}
		if got := ft.CarriesI64(); got != i64[ft] {
			t.Errorf("%v.CarriesI64() = %v, want %v", ft, got, i64[ft])
		}
	}
}
