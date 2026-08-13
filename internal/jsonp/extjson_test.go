package jsonp

import "testing"

func TestExtendedNumberLong(t *testing.T) {
	cases := []struct {
		name string
		in   string
		raw  string
	}{
		{"basico", `{"n":{"$numberLong":"66952"}}`, "66952"},
		{"negativo", `{"n":{"$numberLong":"-5"}}`, "-5"},
		{"whitespace interno", `{"n":{ "$numberLong" : "1" }}`, "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertFields(t, collect(t, tc.in), []ownedField{
				{Key: "n", Kind: KindNumber, Ext: ExtNumberLong, Raw: tc.raw},
			})
		})
	}
}

func TestExtendedDateRelaxed(t *testing.T) {
	assertFields(t, collect(t, `{"d":{"$date":"2026-08-11T22:28:10.000Z"}}`), []ownedField{
		{Key: "d", Kind: KindString, Ext: ExtDate, Raw: "2026-08-11T22:28:10.000Z"},
	})
}

func TestExtendedDateCanonical(t *testing.T) {
	assertFields(t, collect(t, `{"d":{"$date":{"$numberLong":"1755037690000"}}}`), []ownedField{
		{Key: "d", Kind: KindNumber, Ext: ExtDate, Raw: "1755037690000"},
	})
}

func TestExtendedFollowedByOtherFields(t *testing.T) {
	in := `{"_SEQ":{"$numberLong":"101"},"status":"Start"}`
	assertFields(t, collect(t, in), []ownedField{
		{Key: "_SEQ", Kind: KindNumber, Ext: ExtNumberLong, Raw: "101"},
		{Key: "status", Kind: KindString, Raw: "Start"},
	})
}

// Shapes que NÃO são wrapper caem no fallback: objeto opaco, Ext=ExtNone.
func TestNotAWrapperFallsBackToOpaque(t *testing.T) {
	cases := []struct {
		name string
		in   string
		raw  string
	}{
		{"duas chaves", `{"n":{"$numberLong":"1","x":2}}`, `{"$numberLong":"1","x":2}`},
		{"outro dollar", `{"n":{"$oid":"abc"}}`, `{"$oid":"abc"}`},
		{"numberLong valor nao-string", `{"n":{"$numberLong":123}}`, `{"$numberLong":123}`},
		{"date valor numero", `{"n":{"$date":123}}`, `{"$date":123}`},
		{"date canonical com outra chave", `{"n":{"$date":{"$numberInt":"1"}}}`, `{"$date":{"$numberInt":"1"}}`},
		{"chave comum", `{"n":{"numberLong":"1"}}`, `{"numberLong":"1"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertFields(t, collect(t, tc.in), []ownedField{
				{Key: "n", Kind: KindObject, Ext: ExtNone, Raw: tc.raw},
			})
		})
	}
}

// Wrapper aninhado dentro de array/objeto opaco NÃO é desembrulhado:
// só valores diretos de campos top-level são inspecionados.
func TestWrapperInsideNestedStaysRaw(t *testing.T) {
	assertFields(t, collect(t, `{"a":[{"$numberLong":"1"}]}`), []ownedField{
		{Key: "a", Kind: KindArray, Raw: `[{"$numberLong":"1"}]`},
	})
}

func TestExtendedWithEscapes(t *testing.T) {
	// . = '.'
	assertFields(t, collect(t, `{"d":{"$date":"2026-08-11T22:28:10.000Z"}}`), []ownedField{
		{Key: "d", Kind: KindString, Ext: ExtDate, Raw: "2026-08-11T22:28:10.000Z"},
	})
}
