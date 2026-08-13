package jsonp

import "testing"

func TestNumberValues(t *testing.T) {
	cases := []struct {
		name string
		in   string
		raw  string
	}{
		{"integer", `{"n":66952}`, "66952"},
		{"zero", `{"n":0}`, "0"},
		{"negative", `{"n":-42}`, "-42"},
		{"fraction", `{"n":3.14}`, "3.14"},
		{"negative fraction", `{"n":-0.5}`, "-0.5"},
		{"exponent", `{"n":1e9}`, "1e9"},
		{"exponent sign", `{"n":2.5E-3}`, "2.5E-3"},
		{"big int64", `{"n":7455409733}`, "7455409733"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertFields(t, collect(t, tc.in), []ownedField{
				{Key: "n", Kind: KindNumber, Raw: tc.raw},
			})
		})
	}
}

func TestBoolValues(t *testing.T) {
	assertFields(t, collect(t, `{"a":true,"b":false}`), []ownedField{
		{Key: "a", Kind: KindBool, Raw: "true"},
		{Key: "b", Kind: KindBool, Raw: "false"},
	})
}

func TestNullValue(t *testing.T) {
	assertFields(t, collect(t, `{"a":null}`), []ownedField{
		{Key: "a", Kind: KindNull, Raw: ""},
	})
}

func TestNumberRejectsLeadingZero(t *testing.T) {
	for _, in := range []string{`{"a":01}`, `{"a":-01}`, `{"a":00}`} {
		var p Parser
		p.Reset([]byte(in))
		if _, _, err := p.Next(); err == nil {
			t.Errorf("esperava erro de leading zero para %q", in)
		}
	}
}

func TestMixedScalars(t *testing.T) {
	in := `{"id":"ABC","_SEQ":101,"up":true,"ip":null,"ratio":0.75}`
	assertFields(t, collect(t, in), []ownedField{
		{Key: "id", Kind: KindString, Raw: "ABC"},
		{Key: "_SEQ", Kind: KindNumber, Raw: "101"},
		{Key: "up", Kind: KindBool, Raw: "true"},
		{Key: "ip", Kind: KindNull, Raw: ""},
		{Key: "ratio", Kind: KindNumber, Raw: "0.75"},
	})
}
