package jsonp

import "testing"

func TestNestedObjectOpaque(t *testing.T) {
	cases := []struct {
		name string
		in   string
		kind Kind
		raw  string
	}{
		{"objeto simples", `{"a":{"x":1}}`, KindObject, `{"x":1}`},
		{"objeto vazio", `{"a":{}}`, KindObject, `{}`},
		{"objeto profundo", `{"a":{"x":{"y":[1,{"z":2}]}}}`, KindObject, `{"x":{"y":[1,{"z":2}]}}`},
		{"array simples", `{"a":[1,2,3]}`, KindArray, `[1,2,3]`},
		{"array vazio", `{"a":[]}`, KindArray, `[]`},
		{"array de objetos", `{"a":[{"b":1},{"c":2}]}`, KindArray, `[{"b":1},{"c":2}]`},
		// Strings internas com chaves/colchetes/escapes não podem confundir o skip.
		{"chave dentro de string", `{"a":{"s":"tem } e ] aqui"}}`, KindObject, `{"s":"tem } e ] aqui"}`},
		{"escape dentro de string", `{"a":{"s":"aspas \" e barra \\"}}`, KindObject, `{"s":"aspas \" e barra \\"}`},
		{"whitespace preservado", `{"a":{ "x" : 1 }}`, KindObject, `{ "x" : 1 }`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertFields(t, collect(t, tc.in), []ownedField{
				{Key: "a", Kind: tc.kind, Raw: tc.raw},
			})
		})
	}
}

func TestNestedFollowedByScalar(t *testing.T) {
	assertFields(t, collect(t, `{"obj":{"x":1},"n":7}`), []ownedField{
		{Key: "obj", Kind: KindObject, Raw: `{"x":1}`},
		{Key: "n", Kind: KindNumber, Raw: "7"},
	})
}

func TestNestedUnterminated(t *testing.T) {
	for _, in := range []string{`{"a":{"x":1}`, `{"a":[1,2}`, `{"a":{"s":"aberta}`} {
		var p Parser
		p.Reset([]byte(in))
		_, _, err := p.Next()
		if err == nil {
			t.Errorf("esperava erro para %q", in)
		}
	}
}
