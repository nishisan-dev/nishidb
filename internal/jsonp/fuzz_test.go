package jsonp

import (
	"bytes"
	"encoding/json"
	"testing"
)

// FuzzParser verifica que o parser nunca panica, sempre termina e é
// determinístico. Para inputs SEM valores aninhados (onde o parser é um
// validador completo do subset), verifica também que aceitar implica
// validade segundo encoding/json — exceto pelas leniências documentadas
// (nenhuma hoje para inputs planos; nested é opaco por design e a stdlib
// é mais permissiva com surrogates isolados, que nós rejeitamos).
func FuzzParser(f *testing.F) {
	seeds := []string{
		`{}`,
		`{"a":"b"}`,
		`{"a":1,"b":-2.5e3,"c":true,"d":null}`,
		`{"n":{"$numberLong":"66952"}}`,
		`{"d":{"$date":"2026-08-11T22:28:10.000Z"}}`,
		`{"d":{"$date":{"$numberLong":"1755037690000"}}}`,
		`{"o":{"x":[1,{"y":"z"}]}}`,
		`{"s":"esc \" \\ \/ \b \f \n \r \t ç 😀"}`,
		`{"a":{"$oid":"abc"},"b":[],"c":{}}`,
		`{"a":01}`,
		`{"a":"\ud800"}`,
		`{"a":1} lixo`,
		`[1,2,3]`,
		`{"a"`,
		"",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		run := func() (fields []ownedField, err error) {
			var p Parser
			p.Reset(data)
			for i := 0; ; i++ {
				if i > len(data)+2 {
					t.Fatalf("parser não termina para %q", data)
				}
				fld, ok, e := p.Next()
				if e != nil {
					return fields, e
				}
				if !ok {
					return fields, nil
				}
				fields = append(fields, ownedField{
					Key: string(fld.Key), Kind: fld.Kind, Ext: fld.Ext, Raw: string(fld.Raw),
				})
			}
		}
		f1, err1 := run()
		f2, err2 := run()
		if (err1 == nil) != (err2 == nil) || len(f1) != len(f2) {
			t.Fatalf("não determinístico para %q: %v/%v vs %v/%v", data, f1, err1, f2, err2)
		}
		for i := range f1 {
			if f1[i] != f2[i] {
				t.Fatalf("não determinístico no campo %d para %q", i, data)
			}
		}
		if err1 != nil {
			return
		}
		// Aceito sem nested nem Extended JSON ⇒ deve ser JSON válido pela
		// stdlib (nested é opaco e Extended reescreve o shape; surrogates
		// isolados nós rejeitamos, então não invalidam esta direção).
		flat := true
		for _, fld := range f1 {
			if fld.Kind == KindObject || fld.Kind == KindArray || fld.Ext != ExtNone {
				flat = false
				break
			}
		}
		if flat && !json.Valid(bytes.TrimSpace(data)) {
			t.Errorf("parser aceitou input plano que a stdlib rejeita: %q", data)
		}
	})
}
