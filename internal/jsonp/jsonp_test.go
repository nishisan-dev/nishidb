package jsonp

import (
	"testing"
)

// collect drena o parser e devolve os campos com Key/Raw copiados (os
// originais são válidos só até o próximo Next — o teste precisa de cópias).
type ownedField struct {
	Key  string
	Kind Kind
	Ext  Ext
	Raw  string
}

func collect(t *testing.T, data string) []ownedField {
	t.Helper()
	var p Parser
	p.Reset([]byte(data))
	var out []ownedField
	for {
		f, ok, err := p.Next()
		if err != nil {
			t.Fatalf("Next() erro inesperado em %q: %v", data, err)
		}
		if !ok {
			return out
		}
		out = append(out, ownedField{
			Key:  string(f.Key),
			Kind: f.Kind,
			Ext:  f.Ext,
			Raw:  string(f.Raw),
		})
	}
}

func assertFields(t *testing.T, got []ownedField, want []ownedField) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("campos = %d, want %d\ngot: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("campo[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestEmptyObject(t *testing.T) {
	assertFields(t, collect(t, `{}`), nil)
}

func TestSingleStringField(t *testing.T) {
	assertFields(t, collect(t, `{"id":"CMP-V0001PPCLP-013"}`), []ownedField{
		{Key: "id", Kind: KindString, Raw: "CMP-V0001PPCLP-013"},
	})
}

func TestMultipleFields(t *testing.T) {
	assertFields(t, collect(t, `{"a":"1","b":"2","c":"3"}`), []ownedField{
		{Key: "a", Kind: KindString, Raw: "1"},
		{Key: "b", Kind: KindString, Raw: "2"},
		{Key: "c", Kind: KindString, Raw: "3"},
	})
}

func TestWhitespaceEverywhere(t *testing.T) {
	assertFields(t, collect(t, " \t\n{ \"a\" : \"1\" , \"b\" : \"2\" }\r\n "), []ownedField{
		{Key: "a", Kind: KindString, Raw: "1"},
		{Key: "b", Kind: KindString, Raw: "2"},
	})
}

func TestEmptyStringValue(t *testing.T) {
	assertFields(t, collect(t, `{"a":""}`), []ownedField{
		{Key: "a", Kind: KindString, Raw: ""},
	})
}

func TestResetReusesParser(t *testing.T) {
	var p Parser
	p.Reset([]byte(`{"a":"1"}`))
	f, ok, err := p.Next()
	if err != nil || !ok || string(f.Key) != "a" {
		t.Fatalf("primeiro uso: f=%+v ok=%v err=%v", f, ok, err)
	}
	p.Reset([]byte(`{"b":"2"}`))
	f, ok, err = p.Next()
	if err != nil || !ok || string(f.Key) != "b" || string(f.Raw) != "2" {
		t.Fatalf("apos Reset: f=%+v ok=%v err=%v", f, ok, err)
	}
	if _, ok, _ := p.Next(); ok {
		t.Fatal("esperava fim do objeto apos unico campo")
	}
}
