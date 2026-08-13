package jsonp

import (
	"errors"
	"strings"
	"testing"
)

func TestInvalidJSON(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"vazio", ``},
		{"so whitespace", `   `},
		{"nao objeto: array", `[1,2]`},
		{"nao objeto: string", `"abc"`},
		{"nao objeto: numero", `42`},
		{"chave sem aspas", `{a:1}`},
		{"sem dois pontos", `{"a" 1}`},
		{"virgula pendurada", `{"a":1,}`},
		{"virgula dupla", `{"a":1,,"b":2}`},
		{"sem virgula", `{"a":1 "b":2}`},
		{"objeto nao fechado", `{"a":1`},
		{"valor ausente", `{"a":}`},
		{"valor truncado", `{"a":tru}`},
		{"literal invalido", `{"a":True}`},
		{"numero so menos", `{"a":-}`},
		{"numero fracao vazia", `{"a":1.}`},
		{"numero expoente vazio", `{"a":1e}`},
		{"string nao terminada", `{"a":"abc`},
		{"lixo apos fim", `{"a":1} extra`},
		{"segundo objeto", `{"a":1}{"b":2}`},
		{"chave truncada", `{"ab`},
		{"control char em valor", "{\"a\":\"x\x1ey\"}"},
		{"control char em chave", "{\"a\x01b\":\"v\"}"},
		{"newline cru em string", "{\"a\":\"l1\nl2\"}"},
		{"control char em string com escape", "{\"a\":\"\\n\x02\"}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p Parser
			p.Reset([]byte(tc.in))
			var err error
			for {
				_, ok, e := p.Next()
				if e != nil {
					err = e
					break
				}
				if !ok {
					break
				}
			}
			if err == nil {
				t.Fatalf("esperava erro para %q", tc.in)
			}
			if !errors.Is(err, ErrInvalidJSON) {
				t.Errorf("errors.Is(err, ErrInvalidJSON) = false para %v", err)
			}
			var se *SyntaxError
			if !errors.As(err, &se) {
				t.Errorf("erro %v não é *SyntaxError", err)
			}
		})
	}
}

func TestErrorIsSticky(t *testing.T) {
	var p Parser
	p.Reset([]byte(`{"a":`))
	_, _, err1 := p.Next()
	_, _, err2 := p.Next()
	if err1 == nil || err2 == nil || err1 != err2 {
		t.Fatalf("erro deve ser sticky: err1=%v err2=%v", err1, err2)
	}
}

func TestSyntaxErrorHasPosition(t *testing.T) {
	var p Parser
	p.Reset([]byte(`{"key":###}`))
	_, _, err := p.Next()
	var se *SyntaxError
	if !errors.As(err, &se) {
		t.Fatalf("esperava SyntaxError, veio %v", err)
	}
	if se.Pos != 7 {
		t.Errorf("Pos = %d, want 7 (início do valor inválido)", se.Pos)
	}
	if !strings.Contains(se.Error(), "7") {
		t.Errorf("mensagem deve conter a posição: %q", se.Error())
	}
}

// Chave duplicada é emitida duas vezes, na ordem: last-wins é decisão do
// consumidor (PatchBuffer), não do tokenizer.
func TestDuplicateKeyEmitsBoth(t *testing.T) {
	assertFields(t, collect(t, `{"a":"1","a":"2"}`), []ownedField{
		{Key: "a", Kind: KindString, Raw: "1"},
		{Key: "a", Kind: KindString, Raw: "2"},
	})
}

// Evento RADIUS realista: mistura de tudo que o materializer vai consumir.
func TestRealisticRadiusEvent(t *testing.T) {
	in := `{
		"id": "CMP-V0001PPCLP-013",
		"_id": "68992d1a77b6f2618d867d43",
		"_SEQ": {"$numberLong": "7455409733"},
		"Acct-Status-Type": "Interim-Update",
		"Acct-Session-Id": "D0D94381DB27B076",
		"Framed-IP-Address": "100.65.223.104",
		"NAS-Port": 1063256064,
		"Acct-Input-Octets": 66952,
		"Event-Timestamp": {"$date": "2026-08-11T22:28:10.000Z"},
		"EventExpireDate": {"$date": {"$numberLong": "1786399690000"}},
		"Acct-Delay-Time": null,
		"roaming": false,
		"vendor-attrs": {"vsa": [1, 2, 3]}
	}`
	assertFields(t, collect(t, in), []ownedField{
		{Key: "id", Kind: KindString, Raw: "CMP-V0001PPCLP-013"},
		{Key: "_id", Kind: KindString, Raw: "68992d1a77b6f2618d867d43"},
		{Key: "_SEQ", Kind: KindNumber, Ext: ExtNumberLong, Raw: "7455409733"},
		{Key: "Acct-Status-Type", Kind: KindString, Raw: "Interim-Update"},
		{Key: "Acct-Session-Id", Kind: KindString, Raw: "D0D94381DB27B076"},
		{Key: "Framed-IP-Address", Kind: KindString, Raw: "100.65.223.104"},
		{Key: "NAS-Port", Kind: KindNumber, Raw: "1063256064"},
		{Key: "Acct-Input-Octets", Kind: KindNumber, Raw: "66952"},
		{Key: "Event-Timestamp", Kind: KindString, Ext: ExtDate, Raw: "2026-08-11T22:28:10.000Z"},
		{Key: "EventExpireDate", Kind: KindNumber, Ext: ExtDate, Raw: "1786399690000"},
		{Key: "Acct-Delay-Time", Kind: KindNull, Raw: ""},
		{Key: "roaming", Kind: KindBool, Raw: "false"},
		{Key: "vendor-attrs", Kind: KindObject, Raw: `{"vsa": [1, 2, 3]}`},
	})
}
