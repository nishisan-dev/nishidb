package jsonp

import "testing"

func TestStringEscapes(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"quote", `{"a":"say \"hi\""}`, `say "hi"`},
		{"backslash", `{"a":"c:\\tmp"}`, `c:\tmp`},
		{"slash", `{"a":"a\/b"}`, "a/b"},
		{"newline tab", `{"a":"l1\nl2\tend"}`, "l1\nl2\tend"},
		{"cr backspace formfeed", `{"a":"\r\b\f"}`, "\r\b\f"},
		{"unicode bmp", `{"a":"\u00e7\u00e3o"}`, "ção"},
		{"unicode ascii", `{"a":"\u0041\u005a"}`, "AZ"},
		{"surrogate pair", `{"a":"\ud83d\ude00"}`, "😀"},
		{"escape entre literais", `{"a":"pre\u0020pos"}`, "pre pos"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertFields(t, collect(t, tc.in), []ownedField{
				{Key: "a", Kind: KindString, Raw: tc.want},
			})
		})
	}
}

func TestEscapedKey(t *testing.T) {
	assertFields(t, collect(t, `{"a\u0026b":"v"}`), []ownedField{
		{Key: "a&b", Kind: KindString, Raw: "v"},
	})
}

func TestEscapedKeyAndValueSameField(t *testing.T) {
	// Chave e valor escapados no MESMO campo: ambos vivem no scratch
	// simultaneamente e precisam permanecer válidos até o próximo Next.
	var p Parser
	p.Reset([]byte(`{"k\ney":"v\nal","next":"plain"}`))
	f, ok, err := p.Next()
	if err != nil || !ok {
		t.Fatalf("Next: ok=%v err=%v", ok, err)
	}
	key, raw := string(f.Key), string(f.Raw)
	if key != "k\ney" || raw != "v\nal" {
		t.Fatalf("key=%q raw=%q, want %q %q", key, raw, "k\ney", "v\nal")
	}
	f, ok, err = p.Next()
	if err != nil || !ok || string(f.Key) != "next" || string(f.Raw) != "plain" {
		t.Fatalf("segundo campo: f=%+v ok=%v err=%v", f, ok, err)
	}
}

func TestInvalidEscapes(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"unknown escape", `{"a":"\x41"}`},
		{"truncated unicode", `{"a":"\u00"}`},
		{"bad hex", `{"a":"\u00zz"}`},
		{"lone high surrogate", `{"a":"\ud83d"}`},
		{"lone high surrogate then text", `{"a":"\ud83dxx"}`},
		{"trailing backslash", `{"a":"\`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p Parser
			p.Reset([]byte(tc.in))
			_, _, err := p.Next()
			if err == nil {
				t.Fatalf("esperava erro para %q", tc.in)
			}
		})
	}
}
