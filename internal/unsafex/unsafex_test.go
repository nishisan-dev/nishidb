package unsafex

import (
	"testing"
	"unsafe"
)

func TestStringContent(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"ascii", []byte("hello"), "hello"},
		{"utf8 multibyte", []byte("ação-ipv6-✓"), "ação-ipv6-✓"},
		{"embedded zero", []byte{'a', 0x00, 'b'}, "a\x00b"},
		{"empty", []byte{}, ""},
		{"nil", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := String(tc.in); got != tc.want {
				t.Errorf("String(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestStringZeroCopy(t *testing.T) {
	b := []byte("zero-copy")
	s := String(b)
	if unsafe.StringData(s) != &b[0] {
		t.Error("String deve aliasar o array subjacente de b, sem cópia")
	}
}

func TestStringReflectsBufferMutation(t *testing.T) {
	// Documenta o contrato: a string aliasa b — mutação de b aparece na
	// string. Por isso o callee NUNCA pode reter a string além da vida de b.
	b := []byte("abc")
	s := String(b)
	b[0] = 'x'
	if s != "xbc" {
		t.Errorf("string aliasada = %q, want %q", s, "xbc")
	}
}
