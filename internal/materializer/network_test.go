package materializer

import (
	"bytes"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestConvertIPv4(t *testing.T) {
	cases := []struct {
		in   string
		want []byte // network byte order
	}{
		{"100.65.223.104", []byte{100, 65, 223, 104}},
		{"0.0.0.0", []byte{0, 0, 0, 0}},
		{"255.255.255.255", []byte{255, 255, 255, 255}},
		{"10.0.0.1", []byte{10, 0, 0, 1}},
	}
	for _, tc := range cases {
		v, _, err := Convert(str(tc.in), types.TypeIPv4, nil, nil)
		if err != nil {
			t.Fatalf("Convert(%q): %v", tc.in, err)
		}
		if !bytes.Equal(v.Bytes, tc.want) {
			t.Errorf("Convert(%q) = %v, want %v", tc.in, v.Bytes, tc.want)
		}
	}
	invalid := []string{
		"", "1.2.3", "1.2.3.4.5", "256.1.1.1", "1.2.3.256", "01.2.3.4",
		"1.2.3.04", "a.b.c.d", "1..2.3", "1.2.3.4 ", " 1.2.3.4", "-1.2.3.4",
		"2001:db8::1", "1.2.3.4/24",
	}
	for _, in := range invalid {
		convErr(t, str(in), types.TypeIPv4)
	}
	convErr(t, num("167772161"), types.TypeIPv4) // número não é aceito
}

func TestConvertIPv6(t *testing.T) {
	cases := []struct {
		in   string
		want []byte
	}{
		{"2001:db8::1", []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"::", make([]byte, 16)},
		{"fe80::a00:27ff:fe4e:66a1", []byte{0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0x0a, 0x00, 0x27, 0xff, 0xfe, 0x4e, 0x66, 0xa1}},
		// Mapped (::ffff:a.b.c.d): aceito como ipv6 — netip o trata como
		// endereço v6 (Is4()==false), e o roundtrip preserva os 16 bytes.
		{"::ffff:192.0.2.1", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 1}},
	}
	for _, tc := range cases {
		v, _, err := Convert(str(tc.in), types.TypeIPv6, nil, nil)
		if err != nil {
			t.Fatalf("Convert(%q): %v", tc.in, err)
		}
		if !bytes.Equal(v.Bytes, tc.want) {
			t.Errorf("Convert(%q) = %x, want %x", tc.in, v.Bytes, tc.want)
		}
	}
	for _, in := range []string{"", "1.2.3.4", "2001:db8::zz", "2001:db8::1/64", "gggg::"} {
		convErr(t, str(in), types.TypeIPv6)
	}
}

func TestConvertIPv4Prefix(t *testing.T) {
	v, _, err := Convert(str("10.20.0.0/16"), types.TypeIPv4Prefix, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v.Bytes, []byte{10, 20, 0, 0, 16}) {
		t.Errorf("Bytes = %v", v.Bytes)
	}
	v, _, err = Convert(str("0.0.0.0/0"), types.TypeIPv4Prefix, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v.Bytes, []byte{0, 0, 0, 0, 0}) {
		t.Errorf("Bytes = %v", v.Bytes)
	}
	for _, in := range []string{"", "10.20.0.0", "10.20.0.0/33", "2001:db8::/32", "10.20.0.0/-1", "10.20.0.0/a"} {
		convErr(t, str(in), types.TypeIPv4Prefix)
	}
}

func TestConvertIPv6Prefix(t *testing.T) {
	v, _, err := Convert(str("2804:14c::/32"), types.TypeIPv6Prefix, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{0x28, 0x04, 0x01, 0x4c}, make([]byte, 12)...)
	want = append(want, 32)
	if !bytes.Equal(v.Bytes, want) {
		t.Errorf("Bytes = %x, want %x", v.Bytes, want)
	}
	for _, in := range []string{"", "2804:14c::", "2804:14c::/129", "10.0.0.0/8"} {
		convErr(t, str(in), types.TypeIPv6Prefix)
	}
}

func TestConvertBytesBase64(t *testing.T) {
	v, _, err := Convert(str("aGVsbG8="), types.TypeBytes, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(v.Bytes) != "hello" {
		t.Errorf("Bytes = %q", v.Bytes)
	}
	v, _, err = Convert(str(""), types.TypeBytes, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Bytes) != 0 {
		t.Errorf("base64 vazio deve dar bytes vazios, veio %q", v.Bytes)
	}
	for _, in := range []string{"não base64!", "aGVsbG8", "====", "aGVs bG8="} {
		convErr(t, str(in), types.TypeBytes)
	}
	convErr(t, num("1"), types.TypeBytes)
}

// Todos os tipos de rede escrevem no arena — nunca aliasam f.Raw.
func TestNetworkValuesGoToArena(t *testing.T) {
	arena := make([]byte, 0, 64)
	v4, arena, _ := Convert(str("1.2.3.4"), types.TypeIPv4, nil, arena)
	v6, arena, _ := Convert(str("::1"), types.TypeIPv6, nil, arena)
	if len(arena) != 4+16 {
		t.Fatalf("arena len = %d, want 20", len(arena))
	}
	if !bytes.Equal(v4.Bytes, []byte{1, 2, 3, 4}) || v6.Bytes[15] != 1 {
		t.Errorf("valores incorretos no arena: v4=%v v6=%x", v4.Bytes, v6.Bytes)
	}
}
