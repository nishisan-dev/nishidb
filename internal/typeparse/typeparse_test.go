package typeparse

import (
	"errors"
	"math"
	"testing"
)

// A cobertura fina (célula a célula, por tipo alvo) vive nos testes do
// materializer, que exercitam estes parsers via Convert. Aqui ficam os
// contratos do pacote: valores de borda, categoria de erro e assinaturas.

func TestInt64(t *testing.T) {
	ok := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"1000000000", 1000000000},
		{"-5", -5},
		{"9223372036854775807", math.MaxInt64},
		{"-9223372036854775808", math.MinInt64},
	}
	for _, tc := range ok {
		got, err := Int64([]byte(tc.in))
		if err != nil || got != tc.want {
			t.Errorf("Int64(%q) = %d, %v", tc.in, got, err)
		}
	}
	for _, in := range []string{"", "-", "1.5", "1e3", "12a", " 12", "9223372036854775808", "-9223372036854775809"} {
		if _, err := Int64([]byte(in)); !errors.Is(err, ErrParse) {
			t.Errorf("Int64(%q): err = %v, want ErrParse", in, err)
		}
	}
}

func TestFloat64(t *testing.T) {
	got, err := Float64([]byte("3.14"))
	if err != nil || got != 3.14 {
		t.Errorf("Float64 = %v, %v", got, err)
	}
	if _, err := Float64([]byte("abc")); !errors.Is(err, ErrParse) {
		t.Errorf("err = %v", err)
	}
}

func TestRFC3339Millis(t *testing.T) {
	// 2026-08-12T21:00:08.000+00:00 (formato real da amostra RADIUS).
	got, err := RFC3339Millis([]byte("2026-08-12T21:00:08.000+00:00"))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := RFC3339Millis([]byte("2026-08-12T21:00:08Z"))
	if got != want {
		t.Errorf("offset +00:00 = %d, Z = %d", got, want)
	}
	if _, err := RFC3339Millis([]byte("2026-02-30T00:00:00Z")); !errors.Is(err, ErrParse) {
		t.Errorf("data inexistente: err = %v", err)
	}
}

func TestIPv4(t *testing.T) {
	a, b, c, d, ok := IPv4([]byte("10.65.223.104"))
	if !ok || a != 10 || b != 65 || c != 223 || d != 104 {
		t.Errorf("IPv4 = %d.%d.%d.%d, %v", a, b, c, d, ok)
	}
	for _, in := range []string{"1.2.3", "1.2.3.256", "01.2.3.4", "1.2.3.4.5", ""} {
		if _, _, _, _, ok := IPv4([]byte(in)); ok {
			t.Errorf("IPv4(%q) deveria falhar", in)
		}
	}
}

func TestDigits(t *testing.T) {
	if v, ok := Digits2([]byte("07")); !ok || v != 7 {
		t.Errorf("Digits2 = %d, %v", v, ok)
	}
	if _, ok := Digits2([]byte("a7")); ok {
		t.Error("Digits2 aceitou não-dígito")
	}
	if v, ok := Digits4([]byte("2026")); !ok || v != 2026 {
		t.Errorf("Digits4 = %d, %v", v, ok)
	}
}

func TestIntegralNumber(t *testing.T) {
	if !IntegralNumber([]byte("123")) || IntegralNumber([]byte("1.5")) || IntegralNumber([]byte("1e3")) {
		t.Error("IntegralNumber inconsistente")
	}
}
