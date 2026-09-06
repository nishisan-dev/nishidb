package typeparse_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/typeparse"
)

// As tabelas abaixo foram CAPTURADAS do comportamento da ingestão antes da
// extração (internal/materializer/network.go, que chamava net/netip direto).
// São bytes literais de propósito: comparar a função nova contra a função
// nova não provaria paridade nenhuma, e paridade é a razão de o pacote
// existir — um valor gravado pela ingestão e um literal de query comparado a
// ele têm de ser os MESMOS bytes.

func TestIPv6(t *testing.T) {
	valid := []struct {
		raw  string
		want [16]byte
	}{
		{"2001:db8::1", [16]byte{32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"::", [16]byte{}},
		// v4-mapped é ACEITO (Is4In6): a ingestão sempre aceitou, e recusar
		// aqui faria a query não achar o que foi gravado.
		{"::ffff:1.2.3.4", [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 1, 2, 3, 4}},
		{"fe80::1", [16]byte{254, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"2804:1b2:d102:b1d::", [16]byte{40, 4, 1, 178, 209, 2, 11, 29, 0, 0, 0, 0, 0, 0, 0, 0}},
		// Forma expandida produz os mesmos bytes da comprimida.
		{"2001:0db8:0000:0000:0000:0000:0000:0001", [16]byte{32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"::1", [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", [16]byte{255, 255, 255, 255, 255, 255, 255, 255,
			255, 255, 255, 255, 255, 255, 255, 255}},
	}
	for _, tc := range valid {
		t.Run(tc.raw, func(t *testing.T) {
			got, ok := typeparse.IPv6([]byte(tc.raw))
			if !ok {
				t.Fatalf("IPv6(%q) rejeitou", tc.raw)
			}
			if got != tc.want {
				t.Errorf("IPv6(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}

	invalid := []struct {
		raw    string
		reason string
	}{
		{"10.0.0.1", "v4 puro: netip aceita, mas gera 16 bytes que a ingestão nunca gravou"},
		{"fe80::1%eth0", "zona não faz parte do valor armazenado"},
		{"nope", "lixo"},
		{"", "vazio"},
		{"2001:db8::1 ", "espaço à direita"},
		{" 2001:db8::1", "espaço à esquerda"},
		{"2001:db8::1/64", "prefixo não é endereço"},
	}
	for _, tc := range invalid {
		t.Run("invalido/"+tc.raw, func(t *testing.T) {
			if _, ok := typeparse.IPv6([]byte(tc.raw)); ok {
				t.Errorf("IPv6(%q) aceitou (%s)", tc.raw, tc.reason)
			}
		})
	}
}

func TestIPv4Prefix(t *testing.T) {
	valid := []struct {
		raw  string
		want [4]byte
		bits byte
	}{
		{"10.0.0.0/8", [4]byte{10, 0, 0, 0}, 8},
		// Host bits PRESERVADOS: netip.ParsePrefix não mascara, e é o que a
		// ingestão gravou. Se alguém "otimizar" com Masked(), este caso cai.
		{"10.0.0.1/24", [4]byte{10, 0, 0, 1}, 24},
		{"192.168.1.128/25", [4]byte{192, 168, 1, 128}, 25},
		{"0.0.0.0/0", [4]byte{0, 0, 0, 0}, 0},
		{"255.255.255.255/32", [4]byte{255, 255, 255, 255}, 32},
	}
	for _, tc := range valid {
		t.Run(tc.raw, func(t *testing.T) {
			addr, bits, ok := typeparse.IPv4Prefix([]byte(tc.raw))
			if !ok {
				t.Fatalf("IPv4Prefix(%q) rejeitou", tc.raw)
			}
			if addr != tc.want || bits != tc.bits {
				t.Errorf("IPv4Prefix(%q) = %v/%d, want %v/%d", tc.raw, addr, bits, tc.want, tc.bits)
			}
		})
	}

	invalid := []string{
		"2001:db8::/32", "10.0.0.0/33", "10.0.0.0", "10.0.0.0/", "", "10.0.0.0/8 ",
		// Zona: mesma regra do IPv6 solto — não faz parte do valor armazenado,
		// então um literal com zona não pode casar com nada gravado.
		"10.0.0.0%eth0/8",
	}
	for _, raw := range invalid {
		t.Run("invalido/"+raw, func(t *testing.T) {
			if _, _, ok := typeparse.IPv4Prefix([]byte(raw)); ok {
				t.Errorf("IPv4Prefix(%q) aceitou", raw)
			}
		})
	}
}

func TestIPv6Prefix(t *testing.T) {
	valid := []struct {
		raw  string
		want [16]byte
		bits byte
	}{
		{"2001:db8::/32", [16]byte{32, 1, 13, 184}, 32},
		{"::/0", [16]byte{}, 0},
		{"2001:db8::1/128", [16]byte{32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 128},
		{"2804:1b2:d102:b1d::/64", [16]byte{40, 4, 1, 178, 209, 2, 11, 29}, 64},
		{"2804:1b2:d080:4ea2::/64", [16]byte{40, 4, 1, 178, 208, 128, 78, 162}, 64},
	}
	for _, tc := range valid {
		t.Run(tc.raw, func(t *testing.T) {
			addr, bits, ok := typeparse.IPv6Prefix([]byte(tc.raw))
			if !ok {
				t.Fatalf("IPv6Prefix(%q) rejeitou", tc.raw)
			}
			if addr != tc.want || bits != tc.bits {
				t.Errorf("IPv6Prefix(%q) = %v/%d, want %v/%d", tc.raw, addr, bits, tc.want, tc.bits)
			}
		})
	}

	invalid := []string{
		"10.0.0.0/8", "2001:db8::/129", "2001:db8::", "", "2001:db8::/32 ",
		// Zona é recusada nas DUAS grafias (literal e percent-encoded). Hoje
		// quem recusa é netip.ParsePrefix, não uma checagem nossa: o teste
		// existe para que trocar a implementação por um split manual em "/" +
		// ParseAddr (que ACEITA zona) não passe despercebido.
		"fe80::1%eth0/64", "fe80::1%25eth0/64",
	}
	for _, raw := range invalid {
		t.Run("invalido/"+raw, func(t *testing.T) {
			if _, _, ok := typeparse.IPv6Prefix([]byte(raw)); ok {
				t.Errorf("IPv6Prefix(%q) aceitou", raw)
			}
		})
	}
}

// Nenhum dos parsers pode reter o slice recebido — é contrato do pacote, e o
// materializer passa uma janela do buffer do evento, que é reutilizado.
func TestParsersNaoRetemEntrada(t *testing.T) {
	raw := []byte("2001:db8::1")
	got, ok := typeparse.IPv6(raw)
	if !ok {
		t.Fatal("IPv6 rejeitou")
	}
	for i := range raw {
		raw[i] = 'X'
	}
	if got != [16]byte{32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1} {
		t.Errorf("resultado mudou ao sobrescrever a entrada: %v", got)
	}
}
