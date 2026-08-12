package primary

import (
	"encoding/binary"
	"math/bits"
	"testing"
)

// Vetores públicos de FNV-1a 64 bits.
func TestFnv1a64VetoresPublicos(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"", 0xcbf29ce484222325},
		{"a", 0xaf63dc4c8601ec8c},
		{"foobar", 0x85944171f73967e8},
	}
	for _, c := range cases {
		if got := fnv1a64([]byte(c.in)); got != c.want {
			t.Errorf("fnv1a64(%q): esperado %#x, obtido %#x", c.in, c.want, got)
		}
	}
}

// Golden vectors do HashKey COMPOSTO (remapReserved(fmix64(fnv1a64(k)))),
// calculados com implementação independente (Python) pelo revisor.
//
// ATENÇÃO: primary.idx é um arquivo persistente indexado por este hash.
// Mudar qualquer valor abaixo significa que TODO índice existente aponta
// para o slot errado — exige bump de hashKind no header e rebuild da
// primary.idx. Estes vetores fixam a composição exata das três etapas
// (reordená-las passaria em todos os testes de propriedade sem isto).
func TestHashKeyGoldenVectors(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"", 0xefd01f60ba992926},
		{"a", 0x82a2a958a9bece5b},
		{"foobar", 0x2c22194922d1672b},
		{"CMP-V0001PPCLP-013", 0xa246190b6f3496c4},
	}
	for _, c := range cases {
		if got := HashKey([]byte(c.in)); got != c.want {
			t.Errorf("HashKey(%q): esperado %#x, obtido %#x", c.in, c.want, got)
		}
	}
}

func TestFmix64Propriedades(t *testing.T) {
	if fmix64(0) != 0 {
		t.Fatalf("fmix64(0) deve ser 0, obtido %#x", fmix64(0))
	}
	// Avalanche fraco: flip de 1 bit da entrada muda ~metade dos bits da
	// saída; toleramos [16, 48] por amostra.
	base := fmix64(0x0123456789abcdef)
	for bit := 0; bit < 64; bit++ {
		diff := bits.OnesCount64(base ^ fmix64(0x0123456789abcdef^(1<<bit)))
		if diff < 16 || diff > 48 {
			t.Fatalf("avalanche fraco no bit %d: %d bits diferentes", bit, diff)
		}
	}
}

func TestHashKeyNuncaDevolveValoresReservados(t *testing.T) {
	// remap direto: 0 e 1 são reservados para EMPTY/DELETED.
	if got := remapReserved(0); got != 2 {
		t.Fatalf("remap(0): esperado 2, obtido %d", got)
	}
	if got := remapReserved(1); got != 3 {
		t.Fatalf("remap(1): esperado 3, obtido %d", got)
	}
	if got := remapReserved(2); got != 2 {
		t.Fatalf("remap(2): esperado 2 (sem alteração), obtido %d", got)
	}
	for i := 0; i < 100_000; i++ {
		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		if h := HashKey(key[:]); h < 2 {
			t.Fatalf("HashKey devolveu valor reservado %d para chave %d", h, i)
		}
	}
}

func TestHashKeyDeterministico(t *testing.T) {
	key := []byte("CMP-V0001PPCLP-013")
	h1, h2 := HashKey(key), HashKey(key)
	if h1 != h2 {
		t.Fatalf("HashKey não determinístico: %#x vs %#x", h1, h2)
	}
	if HashKey([]byte("CMP-V0001PPCLP-014")) == h1 {
		t.Fatal("chaves distintas com hash igual (colisão improvável demais para não ser bug)")
	}
}

// Distribuição sob máscara de potência de 2 (uso real: slot = hash & mask).
func TestHashKeyDistribuicaoSobMascara(t *testing.T) {
	const buckets = 1024
	const n = 100_000
	counts := make([]int, buckets)
	for i := 0; i < n; i++ {
		key := []byte("CMP-V0001PPCLP-013-" + string(rune('a'+i%26)))
		key = binary.LittleEndian.AppendUint64(key, uint64(i))
		counts[HashKey(key)&(buckets-1)]++
	}
	expected := n / buckets // ~97
	for b, c := range counts {
		if c < expected/3 || c > expected*3 {
			t.Fatalf("bucket %d com %d chaves (esperado ~%d): dispersão ruim nos bits baixos", b, c, expected)
		}
	}
}

func BenchmarkHashKey(b *testing.B) {
	key := []byte("CMP-V0001PPCLP-013")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = HashKey(key)
	}
}
