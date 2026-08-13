package radiusgen_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
)

// Gate da Q17: o gerador não pode alocar em regime. Alocação aqui entraria na
// conta de allocs/evento do harness macro e falsearia justamente o número que
// a spec §111 cobra da engine.
func BenchmarkNext(b *testing.B) {
	cfg := radiusgen.DefaultConfig()
	cfg.Seed = 1
	cfg.Entities = 100_000
	g := radiusgen.MustNew(cfg)

	buf := make([]byte, 0, 4096)
	var n int64
	b.ReportAllocs()
	for b.Loop() {
		buf = g.Next(buf[:0])
		n = int64(len(buf))
	}
	b.SetBytes(n)
}

// Evento cheio (~43 campos) isolado: é o pior caso de escrita.
func BenchmarkNextEventoCheio(b *testing.B) {
	g := radiusgen.MustNew(radiusgen.Config{Seed: 2, Entities: 100_000})

	buf := make([]byte, 0, 4096)
	var n int64
	b.ReportAllocs()
	for b.Loop() {
		buf = g.Next(buf[:0])
		n = int64(len(buf))
	}
	b.SetBytes(n)
}

// Patch puro: o formato de 85% do corpus.
func BenchmarkNextPatch(b *testing.B) {
	g := radiusgen.MustNew(radiusgen.Config{Seed: 3, Entities: 100_000, PatchRatio: 1})

	buf := make([]byte, 0, 4096)
	var n int64
	b.ReportAllocs()
	for b.Loop() {
		buf = g.Next(buf[:0])
		n = int64(len(buf))
	}
	b.SetBytes(n)
}
