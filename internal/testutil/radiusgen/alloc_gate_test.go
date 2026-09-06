//go:build !race

package radiusgen_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
)

// Gate executável (não só no `make bench`): o Next não aloca. Um gerador que
// alocasse por evento apareceria no harness macro como alocação da ENGINE, e
// a rodada de 8M mediria o instrumento em vez do instrumentado.
//
// Fora do build com -race: a instrumentação do detector altera a contagem.
func TestNextNaoAloca(t *testing.T) {
	cases := []struct {
		name string
		cfg  radiusgen.Config
	}{
		{"referência", func() radiusgen.Config {
			c := radiusgen.DefaultConfig()
			c.Seed = 5
			c.Entities = 50_000
			return c
		}()},
		{"evento cheio", radiusgen.Config{Seed: 6, Entities: 50_000}},
		{"patch puro", radiusgen.Config{Seed: 7, Entities: 50_000, PatchRatio: 1}},
		{"extended json", radiusgen.Config{Seed: 8, Entities: 50_000, PatchRatio: 1, ExtendedJSONRatio: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := radiusgen.MustNew(tc.cfg)
			buf := make([]byte, 0, 8192)
			g.Next(buf[:0]) // warm-up

			got := testing.AllocsPerRun(200, func() {
				buf = g.Next(buf[:0])
			})
			if got != 0 {
				t.Errorf("Next alocou %.1f objetos/evento, want 0", got)
			}
		})
	}
}
