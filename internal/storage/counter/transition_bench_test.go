package counter

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// BenchmarkTransitionHotPath mede o caminho quente do contador no regime: os
// grupos já internados e as linhas já presentes no overlay, alternando grupo e
// condição para percorrer as duas pontas do delta — o mesmo regime do gate de
// alocação (TestContadorNaoAlocaNoCaminhoQuente). É o custo que TODA escrita do
// dataset paga por contador declarado, dentro do applyRecord.
func BenchmarkTransitionHotPath(b *testing.B) {
	c, err := Open(b.TempDir(), defByPON("pon", 100), 1024, false)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	benchTransitions(b, c)
}

// benchTransitions popula o overlay fora da medição e mede a transição.
func benchTransitions(b *testing.B, c *Counter) {
	b.Helper()
	const rows = 1 << 12
	ps := [][]byte{
		payloadBench([]byte("A"), 1, 1, true),
		payloadBench([]byte("B"), 2, 3, false),
		payloadBench([]byte("A"), 1, 1, false),
		payloadBench([]byte("B"), 2, 3, true),
	}
	for r := types.RowID(0); r < rows; r++ {
		if err := c.Transition(r, ps[int(r)%len(ps)], false); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	var r types.RowID
	i := 0
	for n := 0; n < b.N; n++ {
		r = (r + 1) & (rows - 1)
		i = (i + 1) % len(ps)
		if err := c.Transition(r, ps[i], false); err != nil {
			b.Fatal(err)
		}
	}
}
