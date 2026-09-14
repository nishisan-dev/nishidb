//go:build !race

package query_test

import (
	"context"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/executor"
)

func TestAliasScanAndEncodingAllocateOnlyPerQuery(t *testing.T) {
	const sql = `SELECT id AS "com""aspas", nome "com""aspas", n AS numero FROM linhas`
	measure := func(count int) float64 {
		h := newStubHarness(t, count, 1_000_000, "30s")
		dst := make([]byte, 0, 1024)
		return testing.AllocsPerRun(20, func() {
			it := h.open(context.Background(), sql)
			rows := 0
			for it.Next() {
				dst = executor.AppendRow(dst[:0], it.Row())
				rows++
			}
			if it.Err() != nil || rows != count {
				t.Fatalf("linhas = %d, erro = %v", rows, it.Err())
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := measure(1000), measure(8000)
	if small != large {
		t.Fatalf("alocações: 1000 linhas = %g, 8000 = %g", small, large)
	}
	h := newStubHarness(t, 1, 1_000_000, "30s")
	it := h.open(context.Background(), sql)
	defer func() {
		if err := it.Close(); err != nil {
			t.Error(err)
		}
	}()
	if !it.Next() {
		t.Fatal("fixture sem linha")
	}
	dst := make([]byte, 0, 1024)
	if got := testing.AllocsPerRun(1000, func() { dst = executor.AppendRow(dst[:0], it.Row()) }); got != 0 {
		t.Fatalf("encoder alocou %g por linha", got)
	}
	t.Logf("alocações por consulta: %g; encoder: zero por linha", small)
}
