package counter

import (
	"os"
	"strconv"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// benchDirFor devolve o diretório do benchmark: dentro de NISHIDB_BENCH_DIR
// quando definida, senão o TempDir. O custo que estes benchmarks medem é o
// fsync, e em tmpfs ele não custa nada — o número só vale em disco real.
func benchDirFor(b *testing.B) string {
	b.Helper()
	root := os.Getenv("NISHIDB_BENCH_DIR")
	if root == "" {
		b.Log("NISHIDB_BENCH_DIR não definida: o fsync medido é o do TempDir, que pode ser tmpfs")
		return b.TempDir()
	}
	dir, err := os.MkdirTemp(root, "counter-bench-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// BenchmarkNewGroupUntilCheckpoint mede o custo de um grupo NOVO de ponta a
// ponta: a transição que o interna e a parte que lhe cabe do checkpoint que o
// torna durável (o ApplyOverlay e o msync do vetor). É a régua do débito 108:
// com fsync por internação, o custo por grupo é o de um fsync; com o fsync no
// checkpoint, ele se dilui por todos os grupos do intervalo.
//
// Uma linha por grupo, cada uma com uma OLT diferente, e os payloads montados
// fora da medição.
func BenchmarkNewGroupUntilCheckpoint(b *testing.B) {
	c, err := Open(benchDirFor(b), defByOLT("olt", false, 1<<30), 1024, false, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ps := make([][]byte, b.N)
	for i := range ps {
		ps[i] = payloadBench([]byte("OLT"+strconv.Itoa(i)), 1, 1, false)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Transition(types.RowID(i), ps[i], false); err != nil {
			b.Fatal(err)
		}
	}
	if err := benchCheckpoint(c); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if got := c.Stats().Groups; got != uint32(b.N) {
		b.Fatalf("%d grupos internados, quer %d", got, b.N)
	}
}

// benchCheckpoint é a parte do checkpoint que cabe ao contador: o overlay ao
// mmap e o msync do vetor.
func benchCheckpoint(c *Counter) error {
	if err := c.ApplyOverlay(nil); err != nil {
		return err
	}
	return c.Msync()
}
