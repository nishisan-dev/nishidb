package storage

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// coldBuildGroups é o tamanho do dataset da bancada de construção a frio: uma
// linha por grupo, para que cada linha varrida interne um grupo novo — o pior
// caso do custo por grupo, e o que a carga fria de um contador por splitter
// paga (issue #88).
const coldBuildGroups = 4096

// BenchmarkCounterColdBuild mede a abertura que CONSTRÓI um contador sobre um
// dataset de coldBuildGroups linhas, cada uma com uma OLT diferente, contra a
// abertura do mesmo dataset sem contador. A diferença por grupo
// (ns/group) é o custo da construção — varredura, internação e o checkpoint do
// fim do Open — e é o número que separa "dominado pelo fsync por grupo" de
// "dominado pela varredura".
//
// Exige NISHIDB_BENCH_DIR em disco real: em tmpfs o fsync não custa nada.
func BenchmarkCounterColdBuild(b *testing.B) {
	root := os.Getenv("NISHIDB_BENCH_DIR")
	if root == "" {
		b.Skip("NISHIDB_BENCH_DIR não definida: a bancada mede fsync e só vale em disco real")
	}
	dir, err := os.MkdirTemp(root, "counter-cold-build-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(dir) })
	quiet := logging.New(io.Discard, logging.FormatText, slog.LevelError)

	// O dataset é criado sem contador, e a variante sem contador roda ANTES da
	// que constrói: depois que o manifest declara o contador, abrir sem o
	// callback é recusado.
	opts := Options{Dir: dir, SegmentSize: 64 << 20, Log: quiet}
	s, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	dict := s.FieldDict()
	idID, err := dict.LookupOrCreate([]byte("id"))
	if err != nil {
		b.Fatal(err)
	}
	oltID, err := dict.LookupOrCreate([]byte("olt"))
	if err != nil {
		b.Fatal(err)
	}
	first, second := idID, oltID
	if second < first {
		first, second = second, first
	}
	var buf, key, olt []byte
	for i := 0; i < coldBuildGroups; i++ {
		key = strconv.AppendInt(key[:0], int64(i), 10)
		olt = strconv.AppendInt(append(olt[:0], "OLT"...), int64(i), 10)
		buf = buf[:0]
		for _, id := range []types.FieldID{first, second} {
			v := types.Value{Type: types.TypeString, Bytes: key}
			if id == oltID {
				v.Bytes = olt
			}
			buf = mustAppendField(b, buf, id, v)
		}
		if _, _, err := s.Put(key, 0, 0, buf, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}

	withCounter := opts
	withCounter.CompileCountersCallbackFunction = callbackOfCounters(counterOfTest{name: "spl", groupBy: []string{"olt"}, ceiling: 1 << 20})
	counterDir := filepath.Join(dir, "indexes", diretorioOfCounters, "spl")
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"open_without_counter", opts},
		{"open_building_counter", withCounter},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var elapsed time.Duration
			for i := 0; i < b.N; i++ {
				// Sem os arquivos do contador, a abertura cai no ramo que
				// constrói; sem contador declarado, o RemoveAll é no-op.
				if err := os.RemoveAll(counterDir); err != nil {
					b.Fatal(err)
				}
				start := time.Now()
				st, err := Open(tc.opts)
				if err != nil {
					b.Fatal(err)
				}
				elapsed += time.Since(start)
				if len(st.Counters()) > 0 && st.Counters()[0].Stats().Groups != coldBuildGroups {
					b.Fatalf("construiu %d grupos, quer %d", st.Counters()[0].Stats().Groups, coldBuildGroups)
				}
				if err := st.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N)/coldBuildGroups, "ns/group")
		})
	}
}
