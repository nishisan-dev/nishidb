package micro

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// specPath é o YAML de PRODUÇÃO (spec §12) — os micro-benchmarks medem o
// pipeline com o schema real, não com um recorte conveniente.
const specPath = "../../testdata/radius.materialization.yaml"

// benchDirEnv aponta para disco real. Ver o doc do pacote: sem ela, os
// benchmarks que escrevem no Store real pulam.
const benchDirEnv = "NISHIDB_BENCH_DIR"

// corpusEntities é a cardinalidade dos corpora: alta o bastante para o
// dicionário de valores e os índices se comportarem como em produção, baixa o
// bastante para o setup do benchmark não dominar a rodada.
const corpusEntities = 20_000

func loadSpec(tb testing.TB) *schema.DatasetSpec {
	tb.Helper()
	spec, err := schema.Load(specPath, schema.BuiltinDefaults)
	if err != nil {
		tb.Fatalf("carregando %s: %v", specPath, err)
	}
	return spec
}

// indexNames sai do SPEC e não do Compiled porque o Store precisa dos nomes
// ANTES de existir dicionário para compilar contra — é o Open que cria as
// entradas. O laço local virou chamada a schema.IndexedFieldNames: ele era mais
// uma cópia do laço que aquela função existe para centralizar, e não conhecia
// coluna derivada.
func indexNames(spec *schema.DatasetSpec) []string { return schema.IndexedFieldNames(spec) }

// compiledSetup compila o YAML de produção contra um dicionário isolado.
func compiledSetup(tb testing.TB) *schema.Compiled {
	tb.Helper()
	c, err := loadSpec(tb).Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

// fakeSetup monta materializer + dublê com os índices do YAML.
func fakeSetup(tb testing.TB) (*materializer.Materializer, *fakestorage.Store, *schema.Compiled) {
	tb.Helper()
	spec := loadSpec(tb)
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	var idx fakestorage.Indexes
	for _, name := range indexNames(spec) {
		f, ok := c.FieldByName([]byte(name))
		if !ok {
			tb.Fatalf("campo %q com índice no YAML não foi compilado", name)
		}
		idx.Fields = append(idx.Fields, f.ID)
	}
	st := fakestorage.New(idx)
	m, err := materializer.New(c, st)
	if err != nil {
		tb.Fatal(err)
	}
	return m, st, c
}

// realSetup abre um Store REAL em dir e monta o materializer sobre ele.
//
// A ordem importa: o Open resolve os nomes de campo indexado no dicionário do
// dataset, e só depois o Compile atribui os IDs contra o MESMO dicionário —
// compilar antes exigiria abrir o dataset, fechar e reabrir.
func realSetup(tb testing.TB, dir string, estimatedRows uint64) (*materializer.Materializer, *storage.Store, *schema.Compiled) {
	tb.Helper()
	spec := loadSpec(tb)
	st, err := storage.Open(storage.Options{
		Dir: dir,
		// 64 MiB em vez dos 512 MiB default: o fallocate é REAL, e um
		// benchmark não deve reservar meio giga para gravar alguns MiB.
		SegmentSize:         64 << 20,
		EstimatedRows:       estimatedRows,
		IndexedFieldsByName: indexNames(spec),
	})
	if err != nil {
		tb.Fatalf("abrindo store em %s: %v", dir, err)
	}
	tb.Cleanup(func() {
		if err := st.Close(); err != nil {
			tb.Errorf("fechando store: %v", err)
		}
	})
	c, err := spec.Compile(st.FieldDict())
	if err != nil {
		tb.Fatal(err)
	}
	m, err := materializer.New(c, st)
	if err != nil {
		tb.Fatal(err)
	}
	return m, st, c
}

// writableBenchDir devolve o diretório de escrita para benchmarks pesados, ou
// pula com instrução. Ver o doc do pacote para o porquê.
func writableBenchDir(b *testing.B) string {
	b.Helper()
	root := os.Getenv(benchDirEnv)
	if root == "" {
		b.Skipf("defina %s apontando para DISCO REAL (não tmpfs): este benchmark "+
			"appenda um record por iteração e gera centenas de MiB por rodada", benchDirEnv)
	}
	dir, err := os.MkdirTemp(root, "micro-")
	if err != nil {
		b.Fatalf("criando diretório de benchmark em %s: %v", root, err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// readOnlyDir é para os benchmarks de leitura: dataset pequeno, cabe em
// qualquer TMPDIR. Respeita NISHIDB_BENCH_DIR quando definida.
func readOnlyDir(b *testing.B) string {
	b.Helper()
	if root := os.Getenv(benchDirEnv); root != "" {
		dir, err := os.MkdirTemp(root, "micro-ro-")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(dir) })
		return dir
	}
	return filepath.Join(b.TempDir(), "dataset")
}

// corpus gera eventos com o gerador determinístico.
func corpus(tb testing.TB, cfg radiusgen.Config, n int) [][]byte {
	tb.Helper()
	events, err := radiusgen.GenerateCorpus(cfg, n)
	if err != nil {
		tb.Fatal(err)
	}
	return events
}

// corpusCheio é o corpus de eventos completos (fase LOAD).
func corpusCheio(tb testing.TB, seed uint64, n int) [][]byte {
	return corpus(tb, radiusgen.Config{Seed: seed, Entities: corpusEntities}, n)
}

// corpusMisto é a carga de referência (85% patch), a mesma que o harness de
// 8M vai usar.
func corpusMisto(tb testing.TB, seed uint64, n int) [][]byte {
	cfg := radiusgen.DefaultConfig()
	cfg.Seed = seed
	cfg.Entities = corpusEntities
	return corpus(tb, cfg, n)
}

// corpusPatch é o corpus de interim-updates parciais (fase STEADY).
func corpusPatch(tb testing.TB, seed uint64, n int) [][]byte {
	return corpus(tb, radiusgen.Config{
		Seed: seed, Entities: corpusEntities, PatchRatio: 1,
		ExtendedJSONRatio: 0.10, UnknownFieldRatio: 0.01, NullFieldRatio: 0.05,
	}, n)
}

// aplicaTudo ingere o corpus exigindo que nenhum evento seja descartado — um
// benchmark que medisse eventos rejeitados mediria o caminho de erro.
func aplicaTudo(tb testing.TB, m *materializer.Materializer, events [][]byte) {
	tb.Helper()
	for i, e := range events {
		r := m.Apply(e)
		switch r.Action {
		case materializer.ActionCreated, materializer.ActionUpdated, materializer.ActionReplaced:
		default:
			tb.Fatalf("evento %d: Action=%v Category=%v Err=%v", i, r.Action, r.Category, r.Err)
		}
	}
}

// fieldID resolve o ID de um campo do schema compilado.
func fieldID(tb testing.TB, c *schema.Compiled, name string) types.FieldID {
	tb.Helper()
	f, ok := c.FieldByName([]byte(name))
	if !ok {
		tb.Fatalf("campo %q não compilado", name)
	}
	return f.ID
}
