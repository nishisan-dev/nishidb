package micro

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// blocoDeContadores são dois contadores sobre o corpus RADIUS: o denominador
// (sessões por NAS) e o numerador (sessões cujo último pacote é Start) — a
// mesma forma do caso de uso da proposta, sobre os campos que o corpus tem.
const blocoDeContadores = `
counters:
  - name: sessoes_por_nas
    groupBy: [NAS-Identifier]
  - name: iniciadas_por_nas
    groupBy: [NAS-Identifier]
    where: status = 'Start'
`

// realSetupCom é o realSetup com o logger silenciado (o INFO de subida dos
// contadores quebra a linha de resultado do benchmark) e, opcionalmente, o
// bloco counters acrescentado ao YAML de produção com o callback de
// compilação ligado — o mesmo caminho do servidor.
func realSetupCom(tb testing.TB, dir string, estimatedRows uint64, comContadores bool) *materializer.Materializer {
	tb.Helper()
	yml, err := os.ReadFile(specPath)
	if err != nil {
		tb.Fatal(err)
	}
	if comContadores {
		yml = append(yml, blocoDeContadores...)
	}
	spec, err := schema.Parse(yml, schema.BuiltinDefaults)
	if err != nil {
		tb.Fatalf("spec: %v", err)
	}
	comp := counters.NovoCompilador(spec)
	st, err := storage.Open(storage.Options{
		Dir:                    dir,
		SegmentSize:            64 << 20,
		EstimatedRows:          estimatedRows,
		CamposIndexadosPorNome: indexNames(spec),
		CompilaContadores:      comp.Callback(),
		Log:                    logging.New(io.Discard, logging.FormatText, slog.LevelError),
	})
	if err != nil {
		tb.Fatalf("abrindo store em %s: %v", dir, err)
	}
	tb.Cleanup(func() {
		if err := st.Close(); err != nil {
			tb.Errorf("fechando store: %v", err)
		}
	})
	m, err := materializer.New(comp.Compilado(), st)
	if err != nil {
		tb.Fatal(err)
	}
	return m
}

// Os quatro benchmarks abaixo medem o custo do passo 7c por evento, e são
// quatro porque há DUAS perguntas:
//
//   - REGIME: os grupos já existem e a linha já está no overlay. É o custo que
//     uma ingestão 24/7 paga por evento. Aquecido com o corpus INTEIRO nas duas
//     variantes, para que a diferença seja só o contador.
//   - FRIO: o mesmo aquecimento de 512 eventos do BenchmarkApplyIngestReal. A
//     variante com contadores inclui a internação de cada grupo novo — um
//     fsync por NAS — e é o custo da carga inicial, bounded pelo inventário
//     (débito 108).
//
// Todos exigem NISHIDB_BENCH_DIR em disco real.

func BenchmarkApplyIngestRealRegime(b *testing.B) {
	m := realSetupCom(b, writableBenchDir(b), corpusEntities, false)
	events := corpusMisto(b, 11, 8192)
	aplicaTudo(b, m, events)
	aplicaCiclando(b, m, events, materializer.ActionFailed)
}

func BenchmarkApplyIngestRealRegimeComContadores(b *testing.B) {
	m := realSetupCom(b, writableBenchDir(b), corpusEntities, true)
	events := corpusMisto(b, 11, 8192)
	aplicaTudo(b, m, events)
	aplicaCiclando(b, m, events, materializer.ActionFailed)
}

func BenchmarkApplyIngestRealFrio(b *testing.B) {
	m := realSetupCom(b, writableBenchDir(b), corpusEntities, false)
	events := corpusMisto(b, 11, 8192)
	aplicaTudo(b, m, events[:512])
	aplicaCiclando(b, m, events, materializer.ActionFailed)
}

func BenchmarkApplyIngestRealFrioComContadores(b *testing.B) {
	m := realSetupCom(b, writableBenchDir(b), corpusEntities, true)
	events := corpusMisto(b, 11, 8192)
	aplicaTudo(b, m, events[:512])
	aplicaCiclando(b, m, events, materializer.ActionFailed)
}
