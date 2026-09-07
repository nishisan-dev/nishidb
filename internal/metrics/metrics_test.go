package metrics_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
)

func exposition(t *testing.T, r *metrics.Registry) string {
	t.Helper()
	var sb strings.Builder
	if _, err := r.WriteTo(&sb); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return sb.String()
}

func requireLine(t *testing.T, output, line string) {
	t.Helper()
	for _, l := range strings.Split(output, "\n") {
		if l == line {
			return
		}
	}
	t.Errorf("linha ausente:\n  quer: %s\n  tem:\n%s", line, output)
}

func TestContadorEmiteHelpTypeEValor(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.Counter("nishidb_ingest_events_total", "Eventos oferecidos à ingestão.", "dataset")
	c.With("radius_sessions").Add(8)
	c.With("radius_sessions").Inc()

	s := exposition(t, r)
	requireLine(t, s, "# HELP nishidb_ingest_events_total Eventos oferecidos à ingestão.")
	requireLine(t, s, "# TYPE nishidb_ingest_events_total counter")
	requireLine(t, s, `nishidb_ingest_events_total{dataset="radius_sessions"} 9`)
}

// HELP e TYPE saem UMA vez por família, mesmo com várias séries. Repetir o
// cabeçalho faz o Prometheus recusar o scrape inteiro.
func TestCabecalhoSaiUmaVezPorFamilia(t *testing.T) {
	r := metrics.NewRegistry()
	g := r.Gauge("nishidb_kafka_lag", "Lag por partição.", "dataset", "partition")
	g.With("radius_sessions", "0").Set(12)
	g.With("radius_sessions", "1").Set(0)
	g.With("outro", "0").Set(3)

	s := exposition(t, r)
	if n := strings.Count(s, "# TYPE nishidb_kafka_lag "); n != 1 {
		t.Errorf("TYPE apareceu %d vezes, quer 1:\n%s", n, s)
	}
	if n := strings.Count(s, "# HELP nishidb_kafka_lag "); n != 1 {
		t.Errorf("HELP apareceu %d vezes, quer 1:\n%s", n, s)
	}
	requireLine(t, s, `nishidb_kafka_lag{dataset="radius_sessions",partition="0"} 12`)
	requireLine(t, s, `nishidb_kafka_lag{dataset="radius_sessions",partition="1"} 0`)
	requireLine(t, s, `nishidb_kafka_lag{dataset="outro",partition="0"} 3`)
}

func TestMetricaSemLabelNaoEmiteChaves(t *testing.T) {
	r := metrics.NewRegistry()
	r.Gauge("nishidb_compaction_running", "Compaction em curso.").Unlabeled().Set(1)

	requireLine(t, exposition(t, r), "nishidb_compaction_running 1")
}

// O offset e o lag do §92 são inteiros grandes: 1e7 não pode sair como "1e+07",
// que o Prometheus até aceita, mas que quebra o olho de quem lê o scrape.
func TestValorInteiroSaiSemNotacaoCientifica(t *testing.T) {
	r := metrics.NewRegistry()
	r.Gauge("nishidb_kafka_offset", "Offset.").Unlabeled().Set(918273645)
	r.Gauge("nishidb_storage_bytes", "Bytes.").Unlabeled().Set(2481038746)

	s := exposition(t, r)
	requireLine(t, s, "nishidb_kafka_offset 918273645")
	requireLine(t, s, "nishidb_storage_bytes 2481038746")
}

func TestValorFracionarioMantemPrecisao(t *testing.T) {
	r := metrics.NewRegistry()
	r.Gauge("nishidb_ingest_events_rate", "Taxa.").Unlabeled().Set(132931.5)

	requireLine(t, exposition(t, r), "nishidb_ingest_events_rate 132931.5")
}

func TestHistogramaEmiteBucketsSomaEContagem(t *testing.T) {
	r := metrics.NewRegistry()
	h := r.Histogram("nishidb_query_latency_seconds", "Latência de consulta.",
		[]float64{0.001, 0.01, 0.1}, "dataset")
	series := h.With("radius_sessions")
	series.Observe(0.0005) // cai no primeiro bucket
	series.Observe(0.05)   // cai no terceiro
	series.Observe(1.0)    // só no +Inf

	s := exposition(t, r)
	requireLine(t, s, "# TYPE nishidb_query_latency_seconds histogram")
	requireLine(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="0.001"} 1`)
	requireLine(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="0.01"} 1`)
	requireLine(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="0.1"} 2`)
	requireLine(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="+Inf"} 3`)
	requireLine(t, s, `nishidb_query_latency_seconds_count{dataset="radius_sessions"} 3`)
	requireLine(t, s, `nishidb_query_latency_seconds_sum{dataset="radius_sessions"} 1.0505`)
	// O cabeçalho é do nome-base, não de cada sufixo.
	if strings.Contains(s, "# TYPE nishidb_query_latency_seconds_bucket") {
		t.Errorf("emitiu TYPE para o sufixo _bucket:\n%s", s)
	}
}

// O registry é PULL: os contadores que já existem no projeto (materializer,
// executor, planner, Store.Stats) não são copiados para cá — são lidos na hora
// do scrape. Duplicar o estado criaria um segundo contador com o mesmo nome de
// spec, que é bug de observabilidade.
func TestColetorELidoNaHoraDoScrape(t *testing.T) {
	r := metrics.NewRegistry()
	value := 1.0
	r.Register(metrics.CollectorFunc(func() []metrics.Sample {
		return []metrics.Sample{{
			Family: "nishidb_materialized_rows",
			Name:   "nishidb_materialized_rows",
			Help:   "Rows materializadas.",
			Type:   metrics.TypeGauge,
			Labels: []metrics.Label{{Name: "dataset", Value: "radius_sessions"}},
			Value:  value,
		}}
	}))

	requireLine(t, exposition(t, r), `nishidb_materialized_rows{dataset="radius_sessions"} 1`)
	value = 8000000
	requireLine(t, exposition(t, r), `nishidb_materialized_rows{dataset="radius_sessions"} 8000000`)
}

// Aspas, barra e quebra de linha num valor de label quebrariam o formato de
// exposição — o scrape inteiro é recusado, não só a linha.
func TestValorDeLabelEEscapado(t *testing.T) {
	r := metrics.NewRegistry()
	r.Counter("nishidb_ingest_invalid_total", "Inválidos.", "motivo").
		With("aspa \" barra \\ quebra \n fim").Inc()

	requireLine(t, exposition(t, r),
		`nishidb_ingest_invalid_total{motivo="aspa \" barra \\ quebra \n fim"} 1`)
}

func TestAjudaEEscapada(t *testing.T) {
	r := metrics.NewRegistry()
	r.Counter("nishidb_teste_total", "linha 1\nlinha 2 com \\ barra").Unlabeled().Inc()

	requireLine(t, exposition(t, r), `# HELP nishidb_teste_total linha 1\nlinha 2 com \\ barra`)
}

// A ordem tem de ser estável entre scrapes: um diff de dois scrapes é
// ferramenta de diagnóstico, e ordem de mapa a inutilizaria.
func TestOrdemDeEmissaoEEstavel(t *testing.T) {
	r := metrics.NewRegistry()
	a := r.Counter("z_ultimo_total", "z", "k")
	b := r.Counter("a_primeiro_total", "a", "k")
	for _, v := range []string{"c", "a", "b"} {
		a.With(v).Inc()
		b.With(v).Inc()
	}
	first := exposition(t, r)
	for i := 0; i < 20; i++ {
		if s := exposition(t, r); s != first {
			t.Fatalf("scrape %d divergiu:\n%s\n---\n%s", i, first, s)
		}
	}
	// Ordem de REGISTRO, não alfabética: z_ultimo foi registrado antes.
	if strings.Index(first, "z_ultimo_total") > strings.Index(first, "a_primeiro_total") {
		t.Errorf("famílias não saíram na ordem de registro:\n%s", first)
	}
}

func TestNomeDuplicadoDevolveAMesmaFamilia(t *testing.T) {
	r := metrics.NewRegistry()
	a := r.Counter("nishidb_x_total", "x", "dataset")
	b := r.Counter("nishidb_x_total", "x", "dataset")
	a.With("d").Inc()
	b.With("d").Inc()

	s := exposition(t, r)
	requireLine(t, s, `nishidb_x_total{dataset="d"} 2`)
	if n := strings.Count(s, "# TYPE nishidb_x_total"); n != 1 {
		t.Errorf("família duplicada: TYPE apareceu %d vezes", n)
	}
}

func TestQuantidadeDeLabelsErradaEntraEmPanico(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("quer panic: label a menos é bug de programação, não estado de runtime")
		}
	}()
	r := metrics.NewRegistry()
	r.Counter("nishidb_y_total", "y", "dataset", "partition").With("so_um")
}

func TestSeriesConcorrentesNaoSePerdem(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.Counter("nishidb_conc_total", "conc", "dataset")

	const goroutines, perGoroutine = 8, 500
	ready := make(chan struct{})
	done := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			<-ready
			for i := 0; i < perGoroutine; i++ {
				c.With("d").Inc()
			}
			done <- struct{}{}
		}()
	}
	close(ready)
	for g := 0; g < goroutines; g++ {
		<-done
	}
	requireLine(t, exposition(t, r), `nishidb_conc_total{dataset="d"} 4000`)
}
