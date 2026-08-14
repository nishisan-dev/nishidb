package metrics_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
)

func exposicao(t *testing.T, r *metrics.Registry) string {
	t.Helper()
	var sb strings.Builder
	if _, err := r.WriteTo(&sb); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return sb.String()
}

func exigeLinha(t *testing.T, saida, linha string) {
	t.Helper()
	for _, l := range strings.Split(saida, "\n") {
		if l == linha {
			return
		}
	}
	t.Errorf("linha ausente:\n  quer: %s\n  tem:\n%s", linha, saida)
}

func TestContadorEmiteHelpTypeEValor(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.Contador("nishidb_ingest_events_total", "Eventos oferecidos à ingestão.", "dataset")
	c.Com("radius_sessions").Add(8)
	c.Com("radius_sessions").Inc()

	s := exposicao(t, r)
	exigeLinha(t, s, "# HELP nishidb_ingest_events_total Eventos oferecidos à ingestão.")
	exigeLinha(t, s, "# TYPE nishidb_ingest_events_total counter")
	exigeLinha(t, s, `nishidb_ingest_events_total{dataset="radius_sessions"} 9`)
}

// HELP e TYPE saem UMA vez por família, mesmo com várias séries. Repetir o
// cabeçalho faz o Prometheus recusar o scrape inteiro.
func TestCabecalhoSaiUmaVezPorFamilia(t *testing.T) {
	r := metrics.NewRegistry()
	g := r.Medidor("nishidb_kafka_lag", "Lag por partição.", "dataset", "partition")
	g.Com("radius_sessions", "0").Set(12)
	g.Com("radius_sessions", "1").Set(0)
	g.Com("outro", "0").Set(3)

	s := exposicao(t, r)
	if n := strings.Count(s, "# TYPE nishidb_kafka_lag "); n != 1 {
		t.Errorf("TYPE apareceu %d vezes, quer 1:\n%s", n, s)
	}
	if n := strings.Count(s, "# HELP nishidb_kafka_lag "); n != 1 {
		t.Errorf("HELP apareceu %d vezes, quer 1:\n%s", n, s)
	}
	exigeLinha(t, s, `nishidb_kafka_lag{dataset="radius_sessions",partition="0"} 12`)
	exigeLinha(t, s, `nishidb_kafka_lag{dataset="radius_sessions",partition="1"} 0`)
	exigeLinha(t, s, `nishidb_kafka_lag{dataset="outro",partition="0"} 3`)
}

func TestMetricaSemLabelNaoEmiteChaves(t *testing.T) {
	r := metrics.NewRegistry()
	r.Medidor("nishidb_compaction_running", "Compaction em curso.").Sozinha().Set(1)

	exigeLinha(t, exposicao(t, r), "nishidb_compaction_running 1")
}

// O offset e o lag do §92 são inteiros grandes: 1e7 não pode sair como "1e+07",
// que o Prometheus até aceita, mas que quebra o olho de quem lê o scrape.
func TestValorInteiroSaiSemNotacaoCientifica(t *testing.T) {
	r := metrics.NewRegistry()
	r.Medidor("nishidb_kafka_offset", "Offset.").Sozinha().Set(918273645)
	r.Medidor("nishidb_storage_bytes", "Bytes.").Sozinha().Set(2481038746)

	s := exposicao(t, r)
	exigeLinha(t, s, "nishidb_kafka_offset 918273645")
	exigeLinha(t, s, "nishidb_storage_bytes 2481038746")
}

func TestValorFracionarioMantemPrecisao(t *testing.T) {
	r := metrics.NewRegistry()
	r.Medidor("nishidb_ingest_events_rate", "Taxa.").Sozinha().Set(132931.5)

	exigeLinha(t, exposicao(t, r), "nishidb_ingest_events_rate 132931.5")
}

func TestHistogramaEmiteBucketsSomaEContagem(t *testing.T) {
	r := metrics.NewRegistry()
	h := r.Histograma("nishidb_query_latency_seconds", "Latência de consulta.",
		[]float64{0.001, 0.01, 0.1}, "dataset")
	serie := h.Com("radius_sessions")
	serie.Observa(0.0005) // cai no primeiro bucket
	serie.Observa(0.05)   // cai no terceiro
	serie.Observa(1.0)    // só no +Inf

	s := exposicao(t, r)
	exigeLinha(t, s, "# TYPE nishidb_query_latency_seconds histogram")
	exigeLinha(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="0.001"} 1`)
	exigeLinha(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="0.01"} 1`)
	exigeLinha(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="0.1"} 2`)
	exigeLinha(t, s, `nishidb_query_latency_seconds_bucket{dataset="radius_sessions",le="+Inf"} 3`)
	exigeLinha(t, s, `nishidb_query_latency_seconds_count{dataset="radius_sessions"} 3`)
	exigeLinha(t, s, `nishidb_query_latency_seconds_sum{dataset="radius_sessions"} 1.0505`)
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
	valor := 1.0
	r.Coletor(metrics.ColetorFunc(func() []metrics.Amostra {
		return []metrics.Amostra{{
			Familia: "nishidb_materialized_rows",
			Nome:    "nishidb_materialized_rows",
			Ajuda:   "Rows materializadas.",
			Tipo:    metrics.TipoMedidor,
			Labels:  []metrics.Label{{Nome: "dataset", Valor: "radius_sessions"}},
			Valor:   valor,
		}}
	}))

	exigeLinha(t, exposicao(t, r), `nishidb_materialized_rows{dataset="radius_sessions"} 1`)
	valor = 8000000
	exigeLinha(t, exposicao(t, r), `nishidb_materialized_rows{dataset="radius_sessions"} 8000000`)
}

// Aspas, barra e quebra de linha num valor de label quebrariam o formato de
// exposição — o scrape inteiro é recusado, não só a linha.
func TestValorDeLabelEEscapado(t *testing.T) {
	r := metrics.NewRegistry()
	r.Contador("nishidb_ingest_invalid_total", "Inválidos.", "motivo").
		Com("aspa \" barra \\ quebra \n fim").Inc()

	exigeLinha(t, exposicao(t, r),
		`nishidb_ingest_invalid_total{motivo="aspa \" barra \\ quebra \n fim"} 1`)
}

func TestAjudaEEscapada(t *testing.T) {
	r := metrics.NewRegistry()
	r.Contador("nishidb_teste_total", "linha 1\nlinha 2 com \\ barra").Sozinha().Inc()

	exigeLinha(t, exposicao(t, r), `# HELP nishidb_teste_total linha 1\nlinha 2 com \\ barra`)
}

// A ordem tem de ser estável entre scrapes: um diff de dois scrapes é
// ferramenta de diagnóstico, e ordem de mapa a inutilizaria.
func TestOrdemDeEmissaoEEstavel(t *testing.T) {
	r := metrics.NewRegistry()
	a := r.Contador("z_ultimo_total", "z", "k")
	b := r.Contador("a_primeiro_total", "a", "k")
	for _, v := range []string{"c", "a", "b"} {
		a.Com(v).Inc()
		b.Com(v).Inc()
	}
	primeiro := exposicao(t, r)
	for i := 0; i < 20; i++ {
		if s := exposicao(t, r); s != primeiro {
			t.Fatalf("scrape %d divergiu:\n%s\n---\n%s", i, primeiro, s)
		}
	}
	// Ordem de REGISTRO, não alfabética: z_ultimo foi registrado antes.
	if strings.Index(primeiro, "z_ultimo_total") > strings.Index(primeiro, "a_primeiro_total") {
		t.Errorf("famílias não saíram na ordem de registro:\n%s", primeiro)
	}
}

func TestNomeDuplicadoDevolveAMesmaFamilia(t *testing.T) {
	r := metrics.NewRegistry()
	a := r.Contador("nishidb_x_total", "x", "dataset")
	b := r.Contador("nishidb_x_total", "x", "dataset")
	a.Com("d").Inc()
	b.Com("d").Inc()

	s := exposicao(t, r)
	exigeLinha(t, s, `nishidb_x_total{dataset="d"} 2`)
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
	r.Contador("nishidb_y_total", "y", "dataset", "partition").Com("so_um")
}

func TestSeriesConcorrentesNaoSePerdem(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.Contador("nishidb_conc_total", "conc", "dataset")

	const goroutines, porGoroutine = 8, 500
	pronto := make(chan struct{})
	fim := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			<-pronto
			for i := 0; i < porGoroutine; i++ {
				c.Com("d").Inc()
			}
			fim <- struct{}{}
		}()
	}
	close(pronto)
	for g := 0; g < goroutines; g++ {
		<-fim
	}
	exigeLinha(t, exposicao(t, r), `nishidb_conc_total{dataset="d"} 4000`)
}
