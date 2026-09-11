package server_test

import (
	"encoding/json"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
)

var seriesOfCounter = []string{
	metrics.MetCounterGroups,
	metrics.MetCounterGroupsMax,
	metrics.MetCounterGroupsRefusedTotal,
	metrics.MetCounterGroupsRetiredTotal,
	metrics.MetCounterOrphanEntries,
	metrics.MetCounterBytes,
	metrics.MetCounterRows,
	metrics.MetCounterCountedRows,
	metrics.MetCounterAuditDivergentGroups,
	metrics.MetCounterAuditLastSuccessTimestamp,
}

// samplesOfCounter devolve, por série, a amostra do contador nomeado.
func samplesOfCounter(t *testing.T, samples []amostraExposta, counter string) map[string]amostraExposta {
	t.Helper()
	out := map[string]amostraExposta{}
	for _, a := range samples {
		if a.Labels[metrics.LabelCounter] == counter && a.Labels[metrics.LabelDataset] == "servicos" {
			out[a.Name] = a
		}
	}
	return out
}

// TestMetricasDeContadorNascemComZero: um dataset com contadores e nenhuma
// linha emite as dez séries por contador, zeradas (o teto é o único valor
// diferente de zero) — lacuna não se distingue de alvo fora do ar.
func TestMetricasDeContadorNascemComZero(t *testing.T) {
	srv := serverWithServices(t, 0)
	samples := coletaSamples(t, srv)
	for _, name := range []string{"servicos_por_olt", "autenticados_por_olt", "autenticados_por_pon"} {
		bySerie := samplesOfCounter(t, samples, name)
		for _, serie := range seriesOfCounter {
			a, ok := bySerie[serie]
			if !ok {
				t.Fatalf("contador %s sem a série %s no scrape", name, serie)
			}
			if serie == metrics.MetCounterGroupsMax {
				if a.Value == 0 {
					t.Fatalf("%s de %s tem de ser o teto, veio 0", serie, name)
				}
				continue
			}
			if serie == metrics.MetCounterBytes {
				continue // os arquivos existem desde a abertura
			}
			if a.Value != 0 {
				t.Fatalf("%s de %s tinha de nascer em zero, veio %v", serie, name, a.Value)
			}
		}
		if bySerie[metrics.MetCounterGroupsRefusedTotal].Tipo != metrics.TypeCounter {
			t.Fatalf("refused_total tem de ser contador")
		}
		if bySerie[metrics.MetCounterGroups].Tipo != metrics.TypeGauge {
			t.Fatalf("groups tem de ser medidor")
		}
		// Os dois agregados DESCEM quando uma linha é lapidada ou vence, então
		// são medidores. Publicá-los como contador diria ao Prometheus que a
		// queda é um reinício do processo, e o rate() sairia errado.
		for _, serie := range []string{metrics.MetCounterRows, metrics.MetCounterCountedRows} {
			if bySerie[serie].Tipo != metrics.TypeGauge {
				t.Fatalf("%s tem de ser medidor, é %v", serie, bySerie[serie].Tipo)
			}
		}
	}
}

// TestStatsEMetricsLeemOMesmoContador: o número em /v1/stats é o do scrape —
// nenhum contador é mantido dos dois lados.
func TestStatsEMetricsLeemOMesmoContador(t *testing.T) {
	srv := serverWithServices(t, 24)
	var e protocol.Stats
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/stats")), &e); err != nil {
		t.Fatal(err)
	}
	var ds *protocol.DatasetStats
	for i := range e.Datasets {
		if e.Datasets[i].Name == "servicos" {
			ds = &e.Datasets[i]
		}
	}
	if ds == nil {
		t.Fatal("dataset servicos ausente do /v1/stats")
	}
	if len(ds.Counters) != 3 {
		t.Fatalf("%d contadores no /v1/stats, quero 3", len(ds.Counters))
	}
	samples := coletaSamples(t, srv)
	for _, c := range ds.Counters {
		bySerie := samplesOfCounter(t, samples, c.Name)
		if got := bySerie[metrics.MetCounterGroups].Value; got != float64(c.Groups) {
			t.Fatalf("%s: groups no scrape %v, no /v1/stats %d", c.Name, got, c.Groups)
		}
		if got := bySerie[metrics.MetCounterGroupsMax].Value; got != float64(c.MaxGroups) {
			t.Fatalf("%s: maxGroups no scrape %v, no /v1/stats %d", c.Name, got, c.MaxGroups)
		}
		if got := bySerie[metrics.MetCounterRows].Value; got != float64(c.Rows) {
			t.Fatalf("%s: rows no scrape %v, no /v1/stats %d", c.Name, got, c.Rows)
		}
		if got := bySerie[metrics.MetCounterCountedRows].Value; got != float64(c.Total) {
			t.Fatalf("%s: total no scrape %v, no /v1/stats %d", c.Name, got, c.Total)
		}
		if c.Groups == 0 {
			t.Fatalf("%s: 24 serviços em duas OLTs tinham de dar grupos", c.Name)
		}
		// Toda linha tem `olt`, então todas apontam para algum grupo. Os
		// autenticados são os i%3 != 0, que são 16 — e é `rows != total` que
		// impede a troca dos dois de passar verde nos dois lados de uma vez.
		if c.Rows != 24 {
			t.Fatalf("%s: rows = %d, quero 24", c.Name, c.Rows)
		}
		querTotal := int64(16)
		if c.Name == "servicos_por_olt" {
			querTotal = 24 // sem `where`: todo mundo conta
		}
		if c.Total != querTotal {
			t.Fatalf("%s: total = %d, quero %d", c.Name, c.Total, querTotal)
		}
	}
}
