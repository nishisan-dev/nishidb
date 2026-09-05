package server_test

import (
	"encoding/json"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
)

var seriesDeContador = []string{
	metrics.MetCounterGroups,
	metrics.MetCounterGroupsMax,
	metrics.MetCounterGroupsRefusedTotal,
	metrics.MetCounterGroupsRetiredTotal,
	metrics.MetCounterOrphanEntries,
	metrics.MetCounterBytes,
	metrics.MetCounterAuditDivergentGroups,
	metrics.MetCounterAuditLastSuccessTimestamp,
}

// amostrasDoContador devolve, por série, a amostra do contador nomeado.
func amostrasDoContador(t *testing.T, amostras []amostraExposta, contador string) map[string]amostraExposta {
	t.Helper()
	out := map[string]amostraExposta{}
	for _, a := range amostras {
		if a.Labels[metrics.LabelCounter] == contador && a.Labels[metrics.LabelDataset] == "servicos" {
			out[a.Nome] = a
		}
	}
	return out
}

// TestMetricasDeContadorNascemComZero: um dataset com contadores e nenhuma
// linha emite as oito séries por contador, zeradas (o teto é o único valor
// diferente de zero) — lacuna não se distingue de alvo fora do ar.
func TestMetricasDeContadorNascemComZero(t *testing.T) {
	srv := servidorComServicos(t, 0)
	amostras := coletaAmostras(t, srv)
	for _, nome := range []string{"servicos_por_olt", "autenticados_por_olt", "autenticados_por_pon"} {
		porSerie := amostrasDoContador(t, amostras, nome)
		for _, serie := range seriesDeContador {
			a, ok := porSerie[serie]
			if !ok {
				t.Fatalf("contador %s sem a série %s no scrape", nome, serie)
			}
			if serie == metrics.MetCounterGroupsMax {
				if a.Valor == 0 {
					t.Fatalf("%s de %s tem de ser o teto, veio 0", serie, nome)
				}
				continue
			}
			if serie == metrics.MetCounterBytes {
				continue // os arquivos existem desde a abertura
			}
			if a.Valor != 0 {
				t.Fatalf("%s de %s tinha de nascer em zero, veio %v", serie, nome, a.Valor)
			}
		}
		if porSerie[metrics.MetCounterGroupsRefusedTotal].Tipo != metrics.TipoContador {
			t.Fatalf("refused_total tem de ser contador")
		}
		if porSerie[metrics.MetCounterGroups].Tipo != metrics.TipoMedidor {
			t.Fatalf("groups tem de ser medidor")
		}
	}
}

// TestStatsEMetricsLeemOMesmoContador: o número em /v1/stats é o do scrape —
// nenhum contador é mantido dos dois lados.
func TestStatsEMetricsLeemOMesmoContador(t *testing.T) {
	srv := servidorComServicos(t, 24)
	var e protocolo.Estatisticas
	if err := json.Unmarshal([]byte(pega(t, srv, "/v1/stats")), &e); err != nil {
		t.Fatal(err)
	}
	var ds *protocolo.DatasetStats
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
	amostras := coletaAmostras(t, srv)
	for _, c := range ds.Counters {
		porSerie := amostrasDoContador(t, amostras, c.Name)
		if got := porSerie[metrics.MetCounterGroups].Valor; got != float64(c.Groups) {
			t.Fatalf("%s: groups no scrape %v, no /v1/stats %d", c.Name, got, c.Groups)
		}
		if got := porSerie[metrics.MetCounterGroupsMax].Valor; got != float64(c.MaxGroups) {
			t.Fatalf("%s: maxGroups no scrape %v, no /v1/stats %d", c.Name, got, c.MaxGroups)
		}
		if c.Groups == 0 {
			t.Fatalf("%s: 24 serviços em duas OLTs tinham de dar grupos", c.Name)
		}
	}
}
