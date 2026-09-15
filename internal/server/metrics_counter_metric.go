package server

import (
	"github.com/nishisan-dev/nishidb/internal/countermetrics"
	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// registerMetricsOfCounterMetrics publica as séries das métricas de janela dos
// contadores.
//
// É um coletor PULL, pela mesma razão do coletor de alarmes: o estado já existe
// no amostrador (Stats, Active, LastSampleSeconds, RestoredGroups), e manter
// contadores aqui seria uma SEGUNDA fonte para o mesmo fato — que diverge no
// dia em que um caminho de erro esquece de somar.
//
// As séries saem para todo dataset com amostrador — isto é, com algum contador
// que declara `metrics:` —, desde a subida e inclusive antes do Start: o
// `sampling` sai 0 até o laço subir. Série que só aparece quando há o que
// mostrar vira lacuna no gráfico, e lacuna não se distingue de alvo fora do ar.
func registerMetricsOfCounterMetrics(reg *metrics.Registry, s *Server) {
	reg.Register(metrics.CollectorFunc(func() []metrics.Sample {
		var out []metrics.Sample
		for _, d := range s.datasets {
			if d.metrics != nil {
				out = append(out, samplesOfCounterMetrics(d.nome, d.metrics)...)
			}
		}
		return out
	}))
}

func samplesOfCounterMetrics(dataset string, sm *countermetrics.Sampler) []metrics.Sample {
	stats := sm.Stats()
	out := make([]metrics.Sample, 0, len(stats)*5+3)
	previousCounter := ""
	for i, m := range stats {
		byMetric := []metrics.Label{
			{Name: metrics.LabelDataset, Value: dataset},
			{Name: metrics.LabelCounter, Value: m.Counter},
			{Name: metrics.LabelMetric, Value: m.Name},
		}
		out = append(out,
			metrics.Sample{
				Family: metrics.MetCounterMetricGroups,
				Name:   metrics.MetCounterMetricGroups,
				Help:   "posições ocupadas por grupos vivos do contador; o mesmo número em todas as métricas dele",
				Type:   metrics.TypeGauge,
				Labels: byMetric,
				Value:  float64(m.Groups),
			},
			metrics.Sample{
				Family: metrics.MetCounterMetricHeapBytes,
				Name:   metrics.MetCounterMetricHeapBytes,
				Help:   "estimativa das entradas da métrica em heap, em bytes; acompanha o máximo de grupos vivos simultâneos",
				Type:   metrics.TypeGauge,
				Labels: byMetric,
				Value:  float64(m.HeapBytes),
			},
			metrics.Sample{
				Family: metrics.MetCounterMetricDiskBytes,
				Name:   metrics.MetCounterMetricDiskBytes,
				Help:   "tamanho do arquivo mapeado da métrica, em bytes",
				Type:   metrics.TypeGauge,
				Labels: byMetric,
				Value:  float64(m.DiskBytes),
			},
			metrics.Sample{
				Family: metrics.MetCounterMetricResetsTotal,
				Name:   metrics.MetCounterMetricResetsTotal,
				Help:   "recriações do arquivo da métrica por cabeçalho divergente ou ilegível; cada uma perdeu a janela da métrica",
				Type:   metrics.TypeCounter,
				Labels: byMetric,
				Value:  float64(m.ResetsTotal),
			},
		)
		// O mapa id → posição é UM por contador, e o MetricStats o repete em cada
		// métrica dele: sai uma amostra, na primeira métrica de cada contador, e
		// nunca a soma — somar contaria o mesmo mapa uma vez por métrica. Stats
		// vem na ordem contador → métrica, então as métricas de um contador são
		// contíguas e a troca de contador marca a primeira.
		if i == 0 || m.Counter != previousCounter {
			out = append(out, metrics.Sample{
				Family: metrics.MetCounterMetricPositionMapBytes,
				Name:   metrics.MetCounterMetricPositionMapBytes,
				Help:   "mapa id → posição do contador, em bytes; um por contador, compartilhado pelas métricas dele",
				Type:   metrics.TypeGauge,
				Labels: []metrics.Label{
					{Name: metrics.LabelDataset, Value: dataset},
					{Name: metrics.LabelCounter, Value: m.Counter},
				},
				Value: float64(m.PositionMapBytes),
			})
		}
		previousCounter = m.Counter
	}
	// As três do dataset: a passada é do dataset inteiro, e não de uma métrica.
	byDataset := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}}
	sampling := 0.0
	if sm.Active() {
		sampling = 1
	}
	return append(out,
		metrics.Sample{
			Family: metrics.MetCounterMetricSampling,
			Name:   metrics.MetCounterMetricSampling,
			Help:   "1 enquanto o laço de amostragem das métricas de janela está de pé",
			Type:   metrics.TypeGauge,
			Labels: byDataset,
			Value:  sampling,
		},
		metrics.Sample{
			Family: metrics.MetCounterMetricSampleSeconds,
			Name:   metrics.MetCounterMetricSampleSeconds,
			Help:   "duração da última passada completa de amostragem, em segundos",
			Type:   metrics.TypeGauge,
			Labels: byDataset,
			Value:  sm.LastSampleSeconds(),
		},
		metrics.Sample{
			Family: metrics.MetCounterMetricRestoredGroups,
			Name:   metrics.MetCounterMetricRestoredGroups,
			Help:   "entradas que saíram da abertura com amostra na janela, somadas sobre as métricas",
			Type:   metrics.TypeGauge,
			Labels: byDataset,
			Value:  float64(sm.RestoredGroups()),
		},
	)
}
