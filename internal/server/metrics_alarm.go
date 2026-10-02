package server

import (
	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/series"
)

// registerMetricsOfAlarms publica as séries dos alarmes e da emissão periódica.
//
// É um coletor PULL, e não famílias mantidas: o estado já existe no runner e no
// emissor, e criar contadores aqui seria uma SEGUNDA fonte para o mesmo fato —
// que diverge no dia em que um caminho de erro esquece de somar, e em produção
// alguém lê o errado. É a mesma escolha do coletor de dataset.
//
// As séries são publicadas para todo dataset que DECLARA alarme ou emissão,
// inclusive antes da primeira passada e inclusive com todos os números em zero.
// Série que só aparece quando há o que mostrar vira lacuna no gráfico, e lacuna
// não se distingue de alvo fora do ar — zero é uma resposta, ausência não é.
func registerMetricsOfAlarms(reg *metrics.Registry, s *Server) {
	reg.Register(metrics.CollectorFunc(func() []metrics.Sample {
		out := make([]metrics.Sample, 0, len(s.datasets)*8)
		for _, d := range s.datasets {
			for _, delivery := range s.seriesDelivery.Stats(d.nome) {
				out = append(out, samplesOfSeriesDelivery(d.nome, delivery)...)
			}
			for _, delivery := range d.delivery.Stats() {
				out = append(out, samplesOfAlarmDelivery(d.nome, delivery)...)
			}
			if d.alarms != nil {
				out = append(out, samplesOfAlarms(d.nome, d.alarms)...)
			}
			if len(d.emitters) > 0 {
				out = append(out, samplesOfSeries(d.nome, d.emitters)...)
			}
		}
		return out
	}))
}

func samplesOfAlarms(dataset string, r *alarm.Runner) []metrics.Sample {
	stats := r.Stats()
	out := make([]metrics.Sample, 0, len(stats)*7+3)
	for _, a := range stats {
		rotulo := []metrics.Label{
			{Name: metrics.LabelDataset, Value: dataset},
			{Name: metrics.LabelCounter, Value: a.Counter},
			{Name: metrics.LabelAlarm, Value: a.Alarm},
		}
		out = append(out,
			metrics.Sample{
				Family: metrics.MetAlarmGroupsTracked,
				Name:   metrics.MetAlarmGroupsTracked,
				Help:   "grupos em observação: a condição vale para eles, tenham ou não cumprido o prazo",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(a.Tracked),
			},
			metrics.Sample{
				Family: metrics.MetAlarmGroupsFiring,
				Name:   metrics.MetAlarmGroupsFiring,
				Help:   "grupos com o alarme disparado agora",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(a.Firing),
			},
			metrics.Sample{
				Family: metrics.MetAlarmFiredTotal,
				Name:   metrics.MetAlarmFiredTotal,
				Help:   "disparos desde a subida",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(a.Fired),
			},
			metrics.Sample{
				Family: metrics.MetAlarmResolvedTotal,
				Name:   metrics.MetAlarmResolvedTotal,
				Help:   "resoluções desde a subida (condição que passou, grupo aposentado ou desqualificação)",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(a.Resolved),
			},
			metrics.Sample{
				Family: metrics.MetAlarmDisqualifiedTotal,
				Name:   metrics.MetAlarmDisqualifiedTotal,
				Help:   "resoluções por desqualificação desde a subida; subconjunto de nishidb_alarm_resolved_total",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(a.Disqualified),
			},
			metrics.Sample{
				Family: metrics.MetAlarmEnrichFailedTotal,
				Name:   metrics.MetAlarmEnrichFailedTotal,
				Help:   "consultas de enriquecimento que falharam; o alarme saiu sem o bloco",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(a.EnrichFailed),
			},
			metrics.Sample{
				Family: metrics.MetAlarmEnrichSkippedTotal,
				Name:   metrics.MetAlarmEnrichSkippedTotal,
				Help:   "consultas de enriquecimento puladas pelo teto da passada; o alarme saiu sem o bloco",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(a.EnrichSkipped),
			},
		)
	}
	// As três do dataset: a passada é do dataset inteiro, e não de um alarme.
	porDataset := []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}}
	ativo := 0.0
	if r.Active() {
		ativo = 1
	}
	return append(out,
		metrics.Sample{
			Family: metrics.MetAlarmWatching,
			Name:   metrics.MetAlarmWatching,
			Help:   "1 enquanto o laço de varredura de alarmes está de pé",
			Type:   metrics.TypeGauge,
			Labels: porDataset,
			Value:  ativo,
		},
		metrics.Sample{
			Family: metrics.MetAlarmSweepSeconds,
			Name:   metrics.MetAlarmSweepSeconds,
			Help:   "duração da última passada de varredura, em segundos",
			Type:   metrics.TypeGauge,
			Labels: porDataset,
			Value:  r.LastSweep().Seconds(),
		},
		metrics.Sample{
			Family: metrics.MetAlarmRestoredGroups,
			Name:   metrics.MetAlarmRestoredGroups,
			Help:   "cronômetros de sustentação lidos do disco na subida",
			Type:   metrics.TypeGauge,
			Labels: porDataset,
			Value:  float64(r.RestoredGroups()),
		},
	)
}

func samplesOfSeries(dataset string, es []*series.Emitter) []metrics.Sample {
	out := make([]metrics.Sample, 0, len(es)*7+1)
	emitting := 0.0
	for _, e := range es {
		if e.Active() {
			emitting = 1
		}
		st := e.Stats()
		labels := []metrics.Label{
			{Name: metrics.LabelDataset, Value: dataset},
			{Name: metrics.LabelCounter, Value: st.Counter},
		}
		// As duas do laço saem para TODO emissor, inclusive o só-Kafka: o
		// laço e as fronteiras são do emissor, e não do arquivo.
		out = append(out,
			metrics.Sample{
				Family: metrics.MetSeriesSkippedBoundariesTotal,
				Name:   metrics.MetSeriesSkippedBoundariesTotal,
				Help:   "boundaries skipped since startup because the loop resumed late",
				Type:   metrics.TypeCounter,
				Labels: labels,
				Value:  float64(st.SkippedBoundaries),
			},
			metrics.Sample{
				Family: metrics.MetSeriesPassSeconds,
				Name:   metrics.MetSeriesPassSeconds,
				Help:   "duration of the last completed emission pass, in seconds; zero until the first",
				Type:   metrics.TypeGauge,
				Labels: labels,
				Value:  st.LastPassDuration.Seconds(),
			},
		)
		if !st.File {
			continue
		}
		out = append(out,
			metrics.Sample{
				Family: metrics.MetSeriesRowsTotal,
				Name:   metrics.MetSeriesRowsTotal,
				Help:   "series lines written since startup",
				Type:   metrics.TypeCounter,
				Labels: labels,
				Value:  float64(st.Rows),
			},
			metrics.Sample{
				Family: metrics.MetSeriesRotationsTotal,
				Name:   metrics.MetSeriesRotationsTotal,
				Help:   "series file rotations since startup; how much history is being discarded",
				Type:   metrics.TypeCounter,
				Labels: labels,
				Value:  float64(st.Rotations),
			},
			metrics.Sample{
				Family: metrics.MetSeriesErrorsTotal,
				Name:   metrics.MetSeriesErrorsTotal,
				Help:   "series write failures; non-zero means an incomplete series",
				Type:   metrics.TypeCounter,
				Labels: labels,
				Value:  float64(st.Errors),
			},
			metrics.Sample{
				Family: metrics.MetSeriesBytes,
				Name:   metrics.MetSeriesBytes,
				Help:   "size of the current series file, in bytes",
				Type:   metrics.TypeGauge,
				Labels: labels,
				Value:  float64(st.Bytes),
			},
			metrics.Sample{
				Family: metrics.MetSeriesLastSuccessTimestamp,
				Name:   metrics.MetSeriesLastSuccessTimestamp,
				Help:   "last pass that wrote without error, in epoch seconds; zero until the first",
				Type:   metrics.TypeGauge,
				Labels: labels,
				Value:  float64(st.LastSuccessUnixMs) / 1000,
			},
		)
	}
	return append(out, metrics.Sample{
		Family: metrics.MetSeriesEmitting,
		Name:   metrics.MetSeriesEmitting,
		Help:   "1 while any periodic emission loop of the dataset is running",
		Type:   metrics.TypeGauge,
		Labels: []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}},
		Value:  emitting,
	})
}
