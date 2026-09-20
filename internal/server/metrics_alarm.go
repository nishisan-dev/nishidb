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
	out := make([]metrics.Sample, 0, len(es)*5+1)
	emitindo := 0.0
	for _, e := range es {
		if e.Active() {
			emitindo = 1
		}
		st := e.Stats()
		rotulo := []metrics.Label{
			{Name: metrics.LabelDataset, Value: dataset},
			{Name: metrics.LabelCounter, Value: st.Counter},
		}
		out = append(out,
			metrics.Sample{
				Family: metrics.MetSeriesRowsTotal,
				Name:   metrics.MetSeriesRowsTotal,
				Help:   "linhas de série escritas desde a subida",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(st.Rows),
			},
			metrics.Sample{
				Family: metrics.MetSeriesRotationsTotal,
				Name:   metrics.MetSeriesRotationsTotal,
				Help:   "rotações do arquivo de série desde a subida; quanto histórico está sendo descartado",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(st.Rotations),
			},
			metrics.Sample{
				Family: metrics.MetSeriesErrorsTotal,
				Name:   metrics.MetSeriesErrorsTotal,
				Help:   "falhas de escrita da série; diferente de zero significa série incompleta",
				Type:   metrics.TypeCounter,
				Labels: rotulo,
				Value:  float64(st.Errors),
			},
			metrics.Sample{
				Family: metrics.MetSeriesBytes,
				Name:   metrics.MetSeriesBytes,
				Help:   "tamanho do arquivo de série corrente, em bytes",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(st.Bytes),
			},
			metrics.Sample{
				Family: metrics.MetSeriesLastSuccessTimestamp,
				Name:   metrics.MetSeriesLastSuccessTimestamp,
				Help:   "instante da última passada que escreveu sem erro, em segundos do epoch; zero até a primeira",
				Type:   metrics.TypeGauge,
				Labels: rotulo,
				Value:  float64(st.LastSuccessUnixMs) / 1000,
			},
		)
	}
	return append(out, metrics.Sample{
		Family: metrics.MetSeriesEmitting,
		Name:   metrics.MetSeriesEmitting,
		Help:   "1 enquanto algum laço de emissão periódica do dataset está de pé",
		Type:   metrics.TypeGauge,
		Labels: []metrics.Label{{Name: metrics.LabelDataset, Value: dataset}},
		Value:  emitindo,
	})
}
