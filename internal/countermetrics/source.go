package countermetrics

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// MetricSource devolve o amostrador como a fonte das métricas que a tabela do
// contador e o laço de alarmes consomem. É o ÚNICO adaptador entre os dois
// pacotes: o servidor e a CLI local passam este, e nenhum escreve o seu.
//
// Sampler nil devolve MetricSource nil de INTERFACE, que é a fonte válida de
// quem não declara métrica (as colunas saem NULL, ver counters.MetricSource).
// Um counters.MetricSource((*metricSource)(nil)) seria uma interface NÃO-nil, e
// quem a recebesse deixaria de reconhecer "sem fonte".
//
// Importar internal/counters daqui é acíclico: counters declara as interfaces
// no consumidor e não conhece este pacote.
func (s *Sampler) MetricSource() counters.MetricSource {
	if s == nil {
		return nil
	}
	return metricSource{s: s}
}

// metricSource adapta Sampler.Metric, que devolve o tipo concreto, à interface
// do consumidor.
type metricSource struct {
	s *Sampler
}

// Metric resolve a métrica pelo nome. Nome não declarado devolve (nil, false)
// com o nil de INTERFACE: embrulhar o (*Metric)(nil) que Sampler.Metric devolve
// nesse caso daria um leitor não-nil que responde "sem amostra" para sempre, e a
// tabela deixaria de acusar a métrica declarada que a fonte não resolve.
func (a metricSource) Metric(counterName, metric string) (counters.MetricReader, bool) {
	m, ok := a.s.Metric(counterName, metric)
	if !ok {
		return nil, false
	}
	return m, true
}

// CountersWithMetrics monta, na ordem declarada, as entradas do Open para os
// contadores que declaram `metrics:`, casando cada um com o contador aberto de
// mesmo nome. É a montagem única do servidor e da CLI local: as duas abrem o
// amostrador sobre o MESMO conjunto, e uma segunda cópia do filtro divergiria
// da primeira no dia em que o critério mudasse.
//
// Nenhum contador com métrica devolve lista vazia, e quem chama não abre
// amostrador nenhum. Contador com métrica que não está aberto é erro: o Store
// abre todo contador que o YAML declara, e seguir sem ele calaria a métrica.
func CountersWithMetrics(specs []schema.CounterSpec, opened []*counter.Counter) ([]CounterInput, error) {
	byName := make(map[string]*counter.Counter, len(opened))
	for _, c := range opened {
		byName[c.Def().Name] = c
	}
	var out []CounterInput
	for _, spec := range specs {
		if len(spec.Metrics) == 0 {
			continue
		}
		c, ok := byName[spec.Name]
		if !ok {
			return nil, fmt.Errorf("countermetrics: counter %q declares metrics but is not open in this dataset", spec.Name)
		}
		out = append(out, CounterInput{Spec: spec, Reader: c})
	}
	return out, nil
}
