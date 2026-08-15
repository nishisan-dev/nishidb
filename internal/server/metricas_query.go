package server

import (
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/query"
)

// metricasDeQuery são as séries de consulta do §92.
//
// Elas nascem AQUI, e não dentro do motor, porque só o protocolo tem o que
// elas medem: o motor não sabe quantas consultas chegaram, quantas estão em
// voo nem quanto tempo cada uma levou de ponta a ponta — ele conhece
// iteradores, não requisições. Enquanto não havia protocolo, as cinco séries
// existiam só como nome em internal/metrics/nomes.go, sem alimentador nenhum.
type metricasDeQuery struct {
	total    *metrics.FamiliaContador
	ativas   *metrics.FamiliaMedidor
	erros    *metrics.FamiliaContador
	latencia *metrics.FamiliaHistograma
}

// datasetDesconhecido é o valor de label usado quando a requisição falha ANTES
// de o dataset ser resolvido.
//
// Nunca usar o nome vindo do request nesse caso: o label viraria entrada
// controlada pelo cliente, e um cliente em loop com nomes aleatórios criaria
// uma série nova por requisição até o registry consumir a memória do processo.
// Com o valor fixo, o custo de um pedido inválido é uma série, uma vez.
const datasetDesconhecido = "?"

func registraMetricasDeQuery(reg *metrics.Registry, eng *query.Engine) *metricasDeQuery {
	m := &metricasDeQuery{
		total: reg.Contador(metrics.MetQueryTotal,
			"consultas admitidas pelo protocolo remoto", metrics.LabelDataset),
		ativas: reg.Medidor(metrics.MetQueryActive,
			"consultas em execução neste instante", metrics.LabelDataset),
		erros: reg.Contador(metrics.MetQueryErrors,
			"consultas que terminaram em erro, por categoria", metrics.LabelDataset, metrics.LabelCategoria),
		latencia: reg.Histograma(metrics.MetQueryLatencySeconds,
			"duração da consulta, do request ao último byte", metrics.LimitesPadrao, metrics.LabelDataset),
	}

	// full_scans é a quinta série de consulta do §92 e a única que já tinha
	// fonte antes do protocolo: o contador NORMATIVO do §64, mantido no
	// plan-time pelo planner. Entra como coletor PULL — lido no scrape, nunca
	// copiado —, porque duplicá-lo criaria um segundo contador com o mesmo
	// nome de spec, e em produção alguém leria o errado.
	//
	// Sai SEM o label dataset de propósito: planner.Metrics é um por Engine,
	// compartilhado por todos os datasets do processo (internal/query/engine.go).
	// Um label que o dado não sustenta seria pior que label nenhum — quem
	// somasse as séries por dataset contaria o mesmo número N vezes.
	reg.Coletor(metrics.ColetorFunc(func() []metrics.Amostra {
		return []metrics.Amostra{{
			Familia: metrics.MetQueryFullScans,
			Nome:    metrics.MetQueryFullScans,
			Ajuda:   "planos de full scan escolhidos (§64); é do processo, não do dataset",
			Tipo:    metrics.TipoContador,
			Valor:   float64(eng.PlanMetrics().FullScan.Load()),
		}}
	}))
	return m
}

// admite registra a chegada de uma consulta e devolve a função que a encerra.
//
// O par inc/dec do medidor de ativas vive numa closure para que o caminho de
// erro não possa esquecê-lo: um `defer` no chamador cobre todos os retornos,
// inclusive os que ainda não existem.
func (m *metricasDeQuery) admite(dataset string) func() {
	m.total.Com(dataset).Inc()
	m.ativas.Com(dataset).Add(1)
	return func() { m.ativas.Com(dataset).Add(-1) }
}

// erro conta uma falha na categoria dada, que é sempre um código do protocolo
// — conjunto FECHADO, e por isso seguro como label.
func (m *metricasDeQuery) erro(dataset, categoria string) {
	m.erros.Com(dataset, categoria).Inc()
}

// observa registra a duração total, do request ao último byte escrito. Inclui o
// tempo de rede do cliente lento de propósito: é a latência que o usuário sente,
// e separar as duas exigiria instrumentar o socket.
func (m *metricasDeQuery) observa(dataset string, segundos float64) {
	m.latencia.Com(dataset).Observa(segundos)
}
