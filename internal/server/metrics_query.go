package server

import (
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/budget"
)

// metricasDeQuery são as séries de consulta do §92.
//
// Elas nascem AQUI, e não dentro do motor, porque só o protocolo tem o que
// elas medem: o motor não sabe quantas consultas chegaram, quantas estão em
// voo nem quanto tempo cada uma levou de ponta a ponta — ele conhece
// iteradores, não requisições. Enquanto não havia protocolo, as cinco séries
// existiam só como nome em internal/metrics/names.go, sem alimentador nenhum.
type metricasDeQuery struct {
	total    *metrics.CounterFamily
	ativas   *metrics.GaugeFamily
	erros    *metrics.CounterFamily
	latencia *metrics.HistogramFamily
	// longasRecusadas é o contador do SUB-POOL de consulta longa. Sai SEM o
	// label dataset porque o recurso que ele mede é do PROCESSO — o sub-pool é
	// um só para todos os datasets, pela mesma razão que o §82 dá ao
	// maxConcurrent —, e um label que o dado não sustenta faria quem somasse
	// por dataset contar a mesma recusa N vezes.
	longasRecusadas *metrics.CounterFamily
	// orcamentoRecusado conta as linhas barradas pelo orçamento de buffer
	// (ROW_TOO_LARGE). Sem label, e do PROCESSO, pela mesma razão de
	// longasRecusadas: o orçamento é um só para todos os datasets (§82).
	orcamentoRecusado *metrics.CounterFamily
}

// datasetDesconhecido é o valor de label usado quando a requisição falha ANTES
// de o dataset ser resolvido.
//
// Nunca usar o nome vindo do request nesse caso: o label viraria entrada
// controlada pelo cliente, e um cliente em loop com nomes aleatórios criaria
// uma série nova por requisição até o registry consumir a memória do processo.
// Com o valor fixo, o custo de um pedido inválido é uma série, uma vez.
const datasetDesconhecido = "?"

func registraMetricasDeQuery(
	reg *metrics.Registry, eng *query.Engine, voo *emVoo, orc *budget.Global,
) *metricasDeQuery {
	m := &metricasDeQuery{
		total: reg.Counter(metrics.MetQueryTotal,
			"consultas admitidas pelo protocolo remoto", metrics.LabelDataset),
		ativas: reg.Gauge(metrics.MetQueryActive,
			"consultas em execução neste instante", metrics.LabelDataset),
		erros: reg.Counter(metrics.MetQueryErrors,
			"consultas que terminaram em erro, por categoria", metrics.LabelDataset, metrics.LabelCategory),
		latencia: reg.Histogram(metrics.MetQueryLatencySeconds,
			"duração da consulta, do request ao último byte", metrics.DefaultBuckets, metrics.LabelDataset),
		longasRecusadas: reg.Counter(metrics.MetQueryLongRejectedTotal,
			"consultas longas recusadas por falta de vaga no sub-pool (runtime.query.maxLongRunning)"),
		orcamentoRecusado: reg.Counter(metrics.MetQueryBudgetRejectedTotal,
			"linhas recusadas por não caberem no orçamento de buffer (runtime.query.maxBufferPerQuery)"),
	}

	// As duas séries de recusa NASCEM AQUI, com zero, e não no ramo que as
	// incrementa.
	//
	// reg.Counter cria a FAMÍLIA; a série só passa a existir no primeiro
	// Unlabeled(), e enquanto ele não acontece o /metrics não a publica. Como as
	// duas só eram tocadas dentro de recusaLonga() e recusaPorOrcamento(), um
	// servidor que nunca recusou nada não as expunha — e aí
	// `absent(nishidb_query_long_rejected_total)` não distingue "nunca
	// recusou" de "o alvo caiu". Série que ALARMA não pode nascer no primeiro
	// alarme; é a mesma regra que o desistiu.Set(0) do watchdog segue.
	m.longasRecusadas.Unlabeled()
	m.orcamentoRecusado.Unlabeled()

	// A idade da consulta mais antiga entra como coletor PULL, lido na hora do
	// scrape. É o único jeito de ela estar certa: o valor cresce sozinho com o
	// tempo, e um medidor escrito na admissão ficaria congelado no instante em
	// que a última consulta começou — mostrando "0 s" enquanto uma varredura de
	// oito minutos segura o pool, que é o estado que a série existe para
	// denunciar.
	//
	// Sem label, e do PROCESSO: a pergunta é "há alguma consulta presa aqui?",
	// e quebrá-la por dataset obrigaria o alerta a somar máximos, que não é uma
	// operação que signifique coisa alguma.
	reg.Register(metrics.CollectorFunc(func() []metrics.Sample {
		return []metrics.Sample{{
			Family: metrics.MetQueryLongestRunningSeconds,
			Name:   metrics.MetQueryLongestRunningSeconds,
			Help:   "idade da consulta em voo há mais tempo; 0 quando não há nenhuma",
			Type:   metrics.TypeGauge,
			Value:  voo.maisAntiga(time.Now()),
		}}
	}))

	// O orçamento de buffer entra pelo MESMO caminho, e pelo mesmo motivo: o
	// valor é o contador atômico do próprio orçamento, lido no scrape. Copiá-lo
	// para um medidor a cada reserva daria duas fontes para o mesmo número — e
	// a divergência apareceria como um painel que discorda do teto que de fato
	// está sendo aplicado.
	//
	// É a série que responde "o memoryTarget está apertado?" ANTES da primeira
	// recusa. Sem ela, o operador só descobriria o teto pelo erro, e aí já
	// perdeu uma consulta para aprender o que o painel diria de graça.
	reg.Register(metrics.CollectorFunc(func() []metrics.Sample {
		return []metrics.Sample{{
			Family: metrics.MetQueryBufferBytes,
			Name:   metrics.MetQueryBufferBytes,
			Help:   "bytes de buffer de consulta reservados neste instante (runtime.memoryTarget)",
			Type:   metrics.TypeGauge,
			Value:  float64(orc.Used()),
		}}
	}))

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
	reg.Register(metrics.CollectorFunc(func() []metrics.Sample {
		return []metrics.Sample{{
			Family: metrics.MetQueryFullScans,
			Name:   metrics.MetQueryFullScans,
			Help:   "planos de full scan escolhidos (§64); é do processo, não do dataset",
			Type:   metrics.TypeCounter,
			Value:  float64(eng.PlanMetrics().FullScan.Load()),
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
	m.total.With(dataset).Inc()
	m.ativas.With(dataset).Add(1)
	return func() { m.ativas.With(dataset).Add(-1) }
}

// erro conta uma falha na categoria dada, que é sempre um código do protocolo
// — conjunto FECHADO, e por isso seguro como label.
func (m *metricasDeQuery) erro(dataset, categoria string) {
	m.erros.With(dataset, categoria).Inc()
}

// recusaLonga conta uma consulta longa barrada pelo sub-pool.
//
// Chamada ALÉM de erro(), não no lugar dela: a série por categoria continua
// contando a recusa como qualquer outra falha do dataset, e esta responde a
// outra pergunta — "preciso subir runtime.query.maxLongRunning?" —, que é do
// processo e não sai de uma soma sobre datasets.
func (m *metricasDeQuery) recusaLonga() { m.longasRecusadas.Unlabeled().Inc() }

// recusaPorOrcamento conta uma linha barrada pelo teto de buffer da consulta.
// Chamada ALÉM de erro(), pela mesma razão de recusaLonga.
func (m *metricasDeQuery) recusaPorOrcamento() { m.orcamentoRecusado.Unlabeled().Inc() }

// observa registra a duração total, do request ao último byte escrito. Inclui o
// tempo de rede do cliente lento de propósito: é a latência que o usuário sente,
// e separar as duas exigiria instrumentar o socket.
func (m *metricasDeQuery) observa(dataset string, segundos float64) {
	m.latencia.With(dataset).Observe(segundos)
}
