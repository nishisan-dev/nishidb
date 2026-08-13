package executor

import "sync/atomic"

// Metrics são contadores em memória do executor (o endpoint Prometheus é de
// outra fase).
//
// O iterador acumula em campos LOCAIS não atômicos e faz um único flush
// atômico no estado terminal: 8 milhões de incrementos atômicos custam caro e
// não trazem informação nenhuma a mais.
type Metrics struct {
	// SourceFullScan conta fontes de full scan ABERTAS por este executor.
	//
	// NÃO é a métrica normativa `query.full_scan` da spec §64 — essa é contada
	// uma vez, no plan-time, em planner.Metrics.FullScan. Dois contadores com o
	// mesmo nome de spec é bug de observabilidade: em produção alguém lê o
	// errado e conclui que não há full scan. Este existe só para cruzar "plano
	// escolhido" × "plano executado" em teste.
	SourceFullScan atomic.Int64

	PointLookups atomic.Int64
	IndexScans   atomic.Int64

	// Candidates conta candidatos consumidos da fonte; Rejected, os que
	// estavam vivos e falharam o WHERE. Rejected > 0 em produção é a prova de
	// que a revalidação do §44 está fazendo trabalho de verdade.
	Candidates  atomic.Int64
	DeadSkipped atomic.Int64
	Rejected    atomic.Int64

	// TypeMismatch conta valores cujo tipo físico no payload difere do tipo
	// ligado na comparação — payload heterogêneo, que compara falso.
	TypeMismatch atomic.Int64

	Emitted   atomic.Int64
	Truncated atomic.Int64
	Canceled  atomic.Int64
}

// contadores é o acumulador local de uma execução. Um flush por query.
type contadores struct {
	candidates   int64
	deadSkipped  int64
	rejected     int64
	typeMismatch int64
	emitted      int64
}

func (c *contadores) flush(m *Metrics) {
	if m == nil {
		return
	}
	m.Candidates.Add(c.candidates)
	m.DeadSkipped.Add(c.deadSkipped)
	m.Rejected.Add(c.rejected)
	m.TypeMismatch.Add(c.typeMismatch)
	m.Emitted.Add(c.emitted)
	*c = contadores{}
}
