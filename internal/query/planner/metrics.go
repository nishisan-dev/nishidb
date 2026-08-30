package planner

import "sync/atomic"

// Metrics são os contadores de plano do dataset. Só FullScan é NORMATIVO
// (spec §64: `query.full_scan`); os demais existem para observabilidade e para
// os micro-benchmarks. Ponteiro nil é aceito em toda a API.
//
// Este é o ÚNICO contador de `query.full_scan` do projeto. O executor tem um
// contador de diagnóstico da fonte de candidatos, explicitamente NÃO
// normativo: dois contadores com o mesmo nome de spec é bug de
// observabilidade — em produção alguém lê o errado e conclui que não há full
// scan.
//
// A contagem acontece no PLAN-TIME, dentro de Plan(). É correto na v1 porque
// não há cache de planos nem prepared statements: todo plano produzido é
// executado em seguida. Se um cache de planos for introduzido, a contagem
// migra para o executor.
// O contador do plano de hash saiu junto com o tipo de plano que ele contava:
// um contador sem produtor é uma série que nasce em zero e ali fica, e em
// produção alguém a lê como "o índice hash não está sendo usado" em vez de "não
// existe índice hash". IndexScan conta o que BitmapScan contava — nenhuma
// config produzia plano de hash desde a D5, então a série não muda de valor.
type Metrics struct {
	FullScan       atomic.Int64 // §64 — query.full_scan
	PointLookup    atomic.Int64
	IndexScan      atomic.Int64
	IndexIntersect atomic.Int64
	IndexUnion     atomic.Int64
}
