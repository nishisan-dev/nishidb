package planner_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// tetoConjuncts é o teto de alocação do plan-time para uma query de 4
// conjuncts. O número é O(#conjuncts) — NUNCA função do tamanho do dataset.
const tetoConjuncts = 12

// quatroConjuncts: 2 bitmaps + 1 hash + 1 não indexado. O hash vence, mas o
// planner classifica TODOS os conjuncts antes de decidir — é o caso caro.
func quatroConjuncts(tb testing.TB, c *schema.Compiled) binder.BoundExpr {
	tb.Helper()
	return and(
		and(
			and(eq(campo(tb, c, "Acct-Status-Type"), str("Stop")),
				eq(campo(tb, c, "Framed-Protocol"), str("PPPoE"))),
			eq(campo(tb, c, "NAS-IP-Address"), ipv4(tb, "10.0.0.1")),
		),
		eq(campo(tb, c, "User-Name"), str("ze")),
	)
}

func planejador(tb testing.TB) (*planner.Planner, *schema.Compiled) {
	tb.Helper()
	c := compilar(tb)
	return planner.New(todosOsIndices(tb, c), &planner.Metrics{}), c
}

// O caminho sem WHERE é o mais comum em teste e no CLI, e precisa ser
// literalmente grátis: o singleton existe para isso.
func TestGateAlocacaoSemWhere(t *testing.T) {
	p, _ := planejador(t)
	bq := &binder.BoundQuery{Dataset: "radius_sessions"}
	if got := testing.AllocsPerRun(1000, func() { sink = p.Plan(bq) }); got != 0 {
		t.Errorf("Plan sem WHERE = %.1f allocs/op, want 0 (singleton FullScan)", got)
	}
}

// A lista de conjuncts, a de termos de igualdade, a de termos indexados e o
// plano. Se este gate subir, provavelmente alguém trocou uma varredura linear
// por um map — que alocaria por consulta e ainda daria ordem não-determinística
// nos termos.
func TestGateAlocacaoQuatroConjuncts(t *testing.T) {
	p, c := planejador(t)
	bq := consulta(quatroConjuncts(t, c))
	if got := testing.AllocsPerRun(1000, func() { sink = p.Plan(bq) }); got > tetoConjuncts {
		t.Errorf("Plan com 4 conjuncts = %.0f allocs/op, teto %d", got, tetoConjuncts)
	}
}

var sink planner.Plan

func BenchmarkPlanSemWhere(b *testing.B) {
	p, _ := planejador(b)
	bq := &binder.BoundQuery{Dataset: "radius_sessions"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}

func BenchmarkPlanPointLookup(b *testing.B) {
	p, c := planejador(b)
	bq := consulta(eq(campo(b, c, "id"), str("CMP-1")))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}

func BenchmarkPlanBitmapIntersect(b *testing.B) {
	p, c := planejador(b)
	bq := consulta(and(eq(campo(b, c, "Acct-Status-Type"), str("Stop")),
		eq(campo(b, c, "Framed-Protocol"), str("PPPoE"))))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}

func BenchmarkPlanQuatroConjuncts(b *testing.B) {
	p, c := planejador(b)
	bq := consulta(quatroConjuncts(b, c))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}
