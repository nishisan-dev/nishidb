package planner_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// ceilingConjuncts é o teto de alocação do plan-time para uma query de 4
// conjuncts. O número é O(#conjuncts) — NUNCA função do tamanho do dataset.
const ceilingConjuncts = 12

// fourConjuncts: 2 bitmaps + 1 hash + 1 não indexado. O hash vence, mas o
// planner classifica TODOS os conjuncts antes de decidir — é o caso caro.
func fourConjuncts(tb testing.TB, c *schema.Compiled) binder.BoundExpr {
	tb.Helper()
	return and(
		and(
			and(eq(field(tb, c, "Acct-Status-Type"), str("Stop")),
				eq(field(tb, c, "Framed-Protocol"), str("PPPoE"))),
			eq(field(tb, c, "NAS-IP-Address"), ipv4(tb, "10.0.0.1")),
		),
		eq(field(tb, c, "User-Name"), str("ze")),
	)
}

func newPlanner(tb testing.TB) (*planner.Planner, *schema.Compiled) {
	tb.Helper()
	c := compile(tb)
	return planner.New(allIndexes(tb, c), &planner.Metrics{}), c
}

// O caminho sem WHERE é o mais comum em teste e no CLI, e precisa ser
// literalmente grátis: o singleton existe para isso.
func TestGateAlocacaoSemWhere(t *testing.T) {
	p, _ := newPlanner(t)
	bq := &binder.BoundQuery{Dataset: "radius_sessions"}
	if got := testing.AllocsPerRun(1000, func() { sink = p.Plan(bq) }); got != 0 {
		t.Errorf("Plan sem WHERE = %.1f allocs/op, want 0 (singleton FullScan)", got)
	}
}

// Esta query casa a regra do HASH, e por isso ela vigia a lista de conjuncts,
// a de termos de igualdade e o plano — e NÃO o dedup: a R2 retorna antes do
// laço de bitmap, único chamador de appendDedup no caminho AND. Quem vigia o
// dedup são os dois gates abaixo, sobre interseção e união.
//
// (Verificado por mutação: com appendDedup em panic, este gate passa.)
func TestGateAlocacaoQuatroConjuncts(t *testing.T) {
	p, c := newPlanner(t)
	bq := query(fourConjuncts(t, c))
	if got := testing.AllocsPerRun(1000, func() { sink = p.Plan(bq) }); got > ceilingConjuncts {
		t.Errorf("Plan com 4 conjuncts = %.0f allocs/op, teto %d", got, ceilingConjuncts)
	}
}

// Os gates que DE FATO exercitam o appendDedup, com o teto no valor MEDIDO —
// sem folga. Um teto de 12 sobre 6 medidos não acusaria nem uma duplicação do
// custo; com o teto colado, trocar a varredura linear do dedup por um map
// (7 allocs na interseção de 2 termos, 11 na de 3) falha na hora.
const (
	ceilingIntersectTwo   = 6
	ceilingIntersectThree = 8
	ceilingUnionTwo       = 6
	ceilingUnionThree     = 7
)

func TestGateAlocacaoIntersecao(t *testing.T) {
	p, c := newPlanner(t)
	st, pr, tc := field(t, c, "Acct-Status-Type"), field(t, c, "Framed-Protocol"),
		field(t, c, "Acct-Terminate-Cause")

	two := query(and(eq(st, str("Stop")), eq(pr, str("PPPoE"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(two) }); got > ceilingIntersectTwo {
		t.Errorf("interseção de 2 termos = %.0f allocs, teto %d", got, ceilingIntersectTwo)
	}
	three := query(and(and(eq(st, str("Stop")), eq(pr, str("PPPoE"))), eq(tc, str("X"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(three) }); got > ceilingIntersectThree {
		t.Errorf("interseção de 3 termos = %.0f allocs, teto %d", got, ceilingIntersectThree)
	}
}

func TestGateAlocacaoUniao(t *testing.T) {
	p, c := newPlanner(t)
	st, pr, tc := field(t, c, "Acct-Status-Type"), field(t, c, "Framed-Protocol"),
		field(t, c, "Acct-Terminate-Cause")

	two := query(or(eq(st, str("Stop")), eq(pr, str("PPPoE"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(two) }); got > ceilingUnionTwo {
		t.Errorf("união de 2 termos = %.0f allocs, teto %d", got, ceilingUnionTwo)
	}
	three := query(or(or(eq(st, str("Stop")), eq(pr, str("PPPoE"))), eq(tc, str("X"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(three) }); got > ceilingUnionThree {
		t.Errorf("união de 3 termos = %.0f allocs, teto %d", got, ceilingUnionThree)
	}
}

var sink planner.Plan

func BenchmarkPlanSemWhere(b *testing.B) {
	p, _ := newPlanner(b)
	bq := &binder.BoundQuery{Dataset: "radius_sessions"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}

func BenchmarkPlanPointLookup(b *testing.B) {
	p, c := newPlanner(b)
	bq := query(eq(field(b, c, "id"), str("CMP-1")))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}

func BenchmarkPlanBitmapIntersect(b *testing.B) {
	p, c := newPlanner(b)
	bq := query(and(eq(field(b, c, "Acct-Status-Type"), str("Stop")),
		eq(field(b, c, "Framed-Protocol"), str("PPPoE"))))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}

func BenchmarkPlanQuatroConjuncts(b *testing.B) {
	p, c := newPlanner(b)
	bq := query(fourConjuncts(b, c))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = p.Plan(bq)
	}
}
