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

// Esta query casa a regra do HASH, e por isso ela vigia a lista de conjuncts,
// a de termos de igualdade e o plano — e NÃO o dedup: a R2 retorna antes do
// laço de bitmap, único chamador de appendDedup no caminho AND. Quem vigia o
// dedup são os dois gates abaixo, sobre interseção e união.
//
// (Verificado por mutação: com appendDedup em panic, este gate passa.)
func TestGateAlocacaoQuatroConjuncts(t *testing.T) {
	p, c := planejador(t)
	bq := consulta(quatroConjuncts(t, c))
	if got := testing.AllocsPerRun(1000, func() { sink = p.Plan(bq) }); got > tetoConjuncts {
		t.Errorf("Plan com 4 conjuncts = %.0f allocs/op, teto %d", got, tetoConjuncts)
	}
}

// Os gates que DE FATO exercitam o appendDedup, com o teto no valor MEDIDO —
// sem folga. Um teto de 12 sobre 6 medidos não acusaria nem uma duplicação do
// custo; com o teto colado, trocar a varredura linear do dedup por um map
// (7 allocs na interseção de 2 termos, 11 na de 3) falha na hora.
const (
	tetoIntersect2 = 6
	tetoIntersect3 = 8
	tetoUniao2     = 6
	tetoUniao3     = 7
)

func TestGateAlocacaoIntersecao(t *testing.T) {
	p, c := planejador(t)
	st, pr, tc := campo(t, c, "Acct-Status-Type"), campo(t, c, "Framed-Protocol"),
		campo(t, c, "Acct-Terminate-Cause")

	dois := consulta(and(eq(st, str("Stop")), eq(pr, str("PPPoE"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(dois) }); got > tetoIntersect2 {
		t.Errorf("interseção de 2 termos = %.0f allocs, teto %d", got, tetoIntersect2)
	}
	tres := consulta(and(and(eq(st, str("Stop")), eq(pr, str("PPPoE"))), eq(tc, str("X"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(tres) }); got > tetoIntersect3 {
		t.Errorf("interseção de 3 termos = %.0f allocs, teto %d", got, tetoIntersect3)
	}
}

func TestGateAlocacaoUniao(t *testing.T) {
	p, c := planejador(t)
	st, pr, tc := campo(t, c, "Acct-Status-Type"), campo(t, c, "Framed-Protocol"),
		campo(t, c, "Acct-Terminate-Cause")

	dois := consulta(or(eq(st, str("Stop")), eq(pr, str("PPPoE"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(dois) }); got > tetoUniao2 {
		t.Errorf("união de 2 termos = %.0f allocs, teto %d", got, tetoUniao2)
	}
	tres := consulta(or(or(eq(st, str("Stop")), eq(pr, str("PPPoE"))), eq(tc, str("X"))))
	if got := testing.AllocsPerRun(500, func() { sink = p.Plan(tres) }); got > tetoUniao3 {
		t.Errorf("união de 3 termos = %.0f allocs, teto %d", got, tetoUniao3)
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
