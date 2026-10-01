//go:build !race

package alarm

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// testingAllocsPerRun é o AllocsPerRun da stdlib, nomeado aqui para que o gate
// diga o que mede.
var testingAllocsPerRun = testing.AllocsPerRun

// TestPassadaNaoAlocaPorGrupoSaudavel é o gate da guarda que evita criar
// tracker para grupo que não satisfaz a condição.
//
// A guarda NÃO é barreira de comportamento — um tracker criado para um grupo
// saudável morreria no delete da mesma chamada, e o mapa ficaria do mesmo
// tamanho. O que ela evita é a ALOCAÇÃO, e é isso que importa: um contador por
// PON tem milhares de grupos saudáveis, e alocar um tracker para cada um a cada
// passada é lixo proporcional ao inventário, não ao problema. Só um gate de
// alocação prova essa guarda; foi uma mutação que mostrou que nenhum teste de
// comportamento poderia.
func TestPassadaNaoAlocaPorGrupoSaudavel(t *testing.T) { checkHealthyPassAllocs(t, allocsOfPass) }

// TestInterruptiblePassDoesNotAllocatePerHealthyGroup é o mesmo gate pelo
// caminho de produção: o laço passa o cancelamento do Stop à passada, e o
// teste do cancelamento por grupo precisa ficar fora da conta de alocação.
func TestInterruptiblePassDoesNotAllocatePerHealthyGroup(t *testing.T) {
	checkHealthyPassAllocs(t, allocsOfInterruptiblePass)
}

func checkHealthyPassAllocs(t *testing.T, allocs func(*testing.T, *Runner) float64) {
	t.Helper()
	gs := make([]grupo, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: 500, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	r, _, _ := runnerDeTeste(t, f, "")
	// Uma passada antes da medida, para que os buffers reusados já tenham
	// nascido: o gate mede o regime permanente, não a primeira vez.
	passada(t, r)
	base := allocs(t, r)
	if base != 0 {
		t.Fatalf("a passada alocou %v vez(es) por rodada sobre 64 grupos saudáveis", base)
	}
}

// TestSweepWithQualifyWhenDoesNotAllocatePerGroup é o mesmo gate com
// `qualifyWhen` declarado e as colunas das métricas no schema e na linha.
//
// Mede os DOIS casos dominantes do alarme qualificado: o grupo saudável (a
// condição falsa, e o qualifyWhen nem é avaliado) e o grupo com a condição
// valendo e inelegível — a OLT que nunca teve ninguém, que existe aos milhares
// num contador por PON e que, sem a guarda, ganharia tracker a cada passada.
func TestSweepWithQualifyWhenDoesNotAllocatePerGroup(t *testing.T) {
	checkQualifiedSweepAllocs(t, allocsOfPass)
}

// TestInterruptibleSweepWithQualifyWhenDoesNotAllocatePerGroup é o mesmo gate
// pelo caminho de produção, interrompível.
func TestInterruptibleSweepWithQualifyWhenDoesNotAllocatePerGroup(t *testing.T) {
	checkQualifiedSweepAllocs(t, allocsOfInterruptiblePass)
}

func checkQualifiedSweepAllocs(t *testing.T, allocs func(*testing.T, *Runner) float64) {
	t.Helper()
	metrics := defaultWindowMetrics()
	gs := make([]grupo, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	src := newFakeMetricSource(metrics)
	for id := uint32(1); id <= 64; id++ {
		src.setWindows(id, 30, 50)
	}
	r, _, _ := newQualifiedRunner(t, f, src, specWithMetrics(metrics, nearlyEmptyAlarm()), "", t0)
	passada(t, r)
	if n := allocs(t, r); n != 0 {
		t.Fatalf("a passada alocou %v vez(es) por rodada sobre 64 grupos saudáveis com qualifyWhen", n)
	}

	for id := uint32(1); id <= 64; id++ {
		src.setWindows(id, 0, 3)
	}
	passada(t, r)
	if n := allocs(t, r); n != 0 {
		t.Fatalf("a passada alocou %v vez(es) por rodada sobre 64 grupos com a condição valendo e inelegíveis", n)
	}
	if got := r.trackedGroups(); got != 0 {
		t.Fatalf("%d grupos inelegíveis em observação, quero 0", got)
	}
}

// TestPendingGroupsWithDeclaredValueDoNotAllocatePerPass prova só o que um
// gate de AllocsPerRun pode provar: zero alocação por rodada sobre 64 grupos
// pending — condição valendo, prazo não cumprido, sem transição — de um
// alarme com `value:` de métrica declarado.
//
// O que ele NÃO prova: que firedValueOf só roda na transição para o disparo.
// firedValueOf em si não aloca (o FieldReader é valor de pilha e não escapa —
// `go build -gcflags=-m` confirma), então um mutante que a chamasse em TODO
// grupo pending passaria por este gate calado; a razão de ela só rodar na
// transição é custo de CPU (não varrer o payload inteiro por grupo pending do
// inventário a cada passada) e congelamento do valor capturado — ver o
// godoc de Runner.decide e de tracker.firedAt —, não alocação. Este gate
// cobre uma coisa distinta e real: que o caminho pending com `value:`
// declarado não aloca — a contraparte do caminho saudável é
// TestPassadaNaoAlocaPorGrupoSaudavel.
func TestPendingGroupsWithDeclaredValueDoNotAllocatePerPass(t *testing.T) {
	checkPendingPassAllocs(t, allocsOfPass)
}

// TestInterruptiblePendingGroupsWithDeclaredValueDoNotAllocatePerPass é o
// mesmo gate pelo caminho de produção, interrompível.
func TestInterruptiblePendingGroupsWithDeclaredValueDoNotAllocatePerPass(t *testing.T) {
	checkPendingPassAllocs(t, allocsOfInterruptiblePass)
}

func checkPendingPassAllocs(t *testing.T, allocs func(*testing.T, *Runner) float64) {
	t.Helper()
	metrics := defaultWindowMetrics()
	gs := make([]grupo, 0, 64)
	for i := int64(0); i < 64; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: 50, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	src := newFakeMetricSource(metrics)
	for id := uint32(1); id <= 64; id++ {
		src.setWindows(id, 0.5, 50)
	}
	alarm := schema.AlarmSpec{Name: "olt_down", When: "total < 100", Value: "auth_5m_avg", SustainedFor: 10 * time.Minute}
	r, _, clock := newQualifiedRunner(t, f, src, specWithMetrics(metrics, alarm), "", t0)
	// Primeira passada: os 64 grupos entram em observação (Absent -> Pending)
	// — é TRANSIÇÃO, aloca por conta do mapa de trackers, e fica fora do que
	// o gate mede.
	passada(t, r)
	if got := r.trackedGroups(); got != 64 {
		t.Fatalf("%d grupos em observação, quero 64: o cenário não ficou pending para todos", got)
	}
	// Segunda passada antes da medida, para que os buffers reusados já
	// tenham nascido — o gate mede o regime permanente.
	clock.advance(time.Minute)
	passada(t, r)
	base := allocs(t, r)
	if base != 0 {
		t.Fatalf("a passada alocou %v vez(es) por rodada sobre 64 grupos pending com value: declarado", base)
	}
}

func allocsOfPass(t *testing.T, r *Runner) float64 {
	t.Helper()
	return testingAllocsPerRun(100, func() {
		if err := r.EvaluateOnce(); err != nil {
			t.Fatal(err)
		}
	})
}

// allocsOfInterruptiblePass mede a passada como o laço a chama, com um canal
// de Stop ainda aberto. O canal nasce fora da medida: o que se mede é o
// cancelamento observado por grupo, não a criação dele.
func allocsOfInterruptiblePass(t *testing.T, r *Runner) float64 {
	t.Helper()
	stop := make(chan struct{})
	return testingAllocsPerRun(100, func() {
		if err := r.evaluate(stop); err != nil {
			t.Fatal(err)
		}
	})
}
