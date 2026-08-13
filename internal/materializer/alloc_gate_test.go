//go:build !race

package materializer

import (
	"runtime"
	"testing"
)

// allocTolerance é o teto ABSOLUTO de alocações em toda a janela medida.
//
// Não é zero estrito: entre bancadas, o runtime produz 1 malloc esporádico
// (~32 B) em uma execução a cada dezenas, e um gate em zero absoluto seria
// flaky por ruído que não é do código. Com 100 mil eventos na janela, 8
// alocações continuam sendo 0,00008 por evento — qualquer regressão real
// (mesmo 1 alocação a cada 50 eventos, que são 2.000) estoura por duas ordens
// de grandeza.
const allocTolerance = 8

// allocRuns é o tamanho da janela. Precisa ser grande justamente porque o
// alvo é alocação ESPORÁDICA: um caminho raro (crescimento de buffer, campo
// novo no dicionário) não aparece em 100 iterações.
const allocRuns = 100_000

// medirAllocs conta as alocações de N execuções com o total absoluto de
// runtime.MemStats.
//
// Não usa testing.AllocsPerRun de propósito: ele faz DIVISÃO INTEIRA
// (mallocs/runs) e devolve 0,0 para qualquer taxa abaixo de 1 alocação por
// execução. Uma alocação a cada 50 eventos — 160 mil na rodada de 8M —
// passaria com o gate verde, e aumentar runs não corrige: a razão continua
// menor que 1 e a divisão continua zerando.
func medirAllocs(tb testing.TB, runs int, f func()) uint64 {
	tb.Helper()
	// GOMAXPROCS(1) evita que o GC concorrente mexa nos contadores no meio da
	// janela.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	for i := 0; i < 200; i++ { // warm-up: dicionário e buffers em regime
		f()
	}
	runtime.GC()
	var antes, depois runtime.MemStats
	runtime.ReadMemStats(&antes)
	for i := 0; i < runs; i++ {
		f()
	}
	runtime.ReadMemStats(&depois)
	return depois.Mallocs - antes.Mallocs
}

// Gate executável na suíte (não só no `make bench`): o Apply não pode alocar
// em steady state. Sem ele, uma alocação por evento entraria calada e só
// apareceria na rodada de 8M — quando o custo de descobrir já é alto.
//
// Roda contra o nullStore porque o alvo da medição é o Apply: o dublê
// completo aloca por construção e o gate viraria uma medida do dublê.
//
// Fica fora do build com -race: o detector instrumenta o runtime e a contagem
// de alocações deixa de ser a do binário real.
func TestApplyNaoAlocaEmSteadyState(t *testing.T) {
	c := benchCompiled(t)

	casos := []struct {
		nome  string
		m     *Materializer
		event []byte
		want  Action
	}{
		{"create", newNullMaterializer(t, c, false, 0), benchRadiusEvent, ActionCreated},
		{"merge", newNullMaterializer(t, c, true, 1), benchRadiusPatch, ActionUpdated},
		{"stale", newNullMaterializer(t, c, true, 1<<40), benchRadiusPatch, ActionStale},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			got := medirAllocs(t, allocRuns, func() {
				if r := tc.m.Apply(tc.event); r.Action != tc.want {
					t.Fatalf("Action = %v (%v), want %v", r.Action, r.Err, tc.want)
				}
			})
			if got > allocTolerance {
				t.Errorf("Apply alocou %d objetos em %d eventos (%.5f/evento), tolerância %d no total",
					got, allocRuns, float64(got)/allocRuns, allocTolerance)
			}
		})
	}
}

// O merge também é medido isolado: o gate acima cobre o Apply inteiro, este
// falha apontando direto para o merge quando a regressão vier de lá.
func TestMergeNaoAloca(t *testing.T) {
	c := benchCompiled(t)
	state, _ := benchState(t, c)
	patch := benchPatchEntries(t, c, benchRadiusPatch)
	out := make([]byte, 0, 4096)

	got := medirAllocs(t, allocRuns, func() {
		var err error
		out, err = mergeRecords(state, patch, out[:0])
		if err != nil {
			t.Fatal(err)
		}
	})
	if got > allocTolerance {
		t.Errorf("mergeRecords alocou %d objetos em %d execuções, tolerância %d",
			got, allocRuns, allocTolerance)
	}
}
