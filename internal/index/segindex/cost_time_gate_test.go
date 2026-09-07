//go:build !race

package segindex_test

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// A SEGUNDA camada do gate de custo da §6.1. A primeira
// (TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento) conta acessos ao mapeamento e
// prova a FORMA do algoritmo — mas só enxerga o que passa pelo contador. Esta
// pega a regressão cujo custo CRESCE COM O TAMANHO do segmento por um caminho
// que o contador não vê: um crescimento que deixou de amortizar, uma varredura
// proporcional à cadeia escondida atrás de um acesso só.
//
// O QUE ELA NÃO PEGA, por construção — e a primeira versão deste texto
// afirmava o contrário: regressão de FATOR CONSTANTE. Um lock por acesso, uma
// syscall por Set (a classe do débito 102) multiplicam o ns/op das DUAS
// escalas quase igualmente, e a razão os cancela — pelo mesmo argumento que a
// torna imune à dispersão da máquina. Fator constante é caçado pelo
// bench-micro contra a baseline versionada, que compara valores absolutos
// entre commits na mesma máquina; aqui seria um teto absoluto de ns/op, que é
// exatamente o ratchet por máquina que este gate existe para dispensar.
//
// A medida é uma RAZÃO entre duas escalas do mesmo processo, e não um teto
// absoluto de ns/op: a dispersão da máquina afeta as duas medições quase
// igualmente e se cancela na divisão, que é o que torna o teto estável sem
// ratchet por máquina. Set amortizado O(1) mantém a razão perto de 1; o motor
// antigo (varredura da cadeia por Set) daria razão ~100 — o teto de 8 tem uma
// ordem de grandeza de folga para os dois lados.
//
// O mínimo de três rodadas por escala, e não a média, pela regra que as
// baselines deste repositório já seguem: no laptop a dispersão chega a 27%, e
// o mínimo é a medida menos contaminada por ruído de runner.
//
// O LIMITE HONESTO do gate, medido por mutação nas duas formas: um custo
// proporcional à cadeia com passo /64 reprova (razão 11,1); com passo /1024 —
// coeficiente linear pequeno demais para dominar o ns/op de 1M — passa por
// baixo do teto (razão 2,6). O gate fecha a forma DOMINANTE da regressão (a
// varredura de cadeia do motor antigo, razão ~100); coeficiente miúdo é caçado
// pelo bench-micro contra a baseline, não por aqui.
func TestARazaoDeTempoDoSetEntreEscalasEhConstante(t *testing.T) {
	const (
		nSmall  = 10_000
		nLarge  = 1_000_000
		ceiling = 8.0
		rounds  = 3
	)
	measure := func(n int) float64 {
		best := time.Duration(1<<63 - 1)
		for range rounds {
			w := openTestWIP(t)
			start := time.Now()
			for i := range n {
				if err := w.Set(100, types.RowID(i)); err != nil {
					t.Fatalf("Set #%d: %v", i, err)
				}
			}
			elapsed := time.Since(start)
			closeWIP(t, w)
			best = min(best, elapsed)
		}
		return float64(best.Nanoseconds()) / float64(n)
	}
	small := measure(nSmall)
	large := measure(nLarge)
	if small <= 0 {
		t.Fatalf("medição do n pequeno deu %v ns/op: relógio sem resolução para o gate", small)
	}
	ratio := large / small
	if ratio > ceiling {
		t.Errorf("ns/op(1M)/ns/op(10k) = %.2f (%.0f/%.0f ns), teto %.0f — o custo por Set cresceu com o segmento "+
			"por um caminho que o contador de acessos não vê", ratio, large, small, ceiling)
	}
}
