package counter

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// transitionRange transiciona as linhas [from, to) alternando entre os
// payloads dados — poucos grupos, para que o que cresce seja o overlay.
func transitionRange(t *testing.T, c *Counter, from, to int, ps [][]byte) {
	t.Helper()
	for r := from; r < to; r++ {
		if err := c.Transition(types.RowID(r), ps[r%len(ps)], false); err != nil {
			t.Fatal(err)
		}
	}
}

func twoGroupPayloads(t *testing.T) [][]byte {
	t.Helper()
	return [][]byte{payload(t, "A", 1, 1, true), payload(t, "B", 1, 1, false)}
}

// flipped devolve os dois payloads na ordem da rodada: alternar a ordem entre
// rodadas faz TODA linha trocar de grupo, e todo dreno ter o tamanho inteiro
// do intervalo.
func flipped(ps [][]byte, round int) [][]byte {
	if round%2 == 0 {
		return ps
	}
	return [][]byte{ps[1], ps[0]}
}

// TestSmallOverlayDrainKeepsTheMap: abaixo do limiar, o ApplyOverlay só
// esvazia o mapa — a troca seria churn sem memória nenhuma a devolver, e o
// mapa que o caminho quente reaproveita é o mesmo.
func TestSmallOverlayDrainKeepsTheMap(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	ps := twoGroupPayloads(t)
	before := c.overlayIdentityForTest()
	for round := 0; round < 3; round++ {
		transitionRange(t, c, 0, 1000, flipped(ps, round))
		if err := c.ApplyOverlay(nil); err != nil {
			t.Fatal(err)
		}
		if got := c.overlayIdentityForTest(); got != before {
			t.Fatalf("rodada %d: o dreno de 1000 entradas (limiar %d) trocou o mapa do overlay", round, OverlayShrinkThreshold)
		}
	}
}

// TestOverlayMapIsReplacedOnlyByADrainFarAboveTheSteadyState é a histerese,
// nas duas pontas, com um limiar pequeno para não precisar de centenas de
// milhares de transições por rodada:
//
//   - em regime ACIMA do limiar (200 entradas por checkpoint, limiar 64), o
//     mapa não é trocado a cada checkpoint — só nos primeiros, enquanto a média
//     móvel ainda não chegou ao regime;
//   - um dreno muito acima do regime (a construção, 4.000 entradas) troca.
func TestOverlayMapIsReplacedOnlyByADrainFarAboveTheSteadyState(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	c.overlayShrinkThreshold = 64
	ps := twoGroupPayloads(t)
	swaps := 0
	for round := 0; round < 8; round++ {
		before := c.overlayIdentityForTest()
		transitionRange(t, c, 0, 200, flipped(ps, round))
		if err := c.ApplyOverlay(nil); err != nil {
			t.Fatal(err)
		}
		if c.overlayIdentityForTest() != before {
			swaps++
			if round >= 2 {
				t.Fatalf("rodada %d de regime (200 entradas, média %d) trocou o mapa: a troca a cada checkpoint é churn", round, c.drainAvg)
			}
		}
	}
	if swaps == 0 {
		t.Fatal("nenhuma troca nem no primeiro dreno acima do limiar, com a média ainda zerada")
	}
	before := c.overlayIdentityForTest()
	transitionRange(t, c, 1000, 5000, ps)
	if err := c.ApplyOverlay(nil); err != nil {
		t.Fatal(err)
	}
	if c.overlayIdentityForTest() == before {
		t.Fatalf("o dreno de 4000 entradas sobre um regime de 200 (média %d) não trocou o mapa", c.drainAvg)
	}
}

// TestFailedOverlayDrainKeepsTheMap: o ApplyOverlay que falha no meio não
// troca o mapa — as entradas não aplicadas continuam nele para o próximo
// checkpoint, e trocá-lo as perderia.
func TestFailedOverlayDrainKeepsTheMap(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	c.overlayShrinkThreshold = 64
	transitionRange(t, c, 0, 500, twoGroupPayloads(t))
	if err := c.vector.Close(); err != nil { // Set num vetor fechado falha
		t.Fatal(err)
	}
	before := c.overlayIdentityForTest()
	if err := c.ApplyOverlay(nil); err == nil {
		t.Fatal("o ApplyOverlay sobre o vetor fechado tinha de falhar")
	}
	if c.overlayIdentityForTest() != before || c.OverlayLen() == 0 {
		t.Fatalf("o dreno que falhou trocou ou esvaziou o mapa: %d entradas restantes", c.OverlayLen())
	}
}
