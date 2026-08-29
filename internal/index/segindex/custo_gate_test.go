package segindex_test

import (
	"fmt"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Este arquivo NÃO tem build tag, e a ausência é a decisão: o gate abaixo mede
// SONDAS, que a instrumentação do detector de corrida não altera. Ele roda nas
// duas rodadas, e por isso não existe (nem faria sentido existir) uma
// contraparte `//go:build race` que se anuncie como SKIP — nada aqui é pulado.
// Os gates de ALOCAÇÃO deste pacote, que a instrumentação de fato altera, são
// os que têm contraparte nominal, em alloc_gate_race_test.go (débito 53).

// TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento é o gate que este pacote
// inteiro existe para satisfazer. O motor antigo varria a cadeia de probe
// inteira a cada Set para não duplicar, então inserir a N-ésima posting de um
// valor custava N sondas e inserir n postings custava O(n²). No piloto isso deu
// 1,5 ms por Set e 82% do CPU do processo.
//
// A medida é de SONDAS e não de tempo: sem relógio não há ruído de runner, e
// o gate afirma a FORMA do algoritmo em vez de um orçamento que precisa ser
// recalibrado a cada máquina.
//
// Todas as postings são do MESMO valor de propósito — é o caso que derrubou o
// piloto (node: ~3.724 postings por valor).
func TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento(t *testing.T) {
	const tetoDeSondas = 4 // amortizado; o diretório é dimensionado por DISTINTOS
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			w := abreWIPDeTeste(t)
			defer fechaWIP(t, w)
			var pior int
			for i := range n {
				if err := w.Set(100, types.RowID(i)); err != nil {
					t.Fatalf("Set #%d: %v", i, err)
				}
				pior = max(pior, w.SondasDoUltimoSet())
			}
			if pior > tetoDeSondas {
				t.Errorf("pior caso de %d sondas com n=%d, teto %d — a cadeia voltou a crescer com o segmento",
					pior, n, tetoDeSondas)
			}
		})
	}
}
