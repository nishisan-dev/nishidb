//go:build !race

package query_test

import (
	"context"
	"testing"
)

// O gate que define o projeto: a memória do executor é O(batch), NUNCA
// O(resultado).
//
// A medição é por IGUALDADE entre dois tamanhos de entrada, e não por um teto.
// Um teto esconderia alocação por row atrás de folga; a igualdade prova que as
// 7 mil rows a mais não custaram nenhuma alocação — só o custo frio do Open,
// que é o mesmo nos dois lados.
//
// Se este teste falhar com números próximos mas diferentes, procure alocação
// por ROW (crescimento de buffer, string(), boxing em interface); se falhar com
// números muito diferentes, alguém materializou o resultado.
// tetoPorQuery é o valor MEDIDO do custo fixo de uma query (Open: slots,
// colunas, programa do predicado, fonte). Colado, sem folga, pela convenção do
// projeto: um teto folgado não acusa nem uma duplicação do custo.
const tetoPorQuery = 38

func TestScanAllocsIndependenteDoNumeroDeRows(t *testing.T) {
	const sql = `SELECT id, nome, n FROM linhas WHERE nome = 'linha' AND n >= 0`

	varre := func(rows int) func() {
		h := novoStubHarness(t, rows, 1_000_000, "30s")
		return func() {
			it := h.abre(context.Background(), sql)
			for it.Next() {
			}
			if err := it.Err(); err != nil {
				t.Fatalf("varredura de %d rows: %v", rows, err)
			}
			it.Close()
		}
	}

	mil := testing.AllocsPerRun(20, varre(1_000))
	oitoMil := testing.AllocsPerRun(20, varre(8_000))

	// A igualdade prova que não há alocação por ROW; o TETO prova que o custo
	// fixo por query não cresceu. Sem ele, +71 allocs constantes passariam
	// despercebidas — a igualdade continuaria valendo, porque elas são
	// constantes.
	if mil > tetoPorQuery {
		t.Errorf("allocs/query = %.0f, teto %d: o custo fixo do Open cresceu", mil, tetoPorQuery)
	}
	if mil != oitoMil {
		t.Errorf("allocs/query = %.0f com 1.000 rows e %.0f com 8.000: "+
			"a diferença é alocação POR ROW (%.4f por row)",
			mil, oitoMil, (oitoMil-mil)/7000)
	}
	t.Logf("allocs por query (constante, independente do resultado): %.0f", mil)
}

// O mesmo, com LIMIT: o caminho de early-exit não pode alocar por row nem
// vazar o iterador da fonte.
func TestLimitAllocsIndependenteDoNumeroDeRows(t *testing.T) {
	const sql = `SELECT id FROM linhas LIMIT 10`

	varre := func(rows int) func() {
		h := novoStubHarness(t, rows, 1_000_000, "30s")
		return func() {
			it := h.abre(context.Background(), sql)
			for it.Next() {
			}
			it.Close()
		}
	}
	if mil, oitoMil := testing.AllocsPerRun(20, varre(1_000)), testing.AllocsPerRun(20, varre(8_000)); mil != oitoMil {
		t.Errorf("allocs/query = %.0f vs %.0f", mil, oitoMil)
	}
}
