package query_test

import (
	"context"
	"testing"
)

// Os benchmarks medem o executor sobre o dublê SEM alocação (payloads
// pré-montados): o que aparece é o custo do pipeline — fonte, coleta,
// predicado, projeção —, não o do dublê.
//
// Contra o fakestorage os números seriam outros (ele aloca em ScanLive e no
// lookup), e por isso ele não é usado aqui.

func benchVarre(b *testing.B, sql string, rows int) {
	h := newStubHarnessB(b, rows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := h.engine.Execute(context.Background(), "linhas", sql, nil)
		if err != nil {
			b.Fatal(err)
		}
		for it.Next() {
		}
		it.Close()
	}
}

func BenchmarkFullScan1k(b *testing.B) {
	benchVarre(b, `SELECT id, nome, n FROM linhas WHERE nome = 'linha'`, 1000)
}

func BenchmarkFullScanSemWhere1k(b *testing.B) {
	benchVarre(b, `SELECT id FROM linhas`, 1000)
}

// LIMIT sobre um dataset grande: mede o early-exit. Se o custo crescer com o
// tamanho do dataset, a interrupção não está acontecendo.
func BenchmarkScanLimit100(b *testing.B) {
	benchVarre(b, `SELECT id FROM linhas LIMIT 100`, 100_000)
}

func BenchmarkPointLookup(b *testing.B) {
	benchVarre(b, `SELECT id, nome FROM linhas WHERE id = 'K00500'`, 1000)
}

// Predicado com muitos termos: mede o programa linear do predicado, que é o
// que substituiu a avaliação recursiva por row.
func BenchmarkPredicadoDeMuitosTermos(b *testing.B) {
	sql := `SELECT id FROM linhas WHERE nome = 'linha'`
	for i := 0; i < 32; i++ {
		sql += ` AND n >= 0`
	}
	benchVarre(b, sql, 1000)
}
