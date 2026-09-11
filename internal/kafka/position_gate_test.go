//go:build !race

package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// O caminho quente do writer não aloca ao publicar a posição.
//
// avanca roda uma vez POR EVENTO DECIDIDO — ~500/s no piloto, 24/7 — e antes
// chamava famOffset.With(dataset, strconv.Itoa(p)). O Com monta a chave da
// série com strings.Join(valores, "\xff"), que é uma string nova a cada
// chamada, mais o slice variádico do variádico. Era alocação permanente no hot
// path de um projeto cuja doutrina é zero alocação ali, e nenhum gate a
// cobria: os de alocação medem PatchBuffer.Parse e Materializer.Apply, e o
// writer Kafka está fora dos dois.
//
// Medido antes da troca: 1 alocação por evento. A 450 ev/s, que é a taxa do
// piloto, são 450 allocs/s de pressão de GC permanente para publicar um
// inteiro que já estava em memória.
//
// O gate mede positionOf+Store, que é tudo o que avanca faz além do mapa de
// offsets. Medir avanca inteiro contaria a escrita no mapa, que é do writer e
// não da métrica.
func TestPublicarPosicaoNaoAloca(t *testing.T) {
	m := registerMetrics(nil, "eventos")
	// Warm-up: a criação do slot é uma vez por partição, não por evento, e é
	// ela que aloca de propósito.
	for p := int32(0); p < 4; p++ {
		m.positionOf(p).Store(0)
	}
	recs := []*kgo.Record{
		{Partition: 0, Offset: 10}, {Partition: 1, Offset: 11},
		{Partition: 2, Offset: 12}, {Partition: 3, Offset: 13},
	}
	if n := testing.AllocsPerRun(200, func() {
		for _, r := range recs {
			m.positionOf(r.Partition).Store(r.Offset + 1)
		}
	}); n != 0 {
		t.Fatalf("publicar a posição alocou %v vezes por evento; quer 0 — "+
			"a família de medidores voltou ao caminho quente", n)
	}
}
