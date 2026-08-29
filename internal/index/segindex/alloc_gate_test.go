//go:build !race

package segindex_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestSetNoWIPNaoAloca: é o caminho quente da ingestão, chamado uma vez por
// campo indexado por evento. internal/storage/ não tinha nenhum gate de
// alocação até aqui; este é o primeiro da família.
//
// # O que este gate mede, e o que ele NÃO afirma
//
// Ele mede o REGIME. A chamada que faz o índice CRESCER aloca por construção, e
// isso não é regressão nem defeito a consertar: crescer passa por
// mmap.Region.Grow, que chama unix.Mmap, que devolve uma slice nova e registra
// o mapeamento — alocação que não está sob controle deste pacote.
//
// É por isso que abreWIPDeTeste abre folgado (4.096 slots, capacidade de
// postings de sobra): as 1.001 chamadas medidas aqui não crescem nada, e o zero
// que sai é o custo do Set em si. Um WIP apertado mediria o mmap junto e o
// número diria outra coisa.
//
// A leitura correta do gate é "Set não aloca em regime", nunca "Set nunca
// aloca". Quem vir alocação numa medição de produção deve conferir primeiro se
// ela é proporcional aos CRESCIMENTOS — que são logarítmicos no tamanho do
// segmento — antes de tratar como regressão. O crescimento tem gate próprio, de
// corretude: TestOWIPCresceSemPerderPosting.
func TestSetNoWIPNaoAloca(t *testing.T) {
	w := abreWIPDeTeste(t)
	defer fechaWIP(t, w)
	var i int
	media := testing.AllocsPerRun(1000, func() {
		i++
		_ = w.Set(uint64(i%997)+2, types.RowID(i))
	})
	if media != 0 {
		t.Errorf("AllocsPerRun(Set) = %v, quero 0", media)
	}
}

// TestLookupNoWIPAlocaExatamenteUmaVez. A igualdade é deliberada: 0 significa
// que não materializou (e a ordem sai errada, porque a cadeia é percorrida ao
// contrário); 2 significa que o slice cresceu (e o tamanho exato veio da
// contagem errada). Os dois desvios são bugs diferentes, e um teto de "<= 1"
// esconderia o primeiro.
func TestLookupNoWIPAlocaExatamenteUmaVez(t *testing.T) {
	w := abreWIPDeTeste(t)
	defer fechaWIP(t, w)
	for i := range 100 {
		if err := w.Set(100, types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	media := testing.AllocsPerRun(1000, func() { _ = w.Lookup(100) })
	if media != 1 {
		t.Errorf("AllocsPerRun(Lookup) = %v, quero exatamente 1", media)
	}
}

// TestIterarSeladoNaoAlocaPorLinha: o resultado é fatia contígua do mmap, e
// alocação POR LINHA é sinal de cópia indevida. A medida é da ITERAÇÃO, não do
// Lookup: montar a fonte custa um boxing de interface por segmento, que é
// custo de abertura e não de varredura — medi-los juntos esconderia justamente
// a regressão que este gate existe para pegar.
func TestIterarSeladoNaoAlocaPorLinha(t *testing.T) {
	s := abreSeladoComNPostings(t, 1000)
	defer func() {
		if err := s.Fecha(); err != nil {
			t.Errorf("Fecha: %v", err)
		}
	}()
	it, ok := s.Lookup(7, 100)
	if !ok {
		t.Fatal("Lookup não encontrou")
	}
	// visitadas existe para que o gate não possa passar VAZIO: um Lookup que
	// devolvesse uma fonte de zero linhas mediria um laço que não roda, e
	// "AllocsPerRun = 0" viraria carimbo (débito 53). A contagem é lida depois
	// da medida — incrementá-la não aloca.
	visitadas := 0
	media := testing.AllocsPerRun(100, func() {
		for it.Next() {
			_ = it.RowID()
			visitadas++
		}
		it.(interface{ Rebobina() }).Rebobina()
	})
	if media != 0 {
		t.Errorf("AllocsPerRun = %v, quero 0", media)
	}
	if visitadas < 1000 {
		t.Errorf("a varredura medida visitou %d linhas ao todo — o gate mediu um laço vazio", visitadas)
	}
}
