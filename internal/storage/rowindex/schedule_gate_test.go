//go:build !race

package rowindex

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// A agenda não aloca no caminho quente. Nada, nunca.
//
// POR QUE ELE EXISTE, e por que agora. `Update` roda em TODA escrita — é o
// que substituiu a varredura de TTL —, e `internal/storage` não tinha um único
// gate de alocação. A ausência não era neutra: foi exatamente assim que a
// alocação por evento do `Writer.advance` do Kafka atravessou o M2 inteiro sem
// ser vista, e só apareceu quando alguém foi olhar com profile na mão. Uma
// alocação por evento aqui é pressão de GC permanente numa ingestão 24/7.
//
// AS TRÊS ROTAS de `Update` são medidas separadamente porque elas percorrem
// código diferente, e a mais barata esconderia as outras:
//
//   - RENOVAÇÃO (subindo): o caso comum em regime — cada escrita empurra o
//     vencimento para a frente. Uma escrita de folha e, quase sempre, nenhum
//     recompute.
//   - ANTECIPAÇÃO (abaixando): escreve os ancestrais do topo para baixo e a
//     folha por último, que é a ordem que mantém o nó como limite INFERIOR.
//   - LINHA NOVA: entra vinda de ∞, o que é sempre um abaixamento e alcança
//     todos os níveis.
//
// A leitura entra junto porque é ela que o coletor chama a cada volta, e ela
// desce a árvore com um callback — a forma mais fácil de fazer um closure
// escapar para a heap sem ninguém notar.
func TestAgendaNaoAlocaNoCaminhoQuente(t *testing.T) {
	const rows = 1 << 16
	a := openSchedule(t, rows)

	// Popula fora da medição: a primeira escrita de cada bloco cresce a
	// região em mmap, e isso é I/O, não caminho quente.
	for r := types.RowID(0); r < rows; r++ {
		if err := a.Update(r, uint64(1_000_000+r)); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("renovacao", func(t *testing.T) {
		expiry := uint64(2_000_000)
		var r types.RowID
		if n := testing.AllocsPerRun(1000, func() {
			r = (r + 1) & (rows - 1)
			expiry++
			if err := a.Update(r, expiry); err != nil {
				t.Fatal(err)
			}
		}); n != 0 {
			t.Errorf("allocs/renovação = %.1f, quer 0", n)
		}
	})

	t.Run("antecipacao", func(t *testing.T) {
		expiry := uint64(1_000_000)
		var r types.RowID
		if n := testing.AllocsPerRun(1000, func() {
			r = (r + 1) & (rows - 1)
			expiry--
			if err := a.Update(r, expiry); err != nil {
				t.Fatal(err)
			}
		}); n != 0 {
			t.Errorf("allocs/antecipação = %.1f, quer 0", n)
		}
	})

	t.Run("leitura do topo", func(t *testing.T) {
		if n := testing.AllocsPerRun(1000, func() {
			if _, ok := a.NextExpiry(); !ok {
				t.Fatal("NextExpiry não achou nada numa agenda populada")
			}
		}); n != 0 {
			t.Errorf("allocs/NextExpiry = %.1f, quer 0", n)
		}
	})

	t.Run("varredura dos vencidos", func(t *testing.T) {
		// `cutoff` fica logo acima do menor vencimento: a descida encontra
		// poucas linhas, que é o formato da passada real do coletor.
		minExpiry, ok := a.NextExpiry()
		if !ok {
			t.Fatal("agenda vazia")
		}
		if n := testing.AllocsPerRun(300, func() {
			a.VisitExpired(minExpiry, func(types.RowID) bool { return true })
		}); n != 0 {
			t.Errorf("allocs/varredura = %.1f, quer 0", n)
		}
	})
}
