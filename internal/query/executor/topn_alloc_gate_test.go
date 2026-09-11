//go:build !race

package executor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
)

// TestTopNReaproveitaOBufferDaEntradaDespejada é o gate que impede a
// "simplificação" que mataria o modo.
//
// Sem reuso, cada linha ACEITA pelo heap pediria um buffer novo, e a memória do
// operador cresceria com o número de linhas VARRIDAS em vez de com N — o modo
// deixaria de ser limitado, que é a única razão de ele existir. O caminho
// completo já cresce assim de propósito, e é justamente disso que o top-N
// existe para fugir.
//
// A medida é por IGUALDADE entre dois tamanhos de corpus, e não por um teto:
// um teto esconderia alocação por linha atrás de folga. A igualdade prova que
// as 2.800 linhas a mais não custaram nenhuma alocação — só o custo frio do
// Open e do heap, que é o mesmo nos dois lados porque N é o mesmo.
//
// O corpus é o PIOR CASO de propósito: `nome` decrescente na ordem de
// varredura faz toda linha nova ser melhor que a raiz, então toda linha é
// aceita e despeja outra. Num corpus favorável quase nada entraria no heap
// depois do aquecimento e o gate mediria o nada.
//
// Se falhar com números próximos mas diferentes, o buffer voltou a ser alocado
// por aceitação.
func TestTopNReaproveitaOBufferDaEntradaDespejada(t *testing.T) {
	c := compileSchemaTopN(t)
	keys := []binder.BoundOrderKey{keyName(t, c, false, false), keyN(t, c, false)}

	varre := func(rows int) func() {
		corpus := make([]rowTopN, 0, rows)
		for i := 0; i < rows; i++ {
			corpus = append(corpus, rowTopN{
				id:      fmt.Sprintf("k%06d", i),
				name:    fmt.Sprintf("g%06d", rows-i),
				hasName: true,
				n:       int64(i % 7),
			})
		}
		st := seedTopN(t, c, corpus)
		bq := bqTopN(t, c, keys, 32, 0)
		return func() {
			it, err := executor.Open(context.Background(), bq, planner.FullScan(),
				executor.Options{Store: st})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			for it.Next() {
			}
			if err := it.Err(); err != nil {
				t.Fatalf("Err: %v", err)
			}
			if err := it.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}
	}

	poucas := testing.AllocsPerRun(20, varre(400))
	muitas := testing.AllocsPerRun(20, varre(3_200))
	if poucas != muitas {
		t.Errorf("allocs/query = %.0f com 400 linhas e %.0f com 3.200: "+
			"o top-N aloca por linha ACEITA (%.4f por linha) — o buffer da entrada "+
			"despejada não está sendo reaproveitado", poucas, muitas, (muitas-poucas)/2800)
	}
	t.Logf("allocs por query no top-N (constante, independente do varrido): %.0f", poucas)
}
