package hash

import (
	"slices"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestLookupConcorrenteComSetEReconstrucao ataca o ponto mais perigoso do
// arquivo: a reconstrução desmapeia a tabela ANTIGA enquanto pode haver
// leitores dentro dela.
//
// Um Munmap alcançando um Lookup em voo é SIGSEGV — falha incapturável, que
// derruba o processo inteiro e que a política de log-and-skip da ingestão não
// tem como tratar. O que protege é o RWMutex: o Lookup segura o RLock durante
// toda a varredura da cadeia, e a troca de região só acontece sob Lock.
//
// O teste força reconstruções DE VERDADE durante as leituras (a asserção final
// sobre o crescimento é o que impede o caso de o teste não ter exercitado nada)
// e confere que nenhuma posting some no caminho. Roda também sob -race.
func TestLookupConcorrenteComSetEReconstrucao(t *testing.T) {
	ix, _ := abre(t, 7)
	antes := ix.SlotCount()
	const n = 8000

	parar := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-parar:
				return
			default:
			}
			for i := 0; i < n; i += 97 {
				it, err := ix.Lookup(num(int64(i)))
				if err != nil {
					t.Errorf("Lookup concorrente: %v", err)
					return
				}
				var rows []types.RowID
				for it.Next() {
					rows = append(rows, it.RowID())
				}
				_ = it.Close()
				// A posting pode ainda não existir (a escrita corre em
				// paralelo), mas se aparecer tem de ser a certa — e a ordem
				// crescente vale mesmo no meio de uma reconstrução.
				if !slices.IsSorted(rows) {
					t.Errorf("Lookup concorrente devolveu fora de ordem: %v", rows)
					return
				}
				if len(rows) > 0 && !slices.Contains(rows, types.RowID(i)) {
					t.Errorf("Lookup(%d) devolveu %v, sem a própria row", i, rows)
					return
				}
			}
		}
	}()

	for i := 0; i < n; i++ {
		if err := ix.Set(num(int64(i)), types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	close(parar)
	wg.Wait()

	// Sem crescimento real o teste não exercitou o que promete.
	if ix.SlotCount() < antes*8 {
		t.Fatalf("esperado pelo menos 3 reconstruções durante o teste (slots %d → %d)", antes, ix.SlotCount())
	}
	for i := 0; i < n; i++ {
		if got := colheIter(t, ix, num(int64(i))); !slices.Contains(got, types.RowID(i)) {
			t.Fatalf("posting %d se perdeu nas reconstruções concorrentes: %v", i, got)
		}
	}
}
