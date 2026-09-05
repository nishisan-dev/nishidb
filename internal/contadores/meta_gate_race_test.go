//go:build race

package contadores_test

import "testing"

// TestTabelaMetaCountersNaoAlocaPorLinha é gate de medida: sob -race o
// runtime aloca por conta própria e o número não é o do produto. Anuncia-se
// como SKIP em vez de sumir da suíte.
func TestTabelaMetaCountersNaoAlocaPorLinha(t *testing.T) {
	t.Skip("gate de alocação: só vale sem -race (make test)")
}
