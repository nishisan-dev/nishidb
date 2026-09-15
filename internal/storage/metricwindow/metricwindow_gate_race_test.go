//go:build race

package metricwindow_test

import "testing"

// Contraparte visível do gate de alocação da janela da métrica sob -race: a
// instrumentação altera a contagem, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestFileAccessorsDoNotAllocate(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
