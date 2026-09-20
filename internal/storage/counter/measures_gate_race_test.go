//go:build race

package counter

import "testing"

func TestMeasuresDoNotAllocatePerEventOrRead(t *testing.T) {
	t.Skip("gate de alocação exige execução sem instrumentação de corrida")
}
