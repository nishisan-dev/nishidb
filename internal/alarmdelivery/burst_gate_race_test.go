//go:build race

package alarmdelivery

import "testing"

// Contraparte visível do gate de medida da rajada sob -race: a
// instrumentação altera a medida, e o pulo se anuncia em vez de sumir da
// rodada (débito 53).
func TestBurstPersistenceThroughputIsMeasured(t *testing.T) {
	t.Skip("gate de medida não roda sob -race: a instrumentação altera a vazão medida")
}
