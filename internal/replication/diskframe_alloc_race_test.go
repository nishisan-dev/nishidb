//go:build race

package replication

import "testing"

// Contrapartes sob -race dos gates de alocação de diskframe_alloc_test.go: o
// detector de corrida instrumenta as alocações e os números deixam de valer,
// então o gate de medida se anuncia como SKIP em vez de sumir.

func TestDecodeFrameAllocationsDoNotGrowWithEntryCount(t *testing.T) {
	t.Skip("gate de alocação não vale sob -race; roda em `make test`")
}

func TestAppendFrameDoesNotAllocateWithAPresizedDestination(t *testing.T) {
	t.Skip("gate de alocação não vale sob -race; roda em `make test`")
}

func TestAppendFrameAllocatesOnceForAnEmptyDestination(t *testing.T) {
	t.Skip("gate de alocação não vale sob -race; roda em `make test`")
}
