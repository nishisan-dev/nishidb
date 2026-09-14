//go:build race

package query_test

import "testing"

func TestAliasScanAndEncodingAllocateOnlyPerQuery(t *testing.T) {
	t.Skip("gate de alocação exige execução sem -race")
}
