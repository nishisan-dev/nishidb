//go:build !race

package replication

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// manyEntriesBatch monta um lote de n entradas da MESMA tabela, cada uma com
// key e payload não vazios: o pior caso para quem copia por entrada.
func manyEntriesBatch(n int) *Batch {
	b := &Batch{From: 1, To: uint64(n), At: time.Unix(1, 0), Reason: "interval"}
	for i := 0; i < n; i++ {
		b.Entries = append(b.Entries, Entry{
			Sequence:  uint64(i + 1),
			Table:     "events",
			Operation: Update,
			At:        time.Unix(1, 0),
			Record:    types.Record{RowID: types.RowID(i + 1), Key: []byte("some-key-0123456789"), Payload: make([]byte, 64)},
		})
	}
	return b
}

// TestDecodeFrameAllocationsDoNotGrowWithEntryCount trava a garantia "UMA
// arena por quadro": o número de alocações do decode é constante, e não
// proporcional às entradas. Medimos 2 e 200 entradas da mesma tabela; copiar
// key e payload entrada a entrada faria a segunda medida passar de 400.
func TestDecodeFrameAllocationsDoNotGrowWithEntryCount(t *testing.T) {
	measure := func(n int) float64 {
		src := mustAppendFrame(nil, manyEntriesBatch(n), 1)
		return testing.AllocsPerRun(50, func() {
			if _, _, err := decodeFrame(src); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := measure(2), measure(200)
	if large != small {
		t.Fatalf("alocações por decode = %v com 200 entradas e %v com 2; deveriam ser iguais (arena única)", large, small)
	}
	// Reason, slice de entradas, arena e a string da tabela.
	if large > 4 {
		t.Fatalf("alocações por decode = %v, esperado no máximo 4", large)
	}
}

// TestAppendFrameDoesNotAllocateWithAPresizedDestination: o gravador reaproveita
// o buffer entre lotes (buf[:0]); codificar não pode alocar nada por entrada.
func TestAppendFrameDoesNotAllocateWithAPresizedDestination(t *testing.T) {
	b := manyEntriesBatch(200)
	dst := make([]byte, 0, frameSize(b))
	allocs := testing.AllocsPerRun(50, func() {
		dst, _ = appendFrame(dst[:0], b, 1)
	})
	if allocs != 0 {
		t.Fatalf("appendFrame alocou %v vezes com destino já dimensionado, esperado 0", allocs)
	}
}

// TestAppendFrameAllocatesOnceForAnEmptyDestination: o godoc promete um único
// crescimento do destino (frameSize dimensiona de antemão). Sem o Grow, o
// append iria dobrando o slice várias vezes ao longo das 200 entradas.
func TestAppendFrameAllocatesOnceForAnEmptyDestination(t *testing.T) {
	b := manyEntriesBatch(200)
	allocs := testing.AllocsPerRun(50, func() {
		_, _ = appendFrame(nil, b, 1)
	})
	if allocs != 1 {
		t.Fatalf("appendFrame sobre destino nil alocou %v vezes, esperado 1", allocs)
	}
}
