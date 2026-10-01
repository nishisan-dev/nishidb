package counter

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// BenchmarkDobra1M mede a dobra de um vetor de um milhão de linhas — o que a
// abertura paga ANTES do replay, num dataset que adota o contador. É o número
// que decide se a dobra pode ficar no caminho do Open: ela é uma leitura
// sequencial de 4 bytes por linha, e o custo tem de ser de milissegundos, não
// de segundos.
func BenchmarkDobra1M(b *testing.B) {
	const rows = 1 << 20
	c, err := Open(b.TempDir(), defByPON("pon", 4096), rows, false, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	// 64 OLTs × 4 slots × 8 portas = 2048 grupos, distribuídos pelas linhas.
	olts := make([][]byte, 64)
	for i := range olts {
		olts[i] = []byte("OLT" + string(rune('A'+i%26)) + string(rune('A'+i/26)))
	}
	for r := types.RowID(0); r < rows; r++ {
		p := payloadBench(olts[int(r)%64], int64(r%4), int64(r%8), r%3 != 0)
		if err := c.Transition(r, p, false); err != nil {
			b.Fatal(err)
		}
	}
	if err := c.ApplyOverlay(nil); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Fold(); err != nil {
			b.Fatal(err)
		}
	}
}

// payloadBench é o payload de uma linha do benchmark, com os quatro campos do
// dataset de teste em ordem crescente de FieldID.
func payloadBench(olt []byte, slot, port int64, auth bool) []byte {
	var buf []byte
	buf, _ = encoding.AppendField(buf, cOLT, types.Value{Type: types.TypeString, Bytes: olt})
	buf, _ = encoding.AppendField(buf, cSlot, types.Value{Type: types.TypeInt64, I64: slot})
	buf, _ = encoding.AppendField(buf, cPort, types.Value{Type: types.TypeInt64, I64: port})
	a := types.Value{Type: types.TypeBool}
	if auth {
		a.I64 = 1
	}
	buf, _ = encoding.AppendField(buf, cAuth, a)
	return buf
}
