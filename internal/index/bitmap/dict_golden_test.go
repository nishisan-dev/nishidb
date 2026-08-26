package bitmap

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestValuesDictEhGoldenDeBytes ancora o formato do values.dict em bytes
// literais, e existe porque a serialização da chave saiu deste pacote para o
// internal/index/valuekey, onde passou a ter DOIS consumidores (bitmap e
// hash).
//
// A extração só é legítima se for movimento PURO: a chave é gravada em disco
// em cada entrada deste arquivo, e um byte diferente tornaria todo índice já
// materializado ilegível — sem erro de abertura, porque o CRC de cada entrada
// continuaria fechando; o sintoma seria o Lookup deixando de casar com as
// postings e a query respondendo vazio.
//
// O golden foi calculado FORA do Go, a partir do layout documentado no topo de
// bitmap.go (header de 8 bytes; entrada `type u8 | valLen u16 LE | valBytes |
// crc32c u32` sobre os 3+valLen bytes anteriores), justamente para não ser a
// saída do código concordando consigo mesma.
func TestValuesDictEhGoldenDeBytes(t *testing.T) {
	ix, dir := openIndex(t, 42)
	if err := ix.Set(strValue("Start"), 1); err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(types.Value{Type: types.TypeInt64, I64: 7}, 2); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "values.dict"))
	if err != nil {
		t.Fatal(err)
	}
	const golden = "4e4442560100000009050053746172748d9c37f9010800070000000000000033af2381"
	if got := hex.EncodeToString(raw); got != golden {
		t.Fatalf("o values.dict mudou de formato:\nobtido %s\ngolden %s\n"+
			"ATENÇÃO: este arquivo existe em disco em todo dataset com índice bitmap. "+
			"Mudar a chave canônica invalida todos eles em silêncio.", got, golden)
	}
}
