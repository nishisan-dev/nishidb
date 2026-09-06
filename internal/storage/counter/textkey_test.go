package counter

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func str(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
func i64(n int64) types.Value  { return types.Value{Type: types.TypeInt64, I64: n} }
func boolv(b bool) types.Value {
	v := types.Value{Type: types.TypeBool}
	if b {
		v.I64 = 1
	}
	return v
}

// TestTextoDaChaveEhGolden fixa a renderização de `groupKey`: é a chave da
// tabela virtual, e o que a aplicação usa para casar as linhas de dois
// contadores num mapa. Os esperados são literais — o texto é contrato com quem
// consulta, e derivá-lo da função faria o teste concordar com qualquer mudança.
func TestTextoDaChaveEhGolden(t *testing.T) {
	casos := []struct {
		nome  string
		vs    []types.Value
		texto string
	}{
		{"um componente sai sem separador", []types.Value{str("PRCTA_G1M10")}, "PRCTA_G1M10"},
		{"composto", []types.Value{str("PRCTA_G1M10"), i64(1), i64(9)}, "PRCTA_G1M10|1|9"},
		{"negativo", []types.Value{str("A"), i64(-9)}, "A|-9"},
		{"boolean", []types.Value{boolv(true), boolv(false)}, "true|false"},
		{"barra vertical escapada", []types.Value{str("a|b"), i64(1)}, `a\|b|1`},
		{"barra invertida escapada", []types.Value{str(`x\y`)}, `x\\y`},
		{"string vazia", []types.Value{str(""), i64(2)}, "|2"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			chave, err := valuekey.AppendComposta(nil, c.vs)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := TextoDaChave(nil, chave, nil)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != c.texto {
				t.Fatalf("texto de %s = %q, golden %q", c.nome, got, c.texto)
			}
		})
	}
}

func TestTextoDaChaveRespeitaOsBuffersDoChamador(t *testing.T) {
	chave, _ := valuekey.AppendComposta(nil, []types.Value{str("A"), i64(1)})
	dst := make([]byte, 0, 64)
	scratch := make([]types.Value, 0, 4)
	got, scratch2, err := TextoDaChave(dst, chave, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if cap(got) != cap(dst) || &got[:1][0] != &dst[:1][0] {
		t.Fatal("TextoDaChave alocou o texto em vez de usar o buffer do chamador")
	}
	if cap(scratch2) != cap(scratch) {
		t.Fatal("TextoDaChave alocou o scratch em vez de usar o do chamador")
	}
}

func TestTextoDaChaveRecusaChaveInvalida(t *testing.T) {
	if _, _, err := TextoDaChave(nil, []byte{9, 0, 1}, nil); err == nil {
		t.Fatal("chave rasgada tem de ser recusada")
	}
	chave, _ := valuekey.AppendComposta(nil, []types.Value{{Type: types.TypeTimestamp, I64: 1}})
	if _, _, err := TextoDaChave(nil, chave, nil); err == nil {
		t.Fatal("tipo fora de string/int64/boolean não tem renderização declarada")
	}
}
