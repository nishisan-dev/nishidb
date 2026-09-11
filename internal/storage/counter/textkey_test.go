package counter

import (
	"strconv"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
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

// formatOf monta o molde default para os tipos de uma tupla. Os nomes das
// colunas não influem no texto; só a aridade e os tipos.
func formatOf(vs []types.Value) *keyformat.Format {
	names := make([]string, len(vs))
	kinds := make([]types.FieldType, len(vs))
	for i, v := range vs {
		names[i], kinds[i] = "c"+strconv.Itoa(i), v.Type
	}
	return keyformat.Default(keyformat.Columns(names, kinds))
}

// TestTextoDaChaveEhGolden fixa a renderização de `groupKey`: é a chave da
// tabela virtual, e o que a aplicação usa para casar as linhas de dois
// contadores num mapa. Os esperados são literais — o texto é contrato com quem
// consulta, e derivá-lo da função faria o teste concordar com qualquer mudança.
//
// A gramática do molde tem os goldens dela em internal/keyformat; o que este
// arquivo prova é a JUNÇÃO — decodificar a chave composta, que é formato de
// disco, e entregá-la ao molde na ordem certa.
func TestTextoDaChaveEhGolden(t *testing.T) {
	cases := []struct {
		name string
		vs   []types.Value
		text string
	}{
		{"um componente sai sem separador", []types.Value{str("PRCTA_G1M10")}, "PRCTA_G1M10"},
		{"composto", []types.Value{str("PRCTA_G1M10"), i64(1), i64(9)}, "PRCTA_G1M10|1|9"},
		{"negativo", []types.Value{str("A"), i64(-9)}, "A|-9"},
		{"boolean", []types.Value{boolv(true), boolv(false)}, "true|false"},
		{"barra vertical escapada", []types.Value{str("a|b"), i64(1)}, `a\|b|1`},
		{"barra invertida escapada", []types.Value{str(`x\y`)}, `x\\y`},
		{"string vazia", []types.Value{str(""), i64(2)}, "|2"},
		// O último componente NÃO escapa o separador: ali não há fronteira a
		// proteger. Os seis casos acima passam com e sem essa regra — nenhum
		// tem string com `|` na última posição —, e por isso não provam nada
		// sobre ela.
		{"barra vertical no último componente", []types.Value{str("a"), str("b|c")}, "a|b|c"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key, err := valuekey.AppendComposite(nil, c.vs)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := KeyText(nil, formatOf(c.vs), key, nil)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != c.text {
				t.Fatalf("texto de %s = %q, golden %q", c.name, got, c.text)
			}
		})
	}
}

func TestTextoDaChaveRespeitaOsBuffersDoChamador(t *testing.T) {
	key, _ := valuekey.AppendComposite(nil, []types.Value{str("A"), i64(1)})
	dst := make([]byte, 0, 64)
	scratch := make([]types.Value, 0, 4)
	vs := []types.Value{str("A"), i64(1)}
	got, scratch2, err := KeyText(dst, formatOf(vs), key, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if cap(got) != cap(dst) || &got[:1][0] != &dst[:1][0] {
		t.Fatal("KeyText alocou o texto em vez de usar o buffer do chamador")
	}
	if cap(scratch2) != cap(scratch) {
		t.Fatal("KeyText alocou o scratch em vez de usar o do chamador")
	}
}

func TestTextoDaChaveRecusaChaveInvalida(t *testing.T) {
	f := formatOf([]types.Value{str("a")})
	if _, _, err := KeyText(nil, f, []byte{9, 0, 1}, nil); err == nil {
		t.Fatal("chave rasgada tem de ser recusada")
	}
	key, _ := valuekey.AppendComposite(nil, []types.Value{{Type: types.TypeTimestamp, I64: 1}})
	if _, _, err := KeyText(nil, f, key, nil); err == nil {
		t.Fatal("tipo fora de string/int64/boolean não tem renderização declarada")
	}
}
