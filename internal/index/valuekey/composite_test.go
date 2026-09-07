package valuekey

import (
	"encoding/hex"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func str(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
func i64(n int64) types.Value  { return types.Value{Type: types.TypeInt64, I64: n} }
func boolean(b bool) types.Value {
	v := types.Value{Type: types.TypeBool}
	if b {
		v.I64 = 1
	}
	return v
}

// TestChaveCompostaEhGoldenDeDisco é o oráculo da chave de GRUPO do contador
// mantido na escrita: ela é gravada byte a byte no dicionário de grupos, e
// mudá-la deixaria todo contador já materializado sem conseguir reencontrar
// os próprios grupos — o sintoma seria um id novo por chave antiga, e os
// totais antigos órfãos, sem erro na abertura.
//
// O formato é `u16 LE tamanho | chave simples` por componente, e o prefixo de
// tamanho é o que separa "ab"+"c" de "a"+"bc": a chave simples de largura
// variável não tem tamanho próprio (ele vive na entrada do values.dict), então
// concatenar duas seria ambíguo. Os esperados são literais, pela mesma razão do
// golden da chave simples.
func TestChaveCompostaEhGoldenDeDisco(t *testing.T) {
	cases := []struct {
		name string
		vs   []types.Value
		hex  string
	}{
		{"olt só", []types.Value{str("A")}, "0200" + "0941"},
		{"olt, slot, port", []types.Value{str("A"), i64(1), i64(9)},
			"0200" + "0941" + "0900" + "010100000000000000" + "0900" + "010900000000000000"},
		{"string vazia é componente válido", []types.Value{str(""), i64(2)},
			"0100" + "09" + "0900" + "010200000000000000"},
		{"boolean", []types.Value{boolean(true)}, "0900" + "030100000000000000"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := AppendComposite(nil, c.vs)
			if err != nil {
				t.Fatalf("AppendComposite: %v", err)
			}
			if h := hex.EncodeToString(got); h != c.hex {
				t.Fatalf("chave composta de %s mudou: %s, golden %s\n"+
					"ATENÇÃO: estes bytes são o conteúdo do dicionário de grupos em disco.", c.name, h, c.hex)
			}
		})
	}
}

// TestChaveCompostaNaoEhAmbigua: a razão de existir o prefixo de tamanho.
func TestChaveCompostaNaoEhAmbigua(t *testing.T) {
	a, err := AppendComposite(nil, []types.Value{str("ab"), str("c")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := AppendComposite(nil, []types.Value{str("a"), str("bc")})
	if err != nil {
		t.Fatal(err)
	}
	if string(a) == string(b) {
		t.Fatalf("ab|c e a|bc colidiram: %x", a)
	}
}

func TestDecodeCompostaDesfazOAppend(t *testing.T) {
	want := []types.Value{str("PRCTA_G1M10"), i64(1), i64(-9), boolean(false), str("")}
	key, err := AppendComposite(nil, want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeComposite(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("%d componentes, quero %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Type != want[i].Type || got[i].I64 != want[i].I64 || string(got[i].Bytes) != string(want[i].Bytes) {
			t.Fatalf("componente %d: %+v, quero %+v", i, got[i], want[i])
		}
	}
	// Zero-copy: os Bytes de uma string apontam para dentro da chave.
	if len(got[0].Bytes) > 0 && &got[0].Bytes[0] != &key[2+1] {
		t.Fatal("DecodeComposite copiou a string em vez de apontar para a chave")
	}
}

// TestDecodeCompostaRecusaChaveRasgada: corte DENTRO de um componente é
// recusado. Corte exatamente na fronteira entre componentes NÃO é — a chave não
// carrega contagem de componentes, e "abc" sozinho é uma chave válida de um
// componente. Quem sabe quantos componentes a chave tem de ter é o dono dela
// (o contador conhece as próprias colunas), e é ele que confere a contagem.
func TestDecodeCompostaRecusaChaveRasgada(t *testing.T) {
	key, _ := AppendComposite(nil, []types.Value{str("abc"), i64(1)})
	const boundary = 2 + 1 + 3 // prefixo + tag + "abc"
	for cut := 1; cut < len(key); cut++ {
		vals, err := DecodeComposite(key[:cut], nil)
		if cut == boundary {
			if err != nil || len(vals) != 1 {
				t.Fatalf("corte na fronteira é uma chave de UM componente; got %v, %v", vals, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("corte em %d bytes deveria ser recusado", cut)
		}
	}
}

// TestAppendCompostaRespeitaOBufferDoChamador: como o Append simples, a chave
// composta é montada a cada evento no writer e não pode alocar.
func TestAppendCompostaRespeitaOBufferDoChamador(t *testing.T) {
	buf := make([]byte, 0, 64)
	got, err := AppendComposite(buf, []types.Value{str("Stop"), i64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if cap(got) != cap(buf) || &got[:1][0] != &buf[:1][0] {
		t.Fatalf("AppendComposite alocou em vez de usar o buffer do chamador (cap %d vs %d)", cap(got), cap(buf))
	}
	dst := make([]types.Value, 0, 4)
	vals, err := DecodeComposite(got, dst)
	if err != nil {
		t.Fatal(err)
	}
	if cap(vals) != cap(dst) {
		t.Fatal("DecodeComposite alocou em vez de usar o scratch do chamador")
	}
}

func TestAppendCompostaRecusaListaVaziaETipoNaoIndexavel(t *testing.T) {
	if _, err := AppendComposite(nil, nil); err == nil {
		t.Fatal("chave sem componente deveria ser recusada")
	}
	if _, err := AppendComposite(nil, []types.Value{{Type: types.TypeNull}}); err == nil {
		t.Fatal("TypeNull deveria ser recusado")
	}
}
