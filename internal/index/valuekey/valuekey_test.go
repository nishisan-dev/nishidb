package valuekey

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestChaveCanonicaEhGoldenDeDisco é o oráculo que autoriza este pacote a
// existir.
//
// A chave canônica NÃO é um detalhe interno: ela é gravada byte a byte em cada
// entrada do values.dict de todo índice bitmap já materializado. Mover a
// serialização de um pacote para outro é seguro EXATAMENTE enquanto os bytes
// não mudam — um byte a mais, o tipo fora da frente ou um endianness trocado
// tornariam ilegível todo índice em disco, e o sintoma não seria um erro de
// abertura: seria o lookup deixando de casar com as postings gravadas e a
// query devolvendo resultado incompleto em silêncio.
//
// Os esperados abaixo são literais, e é de propósito: derivá-los da própria
// função faria o teste concordar com qualquer mudança que ela sofresse, que é
// o oposto de um golden.
func TestChaveCanonicaEhGoldenDeDisco(t *testing.T) {
	cases := []struct {
		name string
		v    types.Value
		hex  string
	}{
		// Largura fixa: tag + os 8 bytes do I64 em little-endian, SEMPRE os
		// oito — inclusive para boolean, cujo valor lógico cabe num bit. Uma
		// "otimização" que gravasse 1 byte para bool mudaria a chave de todo
		// bitmap de coluna booleana já existente.
		{"int64 1", types.Value{Type: types.TypeInt64, I64: 1}, "010100000000000000"},
		{"int64 -1", types.Value{Type: types.TypeInt64, I64: -1}, "01ffffffffffffffff"},
		{"double bits", types.Value{Type: types.TypeDouble, I64: 0x4008000000000000}, "020000000000000840"},
		{"bool false", types.Value{Type: types.TypeBool, I64: 0}, "030000000000000000"},
		{"bool true", types.Value{Type: types.TypeBool, I64: 1}, "030100000000000000"},
		{"timestamp", types.Value{Type: types.TypeTimestamp, I64: 1765488490000}, "041076500f9b010000"},
		// Largura variável: tag + os bytes crus, sem prefixo de tamanho. O
		// tamanho vive na entrada do values.dict, não na chave.
		{"string Start", types.Value{Type: types.TypeString, Bytes: []byte("Start")}, "095374617274"},
		{"string vazia", types.Value{Type: types.TypeString, Bytes: []byte{}}, "09"},
		{"bytes", types.Value{Type: types.TypeBytes, Bytes: []byte{0xde, 0xad}}, "0adead"},
		{"json", types.Value{Type: types.TypeJSON, Bytes: []byte(`{"a":1}`)}, "0b7b2261223a317d"},
		{"ipv4", types.Value{Type: types.TypeIPv4, Bytes: []byte{10, 0, 0, 1}}, "050a000001"},
		{"ipv6", types.Value{Type: types.TypeIPv6, Bytes: bytes.Repeat([]byte{0x20, 0x01}, 8)}, "0620012001200120012001200120012001"},
		{"ipv4_prefix", types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{192, 168, 0, 0, 16}}, "07c0a8000010"},
		{"ipv6_prefix", types.Value{Type: types.TypeIPv6Prefix, Bytes: append(bytes.Repeat([]byte{0xfe, 0x80}, 8), 64)}, "08fe80fe80fe80fe80fe80fe80fe80fe8040"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Append(nil, c.v)
			if err != nil {
				t.Fatalf("Append: %v", err)
			}
			if h := hex.EncodeToString(got); h != c.hex {
				t.Fatalf("chave canônica de %s mudou: %s, golden %s\n"+
					"ATENÇÃO: estes bytes são o conteúdo do values.dict em disco. "+
					"Alterá-los invalida todo índice já materializado.", c.name, h, c.hex)
			}
		})
	}
}

// TestAppendRespeitaOBufferDoChamador prova que a serialização escreve SOBRE o
// dst recebido em vez de alocar por chamada. Os dois motores de índice reusam
// um scratch por writer, e uma implementação que ignorasse o dst passaria em
// todos os testes de valor e só apareceria como alocação por campo aplicado no
// caminho quente da ingestão.
func TestAppendRespeitaOBufferDoChamador(t *testing.T) {
	buf := make([]byte, 0, 64)
	got, err := Append(buf, types.Value{Type: types.TypeString, Bytes: []byte("Stop")})
	if err != nil {
		t.Fatal(err)
	}
	if cap(got) != cap(buf) || &got[:1][0] != &buf[:1][0] {
		t.Fatalf("Append alocou em vez de usar o buffer do chamador (cap %d vs %d)", cap(got), cap(buf))
	}
}

// TestTipoNaoIndexavelEhErro trava o ramo que impede uma tag desconhecida de
// virar chave silenciosa. Sem ele, um tipo novo cairia num `default` mudo e
// duas Values distintas colapsariam na mesma posting.
func TestTipoNaoIndexavelEhErro(t *testing.T) {
	if _, err := Append(nil, types.Value{Type: types.TypeNull}); err == nil {
		t.Fatal("TypeNull deveria ser recusado: NULL é ausência, não valor")
	}
	if err := Validate(types.Value{Type: types.TypeNull}); err == nil {
		t.Fatal("Validate de TypeNull deveria ser recusado")
	}
}

// TestValidateSeparaCulpaDoEventoDeBugNosso trava o sentinela: o teto do
// values.dict é culpa do EVENTO, e o materializer o separa do balde de erro
// interno por errors.Is(err, types.ErrTooLarge). Um erro cru aqui mandaria o
// operador procurar defeito no engine.
func TestValidateSeparaCulpaDoEventoDeBugNosso(t *testing.T) {
	aboveCeiling := types.Value{Type: types.TypeString, Bytes: make([]byte, types.MaxIndexedValueBytes+1)}
	err := Validate(aboveCeiling)
	if !errors.Is(err, types.ErrTooLarge) {
		t.Fatalf("valor acima do teto deveria trazer ErrTooLarge, veio %v", err)
	}
	atCeiling := types.Value{Type: types.TypeString, Bytes: make([]byte, types.MaxIndexedValueBytes)}
	if err := Validate(atCeiling); err != nil {
		t.Fatalf("o teto é inclusivo (%d bytes cabem): %v", types.MaxIndexedValueBytes, err)
	}
}
