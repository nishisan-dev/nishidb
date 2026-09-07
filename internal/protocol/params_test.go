package protocol_test

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func param(typeName, value string) protocol.Param {
	return protocol.Param{Type: typeName, Value: []byte(value)}
}

func TestOsQuatroTiposDeWireViramValor(t *testing.T) {
	cases := []struct {
		name  string
		p     protocol.Param
		want  types.FieldType
		i64   int64
		bytes string
	}{
		{"string", param("string", `"Stop"`), types.TypeString, 0, "Stop"},
		{"string com escape", param("string", `"a\nb"`), types.TypeString, 0, "a\nb"},
		{"int64", param("int64", `42`), types.TypeInt64, 42, ""},
		{"int64 negativo", param("int64", `-7`), types.TypeInt64, -7, ""},
		{"boolean verdadeiro", param("boolean", `true`), types.TypeBool, 1, ""},
		{"boolean falso", param("boolean", `false`), types.TypeBool, 0, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vs, err := protocol.Values([]protocol.Param{c.p})
			if err != nil {
				t.Fatalf("Values: %v", err)
			}
			if len(vs) != 1 {
				t.Fatalf("len(valores) = %d, quer 1", len(vs))
			}
			if vs[0].Type != c.want {
				t.Fatalf("Type = %v, quer %v", vs[0].Type, c.want)
			}
			if c.bytes != "" && string(vs[0].Bytes) != c.bytes {
				t.Fatalf("Bytes = %q, quer %q", vs[0].Bytes, c.bytes)
			}
			if c.bytes == "" && vs[0].I64 != c.i64 {
				t.Fatalf("I64 = %d, quer %d", vs[0].I64, c.i64)
			}
		})
	}
}

// O double guarda os BITS do float em I64, como o resto do produto. Um valor
// inteiro é aceito: 3 e 3.0 são o mesmo double.
func TestDoubleGuardaOsBitsEAceitaInteiro(t *testing.T) {
	for _, text := range []string{"3.5", "3", "-0.25", "1e3"} {
		vs, err := protocol.Values([]protocol.Param{param("double", text)})
		if err != nil {
			t.Fatalf("Values(%s): %v", text, err)
		}
		if vs[0].Type != types.TypeDouble {
			t.Fatalf("Type = %v, quer double", vs[0].Type)
		}
		f := math.Float64frombits(uint64(vs[0].I64))
		switch text {
		case "3.5":
			if f != 3.5 {
				t.Fatalf("valor = %v, quer 3.5", f)
			}
		case "3":
			if f != 3 {
				t.Fatalf("valor = %v, quer 3", f)
			}
		case "1e3":
			if f != 1000 {
				t.Fatalf("valor = %v, quer 1000", f)
			}
		}
	}
}

// int64 acima de 2^53 é o caso que um decodificador `any` estragaria em
// silêncio: o encoding/json o entregaria como float64 e o valor chegaria
// ERRADO ao motor, sem erro em camada nenhuma.
func TestInt64GrandeNaoPerdePrecisao(t *testing.T) {
	const large = int64(9007199254740993) // 2^53 + 1
	vs, err := protocol.Values([]protocol.Param{param("int64", "9007199254740993")})
	if err != nil {
		t.Fatalf("Values: %v", err)
	}
	if vs[0].I64 != large {
		t.Fatalf("I64 = %d, quer %d", vs[0].I64, large)
	}
}

func TestOTipoDeclaradoManda(t *testing.T) {
	cases := []struct {
		name    string
		p       protocol.Param
		snippet string
	}{
		{"int64 com fração", param("int64", `3.5`), "espera um inteiro"},
		{"int64 com string", param("int64", `"42"`), "espera um número"},
		{"string com número", param("string", `42`), "espera uma string"},
		{"boolean com número", param("boolean", `1`), "espera true ou false"},
		{"double com string", param("double", `"3.5"`), "espera um número"},
		{"int64 estourado", param("int64", `9223372036854775808`), "não cabe em int64"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := protocol.Values([]protocol.Param{c.p})
			if err == nil {
				t.Fatal("quer erro, veio nil")
			}
			if !errors.Is(err, protocol.ErrParam) {
				t.Fatalf("erro não é ErrParam: %v", err)
			}
			if !strings.Contains(err.Error(), c.snippet) {
				t.Fatalf("mensagem %q não contém %q", err, c.snippet)
			}
		})
	}
}

// O tipo fora dos quatro precisa ENSINAR a alternativa: quem manda "ipv4" está
// certo sobre o dado e errado sobre o wire, e a mensagem tem de dizer por onde
// ele passa.
func TestTipoForaDosQuatroNomeiaAAlternativa(t *testing.T) {
	_, err := protocol.Values([]protocol.Param{param("ipv4", `"10.0.0.1"`)})
	if err == nil {
		t.Fatal("quer erro, veio nil")
	}
	msg := err.Error()
	for _, snippet := range []string{"ipv4", "string", "int64", "double", "boolean"} {
		if !strings.Contains(msg, snippet) {
			t.Fatalf("mensagem %q não contém %q", msg, snippet)
		}
	}
}

// Parâmetro nulo é recusado com a mesma razão do binder: `col = ?` com ? nulo
// não tem semântica sem lógica trivalente, e o que o usuário quer é IS NULL.
func TestParametroNuloEnsinaIsNull(t *testing.T) {
	_, err := protocol.Values([]protocol.Param{param("string", `null`)})
	if err == nil {
		t.Fatal("quer erro, veio nil")
	}
	if !strings.Contains(err.Error(), "IS NULL") {
		t.Fatalf("mensagem %q não ensina IS NULL", err)
	}
}

// As mensagens contam os parâmetros a partir de 1, como o binder e como quem
// escreve os `?` na query.
func TestOIndiceNaMensagemEh1Based(t *testing.T) {
	_, err := protocol.Values([]protocol.Param{
		param("string", `"ok"`),
		param("int64", `"não é número"`),
	})
	if err == nil {
		t.Fatal("quer erro, veio nil")
	}
	if !strings.Contains(err.Error(), "parameters[2]") {
		t.Fatalf("mensagem %q não aponta parameters[2]", err)
	}
}

// A mensagem não pode ecoar o valor: ele é dado do cliente, e o §95 proíbe
// registrar payload. A FORMA é o que resolve o problema de quem enviou.
func TestAMensagemNaoEcoaOValorDeString(t *testing.T) {
	const secret = "senha-do-cliente"
	_, err := protocol.Values([]protocol.Param{param("int64", `"`+secret+`"`)})
	if err == nil {
		t.Fatal("quer erro, veio nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("a mensagem ecoou o valor: %q", err)
	}
}

func TestValueAusenteOuTipoAusenteSaoRecusados(t *testing.T) {
	if _, err := protocol.Values([]protocol.Param{{Type: "string"}}); err == nil {
		t.Fatal(`"value" ausente: quer erro, veio nil`)
	}
	if _, err := protocol.Values([]protocol.Param{{Value: []byte(`"x"`)}}); err == nil {
		t.Fatal(`"type" ausente: quer erro, veio nil`)
	}
}

func TestSemParametrosDevolveNil(t *testing.T) {
	vs, err := protocol.Values(nil)
	if err != nil || vs != nil {
		t.Fatalf("Values(nil) = %v, %v; quer nil, nil", vs, err)
	}
}
