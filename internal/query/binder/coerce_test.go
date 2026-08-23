package binder_test

import (
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A tabela §3.6 célula a célula. É a fronteira entre "a query encontra o dado
// gravado" e "a query devolve vazio sem erro", então cada célula tem caso.
func TestComparableTabela(t *testing.T) {
	casos := []struct {
		nome string
		col  types.FieldType
		lit  parser.OperandKind
		op   parser.CmpOp
		want binder.CompareKind
	}{
		{"string com string", types.TypeString, parser.OperandString, parser.CmpEq, binder.CompareBytes},
		{"string com >", types.TypeString, parser.OperandString, parser.CmpGt, binder.CompareBytes},

		{"int64 com int", types.TypeInt64, parser.OperandInt, parser.CmpEq, binder.CompareInt},
		// A única coerção cruzada: o payload guarda inteiro CRU e o literal
		// vira bits de float, então o executor precisa saber converter só um
		// dos lados.
		{"int64 com float", types.TypeInt64, parser.OperandFloat, parser.CmpGt, binder.CompareIntAsFloat},

		{"double com float", types.TypeDouble, parser.OperandFloat, parser.CmpLte, binder.CompareFloatBits},
		{"double com int", types.TypeDouble, parser.OperandInt, parser.CmpEq, binder.CompareFloatBits},

		{"boolean com bool", types.TypeBool, parser.OperandBool, parser.CmpEq, binder.CompareInt},
		{"boolean com !=", types.TypeBool, parser.OperandBool, parser.CmpNe, binder.CompareInt},

		{"timestamp com string", types.TypeTimestamp, parser.OperandString, parser.CmpGte, binder.CompareInt},
		{"timestamp com int", types.TypeTimestamp, parser.OperandInt, parser.CmpLt, binder.CompareInt},

		// datetime aceita o texto no format do campo e o epoch millis.
		{"datetime com string", types.TypeDateTime, parser.OperandString, parser.CmpGte, binder.CompareInt},
		{"datetime com int", types.TypeDateTime, parser.OperandInt, parser.CmpLt, binder.CompareInt},
		// Em time o inteiro é millis do DIA, não epoch: mesma célula da
		// tabela, significado diferente do número.
		{"time com string", types.TypeTime, parser.OperandString, parser.CmpGt, binder.CompareInt},
		{"time com int", types.TypeTime, parser.OperandInt, parser.CmpLte, binder.CompareInt},

		{"ipv4 com string", types.TypeIPv4, parser.OperandString, parser.CmpEq, binder.CompareBytes},
		{"ipv6 com string", types.TypeIPv6, parser.OperandString, parser.CmpEq, binder.CompareBytes},
		{"ipv4_prefix com string", types.TypeIPv4Prefix, parser.OperandString, parser.CmpEq, binder.CompareBytes},
		{"ipv6_prefix com string", types.TypeIPv6Prefix, parser.OperandString, parser.CmpNe, binder.CompareBytes},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			got, err := binder.Comparable(tc.col, false, tc.lit, tc.op)
			if err != nil {
				t.Fatalf("Comparable(%v,%v,%v): %v", tc.col, tc.lit, tc.op, err)
			}
			if got != tc.want {
				t.Errorf("Kind = %v, want %v", got, tc.want)
			}
			if got == binder.CompareUnset {
				t.Error("CompareUnset é sempre bug: o zero value não é modo válido")
			}
		})
	}
}

// Células VAZIAS da tabela: cada uma é um erro que o usuário precisa
// entender, não um resultado vazio.
func TestComparableCelulasVazias(t *testing.T) {
	casos := []struct {
		nome string
		col  types.FieldType
		lit  parser.OperandKind
		op   parser.CmpOp
	}{
		// Sem coerção implícita string↔número na query, ao contrário da
		// ingestão: aqui o usuário escolhe como escrever o literal.
		{"int64 com string", types.TypeInt64, parser.OperandString, parser.CmpEq},
		{"string com int", types.TypeString, parser.OperandInt, parser.CmpEq},
		{"double com string", types.TypeDouble, parser.OperandString, parser.CmpEq},
		{"boolean com int", types.TypeBool, parser.OperandInt, parser.CmpEq},
		{"boolean com string", types.TypeBool, parser.OperandString, parser.CmpEq},
		{"timestamp com float", types.TypeTimestamp, parser.OperandFloat, parser.CmpEq},
		{"timestamp com bool", types.TypeTimestamp, parser.OperandBool, parser.CmpEq},
		{"ipv4 com int", types.TypeIPv4, parser.OperandInt, parser.CmpEq},
		{"ipv6 com bool", types.TypeIPv6, parser.OperandBool, parser.CmpEq},
		{"string com bool", types.TypeString, parser.OperandBool, parser.CmpEq},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			if _, err := binder.Comparable(tc.col, false, tc.lit, tc.op); err == nil {
				t.Errorf("Comparable(%v,%v) deveria falhar", tc.col, tc.lit)
			}
		})
	}
}

// bytes e json não comparam com nada: é limitação conhecida da v1, e a
// mensagem tem de dizer o que É possível.
func TestComparableBytesEJsonNaoComparam(t *testing.T) {
	for _, col := range []types.FieldType{types.TypeBytes, types.TypeJSON} {
		for _, lit := range []parser.OperandKind{
			parser.OperandString, parser.OperandInt, parser.OperandFloat, parser.OperandBool,
		} {
			if _, err := binder.Comparable(col, false, lit, parser.CmpEq); err == nil {
				t.Errorf("coluna %v com literal %v deveria falhar", col, lit)
			}
		}
	}
}

// Ordem só existe onde faz sentido: booleano e prefixo de rede admitem
// igualdade e diferença, e mais nada.
func TestComparableOperadoresRestritos(t *testing.T) {
	restritos := []struct {
		col types.FieldType
		lit parser.OperandKind
	}{
		{types.TypeBool, parser.OperandBool},
		{types.TypeIPv4Prefix, parser.OperandString},
		{types.TypeIPv6Prefix, parser.OperandString},
	}
	for _, r := range restritos {
		for _, op := range []parser.CmpOp{parser.CmpEq, parser.CmpNe} {
			if _, err := binder.Comparable(r.col, false, r.lit, op); err != nil {
				t.Errorf("%v %v deveria ser permitido: %v", r.col, op, err)
			}
		}
		for _, op := range []parser.CmpOp{parser.CmpGt, parser.CmpGte, parser.CmpLt, parser.CmpLte} {
			if _, err := binder.Comparable(r.col, false, r.lit, op); err == nil {
				t.Errorf("%v %v deveria ser recusado", r.col, op)
			}
		}
	}
	// Os demais tipos aceitam ordem.
	for _, col := range []types.FieldType{types.TypeString, types.TypeInt64, types.TypeDouble, types.TypeTimestamp,
		types.TypeIPv4, types.TypeIPv6} {
		lit := parser.OperandString
		switch col {
		case types.TypeInt64:
			lit = parser.OperandInt
		case types.TypeDouble:
			lit = parser.OperandFloat
		}
		if _, err := binder.Comparable(col, false, lit, parser.CmpGte); err != nil {
			t.Errorf("%v >= deveria ser permitido: %v", col, err)
		}
	}
}

// Campo auto SEM tipo fixado é o único estado em que a comparação é recusada
// — e a mensagem precisa dizer que o campo nunca foi visto, não que "auto não
// é comparável".
func TestComparableAutoSemTipo(t *testing.T) {
	_, err := binder.Comparable(types.TypeNull, true, parser.OperandString, parser.CmpEq)
	if err == nil {
		t.Fatal("campo auto sem tipo deveria falhar")
	}
}

// A coerção do valor é conferida fim a fim pelo Bind (ver binder_test.go);
// aqui garantimos que os bits do float não se perdem no caminho.
func TestCompareKindString(t *testing.T) {
	nomes := map[binder.CompareKind]string{
		binder.CompareUnset:      "unset",
		binder.CompareInt:        "int",
		binder.CompareFloatBits:  "float-bits",
		binder.CompareIntAsFloat: "int-as-float",
		binder.CompareBytes:      "bytes",
	}
	for k, want := range nomes {
		if got := k.String(); got != want {
			t.Errorf("CompareKind(%d).String() = %q, want %q", k, got, want)
		}
	}
	if got := binder.CompareKind(200).String(); !strings.Contains(got, "?") {
		t.Errorf("fora de faixa = %q", got)
	}
}

// math.Float64bits sempre via I64: Value não tem campo float, e trocar a
// codificação faria o executor comparar lixo.
func TestBitsDeFloatSobrevivemAoI64(t *testing.T) {
	const f = -1.5e300
	v := types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(f))}
	if got := math.Float64frombits(uint64(v.I64)); got != f {
		t.Errorf("round-trip = %v, want %v", got, f)
	}
}

// A ordem das checagens de Comparable é contrato, e este é o teste que a fixa.
//
// Nas quatro formas abaixo — coluna de operador RESTRITO (boolean, prefixo)
// com literal de tipo INCOMPATÍVEL — as duas checagens falhariam, e só a ordem
// decide qual mensagem o usuário vê. Trocar `celula` e `admitsOp` de lugar
// compila, passa na árvore inteira, e faz o erro dar um conselho ERRADO:
// "use = ou !=" não resolve nada quando o problema é o tipo do literal
// (`ativo = 'x'` dá o mesmo erro de incompatibilidade).
func TestOrdemDasChecagensDeComparable(t *testing.T) {
	casos := []struct {
		nome string
		sql  string
		msg  string
	}{
		{"boolean com string", `SELECT id FROM radius_sessions WHERE ativo > 'x'`,
			"não é comparável com literal do tipo string"},
		{"boolean com int", `SELECT id FROM radius_sessions WHERE ativo >= 1`,
			"não é comparável com literal do tipo int"},
		{"prefixo v4 com int", `SELECT id FROM radius_sessions WHERE rede4 > 1`,
			"não é comparável com literal do tipo int"},
		{"prefixo v6 com boolean", `SELECT id FROM radius_sessions WHERE rede6 < TRUE`,
			"não é comparável com literal do tipo boolean"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			err := bindErr(t, tc.sql)
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q\n want conter %q\n (a ordem celula→admitsOp inverteu?)", err, tc.msg)
			}
			// O conselho do erro de OPERADOR seria enganoso aqui.
			if strings.Contains(err.Error(), "use = ou !=") {
				t.Errorf("erro = %q, want NÃO aconselhar trocar o operador: o problema é o tipo", err)
			}
		})
	}
}

// Os caminhos de ERRO de coerção que não tinham teste. O código estava certo
// nos seis; o que faltava era a rede — e a dos prefixos é a única coerção de
// literal cujo caminho de falha não era exercitado por nada.
func TestCaminhosDeErroDeCoercao(t *testing.T) {
	casos := []struct {
		nome, sql, msg string
	}{
		{"prefixo v4 inválido", `SELECT id FROM radius_sessions WHERE rede4 = 'nao-eh-prefixo'`,
			`literal "nao-eh-prefixo" inválido para a coluna "rede4" (ipv4_prefix)`},
		{"prefixo v4 com bits fora de faixa", `SELECT id FROM radius_sessions WHERE rede4 = '10.0.0.0/33'`,
			`inválido para a coluna "rede4"`},
		{"prefixo v6 inválido", `SELECT id FROM radius_sessions WHERE rede6 = 'xyz'`,
			`literal "xyz" inválido para a coluna "rede6" (ipv6_prefix)`},
		{"prefixo v6 com endereço v4", `SELECT id FROM radius_sessions WHERE rede6 = '10.0.0.0/8'`,
			`inválido para a coluna "rede6"`},
		{"ipv6 inválido", `SELECT id FROM radius_sessions WHERE v6 = 'nao-eh-ipv6'`,
			`inválido para a coluna "v6" (ipv6)`},
		{"timestamp inválido", `SELECT id FROM radius_sessions WHERE quando = 'ontem'`,
			`inválido para a coluna "quando" (timestamp)`},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			err := bindErr(t, tc.sql)
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
			// Toda mensagem de literal inválido aponta a POSIÇÃO do literal.
			if !strings.Contains(err.Error(), "posição") {
				t.Errorf("erro = %q, want citar a posição", err)
			}
		})
	}
}

// Os mesmos caminhos pelo lado do PARÂMETRO: são duas rotas de coerção, e a
// mensagem de parâmetro não tem posição no texto (o valor não está na query).
func TestCaminhosDeErroDeCoercaoComParametro(t *testing.T) {
	casos := []struct {
		nome, sql, msg string
		param          types.Value
	}{
		{"prefixo v4 inválido", `SELECT id FROM radius_sessions WHERE rede4 = ?`,
			"parâmetro 1", wireString("nao-eh-prefixo")},
		{"prefixo v6 inválido", `SELECT id FROM radius_sessions WHERE rede6 = ?`,
			"parâmetro 1", wireString("xyz")},
		{"ipv6 inválido", `SELECT id FROM radius_sessions WHERE v6 = ?`,
			"parâmetro 1", wireString("nope")},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			err := bindErr(t, tc.sql, tc.param)
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
			if strings.Contains(err.Error(), "posição") {
				t.Errorf("erro = %q: parâmetro não tem posição no texto da query", err)
			}
		})
	}
}
