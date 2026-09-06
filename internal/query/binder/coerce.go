package binder

import (
	"math"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// CompareKind diz ao executor COMO comparar o valor da row com Val, sem
// re-derivar tipos por row.
//
// São cinco e não três por uma razão concreta: o payload de uma coluna int64
// guarda o inteiro CRU em Value.I64, enquanto o de double guarda
// math.Float64bits. Com um único "CompareFloat", `WHERE connect_info > 1.5`
// compararia Float64frombits(1000) ≈ 4.94e-321 contra 1.5 — resultado ERRADO,
// e silencioso.
//
// CompareUnset (0) é sempre bug: o zero value não pode ser um modo válido.
type CompareKind uint8

const (
	CompareUnset CompareKind = iota
	// CompareInt: os dois lados são int64 cru em Value.I64 (int64, timestamp,
	// boolean).
	CompareInt
	// CompareFloatBits: os dois lados são math.Float64bits em Value.I64
	// (coluna double).
	//
	// O executor converte os DOIS lados com math.Float64frombits ANTES de
	// comparar, inclusive para ordem. Comparar os bits crus funciona para
	// igualdade e MENTE para ordem: o sinal mora no bit mais alto, então
	// -1 > -2 devolve false no espaço de bits. Com valores positivos as duas
	// ordens coincidem, e é por isso que o erro sobrevive a um teste
	// descuidado.
	CompareFloatBits
	// CompareIntAsFloat: ESQUERDA (payload) é int64 cru, DIREITA (literal) é
	// Float64bits. Único caso: coluna int64 comparada a literal float.
	CompareIntAsFloat
	// CompareBytes: bytes.Compare/Equal sobre Value.Bytes.
	CompareBytes
)

var compareKindNames = [...]string{
	CompareUnset:      "unset",
	CompareInt:        "int",
	CompareFloatBits:  "float-bits",
	CompareIntAsFloat: "int-as-float",
	CompareBytes:      "bytes",
}

func (k CompareKind) String() string {
	if int(k) < len(compareKindNames) && compareKindNames[k] != "" {
		return compareKindNames[k]
	}
	return "CompareKind(?)"
}

// Comparable devolve o modo de comparação de uma coluna contra um tipo de
// literal, ou o erro da tabela normativa §3.6.
//
// col é o tipo EFETIVO da coluna (declarado, ou o tipo fixado de um campo
// auto); isAuto diz que ele veio de campo auto — usado só para a mensagem
// quando col é TypeNull (campo auto nunca observado).
//
// Esta tabela tem UM dono. O executor recebe o Kind pronto e não a
// reimplementa: duas cópias divergiriam em silêncio, que é exatamente o risco
// que a query compartilhar os parsers da ingestão existe para evitar.
func Comparable(col types.FieldType, isAuto bool, lit parser.OperandKind, op parser.CmpOp) (CompareKind, error) {
	// A ordem das checagens é fixa porque determina QUAL erro aparece
	// primeiro, e a mensagem é contrato — fixado por
	// TestOrdemDasChecagensDeComparable.
	//
	// O tipo do literal é checado ANTES do operador: numa coluna de operador
	// restrito com literal incompatível as duas checagens falhariam, e a
	// mensagem de operador daria um conselho ERRADO ("use = ou !=" não resolve
	// quando o problema é o tipo).
	switch col {
	case types.TypeNull:
		if isAuto {
			return CompareUnset, errAutoSemTipo
		}
		return CompareUnset, errColunaSemTipo
	case types.TypeBytes, types.TypeJSON:
		return CompareUnset, errNaoComparavel
	}

	kind, ok := celula(col, lit)
	if !ok {
		return CompareUnset, errLiteralIncompativel
	}
	if !admitsOp(col, op) {
		return CompareUnset, errOperadorNaoAdmitido
	}
	return kind, nil
}

// Sentinelas internos: o Bind os traduz para a mensagem final, que precisa do
// nome da coluna e do tipo — informação que a tabela não tem.
var (
	errAutoSemTipo         = &tabelaErr{"auto-sem-tipo"}
	errColunaSemTipo       = &tabelaErr{"coluna-sem-tipo"}
	errNaoComparavel       = &tabelaErr{"nao-comparavel"}
	errLiteralIncompativel = &tabelaErr{"literal-incompativel"}
	errOperadorNaoAdmitido = &tabelaErr{"operador-nao-admitido"}
)

type tabelaErr struct{ code string }

func (e *tabelaErr) Error() string { return e.code }

// celula devolve o modo de comparação de (coluna × tipo de literal), ou
// ok=false para célula vazia da tabela.
//
// Não existe coerção implícita entre string e número aqui, ao contrário da
// INGESTÃO (que aceita "1000" para coluna int64 por causa do Connect-Info da
// amostra real). A divergência é deliberada: na ingestão a string vem do
// produtor e não há alternativa; na query o usuário escolhe como escrever o
// literal, e aceitar as duas formas cria ambiguidade sem ganho.
func celula(col types.FieldType, lit parser.OperandKind) (CompareKind, bool) {
	switch col {
	case types.TypeString:
		if lit == parser.OperandString {
			return CompareBytes, true
		}
	case types.TypeInt64:
		switch lit {
		case parser.OperandInt:
			return CompareInt, true
		case parser.OperandFloat:
			// Coluna int64 contra literal float: o executor converte o I64 da
			// row para float64 e o literal com Float64frombits. NÃO arredonde
			// o literal aqui — `c = 1.5` tem de dar zero resultados, não
			// `c = 1` nem `c = 2`.
			return CompareIntAsFloat, true
		}
	case types.TypeDouble:
		switch lit {
		case parser.OperandInt, parser.OperandFloat:
			return CompareFloatBits, true
		}
	case types.TypeBool:
		if lit == parser.OperandBool {
			return CompareInt, true
		}
	case types.TypeTimestamp:
		switch lit {
		case parser.OperandString, parser.OperandInt:
			return CompareInt, true
		}
	case types.TypeDateTime:
		// String é o texto no format DECLARADO do campo, não RFC3339 — ver o
		// porquê em coerceLiteral. Inteiro é epoch millis, como no timestamp.
		switch lit {
		case parser.OperandString, parser.OperandInt:
			return CompareInt, true
		}
	case types.TypeTime:
		// O inteiro aqui NÃO é epoch: é millis desde a meia-noite. É a mesma
		// palavra ("número") significando outra coisa, e por isso esta linha
		// da tabela precisa dizê-lo — sem isso alguém escreve
		// `hora = 1786568400000` e recebe zero linhas achando que é dado.
		switch lit {
		case parser.OperandString, parser.OperandInt:
			return CompareInt, true
		}
	case types.TypeIPv4, types.TypeIPv6, types.TypeIPv4Prefix, types.TypeIPv6Prefix:
		if lit == parser.OperandString {
			return CompareBytes, true
		}
	}
	return CompareUnset, false
}

// admitsOp restringe os operadores por tipo. Ordem só faz sentido onde há
// ordem: booleano e prefixo de rede admitem igualdade e diferença, nada mais.
func admitsOp(col types.FieldType, op parser.CmpOp) bool {
	switch col {
	case types.TypeBool, types.TypeIPv4Prefix, types.TypeIPv6Prefix:
		return op == parser.CmpEq || op == parser.CmpNe
	default:
		return true
	}
}

// admiteOrdenacao reporta se uma coluna pode ser CHAVE DE ORDENAÇÃO, e devolve
// o CompareKind a usar.
//
// DIVERGE do admitsOp acima de propósito, nos dois sentidos, e o contraste é o
// ponto:
//
//   - `bool` PODE ser ordenado (false < true). O admitsOp recusa `>` em bool
//     porque `x > true` é filtro sem sentido; `ORDER BY ativo DESC` é pedido
//     legítimo e sem ambiguidade.
//   - `ipv4_prefix`/`ipv6_prefix` NÃO podem. A ordem de bytes deles é
//     endereço-e-máscara, que PARECE ordem de rede e não é — 10.0.0.0/8
//     ordenaria antes de 10.0.0.0/24 por causa do byte da máscara, não do
//     alcance. Ordem que parece certa e não é seria pior que ordem nenhuma.
//
// `bytes` e `json` ficam de fora pela mesma razão que já os mantém fora da
// comparação: não há ordem definida sobre eles neste produto.
func admiteOrdenacao(col types.FieldType) (CompareKind, bool) {
	switch col {
	case types.TypeInt64, types.TypeBool, types.TypeTimestamp,
		types.TypeDateTime, types.TypeTime:
		return CompareInt, true
	case types.TypeDouble:
		return CompareFloatBits, true
	case types.TypeString, types.TypeIPv4, types.TypeIPv6:
		return CompareBytes, true
	default:
		// TypeNull (auto nunca observado), TypeBytes, TypeJSON e os prefixos.
		return CompareUnset, false
	}
}

// coerceLiteral converte o operando da AST para a representação binária da
// COLUNA, com os parsers da ingestão.
//
// arena é o buffer do BoundQuery: os valores parseados (ips, prefixos) são
// anexados nele. O caller materializa as janelas DEPOIS de todos os appends —
// ver a disciplina de offsets no Bind.
func coerceLiteral(f *schema.CompiledField, op parser.CmpOp, lit parser.Operand, arena []byte) (types.Value, CompareKind, []byte, error) {
	t := f.EffectiveType()
	kind, err := Comparable(t, f.IsAuto, lit.Kind, op)
	if err != nil {
		return types.Value{}, CompareUnset, arena, err
	}

	switch t {
	case types.TypeString:
		// Cópia, não janela sobre o SQL: o BoundQuery pode sobreviver ao
		// string original, e devolver bytes que aliasam a query seria um
		// contrato de lifetime não documentado.
		return types.Value{Type: types.TypeString, Bytes: []byte(lit.S)}, kind, arena, nil

	case types.TypeInt64:
		if lit.Kind == parser.OperandInt {
			return types.Value{Type: types.TypeInt64, I64: lit.I}, kind, arena, nil
		}
		return types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(lit.F))}, kind, arena, nil

	case types.TypeDouble:
		f64 := lit.F
		if lit.Kind == parser.OperandInt {
			f64 = float64(lit.I)
		}
		return types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(f64))}, kind, arena, nil

	case types.TypeBool:
		var i int64
		if lit.B {
			i = 1
		}
		return types.Value{Type: types.TypeBool, I64: i}, kind, arena, nil

	case types.TypeTimestamp:
		if lit.Kind == parser.OperandInt {
			// Número é epoch MILLIS, sem heurística de segundos.
			return types.Value{Type: types.TypeTimestamp, I64: lit.I}, kind, arena, nil
		}
		ms, perr := typeparse.RFC3339Millis([]byte(lit.S))
		if perr != nil {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		return types.Value{Type: types.TypeTimestamp, I64: ms}, kind, arena, nil

	case types.TypeDateTime:
		if lit.Kind == parser.OperandInt {
			return types.Value{Type: types.TypeDateTime, I64: lit.I}, kind, arena, nil
		}
		// O literal usa o MESMO format do campo, e não RFC3339. É a razão de
		// existir do typeparse compartilhado: a propriedade que vale é "o
		// texto que o produtor mandou, escrito como literal, acha a row".
		// Aceitar RFC3339 também criaria um texto que a query aceita e a
		// ingestão recusa — a divergência que o risco R3 nomeia. Quem quer
		// uma grafia independente de format escreve o epoch millis.
		//
		// time.Local pelo mesmo motivo da ingestão: os dois lados resolvem o
		// wall-clock com o fuso do host, ou não concordariam sobre o texto.
		if f.Layout == nil {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		ms, perr := f.Layout.DateTime([]byte(lit.S), time.Local)
		if perr != nil {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		return types.Value{Type: types.TypeDateTime, I64: ms}, kind, arena, nil

	case types.TypeTime:
		if lit.Kind == parser.OperandInt {
			// Millis do dia, não epoch. Fora da faixa é literal inválido e
			// não zero linhas: `hora = 1786568400000` é engano de quem
			// escreveu, e devolver vazio o faria parecer resposta.
			if lit.I < 0 || lit.I >= types.MillisPerDay {
				return types.Value{}, CompareUnset, arena, errLiteralInvalido
			}
			return types.Value{Type: types.TypeTime, I64: lit.I}, kind, arena, nil
		}
		if f.Layout == nil {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		ms, perr := f.Layout.TimeOfDay([]byte(lit.S))
		if perr != nil {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		return types.Value{Type: types.TypeTime, I64: ms}, kind, arena, nil

	case types.TypeIPv4:
		a, b, c, d, ok := typeparse.IPv4([]byte(lit.S))
		if !ok {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		start := len(arena)
		arena = append(arena, a, b, c, d)
		return types.Value{Type: types.TypeIPv4, Bytes: arena[start:len(arena):len(arena)]}, kind, arena, nil

	case types.TypeIPv6:
		addr, ok := typeparse.IPv6([]byte(lit.S))
		if !ok {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		start := len(arena)
		arena = append(arena, addr[:]...)
		return types.Value{Type: types.TypeIPv6, Bytes: arena[start:len(arena):len(arena)]}, kind, arena, nil

	case types.TypeIPv4Prefix:
		addr, bits, ok := typeparse.IPv4Prefix([]byte(lit.S))
		if !ok {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		start := len(arena)
		arena = append(arena, addr[:]...)
		arena = append(arena, bits)
		return types.Value{Type: types.TypeIPv4Prefix, Bytes: arena[start:len(arena):len(arena)]}, kind, arena, nil

	case types.TypeIPv6Prefix:
		addr, bits, ok := typeparse.IPv6Prefix([]byte(lit.S))
		if !ok {
			return types.Value{}, CompareUnset, arena, errLiteralInvalido
		}
		start := len(arena)
		arena = append(arena, addr[:]...)
		arena = append(arena, bits)
		return types.Value{Type: types.TypeIPv6Prefix, Bytes: arena[start:len(arena):len(arena)]}, kind, arena, nil
	}
	// Comparable já recusou todo tipo que não está acima.
	return types.Value{}, CompareUnset, arena, errLiteralIncompativel
}

var errLiteralInvalido = &tabelaErr{"literal-invalido"}

// coerceParam converte um parâmetro de wire para a representação binária da
// COLUNA, pela MESMA tabela dos literais.
//
// Os tipos de wire aceitos são só os quatro escalares: string, int64, double
// e boolean. Parâmetro já em representação binária (um TypeIPv4 com 4 bytes,
// por exemplo) é recusado — um caminho de coerção só, um conjunto de testes
// só. Parâmetro NULL também é recusado: `col = ?` com ? nulo não tem
// semântica definida sem lógica trivalente, e o que o usuário quer é IS NULL.
func coerceParam(f *schema.CompiledField, op parser.CmpOp, wire types.Value, arena []byte) (types.Value, CompareKind, []byte, error) {
	lit := parser.Operand{}
	switch wire.Type {
	case types.TypeString:
		lit.Kind = parser.OperandString
		lit.S = string(wire.Bytes)
	case types.TypeInt64:
		lit.Kind = parser.OperandInt
		lit.I = wire.I64
	case types.TypeDouble:
		lit.Kind = parser.OperandFloat
		lit.F = math.Float64frombits(uint64(wire.I64))
	case types.TypeBool:
		lit.Kind = parser.OperandBool
		lit.B = wire.I64 != 0
	case types.TypeNull:
		return types.Value{}, CompareUnset, arena, errParamNulo
	default:
		return types.Value{}, CompareUnset, arena, errParamTipo
	}
	return coerceLiteral(f, op, lit, arena)
}

var (
	errParamNulo = &tabelaErr{"param-nulo"}
	errParamTipo = &tabelaErr{"param-tipo"}
)
