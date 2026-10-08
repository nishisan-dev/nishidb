package predicate

import (
	"fmt"
	"math/bits"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// CompileAlarmCondition é CompileCondition com a comparação linear entre
// colunas ligada (`total * 10 <= rows`): o MESMO parse, bind e programa, com as
// opções do parser e do binder que só a condição de alarme liga. O `where` e o
// `scope` do contador e o `when` do TTL continuam em CompileCondition, que
// recusa coluna à direita do comparador.
func CompileAlarmCondition(label, condition string, sc *schema.Compiled) (*Programa, binder.Slots, error) {
	return compileCondition(label, condition, sc,
		parser.ConditionOptions{AllowsLinearColumnComparison: true},
		binder.ConditionOptions{AllowsLinearColumnComparison: true})
}

// int128 é um inteiro de 128 bits com sinal em complemento de dois: hi carrega
// o sinal, lo é sem sinal. Cabe todo produto de um int64 por um multiplicador
// em [1, 2^63-1] — o módulo fica abaixo de 2^126.
type int128 struct {
	hi int64
	lo uint64
}

// mulInt128 multiplica a (com sinal) por k (>= 1) em 128 bits, sem perda.
//
// bits.Mul64 é sem sinal, e por isso o produto é feito sobre o MÓDULO de a e o
// sinal reaplicado no fim. Dar a bits.Mul64 o complemento de dois de um
// negativo direto seria multiplicar 2^64 - |a| por k: o resultado é um número
// enorme e positivo, e `-3 * 2 < 1` sairia falso.
func mulInt128(a int64, k uint64) int128 {
	mag := uint64(a)
	if a < 0 {
		mag = -mag // MinInt64 incluído: o módulo 2^63 cabe em uint64
	}
	hi, lo := bits.Mul64(mag, k)
	if a < 0 {
		// Negação em 128 bits: inverte tudo e soma um, com o vai-um da metade
		// baixa para a alta.
		lo = ^lo + 1
		hi = ^hi
		if lo == 0 {
			hi++
		}
	}
	return int128{hi: int64(hi), lo: lo}
}

// cmpInt128 devolve -1, 0 ou +1. A metade alta compara com sinal; empatada, a
// baixa compara sem sinal.
func cmpInt128(a, b int128) int {
	switch {
	case a.hi < b.hi:
		return -1
	case a.hi > b.hi:
		return 1
	case a.lo < b.lo:
		return -1
	case a.lo > b.lo:
		return 1
	default:
		return 0
	}
}

// testColumns avalia `esquerda * k1 <op> direita * k2` sobre os dois slots.
// Coluna ausente compara falso para todo operador, como em qualquer termo;
// tipo físico diferente de int64 conta como mismatch e compara falso. Sem
// alocação: dois produtos de 128 bits na pilha.
func (in *instr) testColumns(slots []Slot, mismatches *int64) bool {
	l, r := &slots[in.slot], &slots[in.slot2]
	if !l.Present || !r.Present {
		return false
	}
	if l.Val.Type != types.TypeInt64 || r.Val.Type != types.TypeInt64 {
		if mismatches != nil {
			*mismatches++
		}
		return false
	}
	c := cmpInt128(mulInt128(l.Val.I64, in.leftMul), mulInt128(r.Val.I64, in.rightMul))
	switch in.cmpOp {
	case parser.CmpEq:
		return c == 0
	case parser.CmpNe:
		return c != 0
	case parser.CmpGt:
		return c > 0
	case parser.CmpGte:
		return c >= 0
	case parser.CmpLt:
		return c < 0
	default: // parser.CmpLte
		return c <= 0
	}
}

// emitColumnComparison compila uma BoundColumnComparison. As guardas repetem
// as do binder — slots na faixa, multiplicador >= 1, as duas colunas int64 —
// porque o Compile é público e recebe expressão que qualquer um pode montar,
// e um termo mal formado aqui daria resposta errada em silêncio.
func (p *Programa) emitColumnComparison(n *binder.BoundColumnComparison, nSlots int) error {
	if n.Left == nil || n.Right == nil {
		return fmt.Errorf("%w: column comparison with a nil field", ErrProgramaInvalido)
	}
	for _, side := range []struct {
		f    *schema.CompiledField
		slot int
		k    int64
	}{{n.Left, n.LeftSlot, n.LeftMultiplier}, {n.Right, n.RightSlot, n.RightMultiplier}} {
		if side.slot < 0 || side.slot >= nSlots {
			return fmt.Errorf("%w: column %q with slot %d outside [0,%d)", ErrProgramaInvalido, side.f.Name, side.slot, nSlots)
		}
		if side.k < 1 {
			return fmt.Errorf("%w: column %q with multiplier %d", ErrProgramaInvalido, side.f.Name, side.k)
		}
		if t := side.f.EffectiveType(); t != types.TypeInt64 {
			return fmt.Errorf("%w: column comparison over column %q (%s)", ErrProgramaInvalido, side.f.Name, t)
		}
	}
	p.prog = append(p.prog, instr{
		op: opTest, columns: true, cmpOp: n.Op,
		slot: int32(n.LeftSlot), slot2: int32(n.RightSlot),
		leftMul: uint64(n.LeftMultiplier), rightMul: uint64(n.RightMultiplier),
	})
	return nil
}
