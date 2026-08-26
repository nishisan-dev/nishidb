package predicado

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// O predicado compilado é um PROGRAMA LINEAR COM SALTOS, e a afirmação de que
// ele é equivalente à árvore que o originou vale para precedência,
// agrupamento e curto-circuito ao mesmo tempo. Uma afirmação dessas não se
// verifica por inspeção: verifica-se por DIFERENCIAL contra um avaliador
// recursivo de referência, sobre árvores geradas e sobre todos os estados de
// slot.
//
// A rede antiga não segurava isso. A mutação que desliga o salto do AND
// (`if !acc` → `if !acc && false`) faz `A AND B` devolver o resultado de B, e
// passava na suíte inteira: as únicas queries com AND no nível do executor
// usavam interseção de bitmaps, onde a FONTE já pré-filtra e o predicado é
// redundante. No OR a fonte é união, que não pré-filtra — por isso as
// mutações do OR morriam e as do AND não. A assimetria era estrutural, não
// descuido.

// avalia é o avaliador de REFERÊNCIA: recursivo, óbvio, escrito para ser
// obviamente correto e não para ser rápido. É o oráculo.
func avalia(e binder.BoundExpr, slots []Slot) bool {
	switch n := e.(type) {
	case *binder.BoundLogical:
		if n.Op == parser.LogicalAnd {
			return avalia(n.Left, slots) && avalia(n.Right, slots)
		}
		return avalia(n.Left, slots) || avalia(n.Right, slots)
	case *binder.BoundComparison:
		s := slots[n.Slot]
		if !s.Present {
			return false
		}
		if s.Val.Type != n.Field.EffectiveType() {
			return false
		}
		return ordemInt(n.Op, s.Val.I64, n.Val.I64)
	case *binder.BoundNullCheck:
		return slots[n.Slot].Present == n.Not
	default:
		panic("nó desconhecido no oráculo")
	}
}

func campoInt(id types.FieldID) *schema.CompiledField {
	return &schema.CompiledField{ID: id, Name: fmt.Sprintf("c%d", id), Type: types.TypeInt64}
}

func cmpInt(sl int, op parser.CmpOp, v int64) *binder.BoundComparison {
	return &binder.BoundComparison{
		Field: campoInt(types.FieldID(sl + 1)), Slot: sl, Op: op,
		Kind: binder.CompareInt, Val: types.Value{Type: types.TypeInt64, I64: v},
	}
}

// estados enumera TODAS as combinações de (ausente, valor 0, valor 1) para k
// slots: 3^k. É a varredura exaustiva do espaço que o predicado observa.
func estados(k int) [][]Slot {
	total := 1
	for i := 0; i < k; i++ {
		total *= 3
	}
	out := make([][]Slot, 0, total)
	for n := 0; n < total; n++ {
		s := make([]Slot, k)
		x := n
		for i := 0; i < k; i++ {
			s[i].ID = types.FieldID(i + 1)
			switch x % 3 {
			case 0:
				s[i].Present = false
			case 1:
				s[i].Present, s[i].Val = true, types.Value{Type: types.TypeInt64, I64: 0}
			case 2:
				s[i].Present, s[i].Val = true, types.Value{Type: types.TypeInt64, I64: 1}
			}
			x /= 3
		}
		out = append(out, s)
	}
	return out
}

func confere(t *testing.T, nome string, e binder.BoundExpr, k int) {
	t.Helper()
	p, err := Compila(e, k)
	if err != nil {
		t.Fatalf("%s: Compila: %v", nome, err)
	}
	var mismatches int64
	for _, s := range estados(k) {
		want := avalia(e, s)
		if got := p.Avalia(s, &mismatches); got != want {
			t.Fatalf("%s com slots %v: programa = %v, árvore = %v", nome, resumo(s), got, want)
		}
	}
}

func resumo(s []Slot) string {
	out := ""
	for _, sl := range s {
		if !sl.Present {
			out += "_"
			continue
		}
		out += fmt.Sprintf("%d", sl.Val.I64)
	}
	return out
}

// As dez formas escritas à mão: precedência, agrupamento e as duas ordens de
// curto-circuito. Se o programa divergir da árvore em qualquer estado de
// slot, o teste nomeia a expressão e o estado.
func TestProvaPrecedenciaEAgrupamento(t *testing.T) {
	a := cmpInt(0, parser.CmpEq, 1)
	b := cmpInt(1, parser.CmpEq, 1)
	c := cmpInt(2, parser.CmpEq, 1)
	e := func(op parser.LogicalOp, l, r binder.BoundExpr) *binder.BoundLogical {
		return &binder.BoundLogical{Op: op, Left: l, Right: r}
	}
	and := func(l, r binder.BoundExpr) *binder.BoundLogical { return e(parser.LogicalAnd, l, r) }
	or := func(l, r binder.BoundExpr) *binder.BoundLogical { return e(parser.LogicalOr, l, r) }

	casos := map[string]binder.BoundExpr{
		"a AND b":                and(a, b),
		"a OR b":                 or(a, b),
		"a AND b AND c":          and(and(a, b), c),
		"a OR b OR c":            or(or(a, b), c),
		"(a OR b) AND c":         and(or(a, b), c),
		"a OR (b AND c)":         or(a, and(b, c)),
		"(a AND b) OR c":         or(and(a, b), c),
		"a AND (b OR c)":         and(a, or(b, c)),
		"(a AND b) OR (a AND c)": or(and(a, b), and(a, c)),
		"a IS NULL AND b":        and(&binder.BoundNullCheck{Field: campoInt(1), Slot: 0}, b),
	}
	for nome, expr := range casos {
		t.Run(nome, func(t *testing.T) { confere(t, nome, expr, 3) })
	}
}

// E o diferencial sobre árvores GERADAS: a rede não depende de eu ter
// imaginado a forma certa.
func TestProvaProgramaIgualArvoreEmArvoresGeradas(t *testing.T) {
	const (
		arvores = 400
		slots   = 3
	)
	r := rand.New(rand.NewPCG(42, 1))

	var monta func(prof int) binder.BoundExpr
	monta = func(prof int) binder.BoundExpr {
		if prof == 0 || r.IntN(3) == 0 {
			sl := r.IntN(slots)
			if r.IntN(4) == 0 {
				return &binder.BoundNullCheck{Field: campoInt(types.FieldID(sl + 1)), Slot: sl, Not: r.IntN(2) == 0}
			}
			ops := []parser.CmpOp{parser.CmpEq, parser.CmpNe, parser.CmpGt, parser.CmpGte, parser.CmpLt, parser.CmpLte}
			return cmpInt(sl, ops[r.IntN(len(ops))], int64(r.IntN(2)))
		}
		op := parser.LogicalAnd
		if r.IntN(2) == 0 {
			op = parser.LogicalOr
		}
		return &binder.BoundLogical{Op: op, Left: monta(prof - 1), Right: monta(prof - 1)}
	}

	for i := 0; i < arvores; i++ {
		expr := monta(4)
		confere(t, fmt.Sprintf("árvore %d", i), expr, slots)
	}
}
