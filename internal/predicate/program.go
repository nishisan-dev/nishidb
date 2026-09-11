package predicate

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrProgramaInvalido marca predicado que o binder produziu mal formado.
// É bug de plan-time, não entrada do usuário: compilar em silêncio um
// programa quebrado devolveria resultado vazio varrendo o dataset inteiro.
var ErrProgramaInvalido = errors.New("predicado: programa inválido")

// O predicado é compilado UMA vez, em Open, para um PROGRAMA LINEAR com
// saltos — e não para uma árvore percorrida recursivamente.
//
// A razão é a mesma que tornou a espinha do binder iterativa: `a=1 AND a=1 AND
// …` é left-leaning e cresce um nível por termo, sem parênteses, e uma query
// gerada por máquina chega a dezenas de milhares de termos. Uma avaliação
// recursiva pagaria essa profundidade de pilha A CADA ROW — o único lugar do
// pipeline onde o custo se multiplica pelo tamanho do resultado.
//
// O programa é executado por um laço com program counter, um acumulador e
// nenhuma pilha: curto-circuito vira salto para frente.
type opKind uint8

const (
	opTest        opKind = iota // avalia um termo e escreve no acumulador
	opJumpIfFalse               // curto-circuito do AND
	opJumpIfTrue                // curto-circuito do OR
)

type instr struct {
	op opKind

	// Termo (opTest).
	slot   int32
	isNull bool // teste de presença (IS [NOT] NULL)
	not    bool // IS NOT NULL
	cmpOp  parser.CmpOp
	kind   binder.CompareKind
	// want é o tipo FÍSICO esperado no payload. Vem do campo, não de val:
	// numa coluna int64 comparada a literal float, val.Type é double e o
	// payload continua sendo int64.
	want types.FieldType
	val  types.Value
	// like é o padrão de LIKE já compilado, ou nil. Ponteiro para o padrão do
	// binder, que é imutável depois do Open — a mesma disciplina do programa
	// inteiro, e o que permite compartilhá-lo sem cópia.
	like *binder.LikePattern

	// Salto (opJumpIfFalse/opJumpIfTrue).
	target int32
}

// Programa é o predicado compilado. Imutável após a compilação.
type Programa struct {
	prog []instr
}

// Evaluate roda o programa. Sem alocação, sem chamada recursiva, sem pilha.
//
// mismatches conta os campos cujo tipo físico não bate com o do literal —
// payload heterogêneo na mesma coluna. É ponteiro e aceita nil porque os dois
// chamadores têm necessidades opostas: o executor o publica como métrica de
// query, e a ingestão avalia uma regra por evento e não tem onde pendurar um
// contador por avaliação.
func (p *Programa) Evaluate(slots []Slot, mismatches *int64) bool {
	acc := true
	for pc := 0; pc < len(p.prog); {
		in := &p.prog[pc]
		switch in.op {
		case opTest:
			acc = in.testa(slots, mismatches)
			pc++
		case opJumpIfFalse:
			if !acc {
				pc = int(in.target)
			} else {
				pc++
			}
		default: // opJumpIfTrue
			if acc {
				pc = int(in.target)
			} else {
				pc++
			}
		}
	}
	return acc
}

func (in *instr) testa(slots []Slot, mismatches *int64) bool {
	s := &slots[in.slot]
	if in.isNull {
		// IS NULL é ausência; IS NOT NULL é presença. Vale para qualquer tipo,
		// inclusive bytes/json e campo auto nunca observado.
		return s.Present == in.not
	}
	// Campo AUSENTE compara falso para TODOS os operadores, inclusive `!=`.
	// É a intuição SQL invertida, e é deliberado: sem NOT genérico na v1, a
	// lógica trivalente não tem por onde vazar, e um `!=` "esperto" devolveria
	// justamente as rows que o IS NOT NULL nega.
	if !s.Present {
		return false
	}
	if s.Val.Type != in.want {
		// Payload heterogêneo no mesmo campo: não é ordenável contra o
		// literal, e comparar bytes de tipos diferentes daria resultado
		// arbitrário.
		if mismatches != nil {
			*mismatches++
		}
		return false
	}
	if in.like != nil {
		// LIKE entra DEPOIS das duas guardas acima, e é o que fixa a semântica
		// sem regra nova: campo ausente é false inclusive em NOT LIKE, pela
		// mesma razão que é false em `!=`. `WHERE x NOT LIKE 'a%'` não devolve
		// a linha que não tem `x` — quem quer isso escreve IS NULL.
		return in.like.Match(s.Val.Bytes) != in.not
	}
	// O despacho é pelo KIND, nunca por Val.Type: `octets > 1.5` numa coluna
	// int64 e `ratio > 1.5` numa coluna double produzem um Val IDÊNTICO —
	// mesmo Type (double) e mesmos bits. Só o Kind separa os dois, e trocá-lo
	// por Val.Type devolveria resultado vazio varrendo o dataset inteiro, sem
	// erro nenhum.
	switch in.kind {
	case binder.CompareInt:
		return ordemInt(in.cmpOp, s.Val.I64, in.val.I64)
	case binder.CompareFloatBits:
		return ordemFloat(in.cmpOp,
			math.Float64frombits(uint64(s.Val.I64)), math.Float64frombits(uint64(in.val.I64)))
	case binder.CompareIntAsFloat:
		// Coluna int64 promovida: a ESQUERDA é inteiro cru, a DIREITA é
		// Float64bits. Confundir os dois lados devolve resultado vazio em
		// silêncio.
		return ordemFloat(in.cmpOp, float64(s.Val.I64), math.Float64frombits(uint64(in.val.I64)))
	case binder.CompareBytes:
		return ordemBytes(in.cmpOp, s.Val.Bytes, in.val.Bytes)
	default:
		// Modo desconhecido não cai no caminho de bytes: um Kind novo tratado
		// como bytes compararia representações diferentes e devolveria
		// resultado errado em silêncio. O Open já recusa CompareUnset, então
		// isto é o fecho da enumeração, não uma condição alcançável.
		return false
	}
}

func ordemInt(op parser.CmpOp, a, b int64) bool {
	switch op {
	case parser.CmpEq:
		return a == b
	case parser.CmpNe:
		return a != b
	case parser.CmpGt:
		return a > b
	case parser.CmpGte:
		return a >= b
	case parser.CmpLt:
		return a < b
	default: // parser.CmpLte
		return a <= b
	}
}

func ordemFloat(op parser.CmpOp, a, b float64) bool {
	switch op {
	case parser.CmpEq:
		return a == b
	case parser.CmpNe:
		return a != b
	case parser.CmpGt:
		return a > b
	case parser.CmpGte:
		return a >= b
	case parser.CmpLt:
		return a < b
	default: // parser.CmpLte
		return a <= b
	}
}

// ordemBytes cobre string, ipv4 (4B), ipv6 (16B) e prefixos. Para os
// endereços, a ordem lexicográfica dos bytes em network order É a ordem
// numérica — big-endian existe para isso.
func ordemBytes(op parser.CmpOp, a, b []byte) bool {
	switch op {
	case parser.CmpEq:
		return bytes.Equal(a, b)
	case parser.CmpNe:
		return !bytes.Equal(a, b)
	}
	switch c := bytes.Compare(a, b); op {
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

// ---------------------------------------------------------------- compilação

// tarefa é um item da pilha explícita do compilador. A compilação também é
// iterativa: uma cadeia de 50 mil termos tem 50 mil níveis, e recursão aqui
// estouraria a pilha no Open — antes mesmo de ler a primeira row.
type tarefa struct {
	e binder.BoundExpr // compilar esta expressão

	// Quando e == nil, a tarefa é de controle:
	emiteSalto bool   // emitir o salto de curto-circuito e seguir para dir
	patch      int32  // corrigir o alvo do salto neste índice (-1 = não)
	op         opKind // salto a emitir
	dir        binder.BoundExpr
}

// Compile transforma a expressão ligada num programa linear. nSlots é o
// tamanho da lista de slots contra a qual o programa vai rodar: um slot fora
// da faixa é recusado AQUI, no plan-time, e não silenciosamente na avaliação.
func Compile(e binder.BoundExpr, nSlots int) (*Programa, error) {
	p := &Programa{}
	if e == nil {
		return p, nil
	}
	pilha := []tarefa{{e: e}}
	for len(pilha) > 0 {
		t := pilha[len(pilha)-1]
		pilha = pilha[:len(pilha)-1]

		switch {
		case t.e == nil && t.emiteSalto:
			// Esquerda já emitida: emite o salto e agenda a direita e o patch.
			idx := int32(len(p.prog))
			p.prog = append(p.prog, instr{op: t.op})
			pilha = append(pilha, tarefa{patch: idx}, tarefa{e: t.dir})

		case t.e == nil:
			p.prog[t.patch].target = int32(len(p.prog))

		default:
			if err := p.emite(t.e, nSlots, &pilha); err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

func (p *Programa) emite(e binder.BoundExpr, nSlots int, pilha *[]tarefa) error {
	switch n := e.(type) {
	case *binder.BoundLogical:
		op := opJumpIfFalse
		if n.Op == parser.LogicalOr {
			op = opJumpIfTrue
		}
		// LIFO: a esquerda é compilada primeiro, e só então o salto.
		*pilha = append(*pilha, tarefa{emiteSalto: true, op: op, dir: n.Right}, tarefa{e: n.Left})
		return nil

	case *binder.BoundComparison:
		if n.Field == nil {
			return fmt.Errorf("%w: comparação com campo nulo", ErrProgramaInvalido)
		}
		if n.Kind == binder.CompareUnset {
			return fmt.Errorf("%w: coluna %q com modo de comparação não definido",
				ErrProgramaInvalido, n.Field.Name)
		}
		t := n.Field.EffectiveType()
		if t == types.TypeBytes || t == types.TypeJSON {
			return fmt.Errorf("%w: comparação sobre coluna %q (%s) não é suportada",
				ErrProgramaInvalido, n.Field.Name, t)
		}
		if n.Slot < 0 || n.Slot >= nSlots {
			return fmt.Errorf("%w: coluna %q com slot %d fora de [0,%d)",
				ErrProgramaInvalido, n.Field.Name, n.Slot, nSlots)
		}
		p.prog = append(p.prog, instr{
			op: opTest, slot: int32(n.Slot), cmpOp: n.Op, kind: n.Kind, want: t, val: n.Val,
		})
		return nil

	case *binder.BoundNullCheck:
		if n.Field == nil {
			return fmt.Errorf("%w: IS NULL com campo nulo", ErrProgramaInvalido)
		}
		if n.Slot < 0 || n.Slot >= nSlots {
			return fmt.Errorf("%w: coluna %q com slot %d fora de [0,%d)",
				ErrProgramaInvalido, n.Field.Name, n.Slot, nSlots)
		}
		p.prog = append(p.prog, instr{op: opTest, slot: int32(n.Slot), isNull: true, not: n.Not})
		return nil

	case *binder.BoundLike:
		if n.Field == nil {
			return fmt.Errorf("%w: LIKE com campo nulo", ErrProgramaInvalido)
		}
		if n.Slot < 0 || n.Slot >= nSlots {
			return fmt.Errorf("%w: coluna %q com slot %d fora de [0,%d)",
				ErrProgramaInvalido, n.Field.Name, n.Slot, nSlots)
		}
		if n.Pat.Type == binder.PatternInvalid {
			// Zero value do padrão: BoundLike construído sem CompileLike. É bug
			// de plan-time, e casar nada em silêncio o esconderia.
			return fmt.Errorf("%w: LIKE em %q com padrão não compilado", ErrProgramaInvalido, n.Field.Name)
		}
		p.prog = append(p.prog, instr{
			op: opTest, slot: int32(n.Slot), not: n.Not,
			want: types.TypeString, like: &n.Pat})
		return nil

	default:
		// Nó novo no bound-AST tem de falhar ALTO, não ser ignorado: ignorar um
		// termo do WHERE devolve linhas a mais, em silêncio.
		return fmt.Errorf("%w: expressão de tipo %T não suportada", ErrProgramaInvalido, e)
	}
}
