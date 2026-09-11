package predicate

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// CompileCondition resolve uma condição declarada em configuração — o `when`
// de uma regra de TTL, o `where` de um contador — pelo MESMO caminho do WHERE
// de uma consulta: parse, bind contra o schema compilado, programa linear.
//
// É uma função só para os dois declarantes porque a promessa deste pacote é
// que `type = 2` signifique a mesma coisa em qualquer lugar onde apareça, e
// duas sequências parse→bind→compila divergiriam na primeira recusa que uma
// delas ganhasse e a outra não. A recusa de campo de sistema é uma dessas: nem
// a duração nem o contador podem depender do instante de escrita — o primeiro
// porque ele ajuda a interpretar esse instante, o segundo porque o estado
// deixaria de ser função pura do último evento.
//
// rotulo nomeia quem está compilando nas mensagens ("ttl[1]",
// "counters[0].where"): a recusa sai na subida, para um operador lendo o log,
// e "condição inválida" sem dizer qual é inacionável.
func CompileCondition(rotulo, condition string, sc *schema.Compiled) (*Programa, binder.Slots, error) {
	e, err := parser.ParseCondition(condition)
	if err != nil {
		return nil, binder.Slots{}, fmt.Errorf("%s: condição %q: %w", rotulo, condition, err)
	}
	bc, err := binder.BindCondition(e, sc)
	if err != nil {
		return nil, binder.Slots{}, fmt.Errorf("%s: condição %q: %w", rotulo, condition, err)
	}
	for _, id := range bc.Slots.IDs {
		if f, ok := sc.FieldByID(id); ok && f.IsSystem {
			return nil, binder.Slots{}, fmt.Errorf("%s: condição %q referencia o campo de sistema %q",
				rotulo, condition, f.Name)
		}
	}
	prog, err := Compile(bc.Expr, bc.Slots.Len())
	if err != nil {
		return nil, binder.Slots{}, fmt.Errorf("%s: condição %q: %w", rotulo, condition, err)
	}
	return prog, bc.Slots, nil
}

// AvaliadorOfPayload avalia um programa diretamente sobre um payload de record:
// coleta os slots numa passada e roda o programa, sem alocar.
//
// Tem scratch próprio (slots e reader), e por isso é de UM dono por vez: o
// writer usa o seu dentro do applyRecord, e a auditoria do contador instancia
// outro para varrer sem disputar o do writer. Compartilhar um entre goroutines
// seria data race no scratch — que o -race pega no CI e nem sempre em dev.
type AvaliadorOfPayload struct {
	prog  *Programa
	slots []Slot
	fr    encoding.FieldReader
}

// NewAvaliadorOfPayload prepara o scratch para os ids que o programa lê (os
// Slots.IDs devolvidos por CompileCondition, já ordenados e sem duplicata).
func NewAvaliadorOfPayload(prog *Programa, ids []types.FieldID) *AvaliadorOfPayload {
	return &AvaliadorOfPayload{prog: prog, slots: NovosSlots(ids)}
}

// Evaluate diz se o payload satisfaz a condição. Erro de leitura sobe como
// corrupção, pela mesma razão do Coleta: tratar payload corrompido como
// "não casa" transformaria corrupção em contagem silenciosamente errada.
//
// O contador de mismatch é nil, como na ingestão: uma avaliação por evento
// não tem onde pendurar um contador por avaliação.
func (a *AvaliadorOfPayload) Evaluate(payload []byte) (bool, error) {
	if err := Coleta(a.slots, &a.fr, payload); err != nil {
		return false, err
	}
	return a.prog.Evaluate(a.slots, nil), nil
}
