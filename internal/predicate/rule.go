package predicate

import (
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// CompiledRule é uma regra de TTL pronta para avaliar.
//
// Prog nil é a regra sem `when`; ela só casa com tudo — e é a de FUNDO — se
// também não tiver âncora (Fallback). Anchor é o FieldID do campo temporal de
// que o prazo é contado, e zero quando a regra conta da escrita: o id zero é
// reservado (§32), então não há colisão.
type CompiledRule struct {
	Prog     *Programa
	Slots    binder.Slots
	Anchor   types.FieldID
	Fallback bool
	Duration time.Duration
}

// CompileRules transforma as regras validadas em programas avaliáveis.
//
// Mora AQUI, e não em schema, porque compilar exige o binder — e o binder
// importa schema. Este pacote é o único lugar da árvore que pode importar os
// dois sem fechar ciclo.
//
// O campo da âncora já foi validado pelo schema.Compile (existe, é declarado e
// temporal); aqui ele só é resolvido para o FieldID que o materializer procura
// no payload.
func CompileRules(rules []schema.TTLRule, sc *schema.Compiled) ([]CompiledRule, error) {
	out := make([]CompiledRule, 0, len(rules))
	for i, r := range rules {
		cr := CompiledRule{Duration: r.Duration, Fallback: r.IsFallback()}
		if r.Anchor != "" {
			f, ok := sc.FieldByName([]byte(r.Anchor))
			if !ok {
				return nil, fmt.Errorf("ttl[%d]: anchor %q is not a field of the dataset", i, r.Anchor)
			}
			cr.Anchor = f.ID
		}
		if r.When != "" {
			prog, slots, err := CompileCondition(fmt.Sprintf("ttl[%d]", i), r.When, sc)
			if err != nil {
				return nil, err
			}
			cr.Prog, cr.Slots = prog, slots
		}
		out = append(out, cr)
	}
	return out, nil
}
