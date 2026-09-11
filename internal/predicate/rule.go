package predicate

import (
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// RegraCompilada é uma regra de TTL pronta para avaliar. Prog nil é a regra de
// FUNDO: ela casa com tudo, e é por isso que a lista para nela.
type RegraCompilada struct {
	Prog     *Programa
	Slots    binder.Slots
	Duration time.Duration
}

// CompileRegras transforma as regras validadas em programas avaliáveis.
//
// Mora AQUI, e não em schema, porque compilar exige o binder — e o binder
// importa schema. Este pacote é o único lugar da árvore que pode importar os
// dois sem fechar ciclo.
func CompileRegras(regras []schema.TTLRule, sc *schema.Compiled) ([]RegraCompilada, error) {
	out := make([]RegraCompilada, 0, len(regras))
	for i, r := range regras {
		if r.When == "" {
			out = append(out, RegraCompilada{Duration: r.Duration})
			continue
		}
		prog, slots, err := CompileCondition(fmt.Sprintf("ttl[%d]", i), r.When, sc)
		if err != nil {
			return nil, err
		}
		out = append(out, RegraCompilada{Prog: prog, Slots: slots, Duration: r.Duration})
	}
	return out, nil
}
