package predicado

import (
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// RegraCompilada é uma regra de TTL pronta para avaliar. Prog nil é a regra de
// FUNDO: ela casa com tudo, e é por isso que a lista para nela.
type RegraCompilada struct {
	Prog    *Programa
	Slots   binder.Slots
	Duracao time.Duration
}

// CompilaRegras transforma as regras validadas em programas avaliáveis.
//
// Mora AQUI, e não em schema, porque compilar exige o binder — e o binder
// importa schema. Este pacote é o único lugar da árvore que pode importar os
// dois sem fechar ciclo.
func CompilaRegras(regras []schema.RegraTTL, sc *schema.Compiled) ([]RegraCompilada, error) {
	out := make([]RegraCompilada, 0, len(regras))
	for i, r := range regras {
		if r.When == "" {
			out = append(out, RegraCompilada{Duracao: r.Duracao})
			continue
		}
		e, err := parser.ParseCondicao(r.When)
		if err != nil {
			return nil, fmt.Errorf("ttl[%d]: condição %q: %w", i, r.When, err)
		}
		bc, err := binder.BindCondicao(e, sc)
		if err != nil {
			return nil, fmt.Errorf("ttl[%d]: condição %q: %w", i, r.When, err)
		}
		for _, id := range bc.Slots.IDs {
			// A duração não pode depender de si mesma, nem do instante que ela
			// própria ajuda a interpretar.
			if f, ok := sc.FieldByID(id); ok && f.IsSystem {
				return nil, fmt.Errorf("ttl[%d]: condição %q referencia o campo de sistema %q",
					i, r.When, f.Name)
			}
		}
		prog, err := Compila(bc.Expr, bc.Slots.Len())
		if err != nil {
			return nil, fmt.Errorf("ttl[%d]: condição %q: %w", i, r.When, err)
		}
		out = append(out, RegraCompilada{Prog: prog, Slots: bc.Slots, Duracao: r.Duracao})
	}
	return out, nil
}
