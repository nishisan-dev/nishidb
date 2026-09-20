// Package counters é a cola do contador mantido na escrita com o resto do
// produto: compila o bloco `counters:` do YAML contra o schema (nomes →
// FieldID, `where` pelo mesmo caminho do WHERE), entrega ao storage o callback
// de abertura, e serve as tabelas virtuais `_counters.<name>` ao motor de
// consulta.
//
// Mora fora de storage e fora de query porque precisa dos dois lados —
// schema, binder e predicado de um, contador e Store do outro — e nenhum dos
// dois pode importar o outro.
package counters

import (
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Compile resolve as specs dos contadores contra o schema compilado.
//
// Aqui entram as recusas que a validação do YAML não pode fazer sem o schema:
// o `where` pelo CompileCondition (parse, bind, campo de sistema), e a recusa de
// `where` ou groupBy sobre coluna com `merge: sum` — com a soma, o estado deixa
// de ser função pura do último evento e a idempotência do contador evapora
// (planning §10). É recusa na subida, não documentação.
func Compile(spec *schema.DatasetSpec, sc *schema.Compiled) ([]counter.Definition, error) {
	if spec == nil || sc == nil {
		return nil, fmt.Errorf("contadores: spec ou schema ausentes")
	}
	out := make([]counter.Definition, 0, len(spec.Counters))
	var err error
	for i, cs := range spec.Counters {
		label := fmt.Sprintf("counters[%d]", i)
		def := counter.Definition{
			Name:              cs.Name,
			DisableTotal:      cs.DisableTotal,
			Columns:           make([]types.FieldID, 0, len(cs.GroupBy)),
			Names:             cs.GroupBy,
			Types:             make([]types.FieldType, 0, len(cs.GroupBy)),
			Where:             cs.Where,
			MaxGroups:         cs.MaxGroups,
			RetainEmptyGroups: cs.RetainEmptyGroups,
		}
		if sc.WriteInstantField != nil {
			def.WriteInstantField = sc.WriteInstantField.ID
		}
		for _, m := range cs.Measures {
			def.Measures = append(def.Measures, counter.MeasureDefinition{Name: m.Name, Window: m.Window, Unit: m.UnitDuration()})
		}
		for _, col := range cs.GroupBy {
			f, ok := sc.FieldByName([]byte(col))
			if !ok {
				return nil, fmt.Errorf("%s.groupBy: coluna %q não existe no schema compilado", label, col)
			}
			if f.Accumulates {
				return nil, fmt.Errorf("%s.groupBy: coluna %q tem merge: sum — o valor somado não é estado do último evento, e o grupo mudaria a cada reprocessamento", label, col)
			}
			def.Columns = append(def.Columns, f.ID)
			def.Types = append(def.Types, f.Type)
		}
		cols := keyformat.Columns(def.Names, def.Types)
		if cs.KeyFormat == "" {
			def.Format = keyformat.Default(cols)
		} else if def.Format, err = keyformat.Parse(cs.KeyFormat, cols); err != nil {
			// A spec já recusou na subida, pelo MESMO parser. Chegar aqui é
			// spec e schema compilado discordarem sobre os tipos das colunas.
			return nil, fmt.Errorf("%s.keyFormat: %w", label, err)
		}
		if cs.Where != "" {
			prog, slots, err := predicate.CompileCondition(label+".where", cs.Where, sc)
			if err != nil {
				return nil, err
			}
			for _, id := range slots.IDs {
				if f, ok := sc.FieldByID(id); ok && f.Accumulates {
					return nil, fmt.Errorf("%s.where: condição %q referencia %q, que tem merge: sum — a soma não é idempotente e o contador deixaria de ser função do estado", label, cs.Where, f.Name)
				}
			}
			ids := slots.IDs
			def.NewEvaluator = func() counter.Evaluator {
				return predicate.NewAvaliadorOfPayload(prog, ids)
			}
		}
		out = append(out, def)
	}
	return out, nil
}
