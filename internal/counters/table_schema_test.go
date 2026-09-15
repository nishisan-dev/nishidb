package counters_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestTableSchemaRefusesGroupingColumnNamedAfterFixedColumn fixa a checagem
// defensiva do TableSchema: uma Definition montada sem passar pelo YAML, com
// uma coluna do groupBy chamada como uma coluna fixa, é recusada pela mesma
// lista de colunas da validação (schema.CounterColumns), com as duas pontas
// nomeadas.
func TestTableSchemaRefusesGroupingColumnNamedAfterFixedColumn(t *testing.T) {
	for _, fixed := range []string{schema.CounterKeyColumn, schema.CounterTotalColumn} {
		t.Run(fixed, func(t *testing.T) {
			def := counter.Definition{
				Name:    "c",
				Columns: []types.FieldID{fOlt},
				Names:   []string{fixed},
				Types:   []types.FieldType{types.TypeString},
			}
			_, err := counters.TableSchema(def, nil)
			if err == nil {
				t.Fatalf("groupBy %q aceito; a coluna colide com a fixa da tabela", fixed)
			}
			for _, want := range []string{"groupBy column " + `"` + fixed + `"`, "fixed column " + `"` + fixed + `"`} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("erro %q não menciona %q", err, want)
				}
			}
		})
	}
}
