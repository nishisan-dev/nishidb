package counters_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// registerBaseSpec é o dataset do Register: a key e a coluna do groupBy. Os
// blocos `counters:` de cada caso são acrescentados ao fim.
const registerBaseSpec = `version: 1
name: servicos
schemaName: telecom
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    olt:
      type: string
`

// openRegisteredDataset abre um Store de verdade com os contadores do YAML — o
// mesmo callback de compilação do servidor e do CLI — e registra o dataset num
// motor novo, sem chamar o Register.
func openRegisteredDataset(t *testing.T, yaml string) (*query.Engine, *storage.Store, *schema.DatasetSpec) {
	t.Helper()
	spec, err := schema.Parse([]byte(yaml), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	comp := counters.NewCompiler(spec)
	st, err := storage.Open(storage.Options{
		Dir:                             t.TempDir(),
		SegmentSize:                     1 << 20,
		IndexedFieldsByName:             schema.IndexedFieldNames(spec),
		DerivedColumnNames:              spec.DerivedColumnNames(),
		WriteInstantFieldName:           schema.WriteInstantFieldName,
		TTLFieldName:                    schema.TTLFieldName,
		CompileCountersCallbackFunction: comp.Callback(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng := query.NewEngine()
	if err := eng.Register(spec.Name, comp.Compiled(), st); err != nil {
		t.Fatal(err)
	}
	return eng, st, spec
}

// TestRegisterPublishesCounterMetricColumnsAndMetaTable: pela fiação real, o
// contador com métricas sai com as colunas delas e a `_meta.counter_metrics`
// lista a definição — com o uso NULL, porque não há fonte.
func TestRegisterPublishesCounterMetricColumnsAndMetaTable(t *testing.T) {
	eng, st, spec := openRegisteredDataset(t, registerBaseSpec+`counters:
  - name: por_olt
    groupBy: [olt]
    metrics:
      - name: auth_5m
        type: window
        source: total
        window: 5m
        aggregates: [avg]
`)
	if err := counters.Register(eng, spec.Name, st, spec.Counters, nil); err != nil {
		t.Fatal(err)
	}
	names, _, _ := columnsOf(t, eng, "SELECT * FROM _counters.por_olt")
	sameColumnSet(t, names, "groupKey,olt,total,rows,auth_5m_avg")
	rows := consulta(t, eng, "SELECT id, columns, groups FROM _meta.counter_metrics")
	if len(rows) != 1 || rows[0]["id"] != "por_olt|auth_5m" || rows[0]["columns"] != "auth_5m_avg" || rows[0]["groups"] != "<null>" {
		t.Fatalf("_meta.counter_metrics = %v", rows)
	}
}

// TestRegisterPublishesCounterMetricsMetaTableWithoutMetrics: a tabela é
// registrada SEMPRE — sem contador e com contador sem métrica —, e "nenhuma" é
// uma resposta, não uma tabela desconhecida.
func TestRegisterPublishesCounterMetricsMetaTableWithoutMetrics(t *testing.T) {
	for name, yaml := range map[string]string{
		"sem contador":          registerBaseSpec,
		"contador sem métricas": registerBaseSpec + "counters:\n  - name: por_olt\n    groupBy: [olt]\n",
	} {
		t.Run(name, func(t *testing.T) {
			eng, st, spec := openRegisteredDataset(t, yaml)
			if err := counters.Register(eng, spec.Name, st, spec.Counters, nil); err != nil {
				t.Fatal(err)
			}
			if rows := consulta(t, eng, "SELECT id FROM _meta.counter_metrics"); len(rows) != 0 {
				t.Fatalf("listou %v", rows)
			}
		})
	}
}

// TestRegisterRefusesOpenCounterWithoutDeclaredSpec: o contador aberto sem a
// spec correspondente é erro de programação; seguir sem as métricas publicaria
// uma tabela com schema diferente do que o alarme compila.
func TestRegisterRefusesOpenCounterWithoutDeclaredSpec(t *testing.T) {
	eng, st, spec := openRegisteredDataset(t, registerBaseSpec+"counters:\n  - name: por_olt\n    groupBy: [olt]\n")
	err := counters.Register(eng, spec.Name, st, nil, nil)
	if err == nil || !strings.Contains(err.Error(), `"por_olt"`) {
		t.Fatalf("o contador aberto sem spec tem de ser recusado nomeando-o: %v", err)
	}
}
