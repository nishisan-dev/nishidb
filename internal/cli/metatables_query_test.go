package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// idsOfMetaTables roda `SELECT id FROM _meta.tables ORDER BY id` pelo
// `nishidb query --config` e devolve os endereços juntos, na ordem em que
// saíram.
//
// A comparação é da LISTA INTEIRA, e não de um `Contains`: o que este teste
// cobre é a fiação, e uma asserção que só procurasse `_meta.tables` dentro da
// saída passaria com o catálogo errado ao redor dela.
func idsOfMetaTables(t *testing.T, a *env, dataset string) string {
	t.Helper()
	code := a.run("query", "--config", a.config, "--dataset", dataset,
		"SELECT id FROM _meta.tables ORDER BY id")
	if code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	out := strings.TrimSpace(a.stdout.String())
	if out == "" {
		t.Fatal("a consulta não devolveu linha nenhuma")
	}
	var ids []string
	for _, row := range strings.Split(out, "\n") {
		var l map[string]any
		if err := json.Unmarshal([]byte(row), &l); err != nil {
			t.Fatalf("linha ilegível: %v", err)
		}
		id, ok := l["id"].(string)
		if !ok {
			t.Fatalf("linha sem id textual: %s", row)
		}
		ids = append(ids, id)
	}
	return strings.Join(ids, ",")
}

// TestQueryLocalRespondeMetaTablesComOCatalogoDoDataset: o caminho local abre o
// dataset por conta própria — sem servidor, sem rota HTTP — e precisa registrar
// a tabela pela sua própria fiação. Sem esta asserção, apagar o
// RegisterMetaTables de cli/query.go não derruba teste nenhum.
//
// O dataset do fixture declara um contador, e ele aparece na listagem ao lado
// da base: é o que prova que a tabela lista o catálogo REGISTRADO neste
// processo, e não uma lista fixa.
func TestQueryLocalRespondeMetaTablesComOCatalogoDoDataset(t *testing.T) {
	a := newEnv(t)
	writeDatasetServices(t, a)
	const want = "_counters.alarms,_counters.autenticados_por_olt,_meta.counter_metrics,_meta.counters,_meta.fields,_meta.kafka,_meta.tables,servicos"
	if got := idsOfMetaTables(t, a, "servicos"); got != want {
		t.Fatalf("catálogo = %s, quero %s", got, want)
	}
}

// specWithHistoryCLI declara a tabela histórica irmã, que o caminho local abre
// derivando o nome do dataset base.
const specWithHistoryCLI = `version: 1
name: eventos
schemaName: eventos
materialization:
  key:
    field: id
history:
  maxBytes: 1GiB
schema:
  fields:
    id:
      type: string
`

// TestQueryLocalListaMetaCountersNaIrmaQueOServidorNaoLista trava a assimetria
// do débito 117, dos DOIS lados: na tabela irmã o caminho local registra a
// `_meta.counters` e a `_meta.counter_metrics` — o cli/query.go chama
// counters.Register sem condição, e as duas são registradas mesmo sem contador
// nenhum — e o servidor não, porque na irmã ele só chama RegisterMetaTables.
//
// A consequência é que `SELECT id FROM _meta.tables` contra a MESMA irmã
// responde diferente conforme o processo que atende. A tabela não está mentindo
// em nenhum dos dois — cada uma lista o que o seu processo registrou —, e é a
// fiação que é assimétrica. Consertá-la é decisão de produto; este teste é o
// que muda junto quando ela for tomada.
//
// A view `alarms` NÃO aparece aqui: como `_meta.kafka`, ela é registrada só
// para a base (`spec != nil` em cli/query.go) — a irmã não tem fonte própria de
// alarme, e publicar a view sob ela descreveria um alarme que não é dela.
//
// O par do lado do servidor é TestMetaTablesRespondeNaTabelaIrmaAbertaPeloServidor.
func TestQueryLocalListaMetaCountersNaIrmaQueOServidorNaoLista(t *testing.T) {
	a := newEnv(t)
	if err := os.WriteFile(filepath.Join(a.dir, "datasets", "eventos.yaml"), []byte(specWithHistoryCLI), 0o644); err != nil {
		t.Fatal(err)
	}
	const want = "_meta.counter_metrics,_meta.counters,_meta.fields,_meta.tables,eventos_history"
	if got := idsOfMetaTables(t, a, "eventos_history"); got != want {
		t.Fatalf("catálogo da irmã = %s, quero %s", got, want)
	}
}
