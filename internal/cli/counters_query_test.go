package cli_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

const specServicesCLI = `version: 1
name: servicos
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    authenticated:
      type: boolean
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
`

// writeDatasetServices põe o YAML com contadores no diretório de specs do
// ambiente, ao lado do fixture de RADIUS.
func writeDatasetServices(t *testing.T, a *env) {
	t.Helper()
	specs := filepath.Join(a.dir, "datasets")
	if err := os.WriteFile(filepath.Join(specs, "servicos.yaml"), []byte(specServicesCLI), 0o644); err != nil {
		t.Fatal(err)
	}
}

func eventosOfServico(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		olt := "PRCTA_G1M10"
		if i%4 == 0 {
			olt = "BR_SPOMO_OLT14"
		}
		fmt.Fprintf(&sb, `{"customerId":"C-%04d","olt":%q,"authenticated":%v}`+"\n", i, olt, i%3 != 0)
	}
	return sb.String()
}

// TestQueryLocalRespondeCountersMantidosNaIngestao: o `nishidb ingest` mantém
// os contadores (ele abre o dataset pelo mesmo callback do servidor) e o
// `nishidb query --config` lê a tabela virtual pela mesma costura.
func TestQueryLocalRespondeCountersMantidosNaIngestao(t *testing.T) {
	a := newEnv(t)
	writeDatasetServices(t, a)
	if code := a.runWithEntry(eventosOfServico(24),
		"ingest", "--config", a.config, "--dataset", "servicos", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	code := a.run("query", "--config", a.config, "--dataset", "servicos",
		"SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC")
	if code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	rows := strings.Split(strings.TrimSpace(a.stdout.String()), "\n")
	if len(rows) != 2 {
		t.Fatalf("%d linhas, quero 2:\n%s", len(rows), a.stdout.String())
	}
	if !strings.Contains(rows[0], `"BR_SPOMO_OLT14"`) || !strings.Contains(rows[0], `"total":4`) {
		t.Fatalf("primeira linha = %s", rows[0])
	}
	if !strings.Contains(rows[1], `"PRCTA_G1M10"`) || !strings.Contains(rows[1], `"total":12`) {
		t.Fatalf("segunda linha = %s", rows[1])
	}
}
