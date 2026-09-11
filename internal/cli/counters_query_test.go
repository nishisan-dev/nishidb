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

// specServicesWithMold é o dataset da issue #38: o grupo `olt|slot` tem de sair
// como o inventário o escreve — separador escolhido e slot com zero à esquerda.
const specServicesWithMold = `version: 1
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
    slot:
      type: int64
    authenticated:
      type: boolean
counters:
  - name: autenticados_por_pon
    groupBy: [olt, slot]
    keyFormat: "{olt}.{slot:02}"
    where: authenticated = true
`

// TestQueryLocalConsultaPeloGroupKeyFormatado é o ponta a ponta do formato
// declarável: YAML com o molde, ingestão de verdade e a consulta pelo nome que
// a aplicação já usa. O `WHERE groupKey = …` é o ponto da issue — o texto
// formatado não serve só para LER, ele é a chave de BUSCA.
func TestQueryLocalConsultaPeloGroupKeyFormatado(t *testing.T) {
	a := newEnv(t)
	specs := filepath.Join(a.dir, "datasets")
	if err := os.WriteFile(filepath.Join(specs, "servicos.yaml"), []byte(specServicesWithMold), 0o644); err != nil {
		t.Fatal(err)
	}
	var eventos strings.Builder
	for i := 0; i < 12; i++ {
		fmt.Fprintf(&eventos, `{"customerId":"C-%04d","olt":"BR_SPOAM_OLT","slot":%d,"authenticated":true}`+"\n", i, i%3+8)
	}
	if code := a.runWithEntry(eventos.String(),
		"ingest", "--config", a.config, "--dataset", "servicos", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}

	code := a.run("query", "--config", a.config, "--dataset", "servicos",
		"SELECT groupKey, olt, slot, total FROM _counters.autenticados_por_pon ORDER BY groupKey ASC")
	if code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	saida := strings.TrimSpace(a.stdout.String())
	for _, quero := range []string{`"groupKey":"BR_SPOAM_OLT.08"`, `"groupKey":"BR_SPOAM_OLT.09"`, `"groupKey":"BR_SPOAM_OLT.10"`} {
		if !strings.Contains(saida, quero) {
			t.Errorf("a saída não tem %s:\n%s", quero, saida)
		}
	}
	// O zero à esquerda é do TEXTO; a coluna continua sendo o número.
	if !strings.Contains(saida, `"slot":8`) {
		t.Errorf("a coluna slot tem de continuar int64, sem o preenchimento:\n%s", saida)
	}

	a.stdout.Reset()
	code = a.run("query", "--config", a.config, "--dataset", "servicos",
		"SELECT groupKey, total FROM _counters.autenticados_por_pon WHERE groupKey = 'BR_SPOAM_OLT.08'")
	if code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	linhas := strings.Split(strings.TrimSpace(a.stdout.String()), "\n")
	if len(linhas) != 1 || !strings.Contains(linhas[0], `"groupKey":"BR_SPOAM_OLT.08"`) {
		t.Fatalf("a busca direta pelo nome formatado tem de devolver UMA linha:\n%s", a.stdout.String())
	}

	// E o texto do molde é o ÚNICO que resolve: o separador velho não responde.
	a.stdout.Reset()
	if code := a.run("query", "--config", a.config, "--dataset", "servicos",
		"SELECT groupKey FROM _counters.autenticados_por_pon WHERE groupKey = 'BR_SPOAM_OLT|8'"); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	if got := strings.TrimSpace(a.stdout.String()); got != "" {
		t.Errorf("o texto do separador default não pode resolver quando há molde: %s", got)
	}
}
