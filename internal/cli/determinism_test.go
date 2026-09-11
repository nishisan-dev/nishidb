package cli_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// Datasets virgens criados do MESMO YAML têm de devolver `SELECT *` com as
// colunas nas mesmas posições. A ordem de saída é a de FieldID, e os FieldIDs
// nascem no Open — que resolvia os campos de bitmap na ordem em que o CLI os
// listou, e o CLI os lista iterando um map de Go.
//
// A revisão final reproduziu 6 ordens distintas em 12 datasets do mesmo
// arquivo. É irreversível por dataset: quem criou hoje carrega o mapeamento
// sorteado para sempre, e o contrato do driver JDBC do M3 mapeia POSIÇÃO para
// valor.
func TestDatasetsDoMesmoYAMLTemAMesmaOrdemDeColunas(t *testing.T) {
	const event = `{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop","Filter-Id":"f","Framed-Protocol":"PPP"}`

	var primeira string
	for i := range 8 {
		a := newEnv(t) // raiz nova: dataset virgem, dicionário vazio
		if code := a.runWithEntry(event+"\n",
			"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
			t.Fatalf("ingest: code = %d\n%s", code, a.stderr.String())
		}
		if code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
			"SELECT * FROM radius_sessions"); code != cli.ExitOK {
			t.Fatalf("query: code = %d\n%s", code, a.stderr.String())
		}
		row := strings.TrimSpace(a.stdout.String())
		if row == "" {
			t.Fatal("SELECT * não devolveu linha")
		}
		if i == 0 {
			primeira = row
			continue
		}
		if row != primeira {
			t.Fatalf("dataset %d devolveu as colunas em outra ordem:\n  %s\n  %s", i, primeira, row)
		}
	}
}
