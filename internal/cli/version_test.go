package cli_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
	"github.com/nishisan-dev/nishidb/internal/cli"
)

// TestVersionRespondeComSHAEData cobre as duas formas que o operador digita
// — `--version` (convenção universal de CLI) e `version` (o padrão deste CLI,
// que trata subcomando como primeiro argumento). As duas têm de imprimir
// EXATAMENTE o mesmo texto que buildinfo.String() produz: é a garantia de que
// não existem duas fontes narrando a mesma versão de jeitos diferentes.
func TestVersionRespondeComSHAEData(t *testing.T) {
	vAntiga, cAntiga, dAntiga := buildinfo.Version, buildinfo.Commit, buildinfo.Date
	buildinfo.Version, buildinfo.Commit, buildinfo.Date = "v9.9.9", "cafefeed1234", "2026-01-02T03:04:05Z"
	t.Cleanup(func() { buildinfo.Version, buildinfo.Commit, buildinfo.Date = vAntiga, cAntiga, dAntiga })

	for _, args := range [][]string{{"--version"}, {"version"}} {
		a := &env{}
		if code := a.run(args...); code != cli.ExitOK {
			t.Fatalf("%v: exit = %d, quer %d\n%s", args, code, cli.ExitOK, a.stderr.String())
		}
		got := strings.TrimSpace(a.stdout.String())
		if got != buildinfo.String() {
			t.Errorf("%v: stdout = %q, quer %q", args, got, buildinfo.String())
		}
	}
}

// O texto de uso é o único lugar em que o operador descobre que o comando
// existe (a mesma razão de TestOUsoAnunciaOsTresComandosDeLeitura).
func TestOUsoAnunciaOVersion(t *testing.T) {
	a := &env{}
	if code := a.run("--help"); code != cli.ExitOK {
		t.Fatalf("--help saiu com %d", code)
	}
	if !strings.Contains(a.stdout.String(), "--version") {
		t.Error("o texto de uso não anuncia --version")
	}
}
