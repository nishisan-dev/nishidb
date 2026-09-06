// Package gates — este arquivo confere o aplicador de renames.
package gates_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// "Gate que passa com a ferramenta ausente não é gate, é carimbo" (débito
// 53). O aplicador do ciclo de identificadores em inglês depende de rf e
// gopls; sem eles tem de falhar ALTO no stderr nomeando o go install exato,
// nunca cair para um caminho que "faça o que der".
func TestRenameApplierRefusesWithoutRfNamingTheGoInstall(t *testing.T) {
	script := filepath.Join("scripts", "rename-apply.sh")
	vazio := t.TempDir()
	cmd := exec.Command("bash", script, "internal/types", "/dev/null", filepath.Join(vazio, "d.diff"))
	cmd.Dir = raizDoRepo
	cmd.Env = append(os.Environ(), "PATH="+vazio)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("passou sem rf no PATH:\n%s", out)
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
		t.Fatalf("exit %v, esperado 3:\n%s", err, out)
	}
	if !strings.Contains(string(out), "go install rsc.io/rf@v0.0.0-20260710193434-85dbf8d53aee") {
		t.Fatalf("o stderr não nomeia o go install do rf:\n%s", out)
	}
}
