// Este arquivo confere o aplicador de renames.
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
	emptyDir := t.TempDir()
	cmd := exec.Command("bash", script, "internal/types", "/dev/null", filepath.Join(emptyDir, "d.diff"))
	cmd.Dir = raizDoRepo
	cmd.Env = append(os.Environ(), "PATH="+emptyDir)
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

// requireRealToolchain falha alto, nomeando o go install exato, quando rf ou
// gopls faltam no PATH deste teste — a mesma doutrina que o próprio script
// aplica sobre quem o chama (débito 53). Um teste que pulasse em silêncio sem
// rf/gopls não provaria nada sobre o ramo de script ilegível.
func requireRealToolchain(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rf"); err != nil {
		t.Fatalf("rf não está no PATH deste teste. Instale com: go install rsc.io/rf@v0.0.0-20260710193434-85dbf8d53aee")
	}
	if _, err := exec.LookPath("gopls"); err != nil {
		t.Fatalf("gopls não está no PATH deste teste. Instale com: go install golang.org/x/tools/gopls@v0.23.0")
	}
}

// Sem esta checagem, um `.rf` inexistente ou vazio vira `conteudo=""` sob
// `set -u` (sem `-e`, o `cat` que falha não para o script) e a execução cai
// para `rf -diff ""` — um no-op que mentiria sucesso em vez da recusa alta e
// nomeada que a doutrina dos gates exige. Este teste roda com rf e gopls
// reais no PATH (para alcançar o ramo novo) e afirma exit 2 e o caminho do
// script na mensagem.
func TestRenameApplierRefusesAnUnreadableScriptNamingIt(t *testing.T) {
	requireRealToolchain(t)
	script := filepath.Join("scripts", "rename-apply.sh")
	missing := filepath.Join(t.TempDir(), "nao-existe.rf")
	cmd := exec.Command("bash", script, "internal/types", missing, filepath.Join(t.TempDir(), "d.diff"))
	cmd.Dir = raizDoRepo
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("passou com script .rf inexistente:\n%s", out)
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
		t.Fatalf("exit %v, esperado 2:\n%s", err, out)
	}
	if !strings.Contains(string(out), missing) {
		t.Fatalf("o stderr não nomeia o caminho do script:\n%s", out)
	}
}
