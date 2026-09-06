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
	cmd.Dir = repoRoot
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

// writeFakeTool cria, dentro de dir, um executável chamado name que só sai
// com status 0. É o suficiente para `command -v` do rename-apply.sh achar a
// ferramenta — este pacote não precisa do rf/gopls REAIS para testar a
// checagem de script ilegível, que roda antes de qualquer invocação real.
func writeFakeTool(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("não consegui criar o executável falso %s: %v", path, err)
	}
	return path
}

// A propriedade sob teste (recusa de um .rf ausente/ilegível/vazio) é
// decidida pelas checagens -f/-r/-s do script, que rodam ANTES de qualquer
// invocação real de rf — então rf e gopls REAIS são irrelevantes aqui, e usar
// executáveis FALSOS (`#!/bin/sh; exit 0`) é suficiente para passar pelo
// `command -v` e alcançar o ramo novo. Isso importa porque o CI deste
// repositório (.github/workflows/ci.yml, jobs test e race) só roda
// `actions/setup-go` + `go test ./...` — não instala rf nem gopls — e exigir
// o toolchain real aqui quebraria os dois jobs, e qualquer máquina sem as
// duas ferramentas (o exato "gate que passa com a ferramenta ausente" que a
// doutrina do débito 53 proíbe, desta vez no próprio teste).
//
// Sem a checagem que este teste cobre, um `.rf` inexistente ou vazio vira
// `conteudo=""` sob `set -u` (sem `-e`, o `cat` que falha não para o script)
// e a execução cai para `rf -diff ""` — um no-op que mentiria sucesso em vez
// da recusa alta e nomeada que a doutrina dos gates exige.
func TestRenameApplierRefusesAnUnreadableScriptNamingIt(t *testing.T) {
	fakeToolsDir := t.TempDir()
	writeFakeTool(t, fakeToolsDir, "rf")
	writeFakeTool(t, fakeToolsDir, "gopls")

	script := filepath.Join("scripts", "rename-apply.sh")
	missing := filepath.Join(t.TempDir(), "nao-existe.rf")
	cmd := exec.Command("bash", script, "internal/types", missing, filepath.Join(t.TempDir(), "d.diff"))
	cmd.Dir = repoRoot
	cmd.Env = []string{"PATH=" + fakeToolsDir + ":/usr/bin:/bin"}
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
	if !strings.Contains(string(out), "não existe") {
		t.Fatalf("o stderr não diz que o script não existe:\n%s", out)
	}
}
