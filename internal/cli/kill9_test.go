package cli_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// O critério 25 da §126 é "sobreviver a kill -9 sem perder estado confirmado", e
// até o fecho do M1 a evidência citada era um teste de encerramento LIMPO (ctx
// cancelado, que grava checkpoint final). O mecanismo estava provado por
// injeção no tail; o SIGKILL de verdade — com o processo morto sem chance de
// nenhum defer, e o kernel decidindo o que fez com o writeback — não era
// exercitado por teste nenhum.
//
// Este é o teste que faltava: mata o binário no meio da ingestão e confere que
// o estado confirmado até o último checkpoint sobrevive, que o dataset reabre
// e que o resultado continua sendo JSON válido.
func TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado(t *testing.T) {
	if testing.Short() {
		t.Skip("compila o binário e mata um processo")
	}
	a := newEnv(t)
	bin := filepath.Join(t.TempDir(), "nishidb")
	build := exec.Command("go", "build", "-o", bin, "../../cmd/nishidb")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	// Corpus grande o bastante para o processo ainda estar ingerindo quando o
	// sinal chegar, e checkpoint frequente para haver estado confirmado.
	entry := filepath.Join(a.dir, "eventos.ndjson")
	var sb strings.Builder
	const eventos = 200_000
	for i := range eventos {
		fmt.Fprintf(&sb, `{"id":"K%06d","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n", i)
	}
	if err := os.WriteFile(entry, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "ingest", "--config", a.config,
		"--dataset", "radius_sessions", "--input", entry, "--checkpoint-every", "10000")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Espera o PRIMEIRO checkpoint com dados: é ele que define o que a spec
	// chama de "estado confirmado". Sem isso o teste mataria o processo antes
	// de haver o que preservar, e passaria por vacuidade.
	man := filepath.Join(a.dir, "dados", "radius_sessions", "checkpoint", "manifest.bin")
	var confirmadas uint64
	limit := time.Now().Add(30 * time.Second)
	for time.Now().Before(limit) {
		if m, err := checkpoint.Load(man); err == nil && m.RowCount > 0 {
			confirmadas = m.RowCount
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if confirmadas == 0 {
		cmd.Process.Kill()
		t.Fatal("nenhum checkpoint com rows antes do timeout")
	}

	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	state, err := cmd.Process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if state.ExitCode() != -1 {
		t.Fatalf("o processo saiu com %d em vez de morrer pelo sinal", state.ExitCode())
	}

	// Reabre pelo binário: o Open faz o recovery (manifest + replay do tail,
	// truncando o rasgo que o SIGKILL deixou).
	if code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions"); code != 0 {
		t.Fatalf("a query após o kill -9 saiu com %d\n%s", code, a.stderr.String())
	}
	saida := strings.TrimSpace(a.stdout.String())
	if saida == "" {
		t.Fatal("nenhuma row sobreviveu")
	}
	rows := strings.Split(saida, "\n")
	for i, l := range rows {
		var obj map[string]any
		if err := json.Unmarshal([]byte(l), &obj); err != nil {
			t.Fatalf("linha %d não é JSON válido após o recovery: %v", i+1, err)
		}
	}
	// Estado CONFIRMADO não se perde. O replay do tail pode recuperar mais do
	// que o último checkpoint amarrou — por isso a comparação é >=, e não ==.
	if uint64(len(rows)) < confirmadas {
		t.Errorf("%d rows após o kill -9, mas o checkpoint já confirmava %d", len(rows), confirmadas)
	}
	t.Logf("kill -9 com %d rows confirmadas; %d rows visíveis após o recovery", confirmadas, len(rows))
}
