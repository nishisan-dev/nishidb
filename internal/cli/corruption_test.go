package cli_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// Um byte invertido DENTRO da região durável de um segmento não é detectado no
// Open (a guarda do §14.20 vê encolhimento, não sobrescrita): ele aparece na
// leitura, no meio do resultado. O que estava errado era o exit code — o erro
// real é encoding.ErrCorrupt, que não constava do codigoDoErro e caía no
// default ExitUsageError. O operador recebia "erro de uso ou de configuração", que é
// justamente o único código que NÃO manda olhar o dataset.
func TestDatasetCorrompidoSaiComExitDeDataset(t *testing.T) {
	a := newEnv(t)

	// Uma entidade por evento: todo record fica VIVO, então a varredura
	// completa lê todos e alcança o byte corrompido onde quer que ele caia.
	var sb strings.Builder
	for i := range 200 {
		fmt.Fprintf(&sb, `{"id":"K%04d","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n", i)
	}
	if code := a.runWithEntry(sb.String(),
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest: code = %d\n%s", code, a.stderr.String())
	}

	ds := filepath.Join(a.dir, "dados", "radius_sessions")
	m, err := checkpoint.Load(filepath.Join(ds, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if m.DurableOffset == 0 {
		t.Fatal("nada durável: o ingest não gravou checkpoint")
	}
	sec := filepath.Join(ds, "data", "segment-00000001.dat")
	raw, err := os.ReadFile(sec)
	if err != nil {
		t.Fatal(err)
	}
	raw[m.DurableOffset/2] ^= 0xFF // no MEIO do que já está selado
	if err := os.WriteFile(sec, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions")

	if code != cli.ExitDataset {
		t.Errorf("code = %d, want %d (dataset corrompido)\nstderr: %s", code, cli.ExitDataset, a.stderr.String())
	}
	if msg := a.stderr.String(); !strings.Contains(msg, "corrompido") {
		t.Errorf("stderr = %q, want dizer que o payload está corrompido", msg)
	}
}
