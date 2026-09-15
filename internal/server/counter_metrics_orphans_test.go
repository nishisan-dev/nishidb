package server_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// specServicesWithoutMetricsOnTwoCounters são os mesmos dois contadores de
// specServicesWithMetricsOnTwoCounters, com os blocos `metrics:` removidos.
const specServicesWithoutMetricsOnTwoCounters = `version: 1
name: servicos
schemaName: telecom
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
  - name: servicos_por_olt
    groupBy: [olt]
`

// lockedLogBuffer recebe as linhas de log de mais de uma goroutine.
type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// events devolve as linhas JSON cujo campo event é o pedido.
func (b *lockedLogBuffer) events(t *testing.T, event string) []map[string]any {
	t.Helper()
	b.mu.Lock()
	raw := bytes.Clone(b.buf.Bytes())
	b.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		var line map[string]any
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatalf("linha de log não é JSON: %v\n%s", err, sc.Text())
		}
		if line["event"] == event {
			out = append(out, line)
		}
	}
	return out
}

// windowFilesUnder lista os arquivos .win abaixo do diretório (vazio se ele
// não existe).
func windowFilesUnder(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) && path == dir {
				return filepath.SkipDir
			}
			return err
		}
		if d.Type().IsRegular() && strings.HasSuffix(path, ".win") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestRemovingTheLastWindowMetricRemovesItsFilesOnTheNextStart: com o último
// bloco `metrics:` fora do YAML o amostrador não abre — e sem a limpeza da
// subida os arquivos da janela ficariam no disco para sempre, porque a limpeza
// do Open nunca mais roda.
func TestRemovingTheLastWindowMetricRemovesItsFilesOnTheNextStart(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset(windowDataset, specServicesWithMetricsOnTwoCounters)
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("primeira subida: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("primeiro encerramento: %v", err)
	}
	dir := filepath.Join(a.cfg.DatasetRootDir(windowDataset), "indexes", "counter_metrics")
	if files := windowFilesUnder(t, dir); len(files) != 3 {
		t.Fatalf("%d arquivos .win depois da subida com três métricas, quero 3: %v", len(files), files)
	}

	a.writeDataset(windowDataset, specServicesWithoutMetricsOnTwoCounters)
	logs := &lockedLogBuffer{}
	srv, err = server.Novo(a.load(), server.Options{Log: logging.New(logs, logging.FormatJSON, slog.LevelDebug)})
	if err != nil {
		t.Fatalf("subida sem métrica: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("segundo encerramento: %v", err)
	}
	if files := windowFilesUnder(t, dir); len(files) != 0 {
		t.Fatalf("os arquivos da janela sobreviveram à subida sem métrica declarada: %v", files)
	}
	for _, counter := range []string{"autenticados_por_olt", "servicos_por_olt"} {
		if _, err := os.Stat(filepath.Join(dir, counter)); !os.IsNotExist(err) {
			t.Errorf("o diretório do contador %s sobreviveu (ou não pôde ser lido: %v)", counter, err)
		}
	}
	events := logs.events(t, "counter_metric.orphans")
	if len(events) != 1 || events[0]["dataset"] != windowDataset {
		t.Fatalf("WARN counter_metric.orphans = %v, quero exatamente um do dataset %s", events, windowDataset)
	}
}
