package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// specServicesWithWindowMetricsCLI é o dataset de serviços com as duas
// métricas de janela do contrato do ciclo.
const specServicesWithWindowMetricsCLI = `version: 1
name: servicos
schemaName: servicos
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
    metrics:
      - name: auth_5m
        type: window
        source: total
        window: 5m
        aggregates: [avg]
      - name: auth_24h
        type: window
        source: total
        window: 24h
        aggregates: [max]
`

const windowMetricsSQL = "SELECT groupKey, total, auth_5m_avg, auth_24h_max FROM _counters.autenticados_por_olt"

func writeDatasetServicesWithWindowMetrics(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "datasets", "servicos.yaml")
	if err := os.WriteFile(path, []byte(specServicesWithWindowMetricsCLI), 0o644); err != nil {
		t.Fatal(err)
	}
}

// counterMetricsDir é o diretório das métricas de janela do dataset servicos.
func counterMetricsDir(root string) string {
	return filepath.Join(root, "dados", "servicos", "indexes", "counter_metrics")
}

// filesUnder lê todo arquivo regular abaixo do diretório: caminho → bytes.
// Todo arquivo, e não só os .win: um tmp de crescimento criado pela consulta
// também é escrita.
func filesUnder(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[path] = b
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// localRowsByGroupKey decodifica a saída NDJSON do `nishidb query` por groupKey.
func localRowsByGroupKey(t *testing.T, stdout string) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("linha não é JSON: %v: %s", err, line)
		}
		key, _ := row["groupKey"].(string)
		out[key] = row
	}
	return out
}

// runServerUntilFirstWindowSample sobe o servidor do ambiente, espera a
// primeira passada do amostrador publicar as duas métricas e o ENCERRA — o
// encerramento grava os baldes abertos. Com o relógio real: a consulta local
// que vem logo depois cai dentro das duas janelas.
func runServerUntilFirstWindowSample(t *testing.T, a *envRemote) {
	t.Helper()
	cfg, err := config.Load(a.config)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-end; err != nil {
			t.Errorf("Run: %v", err)
		}
	}
	t.Cleanup(stop)

	body := `{"dataset":"servicos","sql":"SELECT id, sampledAt FROM _meta.counter_metrics"}`
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Post("http://"+a.endereco()+"/v1/query", "application/json", strings.NewReader(body))
		if err == nil {
			text, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK &&
				strings.Count(string(text), `"sampledAt":`) == 2 && !strings.Contains(string(text), `"sampledAt":null`) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("o amostrador não publicou as duas métricas em 10 s (último erro: %v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
}

// TestLocalQueryReadsRestoredWindowMetricsWithoutWritingTheFiles: com o
// servidor parado, `nishidb query --config` lê as colunas das métricas de
// janela pelo mesmo código da restauração do servidor — e não escreve um byte
// nos arquivos, que são do amostrador do servidor.
func TestLocalQueryReadsRestoredWindowMetricsWithoutWritingTheFiles(t *testing.T) {
	a := newEnvRemote(t, 100000)
	writeDatasetServicesWithWindowMetrics(t, a.dir)
	// 24 serviços: G1M10 tem 12 autenticados e OLT14 tem 4 (ver eventosOfServico).
	if code := a.runWithEntry(eventosOfServico(24),
		"ingest", "--config", a.config, "--dataset", "servicos", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	runServerUntilFirstWindowSample(t, a)

	before := filesUnder(t, counterMetricsDir(a.dir))
	if len(before) != 2 {
		t.Fatalf("%d arquivos em %s depois do servidor, quero os 2 .win: %v", len(before), counterMetricsDir(a.dir), keysOf(before))
	}

	if code := a.run("query", "--config", a.config, "--dataset", "servicos", windowMetricsSQL); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	rows := localRowsByGroupKey(t, a.stdout.String())
	for _, want := range []struct {
		groupKey string
		total    float64
	}{
		{"PRCTA_G1M10", 12},
		{"BR_SPOMO_OLT14", 4},
	} {
		row, ok := rows[want.groupKey]
		if !ok {
			t.Fatalf("grupo %s ausente da saída:\n%s", want.groupKey, a.stdout.String())
		}
		if row["total"] != want.total || row["auth_5m_avg"] != want.total || row["auth_24h_max"] != want.total {
			t.Fatalf("grupo %s = %v, quero total, auth_5m_avg e auth_24h_max = %v", want.groupKey, row, want.total)
		}
	}

	after := filesUnder(t, counterMetricsDir(a.dir))
	if len(after) != len(before) {
		t.Fatalf("a consulta local mudou os arquivos: antes %v, depois %v", keysOf(before), keysOf(after))
	}
	for path, b := range before {
		if !bytes.Equal(after[path], b) {
			t.Fatalf("a consulta local escreveu em %s", path)
		}
	}
}

// TestLocalQueryWithoutWindowMetricFilesAnswersNull: um dataset cujo servidor
// nunca amostrou responde as colunas das métricas NULL, sem erro — e a consulta
// não cria o diretório.
func TestLocalQueryWithoutWindowMetricFilesAnswersNull(t *testing.T) {
	a := newEnv(t)
	writeDatasetServicesWithWindowMetrics(t, a.dir)
	if code := a.runWithEntry(eventosOfServico(24),
		"ingest", "--config", a.config, "--dataset", "servicos", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	if code := a.run("query", "--config", a.config, "--dataset", "servicos", windowMetricsSQL); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	rows := localRowsByGroupKey(t, a.stdout.String())
	row, ok := rows["PRCTA_G1M10"]
	if !ok {
		t.Fatalf("grupo PRCTA_G1M10 ausente da saída:\n%s", a.stdout.String())
	}
	if row["total"] != 12.0 || row["auth_5m_avg"] != nil || row["auth_24h_max"] != nil {
		t.Fatalf("PRCTA_G1M10 = %v, quero total 12 e as colunas das métricas NULL", row)
	}
	if _, err := os.Stat(counterMetricsDir(a.dir)); !os.IsNotExist(err) {
		t.Fatalf("a consulta local criou %s (ou ele não pôde ser lido: %v)", counterMetricsDir(a.dir), err)
	}
	// Arquivo ausente é o estado normal de quem só ingere e consulta: não é
	// anomalia e não vai ao stderr, para que o aviso de arquivo corrompido não
	// vire ruído que o operador aprende a ignorar.
	if strings.Contains(a.stderr.String(), "unreadable") {
		t.Fatalf("a consulta local avisou arquivo ilegível para um arquivo que só não existe:\n%s", a.stderr.String())
	}
}

// TestLocalQueryNeverRemovesWindowFilesOfARemovedMetric: a consulta local só
// lê. Com a métrica fora do YAML ela não abre amostrador e também NÃO apaga a
// janela gravada — isso é da subida do servidor.
func TestLocalQueryNeverRemovesWindowFilesOfARemovedMetric(t *testing.T) {
	a := newEnvRemote(t, 100000)
	writeDatasetServicesWithWindowMetrics(t, a.dir)
	cfg, err := config.Load(a.config)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("encerramento: %v", err)
	}
	before := filesUnder(t, counterMetricsDir(a.dir))
	if len(before) != 2 {
		t.Fatalf("%d arquivos em %s depois do servidor, quero os 2 .win: %v", len(before), counterMetricsDir(a.dir), keysOf(before))
	}

	cut := strings.Index(specServicesWithWindowMetricsCLI, "    metrics:\n")
	if cut < 0 {
		t.Fatal("o fixture mudou: o bloco `metrics:` não está mais no YAML do teste")
	}
	path := filepath.Join(a.dir, "datasets", "servicos.yaml")
	if err := os.WriteFile(path, []byte(specServicesWithWindowMetricsCLI[:cut]), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := a.run("query", "--config", a.config, "--dataset", "servicos",
		"SELECT groupKey, total FROM _counters.autenticados_por_olt"); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	after := filesUnder(t, counterMetricsDir(a.dir))
	if len(after) != len(before) {
		t.Fatalf("a consulta local apagou arquivos da janela: antes %v, depois %v", keysOf(before), keysOf(after))
	}
	for p, b := range before {
		if !bytes.Equal(after[p], b) {
			t.Fatalf("a consulta local mexeu em %s", p)
		}
	}
}

// TestLocalQueryWarnsOnStderrWhenAWindowFileIsUnreadable: com o cabeçalho do
// .win corrompido, a consulta local sai 0 com as colunas das métricas NULL — e
// o operador recebe o motivo no stderr, sem que o aviso entre no NDJSON do
// stdout.
func TestLocalQueryWarnsOnStderrWhenAWindowFileIsUnreadable(t *testing.T) {
	a := newEnvRemote(t, 100000)
	writeDatasetServicesWithWindowMetrics(t, a.dir)
	if code := a.runWithEntry(eventosOfServico(24),
		"ingest", "--config", a.config, "--dataset", "servicos", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	cfg, err := config.Load(a.config)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("encerramento: %v", err)
	}
	files := filesUnder(t, counterMetricsDir(a.dir))
	if len(files) != 2 {
		t.Fatalf("%d arquivos em %s depois do servidor, quero os 2 .win: %v", len(files), counterMetricsDir(a.dir), keysOf(files))
	}
	for path := range files {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		// O magic no começo do cabeçalho: o arquivo deixa de ser legível.
		if _, err := f.WriteAt(make([]byte, 8), 0); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if code := a.run("query", "--config", a.config, "--dataset", "servicos", windowMetricsSQL); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	rows := localRowsByGroupKey(t, a.stdout.String())
	row, ok := rows["PRCTA_G1M10"]
	if !ok {
		t.Fatalf("grupo PRCTA_G1M10 ausente da saída:\n%s", a.stdout.String())
	}
	if row["total"] != 12.0 || row["auth_5m_avg"] != nil || row["auth_24h_max"] != nil {
		t.Fatalf("PRCTA_G1M10 = %v, quero total 12 e as colunas das métricas NULL", row)
	}
	if strings.Contains(a.stdout.String(), "counter_metric.unreadable") {
		t.Fatalf("o aviso vazou para o stdout, que é o NDJSON da consulta:\n%s", a.stdout.String())
	}
	if n := strings.Count(a.stderr.String(), "counter_metric.unreadable"); n != 2 {
		t.Fatalf("%d avisos counter_metric.unreadable no stderr, quero um por métrica (2):\n%s", n, a.stderr.String())
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
