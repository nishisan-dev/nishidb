package server_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/server"
)

// mappedWindowFiles devolve as linhas de /proc/self/maps de arquivos .win
// abaixo de root — o mmap que um amostrador não fechado deixa para trás.
//
// Linux-only, que é o alvo do repositório: o storage usa mmap e fallocate
// reais e só roda em Linux. O filtro por root isola o teste de qualquer outro
// mapeamento do processo.
func mappedWindowFiles(t *testing.T, root string) []string {
	t.Helper()
	b, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatalf("lendo /proc/self/maps: %v", err)
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.Contains(line, root) && strings.Contains(line, ".win") {
			out = append(out, line)
		}
	}
	return out
}

// samplerGoroutineFrames conta os frames de goroutine do amostrador no
// processo inteiro.
func samplerGoroutineFrames() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "countermetrics.(*Sampler)")
}

// specWithWindowMetricsNamed é o contrato do ciclo sob outro nome e schema,
// para caber mais de um dataset no mesmo diretório.
func specWithWindowMetricsNamed(name string) string {
	spec := strings.Replace(specServicesWithWindowMetrics, "name: servicos", "name: "+name, 1)
	return strings.Replace(spec, "schemaName: telecom", "schemaName: "+name, 1)
}

// withUnknownQualifyColumn troca a coluna do `qualifyWhen` por uma que não
// existe. A recusa vem da compilação dos alarmes, que roda DEPOIS de o
// amostrador abrir — é o que põe a falha no trecho em que só o fechamento do
// caminho de erro devolve os mmaps.
func withUnknownQualifyColumn(spec string) string {
	return strings.Replace(spec, `qualifyWhen: "auth_24h_max > 10"`, `qualifyWhen: "missing_column > 10"`, 1)
}

// TestDatasetFailingAfterTheSamplerOpenedLeavesNoWindowFileMapped: a abertura
// que falha depois de o amostrador abrir fecha o amostrador — nenhum .win fica
// mapeado e nenhuma goroutine dele fica de pé.
//
// A guarda de vacuidade são os .win no disco: eles só existem se o amostrador
// de fato abriu antes da falha.
func TestDatasetFailingAfterTheSamplerOpenedLeavesNoWindowFileMapped(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset(windowDataset, withUnknownQualifyColumn(specServicesWithWindowMetrics))
	srv, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		_ = srv.Close()
		t.Fatal("o servidor subiu com um qualifyWhen sobre coluna inexistente")
	}
	if !strings.Contains(err.Error(), "missing_column") {
		t.Fatalf("a recusa não nomeia a coluna: %v", err)
	}
	dir := filepath.Join(a.cfg.DatasetRootDir(windowDataset), "indexes", "counter_metrics")
	if files := windowFilesUnder(t, dir); len(files) != 2 {
		t.Fatalf("%d arquivos .win no disco, quero 2: a falha veio antes de o amostrador abrir e o teste não prova nada", len(files))
	}
	if mapped := mappedWindowFiles(t, a.dir); len(mapped) != 0 {
		t.Errorf("a abertura que falhou deixou .win mapeado: %v", mapped)
	}
	// O laço só sobe no Run, então isto guarda contra a abertura passar a
	// subi-lo e o caminho de erro não o parar.
	if frames := samplerGoroutineFrames(); frames != 0 {
		t.Errorf("%d frames de goroutine do amostrador depois da abertura que falhou", frames)
	}
}

// TestAbortedStartupLeavesNoWindowFileOfAnOpenedDatasetMapped: com dois
// datasets, `aaa` abre inteiro e `zzz` falha (a ordem de abertura é
// alfabética). A subida abortada fecha o amostrador do `aaa`, que já estava na
// lista do servidor.
func TestAbortedStartupLeavesNoWindowFileOfAnOpenedDatasetMapped(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("aaa", specWithWindowMetricsNamed("aaa"))
	a.writeDataset("zzz", withUnknownQualifyColumn(specWithWindowMetricsNamed("zzz")))
	srv, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		_ = srv.Close()
		t.Fatal("o servidor subiu com um qualifyWhen sobre coluna inexistente no zzz")
	}
	if !strings.Contains(err.Error(), `"zzz"`) {
		t.Fatalf("a recusa não veio do zzz: %v", err)
	}
	dir := filepath.Join(a.cfg.DatasetRootDir("aaa"), "indexes", "counter_metrics")
	if files := windowFilesUnder(t, dir); len(files) != 2 {
		t.Fatalf("%d arquivos .win do aaa no disco, quero 2: o aaa não abriu e o teste não prova nada", len(files))
	}
	if mapped := mappedWindowFiles(t, a.dir); len(mapped) != 0 {
		t.Errorf("a subida abortada deixou .win mapeado: %v", mapped)
	}
}
