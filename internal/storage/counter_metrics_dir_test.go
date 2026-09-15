package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCounterMetricsDirIsSiblingOfTheCountersDir fixa onde moram os arquivos
// das métricas de janela: dentro do indexes/ que o Open cria, ao lado do
// diretório dos contadores, e nunca dentro dele — a reconstrução de um contador
// não pode depender de conhecer esses arquivos.
func TestCounterMetricsDirIsSiblingOfTheCountersDir(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir})

	got := s.CounterMetricsDir()
	parent := filepath.Dir(got)
	if parent != filepath.Join(dir, "indexes") {
		t.Fatalf("CounterMetricsDir() = %s: o pai tem de ser o indexes/ do dataset (%s)", got, filepath.Join(dir, "indexes"))
	}
	if info, err := os.Stat(parent); err != nil || !info.IsDir() {
		t.Fatalf("o indexes/ que CounterMetricsDir aponta não é o que o Open criou: %v", err)
	}
	countersDir := filepath.Join(parent, diretorioOfCounters)
	if got == countersDir || strings.HasPrefix(got, countersDir+string(filepath.Separator)) {
		t.Fatalf("CounterMetricsDir() = %s está dentro do diretório dos contadores %s", got, countersDir)
	}
	if filepath.Base(got) != counterMetricsDirName {
		t.Fatalf("CounterMetricsDir() = %s, quero o nome em disco %q", got, counterMetricsDirName)
	}
}
