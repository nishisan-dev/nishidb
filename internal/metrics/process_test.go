package metrics_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
)

func TestRegistraProcessoExpoeHeapGo(t *testing.T) {
	r := metrics.NewRegistry()
	metrics.RegisterProcess(r)

	s := exposition(t, r)
	for _, name := range []string{metrics.MetGoHeapBytes, metrics.MetGoHeapObjects} {
		if !strings.Contains(s, "# TYPE "+name+" gauge") {
			t.Errorf("métrica %s ausente:\n%s", name, s)
		}
	}
}

// No Linux o RSS tem de aparecer; fora dele tem de SUMIR, não sair zerado —
// zero é uma medição, e um alerta de "RSS caiu para zero" é pior que a
// ausência do dado.
func TestRSSAparecerSoOndeHaComoMedir(t *testing.T) {
	r := metrics.NewRegistry()
	metrics.RegisterProcess(r)
	s := exposition(t, r)

	hasRSS := strings.Contains(s, metrics.MetProcessRSSBytes)
	if runtime.GOOS == "linux" && !hasRSS {
		t.Errorf("no Linux o RSS tem de ser medido:\n%s", s)
	}
	if runtime.GOOS != "linux" && hasRSS {
		t.Errorf("fora do Linux não há /proc; a série tem de sumir:\n%s", s)
	}
	if strings.Contains(s, metrics.MetProcessRSSBytes+" 0\n") {
		t.Errorf("RSS saiu zerado, que é medição falsa:\n%s", s)
	}
}

// O scrape é lido na mão no diagnóstico: heap de centenas de MiB não pode sair
// como 3.14e+08.
func TestValoresDeProcessoSaemSemNotacaoCientifica(t *testing.T) {
	r := metrics.NewRegistry()
	metrics.RegisterProcess(r)
	for _, line := range strings.Split(exposition(t, r), "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		if strings.Contains(line, "e+") || strings.Contains(line, "e-") {
			t.Errorf("notação científica no scrape: %q", line)
		}
	}
}
