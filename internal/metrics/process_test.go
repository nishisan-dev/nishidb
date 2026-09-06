package metrics_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
)

func TestRegistraProcessoExpoeHeapGo(t *testing.T) {
	r := metrics.NewRegistry()
	metrics.RegistraProcesso(r)

	s := exposicao(t, r)
	for _, nome := range []string{metrics.MetGoHeapBytes, metrics.MetGoHeapObjects} {
		if !strings.Contains(s, "# TYPE "+nome+" gauge") {
			t.Errorf("métrica %s ausente:\n%s", nome, s)
		}
	}
}

// No Linux o RSS tem de aparecer; fora dele tem de SUMIR, não sair zerado —
// zero é uma medição, e um alerta de "RSS caiu para zero" é pior que a
// ausência do dado.
func TestRSSAparecerSoOndeHaComoMedir(t *testing.T) {
	r := metrics.NewRegistry()
	metrics.RegistraProcesso(r)
	s := exposicao(t, r)

	tem := strings.Contains(s, metrics.MetProcessRSSBytes)
	if runtime.GOOS == "linux" && !tem {
		t.Errorf("no Linux o RSS tem de ser medido:\n%s", s)
	}
	if runtime.GOOS != "linux" && tem {
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
	metrics.RegistraProcesso(r)
	for _, linha := range strings.Split(exposicao(t, r), "\n") {
		if strings.HasPrefix(linha, "#") || linha == "" {
			continue
		}
		if strings.Contains(linha, "e+") || strings.Contains(linha, "e-") {
			t.Errorf("notação científica no scrape: %q", linha)
		}
	}
}
