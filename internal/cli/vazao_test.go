package cli_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// A ingestão pelo CLI não pode custar uma ORDEM DE GRANDEZA a mais que a
// medida no harness (48.786 ev/s na rodada de 8M, em disco real). O gate não
// exige bater o número — o harness ingere direto, sem parsear linha nem abrir
// o dataset —, mas uma queda de 10x significa que alguma coisa no caminho do
// comando está custando por evento: buffer redimensionando, checkpoint a cada
// linha, flush por evento.
//
// O piso é folgado de propósito: em máquina de CI compartilhada, um gate
// apertado vira teste intermitente, e teste intermitente é desligado — o que
// custaria a proteção inteira.
const pisoEventosPorSegundo = 4_000

func TestVazaoDaIngestaoPeloCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("mede vazão de ingestão")
	}
	a := novoAmbiente(t)
	const eventos = 20_000
	entrada := corpus(t, eventos)

	inicio := time.Now()
	code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")
	dur := time.Since(inicio)
	if code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}

	taxa := float64(eventos) / dur.Seconds()
	t.Logf("%d eventos em %s: %.0f ev/s pelo CLI", eventos, dur.Round(time.Millisecond), taxa)
	if taxa < pisoEventosPorSegundo {
		t.Errorf("%.0f ev/s, piso %d: o caminho do comando ganhou custo POR EVENTO",
			taxa, pisoEventosPorSegundo)
	}
	if !strings.Contains(a.stderr.String(), "ev/s") {
		t.Error("o resumo deveria reportar a taxa")
	}
}
