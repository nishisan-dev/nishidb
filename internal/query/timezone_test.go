package query_test

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// A decisão de produto é que o fuso do wall-clock vem do HOST. Provar isso
// exige subprocesso: time.Local é resolvido uma vez por processo, na primeira
// leitura, e um t.Setenv("TZ", ...) depois disso não muda mais nada — um
// teste in-process passaria sem testar coisa alguma.
//
// O subprocesso reexecuta ESTE binário de teste com a variável no ambiente e
// só o caso interno, que ingere um texto sem offset e imprime o epoch millis
// resultante. Fusos diferentes têm de dar números diferentes, e cada um tem de
// bater com o oráculo da stdlib no mesmo fuso.
const envFusoFilho = "NISHIDB_TESTE_FUSO_FILHO"

func TestOFusoDoHostVemDaVariavelTZ(t *testing.T) {
	if os.Getenv(envFusoFilho) != "" {
		rodaCasoDeFusoNoFilho(t)
		return
	}

	casos := []struct {
		tz    string
		horas int // offset esperado em relação a UTC, no instante do caso
	}{
		{"UTC", 0},
		{"America/Sao_Paulo", -3},
		{"Asia/Tokyo", +9},
	}
	base := time.Date(2026, 8, 22, 20, 54, 2, 0, time.UTC).UnixMilli()

	for _, c := range casos {
		t.Run(c.tz, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run", "TestOFusoDoHostVemDaVariavelTZ", "-test.v")
			cmd.Env = append(os.Environ(), envFusoFilho+"=1", "TZ="+c.tz)
			saida, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("subprocesso com TZ=%s falhou: %v\n%s", c.tz, err, saida)
			}
			got, ok := extraiMillis(string(saida))
			if !ok {
				t.Fatalf("subprocesso não imprimiu o millis:\n%s", saida)
			}
			// O mesmo texto de parede em fusos diferentes é um instante
			// diferente, e o deslocamento é exatamente o offset da zona.
			want := base - int64(c.horas)*3600_000
			if got != want {
				t.Fatalf("TZ=%s: millis=%d, esperado %d (offset de %+dh)", c.tz, got, want, c.horas)
			}
		})
	}
}

const marcaMillis = "MILLIS_DO_FILHO="

func rodaCasoDeFusoNoFilho(t *testing.T) {
	h := novoTiposHarness(t)
	h.ingere(`{"id":"K1","_SEQ":1,"momento":"Aug 22, 2026 08:54:02 PM"}`)

	// Confere contra o oráculo da stdlib NESTE processo, com ESTE time.Local:
	// é o que prova que a ingestão usou o fuso do host e não um fixo.
	oraculo := time.Date(2026, 8, 22, 20, 54, 2, 0, time.Local).UnixMilli()
	got := h.achaIDs(fmt.Sprintf(`SELECT id FROM todos_os_tipos WHERE momento = %d`, oraculo))
	if len(got) != 1 {
		t.Fatalf("o valor ingerido não bate com time.Date em time.Local (%s)", time.Local)
	}
	fmt.Printf("%s%d\n", marcaMillis, oraculo)
}

func extraiMillis(saida string) (int64, bool) {
	for _, linha := range strings.Split(saida, "\n") {
		linha = strings.TrimSpace(linha)
		if !strings.HasPrefix(linha, marcaMillis) {
			continue
		}
		var v int64
		if _, err := fmt.Sscanf(linha[len(marcaMillis):], "%d", &v); err == nil {
			return v, true
		}
	}
	return 0, false
}
