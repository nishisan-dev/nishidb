package cli_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// A ingestão pelo CLI não pode custar uma ORDEM DE GRANDEZA a mais que o
// caminho interno (materializer + store) sobre os MESMOS eventos. O que o
// gate procura é custo POR EVENTO no envelope do comando: buffer
// redimensionando, checkpoint a cada linha, flush por evento.
//
// A referência é MEDIDA na mesma máquina, no mesmo processo e no mesmo
// instante — não é um número de wall-clock cravado. Um piso absoluto em ev/s
// mede o hardware e a instrumentação do -race junto com o código: sob -race a
// medida cai de ~30.000 para ~4.700 ev/s (6,4x), e com a máquina carregada
// desce a 3.700 — vermelho sem defeito nenhum. Teste intermitente é desligado,
// e desligá-lo custaria a proteção inteira. A razão entre os dois caminhos é
// adimensional: o -race e a carga penalizam os dois lados juntos.
//
// Medido em três regimes (sem -race ocioso, sem -race com a máquina carregada,
// com -race e carga): 0,97x a 1,14x. O fator 3 deixa mais de 2,5x de folga
// sobre o pior caso observado e ainda pega uma regressão de 3,7x — checkpoint
// a cada 500 eventos em vez de 500.000 —, que o piso absoluto de 4.000 ev/s
// deixava passar sem reclamar.
//
// O que este gate NÃO cobre: regressão no caminho de storage, que derruba os
// dois lados na mesma proporção e mantém a razão. Essa é a jurisdição dos
// benchmarks de benchmark/micro e dos baselines em benchmark/baselines.
const fatorMaximoSobreOCaminhoDireto = 3.0

const eventosDaVazao = 20_000

func TestVazaoDaIngestaoPeloCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("mede vazão de ingestão")
	}
	entrada := corpus(t, eventosDaVazao)

	// A referência sai primeiro de propósito: ela paga o aquecimento de page
	// cache e alocador do processo, o que só a torna mais conservadora (piso
	// menor). O gate não pode ser mais frouxo com o CLI por acidente de ordem,
	// e o CLI mede depois, com tudo aquecido.
	referencia := vazaoDoCaminhoDireto(t, entrada)

	a := novoAmbiente(t)
	inicio := time.Now()
	code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")
	dur := time.Since(inicio)
	if code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}

	taxa := float64(eventosDaVazao) / dur.Seconds()
	piso := referencia / fatorMaximoSobreOCaminhoDireto
	t.Logf("%d eventos em %s: %.0f ev/s pelo CLI, %.0f ev/s pelo caminho direto (%.2fx, piso %.0f ev/s)",
		eventosDaVazao, dur.Round(time.Millisecond), taxa, referencia, referencia/taxa, piso)
	if taxa < piso {
		t.Errorf("%.0f ev/s pelo CLI contra %.0f ev/s diretos (%.2fx, máximo %.1fx): "+
			"o caminho do comando ganhou custo POR EVENTO",
			taxa, referencia, referencia/taxa, fatorMaximoSobreOCaminhoDireto)
	}
	if !strings.Contains(a.stderr.String(), "ev/s") {
		t.Error("o resumo deveria reportar a taxa")
	}
}

// vazaoDoCaminhoDireto ingere os mesmos eventos pelo materializer sobre um
// Store real, sem nada do comando: sem flag parsing, sem Scanner, sem resumo.
// O checkpoint final entra porque o CLI também o faz — a referência tem de
// pagar os mesmos custos O(1) da ingestão, senão o gate cobraria do CLI um
// trabalho que a referência não fez.
func vazaoDoCaminhoDireto(t *testing.T, entrada string) float64 {
	t.Helper()
	spec, err := schema.Load("../../testdata/radius.materialization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var bitmaps []string
	for campo, f := range spec.Schema.Fields {
		if f.Index == "bitmap" {
			bitmaps = append(bitmaps, campo)
		}
	}
	st, err := storage.Open(storage.Options{
		Dir:              t.TempDir(),
		SegmentSize:      8 << 20,
		BitmapFieldNames: bitmaps,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	mat := materializer.New(compilado, st)

	// A conversão para bytes fica FORA do cronômetro: o CLI lê de um
	// strings.Reader e fatia o buffer do Scanner, sem copiar por linha.
	dados := []byte(entrada)

	inicio := time.Now()
	n := 0
	for i := 0; i < len(dados); {
		fim := i
		for fim < len(dados) && dados[fim] != '\n' {
			fim++
		}
		if fim > i {
			r := mat.Apply(dados[i:fim])
			if r.Err != nil {
				t.Fatalf("caminho direto: evento %d: %v", n, r.Err)
			}
			n++
		}
		i = fim + 1
	}
	if err := st.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	dur := time.Since(inicio)

	if n != eventosDaVazao {
		t.Fatalf("caminho direto aplicou %d eventos, esperado %d", n, eventosDaVazao)
	}
	return float64(n) / dur.Seconds()
}
