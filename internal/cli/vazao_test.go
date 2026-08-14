//go:build !race

package cli_test

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// A ingestão pelo CLI não pode custar muito mais que o caminho interno
// (materializer + store) sobre os MESMOS eventos. O que o gate procura é custo
// POR EVENTO no envelope do comando — checkpoint fora de política, I/O por
// evento —, e a referência é MEDIDA na mesma máquina, no mesmo processo e
// intercalada no tempo, não cravada no relógio.
//
// COMO A MEDIDA É FEITA, e por que ela mudou duas vezes:
//
//  1. Piso absoluto de 4.000 ev/s: media hardware e instrumentação junto com o
//     código. Vermelho sem defeito nenhum.
//  2. Razão entre as duas pernas medidas EM SEQUÊNCIA (uma perna de 20.000
//     eventos, depois a outra): a razão só é adimensional se a carga da
//     máquina for a mesma nas duas janelas, e numa suíte com pacotes em
//     paralelo ela não é. Medido nesta máquina (12 vCPU, `go test ./...
//     -count=1`, 12 execuções): razões de 0,30x a 1,62x SEM mutação — e um
//     revisor pegou 3,27x, vermelho, em 1 de 6 execuções.
//  3. O desenho atual: 5 rodadas curtas ALTERNADAS (CLI, direto, CLI,
//     direto...) de 5.000 eventos cada, cada rodada num dataset novo em folha,
//     e a decisão sai da MEDIANA das 5 razões PAREADAS. O par vive em janelas
//     de dezenas de milissegundos uma da outra, então os dois lados pegam a
//     mesma carga; a mediana descarta a rodada em que só um dos lados foi
//     atropelado. As rodadas individuais continuam ruidosas (n=80 sem
//     mutação: 0,52x a 2,01x, p50 1,04, p90 1,25) — é a mediana pareada que
//     dá a estabilidade, e por isso ela é o que o gate lê.
//
// Uma mediana acima do fator NÃO reprova de imediato: o teste roda mais 5
// rodadas e decide pela mediana das 10. Dobrar a evidência antes de acusar não
// afrouxa o fator — ele é o mesmo nas duas etapas — e derruba o falso vermelho
// que sobra da cauda. Custa tempo só quando já há suspeita.
//
// CALIBRAÇÃO (tudo medido nesta máquina, `go test ./... -count=1`, suíte
// inteira, que é onde a contenção aparece):
//
//	sem mutação          22 execuções   mediana de 0,96x a 1,24x   0 vermelhos
//	checkpoint a cada    12 execuções   mediana de 1,78x a 2,01x  12 vermelhos
//	  500 eventos
//	intervalo do YAML    12 execuções   mediana de 1,88x a 2,16x  12 vermelhos
//	  1000x menor (bug
//	  de unidade em units.Duracao)
//
// O fator 1,4 fica entre as duas populações: 13% acima do pior caso sem
// mutação e 27% abaixo do melhor caso com mutação.
//
// ONDE O GATE PARA, também medido: checkpoint a cada 2.500 eventos (2
// disparos por rodada, contra 10 da mutação a cada 500) dá 1,26x a 1,34x em 8
// execuções e NÃO é pego; json.Unmarshal por evento dá 1,13x a 1,40x; aplicar
// o evento duas vezes dá 1,03x a 1,31x (as duas últimas, 3 e 4 execuções
// isoladas). A razão é aritmética: o laço direto roda a ~16.100 ev/s (mediana
// de n=80), ~62µs por evento, e esses µs são pagos pelos DOIS lados — o que o
// envelope acrescenta só sai do ruído a partir de ~40% do laço. Regressão
// pequena no envelope este gate não vê; quem vê é o orçamento de alocações e
// os benchmarks de benchmark/micro.
//
// FORA DO BUILD COM -race, e isto foi medido, não presumido: o detector infla
// o laço mas não infla o custo de um checkpoint, então a mesma mutação que dá
// 1,78x-2,01x aqui dá 1,12x-1,30x sob -race (12 execuções), contra 0,96x-1,13x
// sem mutação (16 execuções). As duas populações se tocam: não existe fator
// que separe uma da outra — e é esse o fato que tira o gate desta trilha.
// (O quanto o detector infla o laço varia com máquina, carga e versão do Go:
// medições nesta máquina ficaram entre ~3,7x e ~5x conforme a condição. O
// número não é estável o bastante para virar afirmação; a sobreposição das
// duas populações é, e foi reproduzida.) O gate roda em `make test`, se
// anuncia como SKIP em `make test-race` (vazao_race_test.go), e o que ele NÃO
// cobre continua sendo regressão no storage — que derruba os dois lados na
// mesma proporção e mantém a razão. Essa é a jurisdição de benchmark/micro e
// dos baselines em benchmark/baselines.
const (
	rodadasDaVazao   = 5
	eventosPorRodada = 5_000

	fatorMaximoSobreOCaminhoDireto = 1.4

	// A janela que o gate compara é a que o comando cronometra: aplicar os
	// eventos e gravar o checkpoint final. Abrir o dataset e fechá-lo ficam
	// fora dos DOIS lados, porque são custo O(1) e não custo por evento —
	// cobrá-los só dilui a medida. Abrir e fechar valem 19% a 29% do tempo de
	// parede de uma rodada (n=30), e medir com eles dentro encolhia a mutação
	// de checkpoint de 1,72x-1,91x para 1,58x-1,73x (4 execuções isoladas de
	// cada, mesmo formato).
	//
	// Como o número vem do resumo do próprio comando, ele é conferido contra o
	// relógio de fora: a janela reportada tem de caber no tempo real da
	// chamada. Medido no formato atual (n=30 rodadas), a taxa reportada fica
	// 1,23x a 1,41x acima da taxa de parede; num formato com rodadas menores
	// chegou a 2,61x. O limite de 4,0 acusa um cronômetro quebrado — que
	// desarmaria o gate em silêncio — sem reclamar de máquina carregada.
	fatorMaximoDaJanelaReportada = 4.0
)

var reTaxaDoResumo = regexp.MustCompile(`\(([0-9]+) ev/s\)`)

func TestVazaoDaIngestaoPeloCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("mede vazão de ingestão")
	}
	entrada := corpus(t, eventosPorRodada)

	razoes := rodadasDeVazao(t, entrada, rodadasDaVazao)
	if mediana(razoes) > fatorMaximoSobreOCaminhoDireto {
		t.Logf("ETAPA2 mediana %.2f acima do fator; dobrando a evidência", mediana(razoes))
		razoes = append(razoes, rodadasDeVazao(t, entrada, rodadasDaVazao)...)
	}
	m := mediana(razoes)
	t.Logf("RAZOES %s MEDIANA %.2f", formata(razoes), m)
	if m > fatorMaximoSobreOCaminhoDireto {
		t.Errorf("o laço de ingestão do comando custou %.2fx o do caminho direto sobre os MESMOS eventos "+
			"(máximo %.1fx, mediana de %d rodadas pareadas: %s): o envelope do comando ganhou custo POR EVENTO",
			m, fatorMaximoSobreOCaminhoDireto, len(razoes), formata(razoes))
	}
}

// rodadasDeVazao alterna as duas pernas em rodadas curtas e devolve a razão
// PAREADA de cada rodada.
func rodadasDeVazao(t *testing.T, entrada string, n int) []float64 {
	t.Helper()
	razoes := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		taxaCLI, taxaDeParede := umaRodadaPeloCLI(t, entrada)
		taxaDireta := umaRodadaDireta(t, entrada)
		t.Logf("rodada %d: CLI %.0f ev/s (parede %.0f, %.2fx), direto %.0f ev/s (%.2fx)",
			i+1, taxaCLI, taxaDeParede, taxaCLI/taxaDeParede, taxaDireta, taxaDireta/taxaCLI)
		razoes = append(razoes, taxaDireta/taxaCLI)
	}
	return razoes
}

func formata(v []float64) string {
	var sb strings.Builder
	for i, x := range v {
		if i > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%.2f", x)
	}
	return sb.String()
}

func mediana(v []float64) float64 {
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	if len(c)%2 == 1 {
		return c[len(c)/2]
	}
	return (c[len(c)/2-1] + c[len(c)/2]) / 2
}

// umaRodadaPeloCLI ingere um lote pelo comando, num dataset novo em folha, e
// devolve a taxa do LAÇO de ingestão que o próprio comando reporta.
func umaRodadaPeloCLI(t *testing.T, entrada string) (float64, float64) {
	t.Helper()
	a := novoAmbiente(t)
	inicio := time.Now()
	code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")
	parede := time.Since(inicio)
	if code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	saida := a.stderr.String()
	m := reTaxaDoResumo.FindStringSubmatch(saida)
	if m == nil {
		t.Fatalf("o resumo do ingest não reporta a taxa em ev/s, e o gate mede por ela:\n%s", saida)
	}
	taxa, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatal(err)
	}
	taxaDeParede := float64(eventosPorRodada) / parede.Seconds()
	if taxa > fatorMaximoDaJanelaReportada*taxaDeParede {
		t.Fatalf("o resumo reporta %.0f ev/s contra %.0f ev/s de parede (%.1fx): "+
			"a janela cronometrada pelo comando não fecha com o tempo real",
			taxa, taxaDeParede, taxa/taxaDeParede)
	}
	return taxa, taxaDeParede
}

// umaRodadaDireta faz o MESMO trabalho pelo materializer sobre um Store real,
// sem nada do comando, e cronometra a MESMA janela que o comando reporta:
// aplicar os eventos e gravar o checkpoint final. Abrir o dataset e fechá-lo
// ficam fora dos dois lados.
func umaRodadaDireta(t *testing.T, entrada string) float64 {
	t.Helper()
	spec, err := schema.Load("../../testdata/radius.materialization.yaml", schema.DefaultsPadrao)
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

	if n != eventosPorRodada {
		t.Fatalf("caminho direto aplicou %d eventos, esperado %d", n, eventosPorRodada)
	}
	return float64(n) / dur.Seconds()
}
