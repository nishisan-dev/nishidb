package cli

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
)

// Padrões do checkpoint periódico.
//
// LIGADO por padrão, e este é o ponto mais importante do comando: o overlay de
// locations vive no heap até um checkpoint aplicá-lo ao mmap. Sem checkpoint,
// o consumo de memória acompanha o TAMANHO DO DATASET — medido em 13,19x de
// crescimento de heap para 10x de dados, contra 0,97x com checkpoint. A
// promessa central da arquitetura (heap desacoplado do volume) só vale com ele.
const (
	checkpointEventosPadrao = 500_000
	checkpointTempoPadrao   = 30 * time.Second
)

type resumoIngest struct {
	lidos, aplicados, criados, atualizados, stale int64
	invalidos                                     map[materializer.ErrorCategory]int64
}

func runIngest(args []string, amb Ambiente) int {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		config    = fs.String("config", "", "caminho do nishidb.yaml")
		dataset   = fs.String("dataset", "", "nome do dataset")
		input     = fs.String("input", "-", "arquivo NDJSON de entrada, ou - para stdin")
		everyN    = fs.Int("checkpoint-every", checkpointEventosPadrao, "checkpoint a cada N eventos (0 desliga)")
		everyFlag = false
	)
	if err := fs.Parse(args); err != nil {
		return ExitUso
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "checkpoint-every" {
			everyFlag = true
		}
	})
	if *config == "" || *dataset == "" {
		fmt.Fprintln(amb.Stderr, "ingest: --config e --dataset são obrigatórios")
		return ExitUso
	}
	if *everyN < 0 {
		fmt.Fprintln(amb.Stderr, "ingest: --checkpoint-every não pode ser negativo")
		return ExitUso
	}
	if everyFlag && *everyN == 0 {
		// Desligar é permitido, e nunca em silêncio: quem desliga precisa saber
		// que trocou memória constante por memória proporcional ao dataset.
		fmt.Fprintln(amb.Stderr,
			"ingest: AVISO — checkpoint periódico DESLIGADO (--checkpoint-every 0). "+
				"O consumo de memória vai acompanhar o tamanho do dataset.")
	}

	cfg, err := CarregaConfig(*config)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "ingest:", err)
		return ExitUso
	}

	entrada, fechar, err := abreEntrada(*input, amb.Stdin)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "ingest:", err)
		return ExitUso
	}
	defer fechar()

	st, compilado, err := abreDataset(cfg, *dataset)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "ingest:", err)
		return codigoDoErro(err)
	}
	// O Close faz quiesce e só então devolve o lock — é o que permite ao
	// comando seguinte assumir o dataset.
	defer st.Close()

	mat := materializer.New(compilado, st)
	resumo := resumoIngest{invalidos: map[materializer.ErrorCategory]int64{}}
	inicio := time.Now()
	ultimoCP := inicio

	sc := bufio.NewScanner(entrada)
	// Uma linha é um evento, e um record pode ter até 4 MiB: o buffer padrão
	// de 64 KiB do Scanner cortaria eventos legítimos.
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)

	for sc.Scan() {
		if err := amb.Ctx.Err(); err != nil {
			fmt.Fprintln(amb.Stderr, "ingest: interrompido; gravando checkpoint do que foi aplicado")
			if cperr := st.Checkpoint(); cperr != nil {
				fmt.Fprintln(amb.Stderr, "ingest: checkpoint final:", cperr)
			}
			imprimeResumo(amb.Stderr, resumo, time.Since(inicio))
			return ExitInterromp
		}
		linha := sc.Bytes()
		if len(linha) == 0 {
			continue
		}
		resumo.lidos++

		r := mat.Apply(linha)
		switch {
		case r.Fatal():
			// Writer em modo falho: continuar é desperdício, porque nenhum
			// evento seguinte vai ser aceito até o dataset reabrir.
			fmt.Fprintf(amb.Stderr, "ingest: writer em modo falho no evento %d: %v\n", resumo.lidos, r.Err)
			imprimeResumo(amb.Stderr, resumo, time.Since(inicio))
			return ExitWriter
		case r.Err != nil:
			resumo.invalidos[r.Category]++
		default:
			resumo.aplicados++
			switch r.Action {
			case materializer.ActionCreated:
				resumo.criados++
			case materializer.ActionUpdated:
				resumo.atualizados++
			case materializer.ActionStale:
				resumo.stale++
			}
		}

		if precisaCheckpoint(*everyN, resumo.lidos, ultimoCP, cfg.checkpointInterval) {
			if err := st.Checkpoint(); err != nil {
				fmt.Fprintln(amb.Stderr, "ingest: checkpoint:", err)
				imprimeResumo(amb.Stderr, resumo, time.Since(inicio))
				return codigoDoErro(err)
			}
			ultimoCP = time.Now()
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(amb.Stderr, "ingest: lendo a entrada:", err)
		imprimeResumo(amb.Stderr, resumo, time.Since(inicio))
		return ExitUso
	}

	if err := st.Checkpoint(); err != nil {
		fmt.Fprintln(amb.Stderr, "ingest: checkpoint final:", err)
		imprimeResumo(amb.Stderr, resumo, time.Since(inicio))
		return codigoDoErro(err)
	}
	imprimeResumo(amb.Stderr, resumo, time.Since(inicio))
	return ExitOK
}

// precisaCheckpoint dispara por CONTAGEM ou por TEMPO, o que vier primeiro:
// uma ingestão lenta não pode ficar horas sem checkpoint só porque não atingiu
// a contagem, e uma rápida não pode esperar o relógio.
func precisaCheckpoint(everyN int, lidos int64, ultimo time.Time, intervalo time.Duration) bool {
	if everyN == 0 {
		return false
	}
	if lidos%int64(everyN) == 0 {
		return true
	}
	if intervalo <= 0 {
		intervalo = checkpointTempoPadrao
	}
	return time.Since(ultimo) >= intervalo
}

func abreEntrada(caminho string, stdin io.Reader) (io.Reader, func(), error) {
	if caminho == "-" || caminho == "" {
		if stdin == nil {
			return nil, func() {}, errors.New("--input - exige stdin")
		}
		return stdin, func() {}, nil
	}
	f, err := os.Open(caminho)
	if err != nil {
		return nil, func() {}, err
	}
	return f, func() { f.Close() }, nil
}

func imprimeResumo(w io.Writer, r resumoIngest, dur time.Duration) {
	taxa := 0.0
	if dur > 0 {
		taxa = float64(r.lidos) / dur.Seconds()
	}
	fmt.Fprintf(w, "ingest: %d lidos, %d aplicados (%d criados, %d atualizados, %d stale) em %s (%.0f ev/s)\n",
		r.lidos, r.aplicados, r.criados, r.atualizados, r.stale, dur.Round(time.Millisecond), taxa)
	var total int64
	for _, n := range r.invalidos {
		total += n
	}
	if total == 0 {
		return
	}
	fmt.Fprintf(w, "ingest: %d descartados por categoria:", total)
	// Ordem fixa: a saída é lida por humano e por script, e mapa iterado dá
	// ordem diferente a cada execução.
	for c := materializer.CategoryInvalidJSON; c <= materializer.CategoryStorage; c++ {
		if n := r.invalidos[c]; n > 0 {
			fmt.Fprintf(w, " %v=%d", c, n)
		}
	}
	fmt.Fprintln(w)
}
