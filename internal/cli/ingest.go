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

// resumoIngest separa os DOIS baldes de erro da §101, e a separação é a razão
// de o tipo existir. Evento ruim é decisão tomada: descarta e segue, exit 0.
// O resto — erro do store, inconsistência interna — significa que a row NÃO
// entrou por falha nossa ou do dataset: é perda de dado, e sair com 0 dali faz
// um pipeline que testa $? tratar perda como sucesso.
type resumoIngest struct {
	lidos, aplicados, criados, atualizados, stale int64
	descartados                                   [materializer.NumCategories]int64
	falhas                                        [materializer.NumCategories]int64
	descartadosTotal, falhasTotal                 int64
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
	var resumo resumoIngest
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
		case r.Category.BadEvent():
			resumo.descartados[r.Category]++
			resumo.descartadosTotal++
		case r.Err != nil:
			// Balde 2: a row não entrou e não foi por culpa do evento.
			resumo.falhas[r.Category]++
			resumo.falhasTotal++
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
	if resumo.falhasTotal > 0 {
		// A ingestão terminou, o dataset está íntegro e checkpointado — mas
		// eventos foram perdidos por falha. Quem automatiza precisa distinguir
		// isto de sucesso, e não pode confundir com o writer falho (4), onde
		// nada mais entra até reabrir.
		fmt.Fprintf(amb.Stderr,
			"ingest: %d eventos NÃO entraram no dataset por falha — investigue antes de considerar a carga completa\n",
			resumo.falhasTotal)
		return ExitPerdaDeDados
	}
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

	// Duas linhas distintas para os dois baldes: "descartado" é uma decisão
	// sobre um evento ruim, "não aplicado" é perda. Juntá-las numa linha só foi
	// o que deixou a perda passar por descarte.
	linhaPorCategoria(w, "descartados por categoria", r.descartadosTotal, r.descartados)
	linhaPorCategoria(w, "não aplicados por falha (perda de dado)", r.falhasTotal, r.falhas)
}

// linhaPorCategoria imprime na ordem de DECLARAÇÃO das categorias e percorre
// todas elas. A ordem é fixa porque a saída é lida por script; o limite é
// NumCategories porque escrevê-lo à mão foi o que fez INTERNAL_STATE sair como
// uma linha de contagem sem categoria nenhuma.
func linhaPorCategoria(w io.Writer, rotulo string, total int64, por [materializer.NumCategories]int64) {
	if total == 0 {
		return
	}
	fmt.Fprintf(w, "ingest: %d %s:", total, rotulo)
	for c := materializer.CategoryInvalidJSON; c < materializer.NumCategories; c++ {
		if n := por[c]; n > 0 {
			fmt.Fprintf(w, " %v=%d", c, n)
		}
	}
	fmt.Fprintln(w)
}
