package cli

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
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
	lidos, aplicados, criados, atualizados, substituidos, stale int64
	descartados                                                 [materializer.NumCategories]int64
	falhas                                                      [materializer.NumCategories]int64
	descartadosTotal, falhasTotal                               int64
}

// gravidadeIngest ordena os desfechos possíveis de uma ingestão do MAIS grave
// para o menos grave. A ordem é o contrato com quem lê $?: numa execução que
// termina por um motivo E acumula perda, o código de saída tem de ser o que
// exige a ação mais forte, não o do evento que ocorreu por último.
//
//   - 4 (writer em modo falho) primeiro: é o único que prescreve um passo
//     extra — reabrir o dataset — antes de qualquer reprocessamento, e já
//     implica que a carga está incompleta.
//   - 3 (dataset) em seguida: checkpoint que falha significa delta não
//     durável; o alvo da investigação é o dataset, não os eventos.
//   - 7 (perda) antes de 6 (sinal): a interrupção foi PEDIDA por quem chamou
//     e ele já sabe dela; a perda é notícia, e esconder notícia atrás de um
//     desfecho conhecido é exatamente o defeito que criou o 7.
//   - 1 (usage) por último entre os erros: é o vala-comum do errorCode, o
//     único código que manda o operador olhar a line de comando. Qualquer
//     desfecho nomeado diz mais.
var gravidadeIngest = [...]int{ExitWriter, ExitDataset, ExitDataLoss, ExitPolicyStopped, ExitInterrupted, ExitUsageError}

// desfechoMaisGrave devolve, entre dois códigos de saída, o de maior
// gravidade. Código sem gravidade declarada (nenhum é alcançável hoje pelo
// ingest) vale o mesmo que o vala-comum: perde para os nomeados e vence o
// sucesso, porque sair 0 com desfecho anômalo é a perda silenciosa de volta.
func mostSevereOutcome(a, b int) int {
	if a == b {
		return a
	}
	for _, c := range gravidadeIngest {
		if a == c {
			return a
		}
		if b == c {
			return b
		}
	}
	// Nenhum dos dois é nomeado: o sucesso perde para qualquer anomalia.
	if a == ExitOK {
		return b
	}
	return a
}

func runIngest(args []string, amb Context) int {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig = fs.String("config", "", "caminho do nishidb.yaml")
		dataset   = fs.String("dataset", "", "nome do dataset")
		input     = fs.String("input", "-", "arquivo NDJSON de entrada, ou - para stdin")
		everyN    = fs.Int("checkpoint-every", checkpointEventosPadrao, "checkpoint a cada N eventos (0 desliga)")
		everyFlag = false
	)
	if err := fs.Parse(args); err != nil {
		return ExitUsageError
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "checkpoint-every" {
			everyFlag = true
		}
	})
	if *argConfig == "" || *dataset == "" {
		fmt.Fprintln(amb.Stderr, "ingest: --config e --dataset são obrigatórios")
		return ExitUsageError
	}
	if *everyN < 0 {
		fmt.Fprintln(amb.Stderr, "ingest: --checkpoint-every não pode ser negativo")
		return ExitUsageError
	}
	if everyFlag && *everyN == 0 {
		// Desligar é permitido, e nunca em silêncio: quem desliga precisa saber
		// que trocou memória constante por memória proporcional ao dataset.
		fmt.Fprintln(amb.Stderr,
			"ingest: AVISO — checkpoint periódico DESLIGADO (--checkpoint-every 0). "+
				"O consumo de memória vai acompanhar o tamanho do dataset.")
	}

	cfg, err := nishiconfig.Load(*argConfig)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "ingest:", err)
		return ExitUsageError
	}

	entrada, fechar, err := abreEntrada(*input, amb.Stdin)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "ingest:", err)
		return ExitUsageError
	}
	defer fechar()

	st, compilado, spec, err := openDataset(cfg, *dataset)
	if err != nil {
		fmt.Fprintln(amb.Stderr, "ingest:", err)
		return errorCode(err)
	}
	// O Close faz quiesce e só então devolve o lock — é o que permite ao
	// comando seguinte assumir o dataset.
	defer st.Close()

	// A tabela irmã abre DEPOIS do base e fecha ANTES dele (o defer é LIFO):
	// ela usa o dicionário do base, e fechá-lo primeiro derrubaria o
	// checkpoint dela no meio.
	hist, campoRaw, err := openHistory(cfg, spec, compilado, st.FieldDict())
	if err != nil {
		_, _ = fmt.Fprintln(amb.Stderr, "ingest:", err)
		return errorCode(err)
	}
	if hist != nil {
		// O erro do Close da irmã é REPORTADO, e não engolido como o do base
		// logo acima: ali o quiesce que interessa é o do dataset de produção,
		// e aqui um Close que falha significa linha de evidência que não
		// chegou ao disco. Ele não muda o código de saída — a ingestão do base
		// terminou —, mas quem ingeriu precisa saber.
		defer func() {
			if err := hist.Close(); err != nil {
				_, _ = fmt.Fprintln(amb.Stderr, "ingest: fechando a tabela histórica:", err)
			}
		}()
	}

	mat, err := materializer.New(compilado, st)
	if err != nil {
		// Regra de TTL que não compila é erro de CONFIGURAÇÃO: o dataset não
		// sobe, e a ingestão nem começa.
		fmt.Fprintln(amb.Stderr, "ingest:", err)
		return errorCode(err)
	}
	if hist != nil {
		mat.LigaHistorico(hist, campoRaw)
	}
	var resumo resumoIngest
	inicio := time.Now()
	ultimoCP := inicio

	// encerra é o ÚNICO ponto de saída depois que a ingestão começou. Todo
	// caminho de retorno passa por aqui porque cada retorno antecipado que
	// pulava o teste de falhasTotal transformava perda de dado num código de
	// saída que fala de outra coisa. O motivo do encerramento não substitui a
	// perda: os dois são impressos, e o código devolvido é o MAIS GRAVE.
	encerra := func(motivo int) int {
		printSummary(amb.Stderr, resumo, time.Since(inicio))
		if resumo.falhasTotal == 0 {
			return motivo
		}
		// A ingestão pode ter terminado com o dataset íntegro e checkpointado
		// — mas eventos foram perdidos por falha. Quem automatiza precisa
		// distinguir isto de sucesso, e não pode confundir com o writer falho
		// (4), onde nada mais entra até reabrir.
		fmt.Fprintf(amb.Stderr,
			"ingest: %d eventos NÃO entraram no dataset por falha — investigue antes de considerar a carga completa\n",
			resumo.falhasTotal)
		return mostSevereOutcome(motivo, ExitDataLoss)
	}

	sc := bufio.NewScanner(entrada)
	// Uma linha é um evento, e um record pode ter até 4 MiB: o buffer padrão
	// de 64 KiB do Scanner cortaria eventos legítimos.
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)

	for sc.Scan() {
		if err := amb.Ctx.Err(); err != nil {
			fmt.Fprintln(amb.Stderr, "ingest: interrompido; gravando checkpoint do que foi aplicado")
			motivo := ExitInterrupted
			if cperr := checkpointDoPar(st, hist); cperr != nil {
				fmt.Fprintln(amb.Stderr, "ingest: checkpoint final:", cperr)
				motivo = mostSevereOutcome(motivo, errorCode(cperr))
			}
			return encerra(motivo)
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
			return encerra(ExitWriter)
		case r.Category.BadEvent():
			resumo.descartados[r.Category]++
			resumo.descartadosTotal++
			// §101: a política do dataset decide entre seguir e parar. Até o
			// M2 ela era hardcoded como log-and-skip, e a chave `errors:` do
			// YAML não era sequer parseada — o operador escrevia `stop` e
			// nada acontecia.
			if compilado.Errors.InvalidEvent == schema.InvalidEventStop {
				fmt.Fprintf(amb.Stderr,
					"ingest: errors.invalidEvent=stop e o evento %d é %v: %v\n",
					resumo.lidos, r.Category, r.Err)
				return encerra(ExitPolicyStopped)
			}
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
			case materializer.ActionReplaced:
				resumo.substituidos++
			case materializer.ActionStale:
				resumo.stale++
			}
		}

		if needsCheckpoint(*everyN, resumo.lidos, ultimoCP, cfg.CheckpointInterval()) {
			if err := checkpointDoPar(st, hist); err != nil {
				fmt.Fprintln(amb.Stderr, "ingest: checkpoint:", err)
				return encerra(errorCode(err))
			}
			ultimoCP = time.Now()
		}
	}

	// Erro de leitura é PERDA, não erro de uso: o que vinha depois do erro
	// nunca chegou ao materializador, e a ação de quem recebe o código é a do
	// 7 — investigar e reprocessar —, não conferir a linha de comando. Difere
	// da interrupção por sinal (6) porque ninguém pediu esta parada.
	//
	// O que JÁ entrou é durabilizado antes de sair, do mesmo modo que no
	// caminho do sinal: deixar o delta para o tail scan da recovery seria
	// trocar um desfecho conhecido por um conserto na próxima abertura.
	erroLeitura := sc.Err()
	if erroLeitura != nil {
		fmt.Fprintf(amb.Stderr,
			"ingest: ENTRADA INTERROMPIDA na leitura: %v — o que vinha depois NÃO foi ingerido, a carga está incompleta\n",
			erroLeitura)
	}

	motivo := ExitOK
	if erroLeitura != nil {
		motivo = ExitDataLoss
	}
	if err := checkpointDoPar(st, hist); err != nil {
		fmt.Fprintln(amb.Stderr, "ingest: checkpoint final:", err)
		motivo = mostSevereOutcome(motivo, errorCode(err))
	}
	return encerra(motivo)
}

// precisaCheckpoint dispara por CONTAGEM ou por TEMPO, o que vier primeiro:
// uma ingestão lenta não pode ficar horas sem checkpoint só porque não atingiu
// a contagem, e uma rápida não pode esperar o relógio.
func needsCheckpoint(everyN int, lidos int64, ultimo time.Time, intervalo time.Duration) bool {
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

func printSummary(w io.Writer, r resumoIngest, dur time.Duration) {
	taxa := 0.0
	if dur > 0 {
		taxa = float64(r.lidos) / dur.Seconds()
	}
	// substituidos aparece porque `mode: replace` funcionava e era contado no
	// materializer, mas sumia do resumo — o total não fechava com as parcelas.
	fmt.Fprintf(w, "ingest: %d lidos, %d aplicados (%d criados, %d atualizados, %d substituídos, %d stale) em %s (%.0f ev/s)\n",
		r.lidos, r.aplicados, r.criados, r.atualizados, r.substituidos, r.stale, dur.Round(time.Millisecond), taxa)

	// Duas linhas distintas para os dois baldes: "descartado" é uma decisão
	// sobre um evento ruim, "não aplicado" é perda. Juntá-las numa linha só foi
	// o que deixou a perda passar por descarte.
	rowByCategory(w, "descartados por categoria", r.descartadosTotal, r.descartados)
	rowByCategory(w, "não aplicados por falha (perda de dado)", r.falhasTotal, r.falhas)
}

// linhaPorCategoria imprime na ordem de DECLARAÇÃO das categorias e percorre
// todas elas. A ordem é fixa porque a saída é lida por script; o limite é
// NumCategories porque escrevê-lo à mão foi o que fez INTERNAL_STATE sair como
// uma line de contagem sem categoria nenhuma.
func rowByCategory(w io.Writer, rotulo string, total int64, por [materializer.NumCategories]int64) {
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

// checkpointDoPar durabiliza a tabela irmã ANTES do dataset base.
//
// Mesma ordem do §48 aplicada ao servidor, e pela mesma razão: o checkpoint
// do base é o ponto a partir do qual o trabalho é considerado feito, e um
// crash entre os dois precisa deixar o histórico à FRENTE, nunca atrás. A
// falha da irmã não impede o do base — o produto é o dataset materializado.
func checkpointDoPar(base, hist *storage.Store) error {
	if hist == nil {
		return base.Checkpoint()
	}
	return storage.ParComHistorico{Store: base, Hist: hist}.Checkpoint()
}
