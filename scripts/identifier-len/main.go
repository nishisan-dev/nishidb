// Command identifier-len mede a distribuição REAL de tamanho de um campo
// (por padrão `identifier`) sobre um tópico Kafka INTEIRO — a medição que a
// issue #15 pede antes de decidir entre elevar MaxKeySize, derivar a chave
// por hash ou aceitar (docs/piloto-events.md, item 4). Consome do earliest
// até o high-watermark capturado na partida, sem consumer group e sem
// commit de offset: é leitura, e não pode interferir com a ingestão real que
// compartilha o mesmo tópico nem deixar posição nenhuma para trás.
//
// Não entra no binário `nishidb` — é `go run`, e o comando abaixo é o exato
// para o host do piloto (brokers de docs/piloto-events.md, PLAINTEXT, sem
// TLS/SASL):
//
//	go run ./scripts/identifier-len \
//	    --brokers 10.113.144.229,10.113.144.230,10.113.144.237,10.113.144.238:9093 \
//	    --topic events --field identifier
//
// A rodada NÃO some no tail: como o watermark é capturado uma vez na
// partida, ela termina mesmo com o tópico recebendo tráfego novo o tempo
// todo. Com ~30,6M mensagens retidas (docs/piloto-events.md), espere minutos
// de execução — o --report-every (default 1.000.000) é o que dá visibilidade
// de progresso nesse meio-tempo, no stderr.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// topNRelatorio é o N do "lista dos N maiores" do brief. Não é flag: o brief
// só lista quatro flags (--brokers, --topic, --field, --report-every), e uma
// ferramenta de operador não ganha superfície que ninguém pediu.
const topNRelatorio = 10

func main() {
	os.Exit(executar(os.Args[1:], os.Stdout, os.Stderr))
}

func executar(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("identifier-len", flag.ContinueOnError)
	fs.SetOutput(stderr)

	brokersFlag := fs.String("brokers", "", "brokers Kafka, separados por vírgula (obrigatório; PLAINTEXT)")
	topico := fs.String("topic", "events", "tópico a medir")
	campo := fs.String("field", "identifier", "campo do payload JSON cujo tamanho em bytes é medido")
	reportEvery := fs.Int64("report-every", 1_000_000, "mensagens entre cada linha de progresso no stderr")

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `identifier-len — mede a distribuição de tamanho de um campo sobre um tópico Kafka inteiro (issue #15)

Consome do earliest até o high-watermark capturado na PARTIDA, sem consumer
group e sem commit de offset. O tópico pode continuar recebendo mensagens
durante a rodada: elas não entram na medição, porque o alvo já foi fixado
antes do primeiro fetch.

Uso no host do piloto (docs/piloto-events.md):

  go run ./scripts/identifier-len \
      --brokers 10.113.144.229,10.113.144.230,10.113.144.237,10.113.144.238:9093 \
      --topic events --field identifier

TLS/SASL não são suportados: os brokers do piloto são PLAINTEXT.

`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if strings.TrimSpace(*brokersFlag) == "" {
		_, _ = fmt.Fprintln(stderr, "erro: --brokers é obrigatório")
		fs.Usage()
		return 2
	}
	brokers := strings.Split(*brokersFlag, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	progresso := func(lidos, alvo int64, decorrido time.Duration) {
		var pct, taxa float64
		if alvo > 0 {
			pct = float64(lidos) / float64(alvo) * 100
		}
		if s := decorrido.Seconds(); s > 0 {
			taxa = float64(lidos) / s
		}
		_, _ = fmt.Fprintf(stderr, "progresso: %d/%d lidas (%.1f%% do watermark) — %.0f msgs/s\n",
			lidos, alvo, pct, taxa)
	}

	hist, err := Coleta(ctx, ConfigColeta{
		Brokers:     brokers,
		Topico:      *topico,
		Campo:       *campo,
		ReportEvery: *reportEvery,
	}, progresso)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "erro: %v\n", err)
		return 1
	}

	ImprimeRelatorio(stdout, *campo, hist, topNRelatorio)
	return 0
}
