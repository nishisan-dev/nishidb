package kafka_test

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
)

// cadenciaSemCommit não deixa o checkpoint disparar dentro do teste, e é o
// coração desta bancada: o commit no group só sai DEPOIS do checkpoint durável
// (§48), então sem checkpoint não há commit — que é exatamente o estado em que
// o piloto passa 30 segundos de cada 30.
var cadenciaSemCommit = kafka.Cadencia{Eventos: 1 << 30, Intervalo: time.Hour}

// serie extrai o valor de uma série com labels do texto do /metrics.
func serie(t *testing.T, texto, nome string, part int32) (float64, bool) {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(nome) +
		`\{dataset="[^"]*",partition="` + strconv.Itoa(int(part)) + `"\} (\S+)$`)
	m := re.FindStringSubmatch(texto)
	if m == nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("valor não numérico em %s: %q", nome, m[1])
	}
	return v, true
}

func raspa(t *testing.T, b *bancada) string {
	t.Helper()
	var sb strings.Builder
	if _, err := b.reg.WriteTo(&sb); err != nil {
		t.Fatalf("scrape: %v", err)
	}
	return sb.String()
}

// O TESTE DO DEFEITO: o lag reflete o que foi APLICADO, não o que foi
// commitado no group.
//
// nishidb_kafka_lag vinha de kadm.Lag, que devolve o lag do offset COMMITADO —
// e o commit só sai depois do checkpoint durável (§48), a cada 30s no piloto.
// Entre dois commits o offset do group fica parado e a série cresce à taxa de
// PRODUÇÃO, com o consumidor perfeitamente em dia.
//
// Medido no piloto em 28/08/2026: a série subia até 14.167 e caía para 515 num
// único segundo em que 478 eventos foram consumidos; o crescimento entre
// commits era de 450 ev/s contra 452 ev/s de produção. O painel mandou a
// investigação atrás de um gargalo de vazão que não era o problema.
//
// Aqui a cadência garante que NENHUM checkpoint dispare, logo nenhum commit
// sai. Com a implementação antiga o lag seria o total produzido; com esta ele
// é zero, porque a pergunta passou a ser "quanto falta para o fim do log".
func TestLagNaoDependeDoCommitNoGroup(t *testing.T) {
	const n = 25
	b := novaBancada(t, 1)
	defer b.fecha()

	recs := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		recs = append(recs, evento("E"+strconv.Itoa(i), 1, "start"))
	}
	b.produz(recs...)

	ctx, cancela := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancela()
	cons, err := kafka.Abre(ctx, b.cfg, kafka.Opcoes{
		Store: b.st, Mat: b.mat, Metrics: b.reg, Cadencia: cadenciaSemCommit,
	})
	if err != nil {
		t.Fatalf("abre: %v", err)
	}
	defer cons.Fecha()

	fim := make(chan error, 1)
	go func() { fim <- cons.Roda(ctx) }()

	// Espera o consumo terminar por CONDIÇÃO, não por sleep.
	prazo := time.After(10 * time.Second)
	tique := time.NewTicker(5 * time.Millisecond)
	defer tique.Stop()
	for cons.Desfecho().Eventos < n {
		select {
		case <-tique.C:
		case err := <-fim:
			t.Fatalf("consumidor parou cedo com %d eventos: %v", cons.Desfecho().Eventos, err)
		case <-prazo:
			t.Fatalf("consumiu %d de %d eventos no prazo", cons.Desfecho().Eventos, n)
		}
	}

	cons.AtualizaMarcaDagua(ctx)
	texto := raspa(t, b)

	lag, ok := serie(t, texto, "nishidb_kafka_lag", 0)
	if !ok {
		t.Fatalf("nishidb_kafka_lag não foi emitida:\n%s", texto)
	}
	if lag != 0 {
		t.Fatalf("lag %v com os %d eventos aplicados e nenhum commit; "+
			"a série voltou a medir o offset commitado", lag, n)
	}

	// A invariante que só um coletor único consegue sustentar.
	off, ok1 := serie(t, texto, "nishidb_kafka_offset", 0)
	hwm, ok2 := serie(t, texto, "nishidb_kafka_high_watermark", 0)
	if !ok1 || !ok2 {
		t.Fatalf("offset ou high watermark ausentes:\n%s", texto)
	}
	if hwm-off != lag {
		t.Fatalf("lag=%v mas high_watermark-offset=%v: as três séries deixaram de "+
			"sair do mesmo instantâneo", lag, hwm-off)
	}
	if off != float64(n) {
		t.Fatalf("offset aplicado %v, quer %d", off, n)
	}
	cancela()
	<-fim
}

// Sem marca d'água do broker, as séries que dependem dele NÃO SAEM.
//
// Zero é medição; ausência é ausência de medição. `absent(nishidb_kafka_lag)`
// é um alerta que o operador consegue escrever, e um zero mentiroso não é. É a
// mesma doutrina que o coletor da fila já segue.
//
// O offset aplicado CONTINUA saindo: ele não depende do broker, e sumir com
// ele esconderia o progresso do consumidor justamente quando o broker está
// fora do ar.
func TestSemRespostaDoBrokerOLagNaoEEmitido(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	b.produz(evento("E0", 1, "start"))

	ctx, cancela := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancela()
	cons, err := kafka.Abre(ctx, b.cfg, kafka.Opcoes{
		Store: b.st, Mat: b.mat, Metrics: b.reg, Cadencia: cadenciaSemCommit,
	})
	if err != nil {
		t.Fatalf("abre: %v", err)
	}
	defer cons.Fecha()

	fim := make(chan error, 1)
	go func() { fim <- cons.Roda(ctx) }()
	prazo := time.After(10 * time.Second)
	tique := time.NewTicker(5 * time.Millisecond)
	defer tique.Stop()
	for cons.Desfecho().Eventos < 1 {
		select {
		case <-tique.C:
		case <-fim:
			t.Fatal("consumidor parou antes de aplicar o evento")
		case <-prazo:
			t.Fatal("evento não foi aplicado no prazo")
		}
	}

	// NUNCA chama AtualizaMarcaDagua: é o estado de "o broker não respondeu".
	texto := raspa(t, b)
	if _, ok := serie(t, texto, "nishidb_kafka_lag", 0); ok {
		t.Fatal("lag foi emitido sem nenhuma leitura do broker; " +
			"série publicada sem medição é pior que série ausente")
	}
	if _, ok := serie(t, texto, "nishidb_kafka_high_watermark", 0); ok {
		t.Fatal("high watermark foi emitido sem leitura do broker")
	}
	if _, ok := serie(t, texto, "nishidb_kafka_offset", 0); !ok {
		t.Fatalf("o offset aplicado sumiu junto: ele não depende do broker\n%s", texto)
	}
	cancela()
	<-fim
}
