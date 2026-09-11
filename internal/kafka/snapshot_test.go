package kafka_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
)

// consumidorComEventos sobe um consumidor sobre a bancada, produz n eventos e
// espera todos serem APLICADOS — por condição, nunca por sleep. Devolve o
// consumidor rodando e a função que o encerra.
func consumidorComEventos(t *testing.T, b *bancada, n int) (*kafka.Consumidor, func()) {
	t.Helper()
	recs := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		recs = append(recs, evento("E"+strconv.Itoa(i), 1, "start"))
	}
	if n > 0 {
		b.produz(recs...)
	}
	ctx, cancela := context.WithTimeout(context.Background(), 15*time.Second)
	cons, err := kafka.Abre(ctx, b.cfg, kafka.Opcoes{
		Store: b.st, Mat: b.mat, Metrics: b.reg, Cadencia: cadenciaSemCommit,
	})
	if err != nil {
		cancela()
		t.Fatalf("abre: %v", err)
	}
	fim := make(chan error, 1)
	go func() { fim <- cons.Roda(ctx) }()

	prazo := time.After(10 * time.Second)
	tique := time.NewTicker(5 * time.Millisecond)
	defer tique.Stop()
	for cons.Desfecho().Eventos < int64(n) {
		select {
		case <-tique.C:
		case err := <-fim:
			cancela()
			t.Fatalf("consumidor parou cedo com %d de %d eventos: %v", cons.Desfecho().Eventos, n, err)
		case <-prazo:
			cancela()
			t.Fatalf("aplicou %d de %d eventos no prazo", cons.Desfecho().Eventos, n)
		}
	}
	return cons, func() {
		cancela()
		<-fim
		cons.Fecha()
	}
}

// TestOInstantaneoEAsSeriesDaPosicaoConcordam é o teste da FONTE ÚNICA: a
// tabela `_meta.kafka` e o /metrics leem a mesma estrutura pela mesma regra.
//
// Se o Instantaneo recalculasse o lag por conta própria, os dois números
// concordariam hoje e divergiriam no dia em que um dos dois lados mudasse — que
// é exatamente o defeito que o coletor único fechou quando o lag do painel
// media o offset commitado.
func TestOInstantaneoEAsSeriesDaPosicaoConcordam(t *testing.T) {
	const n = 25
	b := novaBancada(t, 1)
	defer b.fecha()
	cons, para := consumidorComEventos(t, b, n)
	defer para()

	ctx := context.Background()
	inst := cons.Instantaneo(ctx) // a marca nasce vencida: vai ao broker
	texto := raspa(t, b)

	if len(inst.Particoes) != 1 {
		t.Fatalf("%d partições no instantâneo, quero 1: %+v", len(inst.Particoes), inst.Particoes)
	}
	p := inst.Particoes[0]
	if !p.TemFim {
		t.Fatalf("o broker respondeu no Instantaneo, o fim do log tinha de estar lá: %+v", p)
	}
	for _, c := range []struct {
		name  string
		serie string
		quer  float64
	}{
		{"offset", "nishidb_kafka_offset", float64(p.Aplicado)},
		{"high watermark", "nishidb_kafka_high_watermark", float64(p.FimDoLog)},
		{"lag", "nishidb_kafka_lag", float64(p.Atraso)},
	} {
		got, ok := serie(t, texto, c.serie, p.Particao)
		if !ok {
			t.Fatalf("%s ausente do scrape:\n%s", c.serie, texto)
		}
		if got != c.quer {
			t.Fatalf("%s: scrape %v, instantâneo %v — as duas leituras deixaram de sair da mesma regra", c.name, got, c.quer)
		}
	}
	if p.Aplicado != n {
		t.Fatalf("offset aplicado %d, quer %d", p.Aplicado, n)
	}
	// A invariante que o coletor único sustenta vale também aqui dentro.
	if p.FimDoLog-p.Aplicado != p.Atraso {
		t.Fatalf("lag %d, mas fim-aplicado %d", p.Atraso, p.FimDoLog-p.Aplicado)
	}
	if inst.MarcaEm.IsZero() {
		t.Fatal("com fim do log presente, o instante da leitura tem de carimbá-lo")
	}
}

// TestInstantaneoOmiteMarcaDaguaVencida: marca velha não vira número velho,
// vira AUSÊNCIA. Zero é medição, e um lag zerado por marca vencida diria
// "está em dia" a quem perdeu o broker.
func TestInstantaneoOmiteMarcaDaguaVencida(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	cons, para := consumidorComEventos(t, b, 3)
	defer para()

	cons.PublicaMarcaParaTeste(map[int32]int64{0: 999}, time.Now().Add(-2*time.Minute))
	// Contexto já cancelado: a ida ao broker falha, e o que sobra é a decisão
	// sobre a marca vencida — que é o que este teste mede.
	ctx, cancela := context.WithCancel(context.Background())
	cancela()
	inst := cons.Instantaneo(ctx)

	if len(inst.Particoes) != 1 {
		t.Fatalf("a posição aplicada não depende do broker e tinha de sair: %+v", inst.Particoes)
	}
	if p := inst.Particoes[0]; p.TemFim || p.FimDoLog != 0 || p.Atraso != 0 {
		t.Fatalf("marca vencida não pode virar fim do log nem lag: %+v", p)
	}
	if !inst.MarcaEm.IsZero() {
		t.Fatalf("sem marca válida não há instante de leitura: %v", inst.MarcaEm)
	}
}

// TestInstantaneoNaoVaiAoBrokerComMarcaValida: dentro da validade o retrato
// reusa a última leitura. Sem isto, um cliente consultando `_meta.kafka` em
// laço viraria carga no broker.
//
// A prova é uma marca FABRICADA que o broker jamais devolveria: se ela
// sobrevive à chamada, não houve ida.
func TestInstantaneoNaoVaiAoBrokerComMarcaValida(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	cons, para := consumidorComEventos(t, b, 3)
	defer para()

	const fabricado = 987654
	cons.PublicaMarcaParaTeste(map[int32]int64{0: fabricado}, time.Now())
	inst := cons.Instantaneo(context.Background())

	p := inst.Particoes[0]
	if !p.TemFim || p.FimDoLog != fabricado {
		t.Fatalf("a marca válida foi substituída por uma ida ao broker: %+v", p)
	}
	if p.Atraso != fabricado-p.Aplicado {
		t.Fatalf("lag %d, quer %d", p.Atraso, fabricado-p.Aplicado)
	}
}

// TestInstantaneoNaoTemLagNegativo: leitura do broker mais velha que o consumo
// não é lag negativo, é zero — e o fim do log continua saindo como foi lido.
func TestInstantaneoNaoTemLagNegativo(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	cons, para := consumidorComEventos(t, b, 5)
	defer para()

	cons.PublicaMarcaParaTeste(map[int32]int64{0: 1}, time.Now())
	p := cons.Instantaneo(context.Background()).Particoes[0]
	if p.Aplicado <= p.FimDoLog {
		t.Fatalf("a bancada não exercita o caso: aplicado %d, fim %d", p.Aplicado, p.FimDoLog)
	}
	if p.Atraso != 0 {
		t.Fatalf("atraso %d, quer 0", p.Atraso)
	}
	if !p.TemFim || p.FimDoLog != 1 {
		t.Fatalf("o fim do log tem de sair como foi lido: %+v", p)
	}
}

// TestInstantaneoTrazAFilaEOsContadores: as colunas que não dependem do broker
// saem do mesmo retrato, e são as mesmas estruturas do /v1/stats.
func TestInstantaneoTrazAFilaEOsContadores(t *testing.T) {
	const n = 7
	b := novaBancada(t, 1)
	defer b.fecha()
	cons, para := consumidorComEventos(t, b, n)
	defer para()

	inst := cons.Instantaneo(context.Background())
	if inst.Desfecho.Eventos != n || inst.Desfecho.Aplicados != n {
		t.Fatalf("desfecho = %+v, quero %d eventos aplicados", inst.Desfecho, n)
	}
	if inst.FilaRegistros != 0 {
		t.Fatalf("com tudo aplicado a fila tinha de estar vazia: %d", inst.FilaRegistros)
	}
	if inst.ParticoesAtribuidas != 1 || inst.ParticoesDoTopico != 1 {
		t.Fatalf("partições atribuídas %d de %d, quero 1 de 1", inst.ParticoesAtribuidas, inst.ParticoesDoTopico)
	}
	if inst.Pausa != nil {
		t.Fatalf("ingestão saudável não tem pausa: %v", inst.Pausa)
	}
}

// TestUltimoLoteNaoAndaSemProducao: com o tópico parado, o instante do último
// lote fica onde estava — é o silêncio que ele existe para tornar visível.
//
// O QUE ESTE TESTE NÃO PROVA. A guarda `NumRecords() > 0` no poller não é
// observável por esta bancada: o PollRecords BLOQUEIA enquanto não há
// registros, então nenhum poll vazio acontece aqui e mover a marcação para
// fora da guarda deixa a suíte verde. A guarda continua no código porque o
// fetch PODE voltar vazio — rebalance, erro por partição —, e ali ela é a
// diferença entre "o Kafka entregou" e "o cliente acordou". O que se mede
// abaixo é a propriedade de ponta, sustentada pelo bloqueio do poll.
func TestUltimoLoteNaoAndaSemProducao(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	cons, para := consumidorComEventos(t, b, 3)
	defer para()

	primeiro := cons.Instantaneo(context.Background()).UltimoLote
	if primeiro.IsZero() {
		t.Fatal("três eventos consumidos e nenhum lote registrado")
	}
	// Sem produzir nada, o poller fica bloqueado no PollRecords.
	time.Sleep(300 * time.Millisecond)
	depois := cons.Instantaneo(context.Background()).UltimoLote
	if !depois.Equal(primeiro) {
		t.Fatalf("o instante andou sem lote nenhum: %v → %v", primeiro, depois)
	}
}

// TestSemConsumoOUltimoLoteEhZero: nenhum lote é ausência, não "agora".
func TestSemConsumoOUltimoLoteEhZero(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	cons, para := consumidorComEventos(t, b, 0)
	defer para()

	if lote := cons.Instantaneo(context.Background()).UltimoLote; !lote.IsZero() {
		t.Fatalf("sem evento nenhum o último lote tem de ser ausência: %v", lote)
	}
}
