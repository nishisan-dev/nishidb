package kafka_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func eventoRuim() *kgo.Record {
	return &kgo.Record{Key: []byte("X"), Value: []byte(`{isso nao e json}`)}
}

// eventoGigante viola o teto de FORMATO num campo com bitmap. É culpa do
// evento (EVENT_TOO_LARGE) e por isso governado pela política do §101.
func eventoGigante() *kgo.Record {
	return &kgo.Record{
		Key: []byte("G"),
		Value: []byte(`{"id":"G","_SEQ":1,"status":"` +
			strings.Repeat("x", types.MaxIndexedValueBytes+1) + `"}`),
	}
}

// D6/§101 — log-and-skip descarta e AVANÇA o offset. Não avançar aqui seria
// livelock: o mesmo evento voltaria para sempre.
func TestLogAndSkipDescartaEAvancaOOffset(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"), eventoRuim(), evento("B", 1, "Start"))
	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 3 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Descartados != 1 || d.Aplicados != 2 {
		t.Errorf("desfecho = %+v, quer 1 descarte e 2 aplicados", d)
	}
	offsets, _ := b.leOffsets()
	if offsets[0] != 3 {
		t.Errorf("offset = %d, quer 3: o descarte tem de avançar, senão o evento ruim volta para sempre", offsets[0])
	}
}

// D13 — o teto de formato é culpa do evento e cai na MESMA política. Até o M1
// isso era STORAGE_ERROR, e num daemon a escolha seria entre perda invisível
// e livelock.
func TestEventoAcimaDoTetoSegueAPolitica(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"), eventoGigante(), evento("B", 1, "Start"))
	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 3 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Falhas != 0 {
		t.Errorf("falhas = %d, quer 0: teto de formato é culpa do evento", d.Falhas)
	}
	if d.Descartados != 1 {
		t.Errorf("descartados = %d, quer 1", d.Descartados)
	}
	if offsets, _ := b.leOffsets(); offsets[0] != 3 {
		t.Errorf("offset = %d, quer 3", offsets[0])
	}
}

// Issue #15 / Task A5 — nishidb_ingest_invalid_total quebra por `motivo`
// desde esta task (internal/kafka/metricas.go): antes dela, EVENT_TOO_LARGE
// (a cauda de chave que estoura MaxKeySize, medida no piloto em 1099 e 1074
// bytes — docs/piloto-events.md) ficava indistinguível de INVALID_JSON dentro
// do mesmo total, e nem o painel nem um alerta tinham como nomear a causa.
func TestInvalidosContamPorMotivoEmMetrics(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(eventoRuim(), eventoGigante())
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 2 }); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if _, err := b.reg.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	saida := buf.String()
	for _, linha := range []string{
		`nishidb_ingest_invalid_total{dataset="radius_sessions",motivo="EVENT_TOO_LARGE"} 1`,
		`nishidb_ingest_invalid_total{dataset="radius_sessions",motivo="INVALID_JSON"} 1`,
	} {
		if !strings.Contains(saida, linha) {
			t.Errorf("linha %q não apareceu em /metrics:\n%s", linha, saida)
		}
	}
}

// D6 — `stop` PARA a ingestão e NÃO avança o offset, para que o operador veja
// exatamente o evento problemático. E não encerra o processo: o §51 põe as
// consultas antes da ingestão, e o §82 permite vários datasets por instância.
func TestStopParaAIngestaoSemAvancarOOffset(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	b.cfg.InvalidEvent = schema.InvalidEventStop

	b.produz(evento("A", 1, "Start"), eventoRuim(), evento("B", 1, "Start"))

	_, err := b.consomeAte(prazo, func(kafka.Desfecho) bool { return false })
	if !errors.Is(err, kafka.ErrPausado) {
		t.Fatalf("erro = %v, quer ErrPausado", err)
	}

	offsets, existe := b.leOffsets()
	if !existe {
		t.Fatal("kafka.offsets não foi criado; o que já entrou tem de ficar durável")
	}
	// O primeiro evento entrou (offset 0), o segundo parou tudo: a posição
	// tem de ser exatamente 1 — o índice do evento envenenado.
	if offsets[0] != 1 {
		t.Errorf("offset = %d, quer 1 (apontando para o evento que parou a ingestão)", offsets[0])
	}
	// O dataset continua consultável.
	if b.st.RowCount() != 1 {
		t.Errorf("RowCount = %d, quer 1: o que entrou antes da parada tem de estar lá", b.st.RowCount())
	}
}

// D11 — dataset COM linhas e SEM posição é estado anômalo. Aplicar o
// autoOffsetReset aqui reprocessaria o tópico retido inteiro (earliest) ou
// pularia tudo (latest), os dois em silêncio.
func TestDatasetComLinhasESemOffsetsRecusaASubida(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"))
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 1 }); err != nil {
		t.Fatal(err)
	}
	if b.st.RowCount() == 0 {
		t.Fatal("o dataset deveria ter linhas")
	}

	// Apaga a posição, como um restore parcial faria.
	b.apagaOffsets()

	_, err := b.abreConsumidor()
	if !errors.Is(err, kafka.ErrPosicaoDesconhecida) {
		t.Fatalf("erro = %v, quer ErrPosicaoDesconhecida", err)
	}
	if !strings.Contains(err.Error(), "nishidb offsets") {
		t.Errorf("a mensagem não aponta a saída: %v", err)
	}
}

// Dataset VAZIO e sem posição é o caso normal da primeira subida.
func TestDatasetVazioSemOffsetsSobeNormalmente(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	cons, err := b.abreConsumidor()
	if err != nil {
		t.Fatalf("dataset vazio deveria subir: %v", err)
	}
	cons.Fecha()
}

// D14 — offset é número sem significado fora do seu fluxo. Apontar o dataset
// para outro tópico faria seek num offset alheio: o consumo começaria no
// meio, sem erro em camada nenhuma.
func TestTrocaDeTopicoRecusaASubida(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"))
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 1 }); err != nil {
		t.Fatal(err)
	}

	b.cfg.Topic = "outro-topico"
	_, err := b.abreConsumidor()
	if !errors.Is(err, kafka.ErrOrigemDivergente) {
		t.Fatalf("erro = %v, quer ErrOrigemDivergente", err)
	}
	if !strings.Contains(err.Error(), "topic") {
		t.Errorf("a mensagem não nomeia o campo que mudou: %v", err)
	}
}

func TestTrocaDeGroupIdRecusaASubida(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"))
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 1 }); err != nil {
		t.Fatal(err)
	}

	b.cfg.GroupID = "outro-group"
	_, err := b.abreConsumidor()
	if !errors.Is(err, kafka.ErrOrigemDivergente) {
		t.Fatalf("erro = %v, quer ErrOrigemDivergente", err)
	}
	if !strings.Contains(err.Error(), "groupId") {
		t.Errorf("a mensagem não nomeia o campo que mudou: %v", err)
	}
}

// §102 — a chave é aceita no parse (senão o snippet literal da spec seria
// inválido) e recusada na ativação. O que não pode é ser aceita em silêncio.
func TestDeadLetterTopicERecusadoNaAtivacao(t *testing.T) {
	_, err := kafka.Resolve("d",
		&schema.KafkaSpec{
			BootstrapServers: []string{"kafka01:9092"},
			Topic:            "t",
			Consumer:         schema.ConsumerSpec{GroupID: "g"},
		},
		schema.ErrorsSpec{InvalidEvent: schema.InvalidEventLogAndSkip, DeadLetterTopic: "t-dlq"})
	if !errors.Is(err, kafka.ErrNaoImplementado) {
		t.Fatalf("erro = %v, quer ErrNaoImplementado", err)
	}
}

// §95 — a credencial é NOME de variável, e a falta do VALOR é falha de
// ativação com mensagem que nomeia a variável (nunca o valor).
func TestCredencialAusenteFalhaRapidoENomeiaAVariavel(t *testing.T) {
	k := &schema.KafkaSpec{
		BootstrapServers: []string{"kafka01:9092"},
		Topic:            "t",
		Consumer:         schema.ConsumerSpec{GroupID: "g"},
		Security: schema.SecuritySpec{
			Protocol:      schema.ProtocolSASLSSL,
			SASLMechanism: schema.SASLScramSHA512,
			UsernameEnv:   "NISHIDB_TESTE_USUARIO_INEXISTENTE",
			PasswordEnv:   "NISHIDB_TESTE_SENHA_INEXISTENTE",
		},
	}
	_, err := kafka.Resolve("d", k, schema.ErrorsSpec{InvalidEvent: schema.InvalidEventLogAndSkip})
	if !errors.Is(err, kafka.ErrConfig) {
		t.Fatalf("erro = %v, quer ErrConfig", err)
	}
	if !strings.Contains(err.Error(), "NISHIDB_TESTE_USUARIO_INEXISTENTE") {
		t.Errorf("a mensagem não nomeia a variável: %v", err)
	}
}

// Variável definida e VAZIA é o modo de falha do deploy por container: o
// orquestrador injeta a chave sem valor e o processo sobe autenticando errado.
func TestCredencialVaziaEhRecusada(t *testing.T) {
	t.Setenv("NISHIDB_TESTE_USUARIO", "")
	t.Setenv("NISHIDB_TESTE_SENHA", "irrelevante")

	k := &schema.KafkaSpec{
		BootstrapServers: []string{"kafka01:9092"},
		Topic:            "t",
		Consumer:         schema.ConsumerSpec{GroupID: "g"},
		Security: schema.SecuritySpec{
			Protocol:      schema.ProtocolSASLPlaintext,
			SASLMechanism: schema.SASLPlain,
			UsernameEnv:   "NISHIDB_TESTE_USUARIO",
			PasswordEnv:   "NISHIDB_TESTE_SENHA",
		},
	}
	_, err := kafka.Resolve("d", k, schema.ErrorsSpec{InvalidEvent: schema.InvalidEventLogAndSkip})
	if err == nil || !strings.Contains(err.Error(), "definida e vazia") {
		t.Errorf("erro = %v, quer recusa por variável vazia", err)
	}
}

// O encerramento gracioso torna durável o que entrou; sem isso, uma parada
// normal jogaria fora até um intervalo inteiro de checkpoint.
func TestEncerramentoGraciosoCheckpointaOQueEntrou(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"), evento("B", 1, "Start"))
	// Cadência larga: sem o checkpoint de encerramento, nada seria durável.
	d, err := b.consomeComCadencia(prazo, kafka.Cadencia{Eventos: 1_000_000, Intervalo: time.Hour},
		func(d kafka.Desfecho) bool { return d.Eventos >= 2 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Aplicados != 2 {
		t.Fatalf("aplicados = %d, quer 2", d.Aplicados)
	}
	offsets, existe := b.leOffsets()
	if !existe || offsets[0] != 2 {
		t.Errorf("offsets = %v (existe=%v), quer {0:2} pelo checkpoint de encerramento", offsets, existe)
	}
}

// Tópico inexistente tem de RECUSAR a subida.
//
// Sem isto o consumer sobe, não recebe assignment nenhum e fica sentado: o
// health reporta UP, as métricas mostram 0 partições, e nada é consumido — o
// operador vê tudo verde com o dataset parado. A guarda de assignment não
// pega o caso, porque ela compara atribuídas com total e 0 == 0.
func TestTopicoInexistenteRecusaASubida(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.cfg.Topic = "topico-que-nao-existe"
	_, err := b.abreConsumidor()
	if !errors.Is(err, kafka.ErrTopicoAusente) {
		t.Fatalf("erro = %v, quer ErrTopicoAusente", err)
	}
	if !strings.Contains(err.Error(), "topico-que-nao-existe") {
		t.Errorf("a mensagem não nomeia o tópico: %v", err)
	}
}
