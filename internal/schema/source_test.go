package schema_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// baseSemSource é o mínimo válido, sem bloco source nenhum.
const baseSemSource = `version: 1
name: d
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`

func comSource(corpo string) string {
	return "version: 1\nname: d\n" + corpo + `
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`
}

func parseSource(t *testing.T, yml string) *schema.DatasetSpec {
	t.Helper()
	s, err := schema.Parse([]byte(yml), schema.DefaultsPadrao)
	if err != nil {
		t.Fatalf("Parse: %v\n%s", err, yml)
	}
	return s
}

func recusa(t *testing.T, yml, trecho string) {
	t.Helper()
	_, err := schema.Parse([]byte(yml), schema.DefaultsPadrao)
	if err == nil {
		t.Fatalf("quer erro contendo %q, foi aceito:\n%s", trecho, yml)
	}
	if !strings.Contains(err.Error(), trecho) {
		t.Errorf("erro = %q, quer conter %q", err, trecho)
	}
}

const kafkaMinimo = `source:
  type: kafka
  kafka:
    bootstrapServers:
      - kafka01:9092
    topic: radius-accounting
    consumer:
      groupId: nishidb-radius`

// Dataset sem source continua válido: o alimentado por `nishidb ingest` não
// tem fonte declarada, e testdata/todos_os_tipos.yaml precisa continuar
// carregando.
func TestSourceAusenteEValido(t *testing.T) {
	s := parseSource(t, baseSemSource)
	if s.Source.Type != "" || s.Source.Kafka != nil {
		t.Errorf("Source = %+v, quer zerado", s.Source)
	}
}

func TestKafkaMinimoAplicaOsDefaults(t *testing.T) {
	s := parseSource(t, comSource(kafkaMinimo))
	if s.Source.Type != schema.SourceKafka {
		t.Fatalf("Type = %q", s.Source.Type)
	}
	k := s.Source.Kafka
	if k == nil {
		t.Fatal("Kafka nil")
	}
	// O default de enabled é o que impede o fixture do §12 de subir um
	// consumer contra kafka01:9092 numa troca de binário.
	if k.Enabled {
		t.Error("enabled deveria ser false por default")
	}
	if got := k.Consumer.AutoOffsetReset; got != schema.AutoOffsetResetEarliest {
		t.Errorf("autoOffsetReset = %q", got)
	}
	if got := k.Consumer.MaxPollRecords; got != schema.MaxPollRecordsPadrao {
		t.Errorf("maxPollRecords = %d", got)
	}
	if got := k.Consumer.IngestionQueueSize; got != schema.IngestionQueueSizePadrao {
		t.Errorf("ingestionQueueSize = %d", got)
	}
	if got := k.Consumer.IngestionQueueBytes; got != schema.IngestionQueueBytesPadrao {
		t.Errorf("ingestionQueueBytes = %d", got)
	}
	if got := k.Security.Protocol; got != schema.ProtocolPlaintext {
		t.Errorf("protocol = %q", got)
	}
	if s.Errors.InvalidEvent != schema.InvalidEventLogAndSkip {
		t.Errorf("errors.invalidEvent = %q", s.Errors.InvalidEvent)
	}
}

func TestSourceTypeEEnumFechado(t *testing.T) {
	recusa(t, comSource("source:\n  type: kinesis\n  kafka:\n    topic: t"), "source.type")
	recusa(t, comSource("source:\n  kafka:\n    topic: t"), "source.type obrigatório")
	recusa(t, comSource("source:\n  type: kafka"), "exige o bloco source.kafka")
}

// Sem a whitelist manual, `invalidEvant: stop` (typo) parseia limpo, cai no
// default, e a política que o operador escreveu simplesmente não existe.
func TestChaveDesconhecidaERecusada(t *testing.T) {
	casos := []struct{ nome, yml, trecho string }{
		{"em source", comSource("source:\n  type: kafka\n  tipo: kafka\n  kafka:\n    topic: t"), "source.tipo"},
		{"em kafka", comSource(kafkaMinimo + "\n    clientId: x"), "source.kafka.clientId"},
		{"em consumer", comSource(kafkaMinimo + "\n      brokerOffsetCommit: on-checkpoint"), "brokerOffsetCommit"},
		{"em security", comSource(kafkaMinimo + "\n    security:\n      insecureSkipVerify: true"), "insecureSkipVerify"},
		{"typo em errors", comSource(kafkaMinimo) + "errors:\n  invalidEvant: stop\n", "errors.invalidEvant"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) { recusa(t, c.yml, c.trecho) })
	}
}

// Um split ingênuo por ':' tornaria cluster IPv6-only inconfigurável — e não
// com erro de conexão, mas com erro de PARSE, que impediria até
// `nishidb query` de abrir o dataset.
func TestBootstrapAceitaIPv6(t *testing.T) {
	s := parseSource(t, comSource(`source:
  type: kafka
  kafka:
    bootstrapServers:
      - "[2001:db8::1]:9092"
      - "[::1]:9093"
      - kafka01:9092
    topic: t
    consumer:
      groupId: g`))
	if n := len(s.Source.Kafka.BootstrapServers); n != 3 {
		t.Errorf("bootstrapServers = %d itens, quer 3", n)
	}
}

func TestBootstrapInvalidoERecusado(t *testing.T) {
	casos := []struct{ nome, item, trecho string }{
		{"sem porta", "kafka01", "host:porta"},
		{"porta zero", "kafka01:0", "porta inválida"},
		{"porta acima do teto", "kafka01:65536", "porta inválida"},
		{"porta não numérica", "kafka01:noventa", "porta inválida"},
		{"sem host", ":9092", "sem host"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			recusa(t, comSource(`source:
  type: kafka
  kafka:
    bootstrapServers:
      - "`+c.item+`"
    topic: t
    consumer:
      groupId: g`), c.trecho)
		})
	}
	recusa(t, comSource(`source:
  type: kafka
  kafka:
    bootstrapServers: []
    topic: t
    consumer:
      groupId: g`), "lista vazia")
}

func TestObrigatoriosDoKafka(t *testing.T) {
	recusa(t, comSource(`source:
  type: kafka
  kafka:
    topic: t
    consumer:
      groupId: g`), "bootstrapServers obrigatório")

	recusa(t, comSource(`source:
  type: kafka
  kafka:
    bootstrapServers: [kafka01:9092]
    consumer:
      groupId: g`), "topic obrigatório")

	// groupId é obrigatório (D4): sem ele não há identidade de membership, e
	// um default derivado esconderia a colisão entre dois datasets.
	recusa(t, comSource(`source:
  type: kafka
  kafka:
    bootstrapServers: [kafka01:9092]
    topic: t`), "groupId obrigatório")

	recusa(t, comSource(`source:
  type: kafka
  kafka:
    bootstrapServers: [kafka01:9092]
    topic: t
    consumer:
      groupId: ""`), "groupId obrigatório")
}

// Zero e negativo não são "sem valor": uma fila de capacidade 0 não é "sem
// fila", é uma fila que nada atravessa.
func TestInteirosNaoPositivosSaoRecusados(t *testing.T) {
	for _, chave := range []string{"maxPollRecords", "ingestionQueueSize"} {
		for _, v := range []string{"0", "-1"} {
			recusa(t, comSource(kafkaMinimo+"\n      "+chave+": "+v), chave)
		}
	}
	recusa(t, comSource(kafkaMinimo+"\n      ingestionQueueBytes: 0"), "ingestionQueueBytes")
	recusa(t, comSource(kafkaMinimo+"\n      ingestionQueueBytes: 64MB"), "ingestionQueueBytes")
}

func TestIngestionQueueBytesAceitaUnidadeBinaria(t *testing.T) {
	s := parseSource(t, comSource(kafkaMinimo+"\n      ingestionQueueBytes: 128MiB"))
	if got := s.Source.Kafka.Consumer.IngestionQueueBytes; got != 128<<20 {
		t.Errorf("ingestionQueueBytes = %d, quer %d", got, 128<<20)
	}
}

func TestAutoOffsetResetEEnumFechado(t *testing.T) {
	recusa(t, comSource(kafkaMinimo+"\n      autoOffsetReset: none"), "autoOffsetReset")
	s := parseSource(t, comSource(kafkaMinimo+"\n      autoOffsetReset: latest"))
	if got := s.Source.Kafka.Consumer.AutoOffsetReset; got != schema.AutoOffsetResetLatest {
		t.Errorf("autoOffsetReset = %q", got)
	}
}

const securitySASL = `
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-512
      usernameEnv: KAFKA_USERNAME
      passwordEnv: KAFKA_PASSWORD`

func TestSecuritySASLCompletaEAceita(t *testing.T) {
	s := parseSource(t, comSource(kafkaMinimo+securitySASL))
	sec := s.Source.Kafka.Security
	if !sec.SASL() || !sec.TLS() {
		t.Errorf("SASL_SSL deveria ser SASL e TLS: %+v", sec)
	}
	if sec.UsernameEnv != "KAFKA_USERNAME" || sec.PasswordEnv != "KAFKA_PASSWORD" {
		t.Errorf("env vars = %+v", sec)
	}
}

// O parse valida só o NOME da variável. Resolver o valor aqui tornaria o
// fixture do §12 inválido numa suíte que roda sem KAFKA_USERNAME no ambiente.
func TestEnvVarNaoEResolvidaNoParse(t *testing.T) {
	if _, ok := os.LookupEnv("KAFKA_USERNAME"); ok {
		t.Skip("ambiente já define KAFKA_USERNAME; o teste perde o sentido")
	}
	parseSource(t, comSource(kafkaMinimo+securitySASL))
}

func TestCoerenciaSASL(t *testing.T) {
	casos := []struct{ nome, sec, trecho string }{
		{"mecanismo ausente", "\n    security:\n      protocol: SASL_SSL\n      usernameEnv: U\n      passwordEnv: P", "saslMechanism obrigatório"},
		{"mecanismo inválido", "\n    security:\n      protocol: SASL_SSL\n      saslMechanism: SCRAM-SHA-1\n      usernameEnv: U\n      passwordEnv: P", "saslMechanism"},
		{"usernameEnv ausente", "\n    security:\n      protocol: SASL_SSL\n      saslMechanism: PLAIN\n      passwordEnv: P", "usernameEnv obrigatório"},
		{"passwordEnv ausente", "\n    security:\n      protocol: SASL_SSL\n      saslMechanism: PLAIN\n      usernameEnv: U", "passwordEnv obrigatório"},
		{"mecanismo sem SASL", "\n    security:\n      protocol: SSL\n      saslMechanism: PLAIN", "não se aplica"},
		{"credencial sem SASL", "\n    security:\n      protocol: PLAINTEXT\n      usernameEnv: U", "não se aplica"},
		{"caFile sem TLS", "\n    security:\n      protocol: PLAINTEXT\n      caFile: /etc/ca.pem", "caFile não se aplica"},
		{"protocolo inválido", "\n    security:\n      protocol: SASL_TLS", "protocol"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) { recusa(t, comSource(kafkaMinimo+c.sec), c.trecho) })
	}
}

// A última linha de defesa contra o erro que o §95 mais teme: escrever a
// SENHA onde deveria ir o NOME da variável, e ela ir para o repositório.
func TestSenhaNoLugarDoNomeDeEnvVarERecusada(t *testing.T) {
	recusa(t, comSource(kafkaMinimo+`
    security:
      protocol: SASL_SSL
      saslMechanism: PLAIN
      usernameEnv: KAFKA_USERNAME
      passwordEnv: "s3nh@-de-verdade!"`), "NOME da variável")
}

func TestErrorsInvalidEvent(t *testing.T) {
	s := parseSource(t, comSource(kafkaMinimo)+"errors:\n  invalidEvent: stop\n")
	if s.Errors.InvalidEvent != schema.InvalidEventStop {
		t.Errorf("invalidEvent = %q", s.Errors.InvalidEvent)
	}
	recusa(t, comSource(kafkaMinimo)+"errors:\n  invalidEvent: explode\n", "errors.invalidEvent")
}

// §102 mostra a chave literalmente. Recusá-la no parse tornaria o snippet da
// spec inválido; ela é aceita aqui e recusada na subida do consumer.
func TestDeadLetterTopicEAceitoNoParse(t *testing.T) {
	s := parseSource(t, comSource(kafkaMinimo)+"errors:\n  deadLetterTopic: radius-accounting-dlq\n")
	if s.Errors.DeadLetterTopic != "radius-accounting-dlq" {
		t.Errorf("deadLetterTopic = %q", s.Errors.DeadLetterTopic)
	}
	recusa(t, comSource(kafkaMinimo)+"errors:\n  deadLetterTopic: \"\"\n", "deadLetterTopic vazio")
}

// O fixture canônico do projeto é cópia byte a byte do §12. Ele tem de
// continuar válido E INERTE: a suíte e os dois benchmarks do M1 dependem
// dele, e um consumer ligado ali mudaria o significado de todos.
func TestFixtureDoSpec12ContinuaValidoEInerte(t *testing.T) {
	s, err := schema.Load("../../testdata/radius.materialization.yaml", schema.DefaultsPadrao)
	if err != nil {
		t.Fatalf("o fixture do §12 deixou de ser válido: %v", err)
	}
	if s.Source.Type != schema.SourceKafka || s.Source.Kafka == nil {
		t.Fatalf("Source = %+v", s.Source)
	}
	if s.Source.Kafka.Enabled {
		t.Error("o fixture ficou ATIVO: uma troca de binário subiria um consumer contra kafka01:9092")
	}
	k := s.Source.Kafka
	if k.Topic != "radius-accounting" || k.Consumer.GroupID != "nishidb-radius" {
		t.Errorf("kafka = %+v", k)
	}
	if len(k.BootstrapServers) != 3 {
		t.Errorf("bootstrapServers = %v", k.BootstrapServers)
	}
	if k.Consumer.MaxPollRecords != 1000 || k.Consumer.IngestionQueueSize != 5000 {
		t.Errorf("consumer = %+v", k.Consumer)
	}
	if k.Security.Protocol != schema.ProtocolSASLSSL || k.Security.SASLMechanism != schema.SASLScramSHA512 {
		t.Errorf("security = %+v", k.Security)
	}
}

// yamlComConsumer monta uma spec com o bloco consumer dado.
func yamlComConsumer(consumer string) string {
	return "version: 1\nname: t\n" +
		"source:\n  type: kafka\n  kafka:\n    bootstrapServers: [\"h:9092\"]\n    topic: t\n" +
		"    consumer:\n      groupId: g\n" + consumer +
		"materialization:\n  key:\n    field: id\n" +
		"schema:\n  include: [id]\n  fields:\n    id:\n      type: string\n"
}

func TestWatchdogTemDefaultDeUmMinutoETresTentativas(t *testing.T) {
	s, err := schema.Parse([]byte(yamlComConsumer("")), schema.DefaultsPadrao)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	c := s.Source.Kafka.Consumer
	if c.WatchdogTimeout != time.Minute {
		t.Errorf("WatchdogTimeout = %s, quer 1m", c.WatchdogTimeout)
	}
	if c.WatchdogTentativas != 3 {
		t.Errorf("WatchdogTentativas = %d, quer 3", c.WatchdogTentativas)
	}
}

func TestWatchdogEhConfiguravel(t *testing.T) {
	s, err := schema.Parse([]byte(yamlComConsumer("      watchdogTimeout: 90s\n      watchdogTentativas: 5\n")), schema.DefaultsPadrao)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	c := s.Source.Kafka.Consumer
	if c.WatchdogTimeout != 90*time.Second {
		t.Errorf("WatchdogTimeout = %s, quer 90s", c.WatchdogTimeout)
	}
	if c.WatchdogTentativas != 5 {
		t.Errorf("WatchdogTentativas = %d, quer 5", c.WatchdogTentativas)
	}
}

// Zero seria uma janela que nenhum relógio poderia honrar; quem quer desligar
// diz isso com uma palavra.
func TestWatchdogDesligaComNever(t *testing.T) {
	s, err := schema.Parse([]byte(yamlComConsumer("      watchdogTimeout: never\n")), schema.DefaultsPadrao)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := s.Source.Kafka.Consumer.WatchdogTimeout; got != 0 {
		t.Fatalf("WatchdogTimeout = %s, quer zero (desligado)", got)
	}
}

// Abaixo do piso, uma pausa NORMAL de checkpoint viraria recriação de
// consumidor — no piloto ele para o writer por 774 ms.
func TestWatchdogAbaixoDoPisoEhRecusado(t *testing.T) {
	_, err := schema.Parse([]byte(yamlComConsumer("      watchdogTimeout: 3s\n")), schema.DefaultsPadrao)
	if err == nil {
		t.Fatal("3s foi aceito")
	}
	for _, trecho := range []string{"watchdogTimeout", "never"} {
		if !strings.Contains(err.Error(), trecho) {
			t.Errorf("a mensagem precisa nomear %q: %v", trecho, err)
		}
	}
}

// decodeWorkers nasce em 1: a ingestão é serial até alguém medir e decidir o
// contrário. Ligar paralelismo por default mudaria o perfil de memória de todo
// dataset existente — cada slot da esteira retém buffers até a marca d'água —
// sem medição que o justifique no dataset de quem atualiza.
func TestDecodeWorkersNasceEmUm(t *testing.T) {
	s := parseSource(t, comSource(kafkaMinimo))
	if got := s.Source.Kafka.Consumer.DecodeWorkers; got != schema.DecodeWorkersPadrao {
		t.Errorf("decodeWorkers = %d, quer %d", got, schema.DecodeWorkersPadrao)
	}
	if schema.DecodeWorkersPadrao != 1 {
		t.Errorf("DecodeWorkersPadrao = %d; o default é 1 por decisão registrada",
			schema.DecodeWorkersPadrao)
	}
}

func TestDecodeWorkersEhConfiguravel(t *testing.T) {
	s := parseSource(t, comSource(kafkaMinimo+"\n      decodeWorkers: 4"))
	if got := s.Source.Kafka.Consumer.DecodeWorkers; got != 4 {
		t.Errorf("decodeWorkers = %d, quer 4", got)
	}
}

// Zero e negativo seguem a regra dos irmãos: não são "sem valor". E o TETO é
// recusa, não arredondamento — a chave dimensiona goroutinas E memória, e quem
// escreveu 10000 tem uma expectativa errada sobre o que ela faz. Um processo
// que aceitasse em silêncio o deixaria com essa expectativa até o OOM killer
// explicá-la.
func TestDecodeWorkersForaDaFaixaEhRecusado(t *testing.T) {
	for _, v := range []string{"0", "-1"} {
		recusa(t, comSource(kafkaMinimo+"\n      decodeWorkers: "+v), "decodeWorkers")
	}
	recusa(t, comSource(kafkaMinimo+"\n      decodeWorkers: 65"), "decodeWorkers")
	// E o teto EXATO passa: uma recusa em 64 seria off-by-one silencioso.
	s := parseSource(t, comSource(kafkaMinimo+"\n      decodeWorkers: 64"))
	if got := s.Source.Kafka.Consumer.DecodeWorkers; got != schema.DecodeWorkersMaximo {
		t.Errorf("decodeWorkers no teto = %d, quer %d", got, schema.DecodeWorkersMaximo)
	}
}
