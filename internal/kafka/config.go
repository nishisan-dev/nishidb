// Package kafka implementa o consumer da Fase 3 (§115): consumer, bounded
// queue, offsets, checkpoint, seek e recovery.
//
// A arquitetura é a do §27, sem desvio:
//
//	Kafka Poller → Bounded Channel → Single Materialization Writer → Storage
//
// Os decode workers que o §27 marca como OPCIONAIS não existem: o §28 diz que
// a meta de 500 ev/s não justifica múltiplos writers, e o M1 mediu 132.931
// ev/s de aplicação pura. Paralelizar o decode exigiria reordenar antes do
// writer para preservar a ordem POR PARTIÇÃO, que o §26 usa como critério de
// correção quando o dataset não declara `sequence`.
//
// O que preserva essa ordem é o WRITER único consumindo uma FIFO — não o
// número de goroutines do pipeline. Poller e writer sempre foram duas, e desde
// 28/08/2026 elas se sobrepõem de verdade: a barreira que o poller emitia a
// cada lote foi movida para a revogação, que é onde a garantia sobre o commit
// de fato precisa dela. Está escrito aqui porque a barreira PARECIA sustentar
// a ordem, e sem o registro a próxima revisão a reintroduz "por segurança".
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"time"
)

// ErrConfig categoriza erro de configuração efetiva do consumer: o que o
// parse do YAML não tem como saber, porque depende do ambiente.
var ErrConfig = errors.New("kafka: configuração inválida")

// ErrNaoImplementado marca chave da spec aceita no parse e sem implementação.
//
// A doutrina é única no produto: chave que a spec nomeia é ACEITA e validada
// na forma pelo parse, e RECUSADA na ativação. Recusar no parse tornaria
// inválido o snippet literal da spec (o §102 mostra deadLetterTopic) e
// obrigaria o operador a manter dois arquivos; aceitar em silêncio repetiria
// exatamente o defeito que a issue #1 denuncia.
var ErrNaoImplementado = errors.New("kafka: não implementado nesta versão")

// Config é a configuração EFETIVA do consumer: a spec do YAML resolvida
// contra o ambiente.
type Config struct {
	Dataset string
	Topic   string
	GroupID string
	Brokers []string

	AutoOffsetReset string
	MaxPollRecords  int
	QueueSize       int
	QueueBytes      int64
	// WatchdogTimeout e WatchdogTentativas governam a recriação do consumidor
	// quando ele para de receber com o broker declarando mensagens. Zero no
	// timeout desliga. Ver o godoc de vigia.
	WatchdogTimeout    time.Duration
	WatchdogTentativas int

	// InvalidEvent é a política do §101 já resolvida.
	InvalidEvent string

	// dialTLS e mecanismo são nil quando o protocolo não os usa.
	dialTLS   *tls.Config
	mecanismo sasl.Mechanism
}

// Origem é a identidade do fluxo, para amarrar os offsets persistidos à fonte
// de onde vieram (o ClusterID só é conhecido depois de falar com o broker).
func (c *Config) Origem() checkpoint.Origem {
	return checkpoint.Origem{
		Topic:     c.Topic,
		GroupID:   c.GroupID,
		Bootstrap: checkpoint.CanonizaBootstrap(c.Brokers),
	}
}

// Resolve monta a configuração efetiva a partir da spec do dataset.
//
// É AQUI que as variáveis de ambiente são lidas, e não no parse do YAML. A
// separação é deliberada: o parse valida o NOME da variável e nada mais, o
// que mantém um YAML de produção válido numa máquina de desenvolvimento sem
// as credenciais no ambiente. A falta do VALOR é falha de ativação, e falha
// rápido — um consumer que sobe sem credencial só descobre o problema no
// handshake, com uma mensagem do broker que não nomeia a variável faltante.
func Resolve(dataset string, k *schema.KafkaSpec, e schema.ErrorsSpec) (*Config, error) {
	if k == nil {
		return nil, fmt.Errorf("%w: dataset %q não declara source.kafka", ErrConfig, dataset)
	}
	if e.DeadLetterTopic != "" {
		return nil, fmt.Errorf("%w: errors.deadLetterTopic (§102): %w", ErrConfig, ErrNaoImplementado)
	}

	c := &Config{
		Dataset:         dataset,
		Topic:           k.Topic,
		GroupID:         k.Consumer.GroupID,
		Brokers:         append([]string(nil), k.BootstrapServers...),
		AutoOffsetReset: k.Consumer.AutoOffsetReset,
		MaxPollRecords:  k.Consumer.MaxPollRecords,
		QueueSize:       k.Consumer.IngestionQueueSize,
		QueueBytes:      k.Consumer.IngestionQueueBytes,
		InvalidEvent:    e.InvalidEvent,

		WatchdogTimeout:    k.Consumer.WatchdogTimeout,
		WatchdogTentativas: k.Consumer.WatchdogTentativas,
	}

	sec := k.Security
	if sec.TLS() {
		t := &tls.Config{MinVersion: tls.VersionTLS12}
		if sec.CAFile != "" {
			pem, err := os.ReadFile(sec.CAFile)
			if err != nil {
				return nil, fmt.Errorf("%w: security.caFile: %w", ErrConfig, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("%w: security.caFile %s não contém certificado PEM válido",
					ErrConfig, sec.CAFile)
			}
			t.RootCAs = pool
		}
		c.dialTLS = t
	}

	if sec.SASL() {
		usuario, err := env(sec.UsernameEnv)
		if err != nil {
			return nil, fmt.Errorf("%w: security.usernameEnv: %w", ErrConfig, err)
		}
		senha, err := env(sec.PasswordEnv)
		if err != nil {
			return nil, fmt.Errorf("%w: security.passwordEnv: %w", ErrConfig, err)
		}
		auth := func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: usuario, Pass: senha}, nil
		}
		scramAuth := func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: usuario, Pass: senha}, nil
		}
		switch sec.SASLMechanism {
		case schema.SASLPlain:
			c.mecanismo = plain.Plain(auth)
		case schema.SASLScramSHA256:
			c.mecanismo = scram.Sha256(scramAuth)
		case schema.SASLScramSHA512:
			c.mecanismo = scram.Sha512(scramAuth)
		default:
			// O parse já fechou o enum; chegar aqui é bug nosso.
			return nil, fmt.Errorf("%w: saslMechanism %q sem implementação", ErrConfig, sec.SASLMechanism)
		}
	}
	return c, nil
}

// env lê a variável exigindo valor NÃO VAZIO.
//
// Variável definida como string vazia é o modo de falha mais traiçoeiro do
// deploy por container: o orquestrador injeta a chave sem valor, o processo
// sobe, e a autenticação falha longe daqui. A mensagem nomeia a variável e
// NUNCA o valor (§95).
func env(nome string) (string, error) {
	v, ok := os.LookupEnv(nome)
	if !ok {
		return "", fmt.Errorf("variável de ambiente %s não está definida", nome)
	}
	if v == "" {
		return "", fmt.Errorf("variável de ambiente %s está definida e vazia", nome)
	}
	return v, nil
}

// opcoesDoCliente monta as opções do kgo comuns a produção e a teste.
func (c *Config) opcoesDoCliente() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumeTopics(c.Topic),
		kgo.ConsumerGroup(c.GroupID),

		// O commit no broker é EXPLÍCITO e sempre posterior ao checkpoint
		// durável (§48). Com auto-commit, o cliente commitaria em background
		// o que apenas foi ENTREGUE, invertendo a ordem que o §48 proíbe.
		kgo.DisableAutoCommit(),

		// Bloquear o rebalance entre o poll e o fim do processamento do lote
		// é o que torna o single-writer (§27) seguro: sem isso, uma partição
		// pode ser revogada com eventos em voo na fila, e o commit seguinte
		// escreveria offset de partição que já não é nossa. O custo é o risco
		// de estourar o rebalance timeout, que a meta de 500 ev/s do §88 não
		// alcança nem de longe (o M1 mediu 132.931 ev/s de aplicação).
		kgo.BlockRebalanceOnPoll(),

		// Vale só na PRIMEIRA subida, e só quando não há offset local: o
		// checkpoint local é autoritativo (§49) e o seek explícito o
		// sobrepõe em toda partição que já tenha posição conhecida.
		kgo.ConsumeResetOffset(c.offsetInicial()),
	}
	if c.dialTLS != nil {
		opts = append(opts, kgo.DialTLSConfig(c.dialTLS))
	}
	if c.mecanismo != nil {
		opts = append(opts, kgo.SASL(c.mecanismo))
	}
	return opts
}

func (c *Config) offsetInicial() kgo.Offset {
	if c.AutoOffsetReset == schema.AutoOffsetResetLatest {
		return kgo.NewOffset().AtEnd()
	}
	return kgo.NewOffset().AtStart()
}
