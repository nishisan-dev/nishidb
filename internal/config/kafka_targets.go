package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/units"
	"gopkg.in/yaml.v3"
)

// ErrKafkaTarget identifica configuração inválida dos destinos Kafka nomeados
// (`kafkaTargets:`), inclusive a chave antiga `seriesDelivery:`, que é recusada
// na subida.
var ErrKafkaTarget = errors.New("invalid kafka target configuration")

// Limites de `kafkaTargets[].maxMessageBytes`. O default é o default do
// `ProducerBatchMaxBytes` do franz-go (1.000.012 bytes), que é o teto de lote
// que o cliente usava antes de a chave existir: o destino que não a declara se
// comporta exatamente como antes. Não é o `message.max.bytes` do broker, cujo
// default no Kafka atual é 1.048.588. A faixa é recusada, nunca arredondada.
const (
	DefaultKafkaMessageBytes int64 = 1_000_012
	MinKafkaMessageBytes     int64 = 1 << 10
	MaxKafkaMessageBytes     int64 = 64 << 20
)

// KafkaTargetSpec é um destino Kafka nomeado, declarado uma vez no
// nishidb.yaml e referenciado por nome pelos datasets (`emit.target` dos
// contadores). Guarda só a REFERÊNCIA às credenciais, nunca o valor.
//
// MaxMessageBytes não é lido pelo decoder (`yaml:"-"`): o YAML aceita "1MiB" ou
// bytes puros, e o Load o resolve pelo mesmo units.Size das outras chaves de
// tamanho. Depois do Load ele guarda o valor RESOLVIDO (o declarado ou o
// default); montado por um chamador Go, zero significa "não declarado" e vale o
// default — ver MessageBytes.
type KafkaTargetSpec struct {
	Name             string              `yaml:"name"`
	BootstrapServers []string            `yaml:"bootstrapServers"`
	Topic            string              `yaml:"topic"`
	Timeout          string              `yaml:"timeout"`
	MaxMessageBytes  int64               `yaml:"-"`
	Security         schema.SecuritySpec `yaml:"security"`
}

// DeliveryTimeout resolve o prazo limitado de entrega e de rede.
func (k KafkaTargetSpec) DeliveryTimeout() (time.Duration, error) {
	return deliveryTimeout(ErrKafkaTarget, k.Name, k.Timeout)
}

// MessageBytes resolve o teto de bytes de uma mensagem (e do lote do produtor)
// deste destino: zero vale o default, e fora de [1KiB, 64MiB] é recusado.
func (k KafkaTargetSpec) MessageBytes() (int64, error) {
	if k.MaxMessageBytes == 0 {
		return DefaultKafkaMessageBytes, nil
	}
	if !messageBytesInRange(k.MaxMessageBytes) {
		return 0, fmt.Errorf("%w: destination %q maxMessageBytes must be in [1KiB, 64MiB], got %d", ErrKafkaTarget, k.Name, k.MaxMessageBytes)
	}
	return k.MaxMessageBytes, nil
}

// ValidateKafkaTargets também valida o Config montado diretamente por um
// chamador Go, que não passa pelo Load.
func (c *Config) ValidateKafkaTargets() error {
	seen := make(map[string]bool)
	for i, k := range c.KafkaTargets {
		path := fmt.Sprintf("kafkaTargets[%d]", i)
		if !ValidDestinationName(k.Name) || k.Name == "file" || seen[k.Name] {
			return fmt.Errorf("%w: %s invalid, reserved or duplicate name %q", ErrKafkaTarget, path, k.Name)
		}
		seen[k.Name] = true
		target := kafkaTarget{path: path, topic: k.Topic, bootstrap: k.BootstrapServers, security: k.Security}
		if err := target.validate(ErrKafkaTarget); err != nil {
			return err
		}
		if _, err := k.DeliveryTimeout(); err != nil {
			return err
		}
		if _, err := k.MessageBytes(); err != nil {
			return err
		}
	}
	return nil
}

// validateKafkaTargets valida o bloco pela árvore crua do documento.
//
// A recusa de `seriesDelivery:` mora aqui, e não no decoder, porque o decoder
// do nishidb.yaml tem KnownFields DESLIGADO de propósito (ver Load): sem esta
// guarda, o arquivo com a chave antiga subiria em silêncio e sem destino
// nenhum, e todo `emit.target` que o nomeasse cairia na recusa de "unknown
// target" — longe da causa. A mensagem nomeia a chave nova para que o operador
// saiba o que mudar.
func (c *Config) validateKafkaTargets(root *yaml.Node) error {
	doc := documentRoot(root)
	if child(doc, "seriesDelivery") != nil {
		return fmt.Errorf("%w: seriesDelivery was renamed to kafkaTargets; move the seriesDelivery.kafka list to the top-level kafkaTargets key", ErrKafkaTarget)
	}
	list := child(doc, "kafkaTargets")
	if list != nil {
		for list.Kind == yaml.AliasNode {
			list = list.Alias
		}
		if list.Kind != yaml.SequenceNode {
			return fmt.Errorf("%w: kafkaTargets must be a list", ErrKafkaTarget)
		}
		for i, item := range list.Content {
			path := fmt.Sprintf("kafkaTargets[%d]", i)
			if err := knownKeys(item, path, "name", "bootstrapServers", "topic", "timeout", "maxMessageBytes", "security"); err != nil {
				return fmt.Errorf("%w: %v", ErrKafkaTarget, err)
			}
			if timeout := child(item, "timeout"); timeout != nil && strings.TrimSpace(timeout.Value) == "" {
				return fmt.Errorf("%w: %s.timeout must not be blank", ErrKafkaTarget, path)
			}
			size, err := messageBytesOf(child(item, "maxMessageBytes"), path)
			if err != nil {
				return err
			}
			c.KafkaTargets[i].MaxMessageBytes = size
			security, err := schema.ParseKafkaSecurity(child(item, "security"), path+".security")
			if err != nil {
				return fmt.Errorf("%w: %v", ErrKafkaTarget, err)
			}
			c.KafkaTargets[i].Security = security
		}
	}
	return c.ValidateKafkaTargets()
}

// messageBytesOf resolve `maxMessageBytes` de um item: ausente vale o default;
// declarado vazio, com unidade decimal, não positivo ou fora da faixa é
// recusado nomeando o caminho.
func messageBytesOf(n *yaml.Node, path string) (int64, error) {
	if n == nil {
		return DefaultKafkaMessageBytes, nil
	}
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n.Kind != yaml.ScalarNode || strings.TrimSpace(n.Value) == "" {
		return 0, fmt.Errorf("%w: %s.maxMessageBytes must be a non-blank size", ErrKafkaTarget, path)
	}
	size, err := units.Size(n.Value, DefaultKafkaMessageBytes)
	if err != nil {
		return 0, fmt.Errorf("%w: %s.maxMessageBytes: %v", ErrKafkaTarget, path, err)
	}
	if !messageBytesInRange(size) {
		return 0, fmt.Errorf("%w: %s.maxMessageBytes must be in [1KiB, 64MiB], got %d", ErrKafkaTarget, path, size)
	}
	return size, nil
}

// messageBytesInRange é a ÚNICA definição da faixa de maxMessageBytes, usada
// pelo Load (messageBytesOf) e pelo Config montado em Go (MessageBytes): as
// duas portas não podem divergir. As pontas são inclusivas.
func messageBytesInRange(n int64) bool {
	return n >= MinKafkaMessageBytes && n <= MaxKafkaMessageBytes
}
