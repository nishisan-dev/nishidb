package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/units"
	"gopkg.in/yaml.v3"
)

// ErrAlarmDelivery identifica configuração inválida dos destinos de alarme.
var ErrAlarmDelivery = errors.New("invalid alarm delivery configuration")

// AlarmDeliverySpec declara os destinos compartilhados pelos datasets.
//
// Webhooks e Kafka dividem o MESMO espaço de nomes: o `notify:` do alarme
// escolhe um destino pelo nome sem dizer o tipo, e o diretório da fila em
// disco também é só o nome. Um nome repetido entre os dois é recusado.
type AlarmDeliverySpec struct {
	Webhooks []WebhookSpec    `yaml:"webhooks"`
	Kafka    []AlarmKafkaSpec `yaml:"kafka"`
}

// AlarmQueueSpec é o teto da fila durável de um destino de alarme.
type AlarmQueueSpec struct {
	MaxBytes string `yaml:"maxBytes"`
}

// WebhookSpec guarda somente a referência ao segredo, nunca seu valor.
type WebhookSpec struct {
	Name           string         `yaml:"name"`
	URL            string         `yaml:"url"`
	BearerTokenEnv string         `yaml:"bearerTokenEnv"`
	Timeout        string         `yaml:"timeout"`
	Queue          AlarmQueueSpec `yaml:"queue"`
}

// AlarmKafkaSpec é um destino Kafka de alarmes: a conexão tem o MESMO
// contrato da do `seriesDelivery` (bootstrap, tópico, timeout e o bloco
// `security`, com credenciais por nome de variável de ambiente), e a fila
// durável tem o mesmo contrato da do webhook.
type AlarmKafkaSpec struct {
	Name             string              `yaml:"name"`
	BootstrapServers []string            `yaml:"bootstrapServers"`
	Topic            string              `yaml:"topic"`
	Timeout          string              `yaml:"timeout"`
	Security         schema.SecuritySpec `yaml:"security"`
	Queue            AlarmQueueSpec      `yaml:"queue"`
}

// DefaultAlarmQueueBytes é o teto default da fila de um destino de alarme,
// também usado para filas pausadas.
const DefaultAlarmQueueBytes = 256 << 20

// ValidDestinationName também limita os nomes dos diretórios e dos labels.
func ValidDestinationName(name string) bool { return schema.ValidDeliveryName(name) }

// Limits resolve os defaults e recusa limites explícitos inválidos.
func (w WebhookSpec) Limits() (time.Duration, int64, error) {
	return alarmDestinationLimits(w.Name, w.Timeout, w.Queue.MaxBytes)
}

// Limits resolve os defaults e recusa limites explícitos inválidos. O
// timeout é o prazo de UMA tentativa de produção.
func (k AlarmKafkaSpec) Limits() (time.Duration, int64, error) {
	return alarmDestinationLimits(k.Name, k.Timeout, k.Queue.MaxBytes)
}

// alarmDestinationLimits é a regra comum aos dois tipos de destino: o
// timeout de uma tentativa e o teto da fila em disco, que precisa caber um
// evento de 1MiB mais o cabeçalho do registro.
func alarmDestinationLimits(name, timeout, maxBytes string) (time.Duration, int64, error) {
	d, err := deliveryTimeout(ErrAlarmDelivery, name, timeout)
	if err != nil {
		return 0, 0, err
	}
	size, err := units.Size(maxBytes, DefaultAlarmQueueBytes)
	if err != nil || size < (1<<20)+16 {
		return 0, 0, fmt.Errorf("%w: destination %q queue.maxBytes must fit a 1MiB event and its 16-byte header", ErrAlarmDelivery, name)
	}
	return d, size, nil
}

// ValidateAlarmDelivery também cobre Config montado diretamente por um
// chamador Go. Não lê o ambiente nem o disco: token, credencial Kafka e CA
// são resolvidos por alarmdelivery.Validate.
func (c *Config) ValidateAlarmDelivery() error {
	webhooks := make(map[string]bool)
	for _, w := range c.AlarmDelivery.Webhooks {
		if !ValidDestinationName(w.Name) || webhooks[w.Name] {
			return fmt.Errorf("%w: invalid or duplicate webhook name %q", ErrAlarmDelivery, w.Name)
		}
		webhooks[w.Name] = true
		u, err := url.Parse(w.URL)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
			return fmt.Errorf("%w: webhook %q requires an HTTP(S) URL without userinfo or fragment", ErrAlarmDelivery, w.Name)
		}
		if strings.TrimSpace(w.BearerTokenEnv) != w.BearerTokenEnv {
			return fmt.Errorf("%w: webhook %q has an invalid bearerTokenEnv", ErrAlarmDelivery, w.Name)
		}
		if _, _, err := w.Limits(); err != nil {
			return err
		}
	}
	seen := make(map[string]bool)
	for i, k := range c.AlarmDelivery.Kafka {
		path := fmt.Sprintf("alarmDelivery.kafka[%d]", i)
		switch {
		case !ValidDestinationName(k.Name):
			return fmt.Errorf("%w: %s invalid destination name %q", ErrAlarmDelivery, path, k.Name)
		case webhooks[k.Name]:
			return fmt.Errorf("%w: %s name %q is already used by a webhook", ErrAlarmDelivery, path, k.Name)
		case seen[k.Name]:
			return fmt.Errorf("%w: %s duplicate destination name %q", ErrAlarmDelivery, path, k.Name)
		}
		seen[k.Name] = true
		target := kafkaTarget{path: path, topic: k.Topic, bootstrap: k.BootstrapServers, security: k.Security}
		if err := target.validate(ErrAlarmDelivery); err != nil {
			return err
		}
		if _, _, err := k.Limits(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateAlarmDelivery(root *yaml.Node) error {
	n := child(documentRoot(root), "alarmDelivery")
	if err := knownKeys(n, "alarmDelivery", "webhooks", "kafka"); err != nil {
		return fmt.Errorf("%w: %v", ErrAlarmDelivery, err)
	}
	if node := child(n, "webhooks"); node != nil {
		list, err := sequenceNode(node, ErrAlarmDelivery, "alarmDelivery.webhooks")
		if err != nil {
			return err
		}
		for i, item := range list.Content {
			path := fmt.Sprintf("alarmDelivery.webhooks[%d]", i)
			if err := knownKeys(item, path, "name", "url", "bearerTokenEnv", "timeout", "queue"); err != nil {
				return fmt.Errorf("%w: %v", ErrAlarmDelivery, err)
			}
			if timeout := child(item, "timeout"); timeout != nil && strings.TrimSpace(timeout.Value) == "" {
				return fmt.Errorf("%w: %s.timeout must not be blank", ErrAlarmDelivery, path)
			}
			if err := validateQueueNode(item, path); err != nil {
				return err
			}
		}
	}
	if node := child(n, "kafka"); node != nil {
		list, err := sequenceNode(node, ErrAlarmDelivery, "alarmDelivery.kafka")
		if err != nil {
			return err
		}
		for i, item := range list.Content {
			path := fmt.Sprintf("alarmDelivery.kafka[%d]", i)
			security, err := parseKafkaTargetNode(item, path, ErrAlarmDelivery, "queue")
			if err != nil {
				return err
			}
			if err := validateQueueNode(item, path); err != nil {
				return err
			}
			c.AlarmDelivery.Kafka[i].Security = security
		}
	}
	return c.ValidateAlarmDelivery()
}

// validateQueueNode recusa, no bloco `queue` de um destino de alarme, chave
// desconhecida e `maxBytes` em branco — que o default engoliria em silêncio.
func validateQueueNode(item *yaml.Node, path string) error {
	queue := child(item, "queue")
	if maxBytes := child(queue, "maxBytes"); maxBytes != nil && strings.TrimSpace(maxBytes.Value) == "" {
		return fmt.Errorf("%w: %s.queue.maxBytes must not be blank", ErrAlarmDelivery, path)
	}
	if err := knownKeys(queue, path+".queue", "maxBytes"); err != nil {
		return fmt.Errorf("%w: %v", ErrAlarmDelivery, err)
	}
	return nil
}
