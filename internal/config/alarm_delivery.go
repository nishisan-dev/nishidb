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
type AlarmDeliverySpec struct {
	Webhooks []WebhookSpec `yaml:"webhooks"`
}

// WebhookSpec guarda somente a referência ao segredo, nunca seu valor.
type WebhookSpec struct {
	Name           string `yaml:"name"`
	URL            string `yaml:"url"`
	BearerTokenEnv string `yaml:"bearerTokenEnv"`
	Timeout        string `yaml:"timeout"`
	Queue          struct {
		MaxBytes string `yaml:"maxBytes"`
	} `yaml:"queue"`
}

// DefaultWebhookQueueBytes é o limite também usado para filas pausadas.
const DefaultWebhookQueueBytes = 256 << 20

// ValidDestinationName também limita os nomes dos diretórios e dos labels.
func ValidDestinationName(name string) bool { return schema.ValidDeliveryName(name) }

// Limits resolve os defaults e recusa limites explícitos inválidos.
func (w WebhookSpec) Limits() (time.Duration, int64, error) {
	timeout, err := units.Duration(w.Timeout, 5*time.Second)
	if err != nil || timeout <= 0 || timeout > 5*time.Minute {
		return 0, 0, fmt.Errorf("%w: webhook %q timeout must be in (0, 5m]", ErrAlarmDelivery, w.Name)
	}
	size, err := units.Size(w.Queue.MaxBytes, DefaultWebhookQueueBytes)
	if err != nil || size < (1<<20)+16 {
		return 0, 0, fmt.Errorf("%w: webhook %q queue.maxBytes must fit a 1MiB event and its 16-byte header", ErrAlarmDelivery, w.Name)
	}
	return timeout, size, nil
}

// ValidateWebhooks também cobre Config montado diretamente por um chamador Go.
func (c *Config) ValidateWebhooks() error {
	seen := make(map[string]bool)
	for _, w := range c.AlarmDelivery.Webhooks {
		if !ValidDestinationName(w.Name) || seen[w.Name] {
			return fmt.Errorf("%w: invalid or duplicate webhook name %q", ErrAlarmDelivery, w.Name)
		}
		seen[w.Name] = true
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
	return nil
}

func (c *Config) validateAlarmDelivery(root *yaml.Node) error {
	n := child(documentRoot(root), "alarmDelivery")
	if err := knownKeys(n, "alarmDelivery", "webhooks"); err != nil {
		return fmt.Errorf("%w: %v", ErrAlarmDelivery, err)
	}
	list := child(n, "webhooks")
	if list != nil {
		for list.Kind == yaml.AliasNode {
			list = list.Alias
		}
		if list.Kind != yaml.SequenceNode {
			return fmt.Errorf("%w: alarmDelivery.webhooks must be a list", ErrAlarmDelivery)
		}
		for i, item := range list.Content {
			path := fmt.Sprintf("alarmDelivery.webhooks[%d]", i)
			if err := knownKeys(item, path, "name", "url", "bearerTokenEnv", "timeout", "queue"); err != nil {
				return fmt.Errorf("%w: %v", ErrAlarmDelivery, err)
			}
			for _, field := range []struct {
				node *yaml.Node
				name string
			}{
				{child(item, "timeout"), "timeout"}, {child(child(item, "queue"), "maxBytes"), "queue.maxBytes"},
			} {
				if field.node != nil && strings.TrimSpace(field.node.Value) == "" {
					return fmt.Errorf("%w: %s.%s must not be blank", ErrAlarmDelivery, path, field.name)
				}
			}
			if err := knownKeys(child(item, "queue"), path+".queue", "maxBytes"); err != nil {
				return fmt.Errorf("%w: %v", ErrAlarmDelivery, err)
			}
		}
	}
	return c.ValidateWebhooks()
}
