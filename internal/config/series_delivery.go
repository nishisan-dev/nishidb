package config

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/units"
	"gopkg.in/yaml.v3"
)

// ErrSeriesDelivery identifies invalid series destination configuration.
var ErrSeriesDelivery = errors.New("invalid series delivery configuration")

// SeriesDeliverySpec declares globally named counter outputs.
type SeriesDeliverySpec struct {
	Kafka []SeriesKafkaSpec `yaml:"kafka"`
}

// SeriesKafkaSpec contains connection settings, never resolved credentials.
type SeriesKafkaSpec struct {
	Name             string              `yaml:"name"`
	BootstrapServers []string            `yaml:"bootstrapServers"`
	Topic            string              `yaml:"topic"`
	Timeout          string              `yaml:"timeout"`
	Security         schema.SecuritySpec `yaml:"security"`
}

// DeliveryTimeout resolves the bounded delivery and network timeout.
func (k SeriesKafkaSpec) DeliveryTimeout() (time.Duration, error) {
	d, err := units.Duration(k.Timeout, 5*time.Second)
	if err != nil || d <= 0 || d > 5*time.Minute {
		return 0, fmt.Errorf("%w: destination %q timeout must be in (0, 5m]", ErrSeriesDelivery, k.Name)
	}
	return d, nil
}

var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

// ValidateSeriesDelivery also validates Config assembled directly by Go callers.
func (c *Config) ValidateSeriesDelivery() error {
	seen := make(map[string]bool)
	for i, k := range c.SeriesDelivery.Kafka {
		path := fmt.Sprintf("seriesDelivery.kafka[%d]", i)
		if !ValidDestinationName(k.Name) || k.Name == "file" || seen[k.Name] {
			return fmt.Errorf("%w: %s invalid, reserved or duplicate name %q", ErrSeriesDelivery, path, k.Name)
		}
		seen[k.Name] = true
		if !kafkaTopicName.MatchString(k.Topic) || k.Topic == "." || k.Topic == ".." {
			return fmt.Errorf("%w: %s.topic is invalid", ErrSeriesDelivery, path)
		}
		if err := schema.ValidateKafkaBootstrap(k.BootstrapServers, path+".bootstrapServers"); err != nil {
			return fmt.Errorf("%w: %v", ErrSeriesDelivery, err)
		}
		if _, err := k.DeliveryTimeout(); err != nil {
			return err
		}
		var node yaml.Node
		if err := node.Encode(k.Security); err != nil {
			return fmt.Errorf("%w: %v", ErrSeriesDelivery, err)
		}
		if _, err := schema.ParseKafkaSecurity(&node, path+".security"); err != nil {
			return fmt.Errorf("%w: %v", ErrSeriesDelivery, err)
		}
	}
	return nil
}

func (c *Config) validateSeriesDelivery(root *yaml.Node) error {
	n := child(documentRoot(root), "seriesDelivery")
	if err := knownKeys(n, "seriesDelivery", "kafka"); err != nil {
		return fmt.Errorf("%w: %v", ErrSeriesDelivery, err)
	}
	list := child(n, "kafka")
	if list != nil {
		for list.Kind == yaml.AliasNode {
			list = list.Alias
		}
		if list.Kind != yaml.SequenceNode {
			return fmt.Errorf("%w: seriesDelivery.kafka must be a list", ErrSeriesDelivery)
		}
		for i, item := range list.Content {
			path := fmt.Sprintf("seriesDelivery.kafka[%d]", i)
			if err := knownKeys(item, path, "name", "bootstrapServers", "topic", "timeout", "security"); err != nil {
				return fmt.Errorf("%w: %v", ErrSeriesDelivery, err)
			}
			if timeout := child(item, "timeout"); timeout != nil && strings.TrimSpace(timeout.Value) == "" {
				return fmt.Errorf("%w: %s.timeout must not be blank", ErrSeriesDelivery, path)
			}
			security, err := schema.ParseKafkaSecurity(child(item, "security"), path+".security")
			if err != nil {
				return fmt.Errorf("%w: %v", ErrSeriesDelivery, err)
			}
			c.SeriesDelivery.Kafka[i].Security = security
		}
	}
	return c.ValidateSeriesDelivery()
}
