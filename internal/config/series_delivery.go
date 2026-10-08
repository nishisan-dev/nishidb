package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
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
	return deliveryTimeout(ErrSeriesDelivery, k.Name, k.Timeout)
}

// ValidateSeriesDelivery also validates Config assembled directly by Go callers.
func (c *Config) ValidateSeriesDelivery() error {
	seen := make(map[string]bool)
	for i, k := range c.SeriesDelivery.Kafka {
		path := fmt.Sprintf("seriesDelivery.kafka[%d]", i)
		if !ValidDestinationName(k.Name) || k.Name == "file" || seen[k.Name] {
			return fmt.Errorf("%w: %s invalid, reserved or duplicate name %q", ErrSeriesDelivery, path, k.Name)
		}
		seen[k.Name] = true
		target := kafkaTarget{path: path, topic: k.Topic, bootstrap: k.BootstrapServers, security: k.Security}
		if err := target.validate(ErrSeriesDelivery); err != nil {
			return err
		}
		if _, err := k.DeliveryTimeout(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateSeriesDelivery(root *yaml.Node) error {
	n := child(documentRoot(root), "seriesDelivery")
	if err := knownKeys(n, "seriesDelivery", "kafka"); err != nil {
		return fmt.Errorf("%w: %v", ErrSeriesDelivery, err)
	}
	if node := child(n, "kafka"); node != nil {
		list, err := sequenceNode(node, ErrSeriesDelivery, "seriesDelivery.kafka")
		if err != nil {
			return err
		}
		for i, item := range list.Content {
			security, err := parseKafkaTargetNode(item, fmt.Sprintf("seriesDelivery.kafka[%d]", i), ErrSeriesDelivery)
			if err != nil {
				return err
			}
			c.SeriesDelivery.Kafka[i].Security = security
		}
	}
	return c.ValidateSeriesDelivery()
}
