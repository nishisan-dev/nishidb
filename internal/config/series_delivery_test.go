package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

const seriesConfig = "seriesDelivery:\n  kafka:\n    - name: metrics\n      bootstrapServers: ['localhost:9092', '[::1]:9092']\n      topic: counter.samples\n"

func TestSeriesKafkaDefaultsAndSecurity(t *testing.T) {
	c, err := Load(writeConfig(t, seriesConfig))
	if err != nil {
		t.Fatal(err)
	}
	k := c.SeriesDelivery.Kafka[0]
	d, err := k.DeliveryTimeout()
	if err != nil || d != 5*time.Second || k.Security.Protocol != schema.ProtocolPlaintext {
		t.Fatalf("%+v %v %v", k, d, err)
	}
	for _, mechanism := range []string{schema.SASLPlain, schema.SASLScramSHA256, schema.SASLScramSHA512} {
		c, err = Load(writeConfig(t, seriesConfig+"      timeout: 200ms\n      security:\n        protocol: SASL_SSL\n        saslMechanism: "+mechanism+"\n        usernameEnv: TEST_KAFKA_USER\n        passwordEnv: TEST_KAFKA_PASSWORD\n        caFile: /ca.pem\n"))
		if err != nil {
			t.Fatal(err)
		}
		k = c.SeriesDelivery.Kafka[0]
		if k.Security.SASLMechanism != mechanism || k.Security.CAFile != "/ca.pem" {
			t.Fatalf("security lost: %+v", k)
		}
	}
}

func TestSeriesKafkaRejectsInvalidConfiguration(t *testing.T) {
	for _, body := range []string{
		"seriesDelivery:\n  kafka: {}\n", "seriesDelivery:\n  kafak: []\n",
		seriesConfig + "      unexpected: true\n", seriesConfig + "      timeout: ''\n", seriesConfig + "      timeout: 0s\n", seriesConfig + "      timeout: 301s\n",
		strings.Replace(seriesConfig, "name: metrics", "name: file", 1), strings.Replace(seriesConfig, "name: metrics", "name: bad-name", 1),
		strings.Replace(seriesConfig, "counter.samples", "..", 1), strings.Replace(seriesConfig, "counter.samples", "bad topic", 1),
		strings.Replace(seriesConfig, "['localhost:9092', '[::1]:9092']", "[]", 1), strings.Replace(seriesConfig, "localhost:9092", "localhost:99999", 1),
		seriesConfig + "      security:\n        protocol: UNKNOWN\n", seriesConfig + "      security:\n        passwordEnv: PASSWORD\n",
		seriesConfig + "      security:\n        protocol: SASL_SSL\n", seriesConfig + "      security:\n        caFile: /ca.pem\n",
		seriesConfig + "      security:\n        protocoll: SSL\n",
		seriesConfig + "    - name: metrics\n      bootstrapServers: ['localhost:9092']\n      topic: other\n",
	} {
		_, err := Load(writeConfig(t, body))
		if err == nil || !errors.Is(err, ErrSeriesDelivery) && !strings.Contains(err.Error(), "cannot unmarshal") {
			t.Errorf("wrong refusal for %s: %v", body, err)
		}
	}
}
