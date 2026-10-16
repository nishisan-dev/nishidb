package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

const alarmKafkaConfig = "alarmDelivery:\n  kafka:\n    - name: olt_alarms\n      bootstrapServers: ['10.200.20.219:9092']\n      topic: olt_alarms\n"

func TestAlarmKafkaDefaultsAndOverrides(t *testing.T) {
	cfg, err := Load(writeConfig(t, alarmKafkaConfig))
	if err != nil {
		t.Fatal(err)
	}
	k := cfg.AlarmDelivery.Kafka[0]
	timeout, bytes, err := k.Limits()
	if err != nil || timeout != 5*time.Second || bytes != 256<<20 || k.Security.Protocol != schema.ProtocolPlaintext || k.Topic != "olt_alarms" {
		t.Fatalf("defaults incorretos: %+v %s %d %v", k, timeout, bytes, err)
	}
	cfg, err = Load(writeConfig(t, alarmKafkaConfig+"      timeout: 9s\n      queue:\n        maxBytes: 64MiB\n      security:\n        protocol: SASL_SSL\n        saslMechanism: SCRAM-SHA-512\n        usernameEnv: TEST_ALARM_USER\n        passwordEnv: TEST_ALARM_PASSWORD\n        caFile: /ca.pem\n"))
	if err != nil {
		t.Fatal(err)
	}
	k = cfg.AlarmDelivery.Kafka[0]
	timeout, bytes, err = k.Limits()
	if err != nil || timeout != 9*time.Second || bytes != 64<<20 || k.Security.SASLMechanism != schema.SASLScramSHA512 || k.Security.CAFile != "/ca.pem" {
		t.Fatalf("overrides não chegaram: %+v %s %d %v", k, timeout, bytes, err)
	}
	// Webhook e Kafka convivem com nomes distintos.
	if _, err := Load(writeConfig(t, alarmKafkaConfig+"  webhooks:\n    - name: noc\n      url: https://example.com/events\n")); err != nil {
		t.Fatal(err)
	}
}

// Cada caso assevera a mensagem da PRÓPRIA guarda: outra guarda recusando a
// mesma entrada não deixaria o teste verde com a guarda em teste removida.
func TestAlarmKafkaConfigurationRefusesInvalidValuesWithOwnDiagnostic(t *testing.T) {
	base := alarmKafkaConfig
	cases := []struct{ body, message string }{
		{base + "  webhooks:\n    - name: olt_alarms\n      url: https://example.com/events\n", `alarmDelivery.kafka[0] name "olt_alarms" is already used by a webhook`},
		{base + "    - name: olt_alarms\n      bootstrapServers: ['localhost:9092']\n      topic: other\n", `alarmDelivery.kafka[1] duplicate destination name "olt_alarms"`},
		{strings.Replace(base, "name: olt_alarms", "name: Bad-Name", 1), `alarmDelivery.kafka[0] invalid destination name "Bad-Name"`},
		{strings.Replace(base, "topic: olt_alarms", "topic: 'bad topic'", 1), "alarmDelivery.kafka[0].topic is invalid"},
		{strings.Replace(base, "topic: olt_alarms", "topic: '..'", 1), "alarmDelivery.kafka[0].topic is invalid"},
		{strings.Replace(base, "['10.200.20.219:9092']", "[]", 1), "alarmDelivery.kafka[0].bootstrapServers não pode ser lista vazia"},
		{strings.Replace(base, "10.200.20.219:9092", "localhost:99999", 1), "porta inválida"},
		{base + "      timeout: 0s\n", `destination "olt_alarms" timeout must be in (0, 5m]`},
		{base + "      timeout: 6m\n", `destination "olt_alarms" timeout must be in (0, 5m]`},
		{base + "      timeout: ''\n", "alarmDelivery.kafka[0].timeout must not be blank"},
		{base + "      queue:\n        maxBytes: 1MiB\n", `destination "olt_alarms" queue.maxBytes must fit`},
		{base + "      queue:\n        maxBytes: ''\n", "alarmDelivery.kafka[0].queue.maxBytes must not be blank"},
		{base + "      queue:\n        maxByte: 4MiB\n", "alarmDelivery.kafka[0].queue.maxByte"},
		{base + "      unexpected: true\n", "alarmDelivery.kafka[0].unexpected"},
		{base + "      security:\n        protocol: UNKNOWN\n", "alarmDelivery.kafka[0].security.protocol"},
		{base + "      security:\n        protocol: SASL_SSL\n", "alarmDelivery.kafka[0].security.saslMechanism obrigatório"},
		{base + "      security:\n        protocoll: SSL\n", "protocoll"},
		{"alarmDelivery:\n  kafak: []\n", "kafak"},
	}
	for _, tc := range cases {
		_, err := Load(writeConfig(t, tc.body))
		if !errors.Is(err, ErrAlarmDelivery) || !strings.Contains(err.Error(), tc.message) {
			t.Errorf("recusa não veio da guarda esperada %q: %v", tc.message, err)
		}
	}
}

// O Config montado por um chamador Go passa pela mesma validação de conexão,
// inclusive a do contrato de segurança, que o Load já teria feito no nó.
func TestAlarmKafkaValidationCoversConfigAssembledInGo(t *testing.T) {
	c := &Config{}
	c.AlarmDelivery.Kafka = []AlarmKafkaSpec{{Name: "olt_alarms", BootstrapServers: []string{"localhost:9092"}, Topic: "olt_alarms"}}
	if err := c.ValidateAlarmDelivery(); err != nil {
		t.Fatalf("destino válido recusado: %v", err)
	}
	c.AlarmDelivery.Kafka[0].Security = schema.SecuritySpec{Protocol: "BOGUS"}
	if err := c.ValidateAlarmDelivery(); !errors.Is(err, ErrAlarmDelivery) || !strings.Contains(err.Error(), "alarmDelivery.kafka[0].security.protocol") {
		t.Fatalf("segurança inválida não recusada pela guarda própria: %v", err)
	}
}

func TestWebhookDefaultsAndOverrides(t *testing.T) {
	cfg, err := Load(writeConfig(t, "alarmDelivery:\n  webhooks:\n    - name: noc\n      url: http://localhost/events\n"))
	if err != nil {
		t.Fatal(err)
	}
	timeout, bytes, err := cfg.AlarmDelivery.Webhooks[0].Limits()
	if err != nil || timeout != 5*time.Second || bytes != 256<<20 {
		t.Fatalf("defaults incorretos: %s %d %v", timeout, bytes, err)
	}
	cfg, err = Load(writeConfig(t, "alarmDelivery:\n  webhooks:\n    - name: noc\n      url: https://example.com/events\n      timeout: 9s\n      queue:\n        maxBytes: 64MiB\n"))
	if err != nil {
		t.Fatal(err)
	}
	timeout, bytes, err = cfg.AlarmDelivery.Webhooks[0].Limits()
	if err != nil || timeout != 9*time.Second || bytes != 64<<20 {
		t.Fatalf("overrides não chegaram: %s %d %v", timeout, bytes, err)
	}
}

func TestWebhookConfigurationRefusesInvalidValuesWithOwnDiagnostic(t *testing.T) {
	base := "alarmDelivery:\n  webhooks:\n    - name: noc\n      url: https://example.com/events\n"
	cases := []struct{ body, message string }{
		{base + "      timout: 2s\n", "timout"},
		{base + "      timeout: 0s\n", "timeout must"},
		{base + "      timeout: \"\"\n", "timeout must not be blank"},
		{base + "      timeout: 6m\n", "timeout must"},
		{base + "      queue:\n        maxBytes: 1MiB\n", "queue.maxBytes must"},
		{base + "      queue:\n        maxByte: 4MiB\n", "maxByte"},
		{strings.Replace(base, "name: noc", "name: ../noc", 1), "invalid or duplicate webhook name"},
		{strings.Replace(base, "https://example.com/events", "ftp://example.com/events", 1), "requires an HTTP(S) URL"},
		{strings.Replace(base, "https://example.com/events", "https://secret:secret@example.com/events", 1), "requires an HTTP(S) URL"},
		{base + "    - name: noc\n      url: http://example.com\n", "duplicate webhook name"},
		{"alarmDelivery:\n  webhoks: []\n", "webhoks"},
	}
	for _, tc := range cases {
		_, err := Load(writeConfig(t, tc.body))
		if !errors.Is(err, ErrAlarmDelivery) || !strings.Contains(err.Error(), tc.message) {
			t.Errorf("recusa não veio da guarda esperada %q: %v", tc.message, err)
		}
	}
}
