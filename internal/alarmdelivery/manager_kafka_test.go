package alarmdelivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
)

// K8 (referência): `notify` aceita destino Kafka, e o nome desconhecido
// continua recusado com o espaço de nomes comum.
func TestValidateAcceptsNotifyToAKafkaDestination(t *testing.T) {
	cfg := deliveryConfig("noc", "http://localhost/events")
	cfg.AlarmDelivery.Kafka = []config.AlarmKafkaSpec{kafkaSpec("olt_alarms", []string{"localhost:9092"}, "")}
	if err := Validate(cfg, map[string]*schema.DatasetSpec{"d": deliverySpec("noc", "olt_alarms")}); err != nil {
		t.Fatalf("notify para destino Kafka recusado: %v", err)
	}
	err := Validate(cfg, map[string]*schema.DatasetSpec{"d": deliverySpec("olt_alarms", "missing")})
	if !errors.Is(err, config.ErrAlarmDelivery) || !strings.Contains(err.Error(), `unknown or duplicate destination "missing"`) {
		t.Fatalf("referência desconhecida não recusada pela guarda própria: %v", err)
	}
}

// K9: credencial por variável de ambiente ausente, ou CA ilegível, recusa a
// subida — no Validate e no Open —, e o erro nomeia a variável, nunca o
// valor.
func TestKafkaMissingCredentialRefusesStartupWithoutLeakingValues(t *testing.T) {
	const secretUser = "alarm-user-secret-value"
	t.Setenv("NISHIDB_TEST_ALARM_KAFKA_USER", secretUser)
	t.Setenv("NISHIDB_TEST_ALARM_KAFKA_PASSWORD", "")
	cfg := &config.Config{}
	cfg.AlarmDelivery.Kafka = []config.AlarmKafkaSpec{kafkaSpec("olt_alarms", []string{"localhost:9092"}, "")}
	cfg.AlarmDelivery.Kafka[0].Security = schema.SecuritySpec{Protocol: schema.ProtocolSASLSSL, SASLMechanism: schema.SASLPlain,
		UsernameEnv: "NISHIDB_TEST_ALARM_KAFKA_USER", PasswordEnv: "NISHIDB_TEST_ALARM_KAFKA_PASSWORD"}
	specs := map[string]*schema.DatasetSpec{"d": deliverySpec("olt_alarms")}
	check := func(err error, want string) {
		t.Helper()
		if !errors.Is(err, config.ErrAlarmDelivery) || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), `destination "olt_alarms"`) {
			t.Fatalf("recusa não veio da guarda esperada %q: %v", want, err)
		}
		if strings.Contains(err.Error(), secretUser) {
			t.Fatalf("o erro vazou o valor da credencial: %v", err)
		}
	}
	check(Validate(cfg, specs), "NISHIDB_TEST_ALARM_KAFKA_PASSWORD")
	if m, err := Open(t.TempDir(), specs["d"], cfg, logging.Discard()); err == nil {
		_ = m.Close()
		t.Fatal("Open aceitou credencial ausente")
	} else {
		check(err, "NISHIDB_TEST_ALARM_KAFKA_PASSWORD")
	}
	t.Setenv("NISHIDB_TEST_ALARM_KAFKA_PASSWORD", "alarm-password-secret-value")
	if err := Validate(cfg, specs); err != nil {
		t.Fatalf("credencial presente recusada: %v", err)
	}
	cfg.AlarmDelivery.Kafka[0].Security = schema.SecuritySpec{Protocol: schema.ProtocolSSL, CAFile: "/nonexistent/ca.pem"}
	check(Validate(cfg, specs), "security.caFile")
}

// K9: com SASL configurado e o broker recusando, as tentativas falham, o
// WARN sai, e nenhum valor de credencial chega ao log.
func TestKafkaCredentialValuesNeverReachTheLog(t *testing.T) {
	const secretUser, secretPassword = "alarm-user-secret-value", "alarm-password-secret-value"
	t.Setenv("NISHIDB_TEST_ALARM_KAFKA_USER", secretUser)
	t.Setenv("NISHIDB_TEST_ALARM_KAFKA_PASSWORD", secretPassword)
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	cfg := &config.Config{}
	cfg.AlarmDelivery.Kafka = []config.AlarmKafkaSpec{kafkaSpec("olt_alarms", c.ListenAddrs(), "500ms")}
	cfg.AlarmDelivery.Kafka[0].Security = schema.SecuritySpec{Protocol: schema.ProtocolSASLPlaintext, SASLMechanism: schema.SASLPlain,
		UsernameEnv: "NISHIDB_TEST_ALARM_KAFKA_USER", PasswordEnv: "NISHIDB_TEST_ALARM_KAFKA_PASSWORD"}
	logs := &syncBuffer{}
	m, err := Open(t.TempDir(), deliverySpec("olt_alarms"), cfg, logging.New(logs, logging.FormatJSON, slog.LevelDebug))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	m.queues["olt_alarms"].initialDelay, m.queues["olt_alarms"].maxDelay = time.Millisecond, 5*time.Millisecond
	m.Start(context.Background())
	m.Emit(alarm.Event{ID: "sasl", Dataset: "d", Counter: "c", Alarm: "a", State: alarm.StateFiring, GroupKey: "olt", At: time.Now()})
	eventually(t, func() bool { return m.Stats()[0].FailedAttempts >= 2 })
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	text := logs.String()
	if !strings.Contains(text, `"event":"alarm.delivery.retry"`) {
		t.Fatalf("a falha de autenticação não gerou WARN:\n%s", text)
	}
	for _, leaked := range []string{secretUser, secretPassword} {
		if strings.Contains(text, leaked) {
			t.Fatalf("o log vazou o valor de uma credencial:\n%s", text)
		}
	}
}

// K10: destino Kafka removido do config com fila no disco fica pausado, com
// WARN, sem tipo no wire e sem perder nada; reintroduzido, entrega.
func TestRemovedKafkaDestinationPausesAndKeepsItsQueue(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	var healthy atomic.Bool
	failProduce(c, &healthy, kerr.NotEnoughReplicas.Code, nil)
	cfg := &config.Config{}
	cfg.AlarmDelivery.Kafka = []config.AlarmKafkaSpec{kafkaSpec("olt_alarms", c.ListenAddrs(), "")}
	dir := t.TempDir()
	m, err := Open(dir, deliverySpec("olt_alarms"), cfg, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	m.queues["olt_alarms"].initialDelay, m.queues["olt_alarms"].maxDelay = time.Millisecond, 5*time.Millisecond
	m.Start(context.Background())
	m.Emit(alarm.Event{ID: "kept", Dataset: "d", Counter: "c", Alarm: "a", State: alarm.StateFiring, GroupKey: "olt", At: time.Now()})
	eventually(t, func() bool { s := m.Stats()[0]; return s.FailedAttempts >= 1 && s.DiskEvents == 1 })
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	paused, err := Open(dir, &schema.DatasetSpec{Name: "d"}, &config.Config{}, logging.New(logs, logging.FormatJSON, slog.LevelDebug))
	if err != nil {
		t.Fatal(err)
	}
	stats := paused.Stats()
	if len(stats) != 1 || stats[0].State != "paused" || stats[0].DiskEvents != 1 || stats[0].Kind != "" {
		t.Fatalf("remoção não pausou a fila: %+v", stats)
	}
	if !strings.Contains(logs.String(), `"event":"alarm.delivery.paused"`) || !strings.Contains(logs.String(), `"destination":"olt_alarms"`) {
		t.Fatalf("pausa sem WARN:\n%s", logs.String())
	}
	if err := paused.Close(); err != nil {
		t.Fatal(err)
	}
	healthy.Store(true)
	resumed, err := Open(dir, &schema.DatasetSpec{Name: "d"}, cfg, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resumed.Close() })
	resumed.Start(context.Background())
	eventually(t, func() bool { return resumed.Stats()[0].Delivered == 1 })
	if id, _ := header(consume(t, c.ListenAddrs(), 1)[0], "eventId"); id != "kept" {
		t.Fatalf("a fila retomada entregou %q", id)
	}
}

// A fila é do NOME: um destino que troca de webhook para Kafka mantendo o
// nome entrega ao tópico, byte a byte, o corpo que estava pendente para o
// HTTP.
func TestDestinationSwitchedFromWebhookToKafkaDeliversThePendingEvent(t *testing.T) {
	posted := make(chan []byte, 16)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		posted <- body
		w.WriteHeader(503)
	}))
	defer receiver.Close()
	dir := t.TempDir()
	m, err := Open(dir, deliverySpec("noc"), deliveryConfig("noc", receiver.URL), logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	m.Start(context.Background())
	m.Emit(alarm.Event{ID: "switched", Dataset: "d", Counter: "c", Alarm: "a", State: alarm.StateFiring, GroupKey: "olt", At: time.Now()})
	var body []byte
	select {
	case body = <-posted:
	case <-time.After(10 * time.Second):
		t.Fatal("o webhook não recebeu a tentativa")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	cfg := &config.Config{}
	cfg.AlarmDelivery.Kafka = []config.AlarmKafkaSpec{kafkaSpec("noc", c.ListenAddrs(), "")}
	switched, err := Open(dir, deliverySpec("noc"), cfg, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = switched.Close() })
	if s := switched.Stats()[0]; s.Kind != "kafka" || s.DiskEvents != 1 {
		t.Fatalf("a fila do nome não passou ao Kafka: %+v", s)
	}
	switched.Start(context.Background())
	if record := consume(t, c.ListenAddrs(), 1)[0]; !bytes.Equal(record.Value, body) {
		t.Fatalf("o tópico recebeu outro corpo:\n%s\n%s", record.Value, body)
	}
	eventually(t, func() bool { return switched.Stats()[0].Delivered == 1 })
}
