package alarmdelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const alarmTopic = "olt_alarms"

// syncBuffer é o destino de log dos testes: o remetente escreve de outra
// goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newCluster(t *testing.T, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func kafkaSpec(name string, brokers []string, timeout string) config.AlarmKafkaSpec {
	return config.AlarmKafkaSpec{Name: name, BootstrapServers: brokers, Topic: alarmTopic, Timeout: timeout}
}

// kafkaQueue abre a fila sobre um transporte Kafka de verdade.
func kafkaQueue(t *testing.T, dir string, brokers []string, timeout time.Duration) *queue {
	t.Helper()
	target, err := newKafkaTransport(kafkaSpec("noc", brokers, ""), timeout)
	if err != nil {
		t.Fatal(err)
	}
	return testQueue(t, dir, target)
}

// failProduce responde todo Produce com o código de erro enquanto healthy
// for falso, e o deixa seguir para o broker depois.
func failProduce(c *kfake.Cluster, healthy *atomic.Bool, code int16, requests *atomic.Int64) {
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if requests != nil {
			requests.Add(1)
		}
		if healthy.Load() {
			return nil, nil, false
		}
		return produceError(req, code), nil, true
	})
}

// produceError é a resposta de Produce com o código de erro em toda partição.
func produceError(req kmsg.Request, code int16) kmsg.Response {
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(req.GetVersion())
	for _, topic := range req.(*kmsg.ProduceRequest).Topics {
		tr := kmsg.NewProduceResponseTopic()
		tr.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			pr := kmsg.NewProduceResponseTopicPartition()
			pr.Partition = partition.Partition
			pr.ErrorCode = code
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

// holdProduce segura todo Produce no broker até release e anuncia cada
// chegada em arrived.
func holdProduce(c *kfake.Cluster, release <-chan struct{}) <-chan struct{} {
	arrived := make(chan struct{}, 64)
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		arrived <- struct{}{}
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	return arrived
}

// consume lê n registros do tópico desde o início. O prazo é guarda de
// travamento, não critério.
func consume(t *testing.T, brokers []string, n int) []*kgo.Record {
	t.Helper()
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(alarmTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("leitura do tópico parou com %d de %d registros: %v", len(out), n, err)
		}
		out = append(out, fetches.Records()...)
	}
	return out
}

func header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func marshalEvent(t *testing.T, e alarm.Event) []byte {
	t.Helper()
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func firingEvent(id, groupKey string, at time.Time) alarm.Event {
	return alarm.Event{ID: id, Dataset: "service_state", Counter: "pon", Alarm: "pon_down", State: alarm.StateFiring,
		GroupKey: groupKey, Columns: []alarm.Column{{Name: "olt", Value: groupKey}}, Total: 3, Rows: 40,
		Condition: "total = 0", SustainedFor: time.Minute, Since: at.Add(-time.Minute), At: at}
}

// K1: o valor do registro é o corpo que o webhook POSTa, byte a byte, para o
// MESMO evento emitido uma vez para os dois destinos.
func TestKafkaValueIsTheWebhookBodyByteForByte(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	posted := make(chan []byte, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		posted <- body
		w.WriteHeader(204)
	}))
	defer receiver.Close()
	cfg := deliveryConfig("noc", receiver.URL)
	cfg.AlarmDelivery.Kafka = []config.AlarmKafkaSpec{kafkaSpec("olt_alarms", c.ListenAddrs(), "")}
	m, err := Open(t.TempDir(), deliverySpec("noc", "olt_alarms"), cfg, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	m.Start(context.Background())
	e := firingEvent("k1", `BR_X_OLT01 "PON" 4`, time.Now().UTC())
	e.Counter, e.Alarm = "c", "a"
	e.Metrics = []alarm.Column{{Name: "auth_5m_avg", Value: "12.5"}}
	e.Enrichment = []alarm.Enrichment{{Name: "services", Rows: json.RawMessage(`[{"customerId":"cli-1"}]`)}}
	e.Restored, e.DowntimeMs = true, 1500
	m.Emit(e)
	var body []byte
	select {
	case body = <-posted:
	case <-time.After(10 * time.Second):
		t.Fatal("o webhook não recebeu o evento")
	}
	record := consume(t, c.ListenAddrs(), 1)[0]
	if !bytes.Equal(record.Value, body) {
		t.Fatalf("o valor do Kafka divergiu do corpo do webhook:\nkafka:   %s\nwebhook: %s", record.Value, body)
	}
	if !bytes.Equal(body, marshalEvent(t, e)) {
		t.Fatalf("o corpo não é o json.Marshal do evento: %s", body)
	}
}

// K2: a chave é o array JSON compacto [dataset, counter, groupKey], com o
// escape canônico do JSON — golden escrito à mão, com aspas, barra, `\n`,
// outro byte de controle (`\x01` vira `\u0001`, e não o `\x01` do Go),
// não-ASCII cru e um byte UTF-8 inválido (vira o escape `\ufffd`, seis
// caracteres ASCII). O disparo e a resolução do grupo aposentado levam a
// MESMA chave e caem na mesma partição.
func TestKafkaKeyIsTheCompactArrayOfDatasetCounterAndGroupKey(t *testing.T) {
	groupKey := "OLT \"A\" \\ ação\n\x01\xff"
	const goldenPrefix = `["service_state","pon","OLT \"A\" \\ ação\n\u0001`
	const golden = goldenPrefix + `\ufffd"]`
	if got := string(kafkaKey("service_state", "pon", groupKey)); got != golden {
		t.Fatalf("chave fora do golden:\n got: %s\nwant: %s", got, golden)
	}
	var decoded []string
	if err := json.Unmarshal([]byte(golden), &decoded); err != nil || len(decoded) != 3 || decoded[2] != strings.ToValidUTF8(groupKey, "\uFFFD") {
		t.Fatalf("o golden não decodifica para [dataset, counter, groupKey]: %q %v", decoded, err)
	}
	// O corpo gravado já traz o byte inválido como U+FFFD (json.Marshal), e o
	// envio monta a chave do corpo decodificado: ali o rune é válido e sai cru.
	const goldenFromBody = goldenPrefix + "\uFFFD" + `"]`
	c := newCluster(t, kfake.SeedTopics(8, alarmTopic))
	q := kafkaQueue(t, filepath.Join(t.TempDir(), "noc"), c.ListenAddrs(), 10*time.Second)
	at := time.Now().UTC()
	retired := firingEvent("retired", groupKey, at)
	retired.State, retired.Reason, retired.Total, retired.Rows, retired.Columns = alarm.StateResolved, alarm.ReasonGroupRetired, 0, 0, nil
	q.offer(marshalEvent(t, firingEvent("firing", groupKey, at)), at)
	q.offer(marshalEvent(t, retired), at)
	q.start(context.Background())
	records := consume(t, c.ListenAddrs(), 2)
	for _, r := range records {
		if string(r.Key) != goldenFromBody {
			t.Errorf("registro %s com chave fora do golden: %s", r.Value, r.Key)
		}
	}
	if records[0].Partition != records[1].Partition {
		t.Fatal("disparo e resolução do mesmo grupo caíram em partições diferentes")
	}
}

// K3: os cabeçalhos `eventId` e `event` existem e repetem o corpo.
func TestKafkaHeadersCarryTheEventIDAndTheEventName(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	q := kafkaQueue(t, filepath.Join(t.TempDir(), "noc"), c.ListenAddrs(), 10*time.Second)
	at := time.Now().UTC()
	resolved := firingEvent("second", "olt", at)
	resolved.State, resolved.Reason = alarm.StateResolved, alarm.ReasonConditionCleared
	q.offer(marshalEvent(t, firingEvent("first", "olt", at)), at)
	q.offer(marshalEvent(t, resolved), at)
	q.start(context.Background())
	records := consume(t, c.ListenAddrs(), 2)
	for i, want := range []struct{ id, event string }{{"first", "alarm.firing"}, {"second", "alarm.resolved"}} {
		var fields map[string]any
		if err := json.Unmarshal(records[i].Value, &fields); err != nil {
			t.Fatal(err)
		}
		id, okID := header(records[i], "eventId")
		event, okEvent := header(records[i], "event")
		if !okID || !okEvent || id != want.id || event != want.event || fields["eventId"] != id || fields["event"] != event {
			t.Fatalf("cabeçalhos %v divergem do corpo %s", records[i].Headers, records[i].Value)
		}
	}
}

// K4: o arquivo da fila só some depois do ack. Com o Produce segurado no
// broker, o arquivo existe e o evento conta em disco.
func TestKafkaQueueFileIsRemovedOnlyAfterTheAck(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	arrived := holdProduce(c, release)
	q := kafkaQueue(t, filepath.Join(t.TempDir(), "noc"), c.ListenAddrs(), time.Minute)
	q.offer(eventBody(t, "held"), time.Now())
	eventually(t, func() bool { return q.snapshot().DiskEvents == 1 })
	q.start(context.Background())
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("o Produce não chegou ao broker")
	}
	if _, err := os.Stat(q.path(1)); err != nil {
		t.Fatalf("arquivo apagado antes do ack: %v", err)
	}
	if s := q.snapshot(); s.DiskEvents != 1 || s.Delivered != 0 {
		t.Fatalf("evento dado por entregue antes do ack: %+v", s)
	}
	open()
	eventually(t, func() bool { return q.snapshot().Delivered == 1 })
	if _, err := os.Stat(q.path(1)); !os.IsNotExist(err) {
		t.Fatalf("arquivo continua depois do ack: %v", err)
	}
	if s := q.snapshot(); s.DiskEvents != 0 {
		t.Fatalf("contagem em disco não baixou: %+v", s)
	}
}

// K5: com o Kafka recusando, os eventos ficam no disco, FailedAttempts
// cresce, o WARN traz a classe e nada do corpo ou do bootstrap, e quando o
// broker volta tudo é entregue na ordem.
//
// A queda é de EXATAMENTE três Produce sobre quatro eventos: com um número de
// falhas que não é múltiplo do de eventos, uma fila que girasse a cabeça a
// cada falha entregaria fora de ordem, e o FIFO não sai verde por vacuidade.
// O quarto Produce, o primeiro saudável, fica segurado até o teste conferir o
// estado da queda.
func TestKafkaOutageKeepsEventsOnDiskAndDeliversThemInOrder(t *testing.T) {
	const failures = 3
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	healed := make(chan struct{})
	var healOnce sync.Once
	heal := func() { healOnce.Do(func() { close(healed) }) }
	t.Cleanup(heal)
	var requests atomic.Int64
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		switch n := requests.Add(1); {
		case n <= failures:
			return produceError(req, kerr.NotEnoughReplicas.Code), nil, true
		case n == failures+1:
			c.SleepControl(func() { <-healed })
		}
		return nil, nil, false
	})
	logs := &syncBuffer{}
	target, err := newKafkaTransport(kafkaSpec("noc", c.ListenAddrs(), ""), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	q, err := openQueue(filepath.Join(t.TempDir(), "noc"), "noc", 256<<20, target, logging.New(logs, logging.FormatJSON, slog.LevelDebug))
	if err != nil {
		t.Fatal(err)
	}
	q.initialDelay, q.maxDelay = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { _ = q.close() })
	ids := []string{"first", "second", "third", "fourth"}
	for _, id := range ids {
		q.offer(eventBody(t, id), time.Now())
	}
	eventually(t, func() bool { return q.snapshot().DiskEvents == len(ids) })
	q.start(context.Background())
	// O Produce segurado só chega depois da terceira falha contada.
	eventually(t, func() bool { return requests.Load() == failures+1 })
	if s := q.snapshot(); s.FailedAttempts != failures || s.DiskEvents != len(ids) || s.Delivered != 0 {
		t.Fatalf("a falha do Kafka descartou, deu por entregue ou contou errado: %+v", s)
	}
	text := logs.String()
	if !strings.Contains(text, `"event":"alarm.delivery.retry"`) || !strings.Contains(text, `"class":"broker_unavailable"`) || !strings.Contains(text, `"destination":"noc"`) {
		t.Fatalf("WARN de retentativa sem classe ou destino:\n%s", text)
	}
	for _, leaked := range append([]string{c.ListenAddrs()[0], `"groupKey"`}, ids...) {
		if strings.Contains(text, leaked) {
			t.Fatalf("o diagnóstico vazou %q:\n%s", leaked, text)
		}
	}
	heal()
	eventually(t, func() bool { return q.snapshot().Delivered == uint64(len(ids)) })
	records := consume(t, c.ListenAddrs(), len(ids))
	for i, r := range records {
		if id, _ := header(r, "eventId"); id != ids[i] {
			t.Fatalf("ordem quebrada: posição %d trouxe %q", i, id)
		}
	}
	if s := q.snapshot(); s.FailedAttempts != failures {
		t.Fatalf("falhas contadas %d, esperadas %d", s.FailedAttempts, failures)
	}
}

// K6: a fila pendente sobrevive ao reinício e é reentregue na ordem, com o
// MESMO eventId no corpo e no cabeçalho, e com a mesma chave.
func TestKafkaRestartRedeliversInOrderWithTheSameIdentity(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	var healthy atomic.Bool
	failProduce(c, &healthy, kerr.NotEnoughReplicas.Code, nil)
	dir := filepath.Join(t.TempDir(), "noc")
	q := kafkaQueue(t, dir, c.ListenAddrs(), 10*time.Second)
	at := time.Now().UTC()
	bodies := map[string][]byte{}
	ids := []string{"first", "second"}
	for i, id := range ids {
		bodies[id] = marshalEvent(t, firingEvent(id, fmt.Sprintf("olt-%d", i), at))
		q.offer(bodies[id], at)
	}
	q.start(context.Background())
	eventually(t, func() bool { return q.snapshot().FailedAttempts >= 1 && q.snapshot().DiskEvents == 2 })
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
	healthy.Store(true)
	recovered := kafkaQueue(t, dir, c.ListenAddrs(), 10*time.Second)
	if recovered.snapshot().DiskEvents != 2 {
		t.Fatalf("reinício perdeu pendências: %+v", recovered.snapshot())
	}
	recovered.start(context.Background())
	records := consume(t, c.ListenAddrs(), 2)
	for i, r := range records {
		id, _ := header(r, "eventId")
		event, _ := header(r, "event")
		wantKey := string(kafkaKey("service_state", "pon", fmt.Sprintf("olt-%d", i)))
		if id != ids[i] || event != "alarm.firing" || !bytes.Equal(r.Value, bodies[ids[i]]) || string(r.Key) != wantKey {
			t.Fatalf("posição %d: id %q, evento %q, chave %s, corpo %s", i, id, event, r.Key, r.Value)
		}
	}
}

// K7: um evento com `at` de duas horas atrás, que esperou na fila durante a
// queda, é entregue quando o broker volta, e o Timestamp do registro é o da
// produção, nunca o `at`.
func TestOldQueuedEventIsDeliveredAndStampedAtProduction(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	var healthy atomic.Bool
	failProduce(c, &healthy, kerr.NotEnoughReplicas.Code, nil)
	q := kafkaQueue(t, filepath.Join(t.TempDir(), "noc"), c.ListenAddrs(), time.Second)
	at := time.Now().Add(-2 * time.Hour).UTC()
	q.offer(marshalEvent(t, firingEvent("old", "olt", at)), at)
	q.start(context.Background())
	eventually(t, func() bool { return q.snapshot().FailedAttempts >= 2 })
	healedAt := time.Now().Truncate(time.Millisecond)
	healthy.Store(true)
	eventually(t, func() bool { return q.snapshot().Delivered == 1 })
	record := consume(t, c.ListenAddrs(), 1)[0]
	if record.Timestamp.Before(healedAt) {
		t.Fatalf("Timestamp %s anterior à tentativa que entregou (%s); at do evento %s", record.Timestamp, healedAt, at)
	}
}

// K13: o Close com o Produce segurado pelo broker devolve sem esperar o
// broker, e o evento continua na fila.
func TestKafkaCloseDoesNotWaitForTheBroker(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	arrived := holdProduce(c, release)
	dir := filepath.Join(t.TempDir(), "noc")
	q := kafkaQueue(t, dir, c.ListenAddrs(), time.Minute)
	q.offer(eventBody(t, "held"), time.Now())
	q.start(context.Background())
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("o Produce não chegou ao broker")
	}
	done := make(chan error, 1)
	go func() { done <- q.close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		// Solta o broker antes de reprovar: sem isto, o close do Cleanup
		// ficaria preso no mesmo Produce, e a falha viraria travamento.
		open()
		<-done
		t.Fatal("o Close esperou o broker")
	}
	if recovered := testQueue(t, dir, nil); recovered.snapshot().DiskEvents != 1 {
		t.Fatalf("o Close perdeu o evento em voo: %+v", recovered.snapshot())
	}
}

// K14: no timeout ambíguo — o broker grava e a resposta chega depois do
// prazo — a fila retenta, e o tópico fica com duas cópias do MESMO eventId.
// É a duplicata documentada, que o consumidor deduplica pelo eventId.
func TestAmbiguousKafkaTimeoutLeavesTwoCopiesWithTheSameEventID(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	var q atomic.Pointer[queue]
	var requests atomic.Int64
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if requests.Add(1) == 1 {
			// A primeira só segue para o broker depois que a tentativa já
			// desistiu: o broker grava, e a resposta não serve a ninguém.
			c.SleepControl(func() {
				for q.Load().snapshot().FailedAttempts < 1 {
					time.Sleep(time.Millisecond)
				}
			})
		}
		return nil, nil, false
	})
	opened := kafkaQueue(t, filepath.Join(t.TempDir(), "noc"), c.ListenAddrs(), 200*time.Millisecond)
	q.Store(opened)
	body := eventBody(t, "ambiguous")
	opened.offer(body, time.Now())
	opened.start(context.Background())
	records := consume(t, c.ListenAddrs(), 2)
	for _, r := range records {
		if id, _ := header(r, "eventId"); id != "ambiguous" || !bytes.Equal(r.Value, body) {
			t.Fatalf("cópia com identidade ou corpo diferentes: %q %s", id, r.Value)
		}
	}
	eventually(t, func() bool { return opened.snapshot().Delivered == 1 })
}

// K15: broker inacessível não impede o Open nem o Start; o evento fica na
// fila contando tentativas, e o Close não espera.
func TestUnreachableBrokerDoesNotPreventOpenNorStartup(t *testing.T) {
	cfg := &config.Config{}
	cfg.AlarmDelivery.Kafka = []config.AlarmKafkaSpec{kafkaSpec("noc", []string{"127.0.0.1:1"}, "200ms")}
	if err := Validate(cfg, map[string]*schema.DatasetSpec{"d": deliverySpec("noc")}); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	m, err := Open(t.TempDir(), deliverySpec("noc"), cfg, logging.New(logs, logging.FormatJSON, slog.LevelDebug))
	if err != nil {
		t.Fatalf("broker fora do ar impediu a subida: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	m.queues["noc"].initialDelay, m.queues["noc"].maxDelay = time.Millisecond, 5*time.Millisecond
	m.Start(context.Background())
	m.Emit(alarm.Event{ID: "unreachable", Dataset: "d", Counter: "c", Alarm: "a", State: alarm.StateFiring, GroupKey: "olt", At: time.Now()})
	eventually(t, func() bool { s := m.Stats()[0]; return s.FailedAttempts >= 2 && s.DiskEvents == 1 })
	if s := m.Stats()[0]; s.Kind != "kafka" || s.State != "running" {
		t.Fatalf("destino Kafka mal publicado: %+v", s)
	}
	// Conexão recusada é broker indisponível, e não prazo esgotado.
	if !strings.Contains(logs.String(), `"class":"broker_unavailable"`) {
		t.Fatalf("classe da conexão recusada não é broker_unavailable:\n%s", logs.String())
	}
	done := make(chan error, 1)
	go func() { done <- m.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("o Close esperou o broker inacessível")
	}
}

// K16: o evento grande demais para o Kafka é descartado na admissão como
// event_too_large, sem arquivo, e não prende o seguinte. O corpo cabe na
// fila (abaixo de maxEventBytes): só o teto do transporte o recusa.
func TestEventTooLargeForKafkaIsDroppedAtAdmissionAndDoesNotBlockTheNext(t *testing.T) {
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	q := kafkaQueue(t, filepath.Join(t.TempDir(), "noc"), c.ListenAddrs(), 10*time.Second)
	e := firingEvent("large", "olt", time.Now().UTC())
	e.Enrichment = []alarm.Enrichment{{Name: "big", Rows: json.RawMessage(`["` + strings.Repeat("x", 1_000_100) + `"]`)}}
	large := marshalEvent(t, e)
	if len(large) > maxEventBytes || len(large) <= kafka.MaxProducedRecordBytes {
		t.Fatalf("corpo de %d bytes não exercita o teto do transporte", len(large))
	}
	q.start(context.Background())
	q.offer(large, time.Now())
	if s := q.snapshot(); s.Dropped[DropTooLarge] != 1 || s.MemoryEvents != 0 || s.DiskEvents != 0 {
		t.Fatalf("evento grande demais para o Kafka foi admitido: %+v", s)
	}
	q.offer(eventBody(t, "next"), time.Now())
	eventually(t, func() bool { return q.snapshot().Delivered == 1 })
	if id, _ := header(consume(t, c.ListenAddrs(), 1)[0], "eventId"); id != "next" {
		t.Fatalf("o tópico recebeu %q em vez do evento seguinte", id)
	}
}

// O teto é o do registro inteiro — chave, corpo e cabeçalhos —, no byte. A
// conta do tamanho esperado é feita aqui, à mão, e não pela recordBytes que o
// teste vigia.
func TestKafkaAdmissionLimitCountsKeyBodyAndHeadersToTheByte(t *testing.T) {
	target := &kafkaTransport{topic: alarmTopic}
	at := time.Now().UTC()
	sized := func(n int) []byte {
		e := firingEvent("edge", "olt", at)
		e.Enrichment = []alarm.Enrichment{{Name: "pad", Rows: json.RawMessage(`[""]`)}}
		base := marshalEvent(t, e)
		overhead := len(`["service_state","pon","olt"]`) + len("eventId") + len("edge") + len("event") + len("alarm.firing")
		e.Enrichment[0].Rows = json.RawMessage(`["` + strings.Repeat("x", n-overhead-len(base)) + `"]`)
		body := marshalEvent(t, e)
		if len(body)+overhead != n {
			t.Fatalf("registro montado com %d bytes, esperado %d", len(body)+overhead, n)
		}
		return body
	}
	if got := target.reject(sized(kafka.MaxProducedRecordBytes)); got != "" {
		t.Fatalf("registro no teto recusado com %q", got)
	}
	if got := target.reject(sized(kafka.MaxProducedRecordBytes + 1)); got != DropTooLarge {
		t.Fatalf("registro um byte acima do teto admitido (%q)", got)
	}
	if got := target.reject([]byte(`{"eventId":"x","at":"2026-10-02T00:00:00Z"}`)); got != DropEncoding {
		t.Fatalf("corpo sem os campos da chave admitido (%q)", got)
	}
}

// Registro em disco sem os campos da chave não é retentado para sempre nem
// pulado: a fila falha, como num registro inválido, e o arquivo fica para
// investigação. Ele só chega ao disco por uma fila que não era Kafka — aqui,
// a pausada.
func TestKafkaRecordWithoutTheKeyFieldsFailsTheQueue(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "noc")
	paused := testQueue(t, dir, nil)
	paused.offer([]byte(`{"eventId":"keyless","at":"2026-10-02T00:00:00.000Z","event":"alarm.firing"}`), time.Now())
	if err := paused.close(); err != nil {
		t.Fatal(err)
	}
	c := newCluster(t, kfake.SeedTopics(1, alarmTopic))
	q := kafkaQueue(t, dir, c.ListenAddrs(), 10*time.Second)
	q.start(context.Background())
	eventually(t, func() bool { return q.snapshot().State == "failed" })
	if s := q.snapshot(); s.DiskEvents != 1 || s.FailedAttempts != 0 || s.PersistenceErrors != 1 {
		t.Fatalf("registro sem chave não falhou a fila: %+v", s)
	}
	if err := q.close(); !errors.Is(err, ErrQueue) || !strings.Contains(err.Error(), "lacks dataset, counter or groupKey") {
		t.Fatalf("falha não veio da guarda própria: %v", err)
	}
	if _, err := os.Stat(q.path(1)); err != nil {
		t.Fatalf("o registro inválido foi apagado: %v", err)
	}
}

// O produtor não pede a criação do tópico: com a criação automática LIGADA
// no broker, o tópico ausente continua ausente, e a tentativa falha com a
// classe unknown_topic.
func TestKafkaProducerDoesNotCreateTheTopic(t *testing.T) {
	c := newCluster(t, kfake.AllowAutoTopicCreation())
	logs := &syncBuffer{}
	target, err := newKafkaTransport(kafkaSpec("noc", c.ListenAddrs(), ""), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	q, err := openQueue(filepath.Join(t.TempDir(), "noc"), "noc", 256<<20, target, logging.New(logs, logging.FormatJSON, slog.LevelDebug))
	if err != nil {
		t.Fatal(err)
	}
	q.initialDelay, q.maxDelay = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { _ = q.close() })
	q.offer(eventBody(t, "absent"), time.Now())
	q.start(context.Background())
	eventually(t, func() bool { return q.snapshot().FailedAttempts >= 2 })
	if s := q.snapshot(); s.Delivered != 0 || s.DiskEvents != 1 {
		t.Fatalf("produção num tópico ausente foi aceita: %+v", s)
	}
	if !strings.Contains(logs.String(), `"class":"unknown_topic"`) {
		t.Fatalf("classe da falha não é unknown_topic:\n%s", logs.String())
	}
	// Criado o tópico, a fila anda sozinha: o erro era permanente só até a
	// correção, e não exige reinício.
	admin, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(context.Background(), 1, 1, nil, alarmTopic); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return q.snapshot().Delivered == 1 })
}

func TestKafkaErrorClassesAreStable(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	for err, want := range map[error]string{
		context.DeadlineExceeded:                        classTimeout,
		kgo.ErrRecordTimeout:                            classTimeout,
		fmt.Errorf("wrapped: %w", kerr.RequestTimedOut): classTimeout,
		kerr.UnknownTopicOrPartition:                    classUnknownTopic,
		kerr.TopicAuthorizationFailed:                   classUnauthorized,
		kerr.SaslAuthenticationFailed:                   classUnauthorized,
		kerr.MessageTooLarge:                            classRejected,
		kerr.NotEnoughReplicas:                          classBrokerUnavailable,
		refused:                                         classBrokerUnavailable,
		errors.New("anything else"):                     classBrokerUnavailable,
	} {
		if got := kafkaErrorClass(err); got != want {
			t.Errorf("%v: classe %q, esperada %q", err, got, want)
		}
	}
}
