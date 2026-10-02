package alarmdelivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Nomes dos cabeçalhos do registro Kafka. São CONTRATO com o consumidor do
// tópico: `eventId` é o Idempotency-Key do webhook, e é por ele que o
// consumidor deduplica.
const (
	headerEventID = "eventId"
	headerEvent   = "event"
)

// Classes estáveis de falha no WARN `alarm.delivery.retry` do Kafka. É um
// conjunto FECHADO, como o status do HTTP: a classe nunca carrega texto do
// broker, do bootstrap ou da credencial.
const (
	classTimeout           = "timeout"
	classUnknownTopic      = "unknown_topic"
	classUnauthorized      = "unauthorized"
	classRejected          = "rejected"
	classBrokerUnavailable = "broker_unavailable"
)

// producer é o pedaço do kgo.Client que o transporte usa.
type producer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

// kafkaTransport é o destino Kafka: uma produção SÍNCRONA de um registro por
// tentativa. Há um por fila (dataset × destino), dono do próprio cliente kgo,
// criado no Open e fechado no close da fila — o mesmo ciclo de vida do
// http.Client do webhook.
type kafkaTransport struct {
	client  producer
	topic   string
	timeout time.Duration
}

// newKafkaTransport monta o cliente sem falar com o broker: o kgo só conecta
// na primeira produção, e broker fora do ar não impede a subida.
//
// A configuração é FIXA e é a do seriesDelivery, pelos motivos abaixo
// (franz-go v1.21.6):
//
//   - DisableIdempotentWrite. Com o produtor idempotente o kgo não cancela
//     registro em voo: a requisição só usa o contexto do registro com a
//     idempotência desligada (sink.go:588-596), e o próprio kgo documenta que,
//     por padrão, recusa cancelar e espera o desfecho (config.go:1191-1215).
//     Ele também passa por cima do limite de retentativas enquanto não sabe se
//     o lote foi gravado (sink.go:1082). Com isso o timeout por tentativa e o
//     Close deixariam de valer: uma tentativa contra um broker que não
//     responde prenderia o remetente e o encerramento. A idempotência do
//     broker não serve aqui de qualquer forma: a retentativa é da FILA, num
//     registro novo, e o broker não deduplicaria entre eles. Quem deduplica é
//     o consumidor, pelo `eventId`.
//   - RecordRetries(0) e UnknownTopicRetries(0). A retentativa é da fila,
//     para manter a ordem FIFO e a contagem de FailedAttempts honestas: uma
//     tentativa é uma requisição.
//   - RequiredAcks(AllISRAcks()). Sucesso só com o ack das réplicas em
//     sincronia; só ele apaga o arquivo da fila.
//   - ProduceRequestTimeout(max(timeout, 100ms)), RequestTimeoutOverhead(100ms)
//     e DialTimeout(timeout). Nenhuma espera de rede passa do prazo da
//     tentativa por mais que a margem.
//   - StickyKeyPartitioner(nil). A partição sai do hash da chave, então o
//     mesmo grupo cai sempre na mesma partição, e o disparo e a resolução
//     dele chegam em ordem.
//   - SEM RecordDeliveryTimeout. O kgo mede esse prazo a partir do Timestamp
//     do primeiro registro do lote (sink.go:2013-2017). O prazo da tentativa é
//     o contexto de send, criado na tentativa. E o Timestamp do registro fica
//     zero, para o kgo carimbar o instante da produção (sink.go:1611-1613):
//     com o `at` do evento, um evento que esperou na fila durante uma queda
//     teria um Timestamp antigo. O instante do alarme está no corpo.
func newKafkaTransport(spec config.AlarmKafkaSpec, timeout time.Duration) (*kafkaTransport, error) {
	tlsConfig, mechanism, err := kafka.ResolveSecurity(spec.Security)
	if err != nil {
		return nil, fmt.Errorf("%w: destination %q: %v", config.ErrAlarmDelivery, spec.Name, err)
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(spec.BootstrapServers...), kgo.DefaultProduceTopic(spec.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(),
		kgo.RecordRetries(0), kgo.UnknownTopicRetries(0),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProduceRequestTimeout(max(timeout, 100*time.Millisecond)),
		kgo.RequestTimeoutOverhead(100 * time.Millisecond), kgo.DialTimeout(timeout),
	}
	if tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: destination %q: %v", config.ErrAlarmDelivery, spec.Name, err)
	}
	return &kafkaTransport{client: client, topic: spec.Topic, timeout: timeout}, nil
}

func (*kafkaTransport) kind() string { return kindKafka }

func (k *kafkaTransport) close() { k.client.Close() }

// reject descarta na admissão o corpo que o kgo recusaria: o registro inteiro
// (chave, corpo e cabeçalhos) acima de kafka.MaxProducedRecordBytes, como
// `event_too_large`. Sem isto, o registro entraria na fila, o kgo o recusaria
// em toda tentativa, e a fila FIFO ficaria presa nele para sempre. O registro
// medido é montado pela mesma kafkaRecord do envio, a partir do mesmo
// json.Unmarshal, então o tamanho é o que de fato iria ao kgo. Corpo sem os
// campos da chave sai como `encoding_error`.
func (k *kafkaTransport) reject(body []byte) string {
	var identity eventIdentity
	if err := json.Unmarshal(body, &identity); err != nil {
		return DropEncoding
	}
	r, err := kafkaRecord(k.topic, body, identity)
	if err != nil {
		return DropEncoding
	}
	if recordBytes(r) > kafka.MaxProducedRecordBytes {
		return DropTooLarge
	}
	return ""
}

// send produz UM registro e espera o ack, com o prazo de uma tentativa. O
// registro é NOVO a cada tentativa: a produção o modifica (partição,
// Timestamp), e um registro reaproveitado levaria o carimbo da primeira.
//
// Timeout ambíguo gera duplicata: o broker pode ter gravado o registro e a
// resposta não ter chegado no prazo; a fila retenta, e o tópico fica com duas
// cópias do mesmo `eventId`.
func (k *kafkaTransport) send(ctx context.Context, body []byte, identity eventIdentity) attempt {
	r, err := kafkaRecord(k.topic, body, identity)
	if err != nil {
		return attempt{invalid: err}
	}
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	if err := k.client.ProduceSync(ctx, r).FirstErr(); err != nil {
		return attempt{diagnostic: []any{"class", kafkaErrorClass(err)}}
	}
	return attempt{ok: true}
}

// kafkaRecord monta o registro de um evento: o valor é o corpo gravado, byte
// a byte o que o webhook POSTa; a chave é kafkaKey; os cabeçalhos são o
// `eventId` e o nome do evento. Registro sem os campos da chave é inválido
// para este transporte.
func kafkaRecord(topic string, body []byte, identity eventIdentity) (*kgo.Record, error) {
	switch {
	case identity.ID == "":
		return nil, errors.New("queue record has no eventId")
	case identity.Event != alarm.EventFiring && identity.Event != alarm.EventResolved:
		return nil, errors.New("queue record has no valid event name")
	case identity.Dataset == "" || identity.Counter == "" || identity.GroupKey == nil:
		return nil, errors.New("queue record lacks dataset, counter or groupKey")
	}
	return &kgo.Record{
		Topic: topic,
		Key:   kafkaKey(identity.Dataset, identity.Counter, *identity.GroupKey),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: headerEventID, Value: []byte(identity.ID)},
			{Key: headerEvent, Value: []byte(identity.Event)},
		},
	}, nil
}

// kafkaKey é o array JSON compacto [dataset, counter, groupKey].
//
// NÃO é a chave da série (`[dataset, counter, ...groupBy tipado]`): a fila
// guarda só o corpo, onde as colunas do grupo são texto, e o tipo não é
// reconstruível sem mudar o formato do registro em disco. O groupKey é a
// identidade do grupo, então mesmo grupo → mesma chave → mesma partição. O
// escape é o canônico do executor (o mesmo da série e do NDJSON).
func kafkaKey(dataset, counter, groupKey string) []byte {
	key := executor.AppendJSONString([]byte{'['}, []byte(dataset))
	key = executor.AppendJSONString(append(key, ','), []byte(counter))
	key = executor.AppendJSONString(append(key, ','), []byte(groupKey))
	return append(key, ']')
}

// recordBytes é a conta do userSize do kgo (record_and_fetch.go:157-163).
func recordBytes(r *kgo.Record) int {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

// kafkaErrorClass traduz a falha de uma produção numa classe estável.
// Indisponibilidade do broker que se manifesta por esgotar o prazo (host
// inalcançável, broker que não responde) sai como `timeout`.
func kafkaErrorClass(err error) string {
	var networkError net.Error
	var brokerError *kerr.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrRecordTimeout), errors.Is(err, kerr.RequestTimedOut),
		errors.As(err, &networkError) && networkError.Timeout():
		return classTimeout
	case errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.UnknownTopicID):
		return classUnknownTopic
	case errors.Is(err, kerr.TopicAuthorizationFailed), errors.Is(err, kerr.ClusterAuthorizationFailed),
		errors.Is(err, kerr.SaslAuthenticationFailed):
		return classUnauthorized
	case errors.As(err, &brokerError) && !brokerError.Retriable:
		return classRejected
	default:
		return classBrokerUnavailable
	}
}
