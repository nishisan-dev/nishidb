package alarmdelivery

import (
	"context"
	"math/rand/v2"
	"time"
)

// Tipos de destino publicados no wire (`kind` de AlarmDeliveryStats). A fila
// pausada não tem transporte e sai sem tipo.
const (
	kindWebhook = "webhook"
	kindKafka   = "kafka"
)

// transport é o que a fila sabe de um destino: UMA tentativa de entrega por
// vez, sem estado de retentativa. A política de retentativa (backoff, FIFO,
// FailedAttempts) é da fila e vale igual para os dois tipos. Fila sem
// transporte é a fila pausada.
type transport interface {
	// kind é o tipo publicado no wire.
	kind() string
	// reject devolve o motivo de descarte de um corpo que este transporte
	// nunca conseguiria entregar, ou "" para admitir. Roda na admissão, fora
	// do lock da fila, e não faz I/O.
	reject(body []byte) string
	// send faz uma tentativa. O ctx é o do remetente: cancelado no Close, e a
	// tentativa tem de devolver sem esperar o destino.
	send(ctx context.Context, body []byte, identity eventIdentity) attempt
	// close libera conexões e clientes; roda depois que o remetente parou.
	close()
}

// attempt é o desfecho de uma tentativa.
type attempt struct {
	ok bool
	// retryAfter é o piso da próxima espera pedido pelo destino.
	retryAfter time.Duration
	// diagnostic são os atributos ESTÁVEIS do WARN de retentativa: `status`
	// no HTTP e `class` no Kafka. Nunca corpo, URL, bootstrap ou credencial.
	diagnostic []any
	// invalid é o registro que este transporte não sabe enviar. Não é falha
	// do destino, e retentar não muda nada: a fila falha, como num registro
	// inválido em disco.
	invalid error
}

func (q *queue) sendLoop(ctx context.Context) {
	delay := q.initialDelay
	for {
		if ctx.Err() != nil {
			return
		}
		q.mu.Lock()
		var current entry
		available := q.failed == nil && q.head < len(q.pending)
		if available {
			current = q.pending[q.head]
		}
		q.mu.Unlock()
		if !available {
			select {
			case <-ctx.Done():
				return
			case <-q.wake:
				continue
			}
		}
		body, identity, err := readRecord(q.path(current.seq))
		if err != nil {
			q.fail(err)
			return
		}
		result := q.transport.send(ctx, body, identity)
		if ctx.Err() != nil {
			return
		}
		if result.invalid != nil {
			q.fail(result.invalid)
			return
		}
		if result.ok {
			// A exclusão durável é o ACK local. Se cair antes, a mesma identidade
			// pode reaparecer; só avançar depois impede reordenação no reinício.
			if err := q.remove(q.path(current.seq)); err != nil {
				q.fail(err)
				return
			}
			if err := q.syncDir(q.dir); err != nil {
				q.fail(err)
				return
			}
			q.mu.Lock()
			q.head++
			if q.head == len(q.pending) {
				q.pending = q.pending[:0]
				q.head = 0
			} else if q.head >= q.diskLimit/2 && q.head > 0 {
				q.pending = append(q.pending[:0], q.pending[q.head:]...)
				q.head = 0
			}
			q.stats.DiskEvents--
			q.stats.DiskBytes -= current.size
			q.reservedBytes -= current.size
			q.stats.Delivered++
			q.stats.LastSuccessTimestamp = float64(time.Now().UnixMilli()) / 1000
			q.mu.Unlock()
			delay = q.initialDelay
			continue
		}
		q.mu.Lock()
		q.stats.FailedAttempts++
		q.mu.Unlock()
		// URL, bootstrap, corpo, resposta e credencial não entram no
		// diagnóstico de falha: só o destino e o atributo estável do transporte.
		q.log.Warn("alarm.delivery.retry", "delivery attempt failed; retaining event for retry", append([]any{"destination", q.name}, result.diagnostic...)...)
		jitter := delay/2 + time.Duration(rand.Int64N(max(1, int64(delay-delay/2))))
		wait := max(jitter, result.retryAfter)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, q.maxDelay)
	}
}
