package alarmdelivery

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

type endpoint struct {
	url, token             string
	client                 *http.Client
	initialDelay, maxDelay time.Duration
}

func newEndpoint(url, token string, timeout time.Duration) *endpoint {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Nenhuma credencial ou payload pertence a um redirect de outro servidor.
	return &endpoint{url: url, token: token, initialDelay: time.Second, maxDelay: 5 * time.Minute,
		client: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (q *queue) sendLoop(ctx context.Context) {
	delay := q.endpoint.initialDelay
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
		success, status, retryAfter := q.post(ctx, body, identity.ID)
		if ctx.Err() != nil {
			return
		}
		if success {
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
			delay = q.endpoint.initialDelay
			continue
		}
		q.mu.Lock()
		q.stats.FailedAttempts++
		q.mu.Unlock()
		// URL, corpo, resposta e token não entram no diagnóstico de falha.
		q.log.Warn("alarm.delivery.retry", "webhook failed; retaining event for retry", "destination", q.name, "status", status)
		jitter := delay/2 + time.Duration(rand.Int64N(max(1, int64(delay-delay/2))))
		wait := max(jitter, retryAfter)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, q.endpoint.maxDelay)
	}
}

func (q *queue) post(ctx context.Context, body []byte, id string) (bool, int, time.Duration) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.endpoint.url, bytes.NewReader(body))
	if err != nil {
		return false, 0, 0
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", id)
	if q.endpoint.token != "" {
		req.Header.Set("Authorization", "Bearer "+q.endpoint.token)
	}
	response, err := q.endpoint.client.Do(req)
	if err != nil {
		return false, 0, 0
	}
	defer func() { _ = response.Body.Close() }()
	// Limita também a leitura de respostas de um receptor defeituoso.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	retry := time.Duration(0)
	if response.StatusCode == 429 || response.StatusCode == 503 {
		retry = parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}
	return response.StatusCode >= 200 && response.StatusCode < 300, response.StatusCode, retry
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 && seconds <= int64((1<<63-1)/time.Second) {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, at.Sub(now))
	}
	return 0
}
