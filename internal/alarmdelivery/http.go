package alarmdelivery

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"
)

// httpTransport é o destino webhook: um POST por tentativa.
type httpTransport struct {
	url, token string
	client     *http.Client
}

func newHTTPTransport(url, token string, timeout time.Duration) *httpTransport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Nenhuma credencial ou payload pertence a um redirect de outro servidor.
	return &httpTransport{url: url, token: token,
		client: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (*httpTransport) kind() string { return kindWebhook }

// reject não tem teto além do da fila (maxEventBytes, conferido em offer).
func (*httpTransport) reject([]byte) string { return "" }

func (h *httpTransport) close() { h.client.CloseIdleConnections() }

// send confirma só com HTTP 2xx. O status vai no diagnóstico, zero para falha
// de transporte.
func (h *httpTransport) send(ctx context.Context, body []byte, identity eventIdentity) attempt {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return attempt{diagnostic: []any{"status", 0}}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", identity.ID)
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	response, err := h.client.Do(req)
	if err != nil {
		return attempt{diagnostic: []any{"status", 0}}
	}
	defer func() { _ = response.Body.Close() }()
	// Limita também a leitura de respostas de um receptor defeituoso.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	retry := time.Duration(0)
	if response.StatusCode == 429 || response.StatusCode == 503 {
		retry = parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}
	return attempt{ok: response.StatusCode >= 200 && response.StatusCode < 300, retryAfter: retry, diagnostic: []any{"status", response.StatusCode}}
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
