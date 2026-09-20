package alarmdelivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

func deliverySpec(destinations ...string) *schema.DatasetSpec {
	return &schema.DatasetSpec{Name: "d", Counters: []schema.CounterSpec{{Name: "c", GroupBy: []string{"olt"}, Alarms: []schema.AlarmSpec{{Name: "a", Notify: destinations}}}}}
}
func deliveryConfig(name, url string) *config.Config {
	cfg := &config.Config{}
	cfg.AlarmDelivery.Webhooks = []config.WebhookSpec{{Name: name, URL: url}}
	return cfg
}

func TestValidateReferencesReservedFieldsAndCredentials(t *testing.T) {
	cfg := deliveryConfig("noc", "http://localhost/events")
	spec := deliverySpec("missing")
	check := func(want string) {
		t.Helper()
		err := Validate(cfg, map[string]*schema.DatasetSpec{"d": spec})
		if !errors.Is(err, config.ErrAlarmDelivery) || !strings.Contains(err.Error(), want) {
			t.Fatalf("guarda %q não recusou: %v", want, err)
		}
	}
	check("unknown or duplicate destination")
	spec = deliverySpec("noc", "noc")
	check("unknown or duplicate destination")
	spec = deliverySpec("noc")
	spec.Counters[0].GroupBy = []string{"eventId"}
	check("reserved event key")
	spec = deliverySpec("noc")
	cfg.AlarmDelivery.Webhooks[0].BearerTokenEnv = "NISHIDB_TEST_MISSING_TOKEN"
	t.Setenv("NISHIDB_TEST_MISSING_TOKEN", "")
	check("is missing, empty or invalid")
	t.Setenv("NISHIDB_TEST_MISSING_TOKEN", "secret")
	if err := Validate(cfg, map[string]*schema.DatasetSpec{"d": spec}); err != nil {
		t.Fatal(err)
	}
	// Colisão antiga de log não passa a impedir um alarme sem push.
	spec.Counters[0].GroupBy = []string{"eventId"}
	spec.Counters[0].Alarms[0].Notify = nil
	if err := Validate(cfg, map[string]*schema.DatasetSpec{"d": spec}); err != nil {
		t.Fatal(err)
	}
}

func TestFanoutKeepsOtherDestinationRunningAndRemovedRouteStillDrains(t *testing.T) {
	var failedCalls, delivered atomic.Int64
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { failedCalls.Add(1); w.WriteHeader(403) }))
	defer broken.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { delivered.Add(1); w.WriteHeader(204) }))
	defer good.Close()
	cfg := deliveryConfig("broken", broken.URL)
	cfg.AlarmDelivery.Webhooks = append(cfg.AlarmDelivery.Webhooks, config.WebhookSpec{Name: "good", URL: good.URL})
	dir := t.TempDir()
	m, err := Open(dir, deliverySpec("broken", "good"), cfg, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	m.Start(context.Background())
	m.Emit(alarm.Event{ID: "fanout", Dataset: "d", Counter: "c", Alarm: "a", State: alarm.StateFiring, At: time.Now()})
	eventually(t, func() bool { return delivered.Load() == 1 && failedCalls.Load() >= 1 })
	if m.queues["broken"].snapshot().DiskEvents != 1 {
		t.Fatal("destino quebrado perdeu o evento")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// Destino removido conserva a pendência, mesmo sem alarmes no dataset.
	paused, err := Open(dir, &schema.DatasetSpec{Name: "d"}, &config.Config{}, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if paused.queues["broken"].snapshot().State != "paused" || paused.queues["broken"].snapshot().DiskEvents != 1 {
		t.Fatal("remoção não pausou a fila")
	}
	if err := paused.Close(); err != nil {
		t.Fatal(err)
	}
	// O mesmo nome com URL corrigida drena a fila antiga, sem declarar alarme.
	resumed, err := Open(dir, &schema.DatasetSpec{Name: "d"}, deliveryConfig("broken", good.URL), logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resumed.Close() })
	resumed.Start(context.Background())
	eventually(t, func() bool { return resumed.queues["broken"].snapshot().Delivered == 1 })
	if delivered.Load() != 2 {
		t.Fatal("pendência não chegou à nova URL")
	}
}

func TestSlowHTTPDoesNotStopPersistence(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	receiver := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { close(entered); <-release }))
	defer receiver.Close()
	defer close(release)
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(receiver.URL))
	q.offer(eventBody(t, "first"), time.Now())
	q.start(context.Background())
	<-entered
	for i := 0; i < 20; i++ {
		q.offer(eventBody(t, "pending"), time.Now())
	}
	eventually(t, func() bool { return q.snapshot().DiskEvents == 21 })
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutAndDisconnectRetainTheEvent(t *testing.T) {
	for _, mode := range []string{"timeout", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			release := make(chan struct{})
			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if mode == "timeout" {
					<-release
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
			}))
			defer receiver.Close()
			defer close(release)
			target := fastEndpoint(receiver.URL)
			target.client.Timeout = 10 * time.Millisecond
			q := testQueue(t, filepath.Join(t.TempDir(), "noc"), target)
			q.offer(eventBody(t, "kept"), time.Now())
			q.start(context.Background())
			eventually(t, func() bool { return q.snapshot().FailedAttempts >= 2 })
			if q.snapshot().DiskEvents != 1 || q.snapshot().Delivered != 0 {
				t.Fatal("erro de rede perdeu evento")
			}
			_ = q.close()
		})
	}
}

func TestRetryAfterIsUsedBySender(t *testing.T) {
	var calls atomic.Int64
	received := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		received <- struct{}{}
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(503)
	}))
	defer server.Close()
	q := testQueue(t, filepath.Join(t.TempDir(), "noc"), fastEndpoint(server.URL))
	q.offer(eventBody(t, "id"), time.Now())
	q.start(context.Background())
	<-received
	select {
	case <-received:
		t.Fatal("Retry-After foi ignorado")
	case <-time.After(50 * time.Millisecond):
	}
	_ = q.close()
}

func TestEventEncodingFailureIsCounted(t *testing.T) {
	cfg := deliveryConfig("noc", "http://localhost/events")
	m, err := Open(t.TempDir(), deliverySpec("noc"), cfg, logging.Discard())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	e := alarm.Event{ID: "bad", Counter: "c", Alarm: "a", State: alarm.StateFiring, Enrichment: []alarm.Enrichment{{Name: "bad", Rows: json.RawMessage("invalid")}}}
	m.Emit(e)
	if m.Stats()[0].Dropped[DropEncoding] != 1 {
		t.Fatal("falha de serialização não foi contada")
	}
}
