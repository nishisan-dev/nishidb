package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

func startWebhookServer(t *testing.T, srv *server.Server) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, "") }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("Run: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}
func waitDelivery(t *testing.T, srv *server.Server, match func(protocol.AlarmDeliveryStats) bool) protocol.AlarmDeliveryStats {
	t.Helper()
	var latest protocol.AlarmDeliveryStats
	for i := 0; i < 500; i++ {
		for _, d := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
			if d.Name == "servicos" && len(d.AlarmDelivery) > 0 {
				latest = d.AlarmDelivery[0]
				if match(latest) {
					return latest
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("estado de entrega não chegou: %+v", latest)
	return latest
}

// A pendência nasce de uma transição real do runner e é entregue por uma
// segunda instância do servidor, mesmo sem alarme declarado nessa instância.
func TestAlarmWebhookSurvivesServerRestartAndPublishesSharedStats(t *testing.T) {
	t.Setenv("NISHIDB_TEST_WEBHOOK_TOKEN", "local-test-token")
	received := make(chan []byte, 10)
	var repaired atomic.Bool
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var event map[string]any
		if err := json.Unmarshal(body, &event); err != nil {
			t.Error(err)
		}
		if event["eventId"] != r.Header.Get("Idempotency-Key") || r.Header.Get("Authorization") != "Bearer local-test-token" {
			t.Error("identidade ou credencial incorreta")
		}
		received <- body
		if !repaired.Load() {
			w.WriteHeader(403)
			return
		}
		w.WriteHeader(204)
	}))
	defer receiver.Close()
	a := newEnv(t)
	cfgBytes, err := os.ReadFile(a.cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfgBytes = append(cfgBytes, []byte(fmt.Sprintf("\nalarmDelivery:\n  webhooks:\n    - name: noc\n      url: %s\n      bearerTokenEnv: NISHIDB_TEST_WEBHOOK_TOKEN\n", receiver.URL))...)
	if err := os.WriteFile(a.cfgPath, cfgBytes, 0600); err != nil {
		t.Fatal(err)
	}
	spec := strings.Replace(fmt.Sprintf(specServicesComAlarme, "servicos"), "        sustainedFor: 10s", "        sustainedFor: 10s\n        notify: [noc]", 1)
	a.writeDataset("servicos", spec)
	seedServices(t, a, "servicos", 1)
	at := time.Now().UTC()
	clock := newTestClock(at)
	srv, err := server.Novo(a.load(), server.Options{Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	// Confere as séries antes de Run, quando nada pôde ser enviado.
	initial := srv.MetricsExpositionForTest()
	for _, name := range []string{metrics.MetAlarmDeliveryDeliveredTotal, metrics.MetAlarmDeliveryFailedAttemptsTotal, metrics.MetAlarmDeliveryDiskEvents} {
		if !strings.Contains(initial, name+`{dataset="servicos",destination="noc"} 0`) {
			t.Fatalf("série não nasceu em zero: %s\n%s", name, initial)
		}
	}
	stop := startWebhookServer(t, srv)
	for i := 0; i < 400 && !srv.AlarmsActive("servicos"); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if !srv.AlarmsActive("servicos") {
		t.Fatal("laço não subiu")
	}
	srv.StopSamplingAndAlarmsForTest("servicos")
	if err := srv.EvaluateAlarmsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	clock.set(at.Add(11 * time.Second))
	if err := srv.EvaluateAlarmsOnceForTest("servicos"); err != nil {
		t.Fatal(err)
	}
	var first []byte
	select {
	case first = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("alarme não chegou ao webhook")
	}
	pending := waitDelivery(t, srv, func(s protocol.AlarmDeliveryStats) bool { return s.FailedAttempts >= 1 && s.DiskEvents == 1 })
	confereIgual(t, coletaSamples(t, srv), metrics.MetAlarmDeliveryDiskEvents, map[string]string{metrics.LabelDataset: "servicos", metrics.LabelDestination: "noc"}, int64(pending.DiskEvents))
	stop()
	// Remover a regra não remove os arquivos já publicados pela primeira instância.
	a.writeDataset("servicos", spec[:strings.Index(spec, "    alarms:")])
	repaired.Store(true)
	next, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	stopNext := startWebhookServer(t, next)
	var replay []byte
	select {
	case replay = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("reinício não entregou a fila")
	}
	if string(first) != string(replay) {
		t.Fatalf("reinício mudou o corpo:\n%s\n%s", first, replay)
	}
	delivered := waitDelivery(t, next, func(s protocol.AlarmDeliveryStats) bool { return s.Delivered == 1 && s.DiskEvents == 0 })
	if delivered.MemoryEvents != 0 || delivered.LastSuccessTimestamp == 0 {
		t.Fatalf("estatísticas incompletas: %+v", delivered)
	}
	confereIgual(t, coletaSamples(t, next), metrics.MetAlarmDeliveryDeliveredTotal, map[string]string{metrics.LabelDataset: "servicos", metrics.LabelDestination: "noc"}, 1)
	stopNext()
}
