package server_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarmdelivery"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// burstGroups passa várias vezes o buffer de memória da fila (256 eventos):
// a passada só entrega todos se a admissão esperar o gravador.
const burstGroups = 2_000

// specBurstAlarm declara um grupo por OLT e um alarme que dispara em todos:
// cada OLT semeada tem um único serviço autenticado.
const specBurstAlarm = `version: 1
name: %s
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    authenticated:
      type: boolean
counters:
  - name: authenticated_by_olt
    groupBy: [olt]
    where: authenticated = true
    alarms:
      - name: olt_down
        when: "total < 1000"
        sustainedFor: 10s
        notify: [noc]
`

// seedOneServicePerOLT materializa n serviços em n OLTs distintas antes da
// subida, pelo pipeline real, como seedServices.
func seedOneServicePerOLT(t *testing.T, a *env, name string, n int) {
	t.Helper()
	cfg := a.load()
	spec, err := schema.Load(cfg.DatasetSpecPath(name), cfg.QueryDefaults())
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(storage.Options{
		Dir:                 cfg.DatasetDir(name),
		Dataset:             name,
		SegmentSize:         cfg.SegmentSize(),
		IndexedFieldsByName: schema.IndexedFieldNames(spec),
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := spec.Compile(st.FieldDict())
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	mat, err := materializer.New(compiled, st)
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	for i := range n {
		ev := fmt.Sprintf(`{"customerId":"C-%05d","olt":"OLT-%05d","authenticated":true}`, i, i)
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			_ = st.Close()
			t.Fatalf("evento %d: %v", i, r.Err)
		}
	}
	if err := st.Checkpoint(); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// Garantias 6 e 7: milhares de grupos disparando na MESMA passada chegam
// todos ao receptor, sem descarte, e a espera da admissão é publicada pela
// mesma estrutura no /v1/stats e no /metrics, nascendo com zero.
func TestAlarmBurstInOnePassReachesWebhookWithoutDrops(t *testing.T) {
	var mu sync.Mutex
	ids := make(map[string]bool)
	groups := make(map[string]bool)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var event struct {
			ID       string `json:"eventId"`
			Event    string `json:"event"`
			GroupKey string `json:"groupKey"`
		}
		if err := json.Unmarshal(body, &event); err != nil || event.Event != "alarm.firing" {
			t.Errorf("corpo inesperado no receptor: %s (%v)", body, err)
		}
		mu.Lock()
		ids[event.ID] = true
		groups[event.GroupKey] = true
		mu.Unlock()
		w.WriteHeader(204)
	}))
	defer receiver.Close()
	a := newEnv(t)
	cfgBytes, err := os.ReadFile(a.cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfgBytes = append(cfgBytes, []byte(fmt.Sprintf("\nalarmDelivery:\n  webhooks:\n    - name: noc\n      url: %s\n", receiver.URL))...)
	if err := os.WriteFile(a.cfgPath, cfgBytes, 0600); err != nil {
		t.Fatal(err)
	}
	a.writeDataset("burst", fmt.Sprintf(specBurstAlarm, "burst"))
	seedOneServicePerOLT(t, a, "burst", burstGroups)
	at := time.Now().UTC()
	clock := newTestClock(at)
	srv, err := server.Novo(a.load(), server.Options{Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{metrics.LabelDataset: "burst", metrics.LabelDestination: "noc"}
	initial, ok := achaSerie(parseExposition(t, srv.MetricsExpositionForTest()), metrics.MetAlarmDeliveryAdmissionWaitSecondsTotal, labels)
	if !ok || initial.Value != 0 {
		t.Fatalf("a série de espera não nasceu com zero na subida: presente=%v %+v", ok, initial)
	}
	startWebhookServer(t, srv)
	for i := 0; i < 400 && !srv.AlarmsActive("burst"); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if !srv.AlarmsActive("burst") {
		t.Fatal("laço de alarmes não subiu")
	}
	srv.StopSamplingAndAlarmsForTest("burst")
	if err := srv.EvaluateAlarmsOnceForTest("burst"); err != nil {
		t.Fatal(err)
	}
	clock.set(at.Add(11 * time.Second))
	// A passada que dispara os burstGroups alarmes de uma vez.
	passStart := time.Now()
	if err := srv.EvaluateAlarmsOnceForTest("burst"); err != nil {
		t.Fatal(err)
	}
	pass := time.Since(passStart)
	var latest protocol.AlarmDeliveryStats
	deadline := time.Now().Add(2 * time.Minute)
	for {
		for _, d := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
			if d.Name == "burst" && len(d.AlarmDelivery) == 1 {
				latest = d.AlarmDelivery[0]
			}
		}
		for reason, count := range latest.Dropped {
			if count != 0 {
				t.Fatalf("a rajada descartou %d evento(s) por %q: %+v", count, reason, latest)
			}
		}
		if latest.Delivered == burstGroups && latest.DiskEvents == 0 && latest.MemoryEvents == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a rajada não foi entregue no prazo: %+v", latest)
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	if len(ids) != burstGroups || len(groups) != burstGroups {
		t.Errorf("o receptor viu %d eventId(s) e %d grupo(s), esperado %d de cada", len(ids), len(groups), burstGroups)
	}
	mu.Unlock()
	if latest.AdmissionWaitSeconds <= 0 {
		t.Fatalf("a passada não esperou vaga em memória (o gravador acompanhou %d eventos?): a medida de espera não foi exercitada", burstGroups)
	}
	t.Logf("passada com %d disparos: %s, espera acumulada da admissão %.3fs", burstGroups, pass, latest.AdmissionWaitSeconds)
	samples := coletaSamples(t, srv)
	exposed, ok := achaSerie(samples, metrics.MetAlarmDeliveryAdmissionWaitSecondsTotal, labels)
	if !ok || exposed.Value != latest.AdmissionWaitSeconds {
		t.Errorf("/metrics diz %v (presente=%v) e /v1/stats diz %v para a espera: as duas fontes divergem",
			exposed.Value, ok, latest.AdmissionWaitSeconds)
	}
	memoryFull := map[string]string{metrics.LabelDataset: "burst", metrics.LabelDestination: "noc", metrics.LabelReason: alarmdelivery.DropMemoryFull}
	if dropped, ok := achaSerie(samples, metrics.MetAlarmDeliveryDroppedTotal, memoryFull); !ok || dropped.Value != 0 {
		t.Errorf("a série memory_full precisa continuar publicada e em zero: presente=%v %+v", ok, dropped)
	}
	if !strings.Contains(fetch(t, srv, "/v1/stats"), `"admissionWaitSeconds"`) {
		t.Error("o /v1/stats não publica admissionWaitSeconds")
	}
}
