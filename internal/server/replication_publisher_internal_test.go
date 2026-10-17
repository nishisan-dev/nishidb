package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// O seguidor não tem feed, e por isso nunca ganha publicador — nem com
// `targets` na spec, que o parse já recusa no seguidor e que aqui é forçado
// para provar a guarda da montagem, e não a do parse.
func TestFollowerDatasetNeverGetsAReplicationPublisher(t *testing.T) {
	follower := &dataset{
		nome: "replica",
		spec: &schema.DatasetSpec{Name: "replica", Replication: schema.ReplicationSpec{
			Enabled: true, Role: "follower", Targets: []string{"cdc"},
		}},
	}
	s := &Server{
		cfg:      &config.Config{KafkaTargets: []config.KafkaTargetSpec{{Name: "cdc", BootstrapServers: []string{"127.0.0.1:1"}, Topic: "cdc"}}},
		log:      logging.Discard(),
		datasets: []*dataset{follower},
	}
	if err := s.openReplicationPublishers(logging.Discard()); err != nil {
		t.Fatalf("a montagem recusou um seguidor, em vez de pulá-lo: %v", err)
	}
	if len(follower.publishers) != 0 {
		t.Fatalf("o seguidor ganhou %d publicador(es)", len(follower.publishers))
	}
	if st := replicationStats(follower); st != nil {
		t.Fatalf("o seguidor publicou o bloco replication: %+v", st)
	}
}

// As séries saem do MESMO retrato do /v1/stats, e as de motivo e de estado
// saem para TODOS os valores das listas fechadas — inclusive os que o mapa do
// retrato não trouxer —, com o estado em one-hot.
func TestReplicationMetricsCoverTheClosedListsFromTheSharedStats(t *testing.T) {
	st := &protocol.ReplicationStats{
		Feed: protocol.ReplicationFeedStats{InputEntries: 3, OutputEntries: 5, BufferedBytes: 7, EvictedBatches: 11, Invalidations: 13,
			DiskBytes: 31, DiskSegments: 37, DiskErrors: 41},
		Delivery: []protocol.ReplicationDeliveryStats{{
			Destination: "cdc", State: replicationkafka.StateRetrying, LagSequences: 17, Delivered: 19, LastSuccessTimestamp: 23,
			LagBytes: 43, CheckpointCursor: 47,
			// Mapas incompletos de propósito: a série do motivo ausente
			// tem de sair em zero assim mesmo.
			Snapshots: map[string]uint64{"startup": 2},
			Errors:    map[string]uint64{replicationkafka.ErrorTimeout: 29},
		}},
	}
	wantFeed := map[string]float64{
		metrics.MetReplicationFeedInputEntriesTotal:   3,
		metrics.MetReplicationFeedOutputEntriesTotal:  5,
		metrics.MetReplicationFeedBufferedBytes:       7,
		metrics.MetReplicationFeedEvictedBatchesTotal: 11,
		metrics.MetReplicationFeedInvalidationsTotal:  13,
		metrics.MetReplicationFeedDiskBytes:           31,
		metrics.MetReplicationFeedDiskSegments:        37,
		metrics.MetReplicationFeedDiskErrorsTotal:     41,
	}
	wantDelivery := map[string]float64{
		metrics.MetReplicationKafkaDeliveredTotal:       19,
		metrics.MetReplicationKafkaLagSequences:         17,
		metrics.MetReplicationKafkaLastSuccessTimestamp: 23,
		metrics.MetReplicationKafkaLagBytes:             43,
		metrics.MetReplicationKafkaCheckpointSequence:   47,
	}
	// O tipo de cada família: counter só sobe, gauge sobe e desce, e o
	// Prometheus calcula rate() errado sobre o tipo trocado.
	wantType := map[string]metrics.Type{
		metrics.MetReplicationFeedInputEntriesTotal:     metrics.TypeCounter,
		metrics.MetReplicationFeedOutputEntriesTotal:    metrics.TypeCounter,
		metrics.MetReplicationFeedBufferedBytes:         metrics.TypeGauge,
		metrics.MetReplicationFeedEvictedBatchesTotal:   metrics.TypeCounter,
		metrics.MetReplicationFeedInvalidationsTotal:    metrics.TypeCounter,
		metrics.MetReplicationKafkaDeliveredTotal:       metrics.TypeCounter,
		metrics.MetReplicationKafkaLagSequences:         metrics.TypeGauge,
		metrics.MetReplicationKafkaSnapshotsTotal:       metrics.TypeCounter,
		metrics.MetReplicationKafkaErrorsTotal:          metrics.TypeCounter,
		metrics.MetReplicationKafkaState:                metrics.TypeGauge,
		metrics.MetReplicationKafkaLastSuccessTimestamp: metrics.TypeGauge,
		metrics.MetReplicationFeedDiskBytes:             metrics.TypeGauge,
		metrics.MetReplicationFeedDiskSegments:          metrics.TypeGauge,
		metrics.MetReplicationFeedDiskErrorsTotal:       metrics.TypeCounter,
		metrics.MetReplicationKafkaLagBytes:             metrics.TypeGauge,
		metrics.MetReplicationKafkaCheckpointSequence:   metrics.TypeGauge,
	}
	seenFamilies := map[string]bool{}
	snapshots := map[string]float64{}
	errs := map[string]float64{}
	state := map[string]float64{}
	for _, s := range samplesOfReplication("events", st) {
		if s.Family == "" || s.Family != s.Name {
			t.Fatalf("amostra sem família ou com nome de outra família: %+v", s)
		}
		if want, ok := wantType[s.Family]; !ok || s.Type != want {
			t.Errorf("%s com tipo %v, quero %v", s.Family, s.Type, want)
		}
		seenFamilies[s.Family] = true
		if s.Labels[0] != (metrics.Label{Name: metrics.LabelDataset, Value: "events"}) {
			t.Fatalf("rótulo de dataset errado: %+v", s)
		}
		if want, ok := wantFeed[s.Name]; ok {
			if len(s.Labels) != 1 || s.Value != want {
				t.Errorf("%s = %v %+v, quero %v só com dataset", s.Name, s.Value, s.Labels, want)
			}
			delete(wantFeed, s.Name)
			continue
		}
		if s.Labels[1] != (metrics.Label{Name: metrics.LabelDestination, Value: "cdc"}) {
			t.Fatalf("rótulo de destino errado: %+v", s)
		}
		switch s.Name {
		case metrics.MetReplicationKafkaSnapshotsTotal:
			snapshots[labelValue(t, s, metrics.LabelReason)] = s.Value
		case metrics.MetReplicationKafkaErrorsTotal:
			errs[labelValue(t, s, metrics.LabelReason)] = s.Value
		case metrics.MetReplicationKafkaState:
			state[labelValue(t, s, metrics.LabelPublisherState)] = s.Value
		default:
			want, ok := wantDelivery[s.Name]
			if !ok || s.Value != want {
				t.Errorf("%s = %v, inesperada ou com valor errado", s.Name, s.Value)
			}
			delete(wantDelivery, s.Name)
		}
	}
	if len(seenFamilies) != len(wantType) {
		t.Errorf("famílias publicadas %v, quero as %d de %v", seenFamilies, len(wantType), wantType)
	}
	if len(wantFeed) != 0 || len(wantDelivery) != 0 {
		t.Fatalf("séries ausentes: feed %v, entrega %v", wantFeed, wantDelivery)
	}
	for _, reason := range replicationkafka.SnapshotReasons() {
		want := float64(st.Delivery[0].Snapshots[reason])
		if got, ok := snapshots[reason]; !ok || got != want {
			t.Errorf("snapshots{reason=%q} = %v (presente: %v), quero %v", reason, got, ok, want)
		}
	}
	for _, reason := range replicationkafka.ErrorReasons() {
		want := float64(st.Delivery[0].Errors[reason])
		if got, ok := errs[reason]; !ok || got != want {
			t.Errorf("errors{reason=%q} = %v (presente: %v), quero %v", reason, got, ok, want)
		}
	}
	if len(snapshots) != len(replicationkafka.SnapshotReasons()) || len(errs) != len(replicationkafka.ErrorReasons()) {
		t.Errorf("motivos fora da lista fechada: snapshots %v, errors %v", snapshots, errs)
	}
	if len(state) != len(replicationkafka.States()) {
		t.Fatalf("estados = %v, quero a lista fechada inteira", state)
	}
	for name, v := range state {
		want := 0.0
		if name == replicationkafka.StateRetrying {
			want = 1
		}
		if v != want {
			t.Errorf("state{state=%q} = %v, quero %v", name, v, want)
		}
	}
}

// O inventário de docs/metricas.md lista, na linha de cada série de motivo e
// de estado, TODOS os valores da lista fechada do publicador. As listas são
// lidas do código: o motivo novo que não entrar no documento reprova aqui.
func TestMetricsDocListsEveryReplicationReasonAndState(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "metricas.md"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(body), "\n")
	for family, values := range map[string][]string{
		metrics.MetReplicationKafkaSnapshotsTotal: replicationkafka.SnapshotReasons(),
		metrics.MetReplicationKafkaErrorsTotal:    replicationkafka.ErrorReasons(),
		metrics.MetReplicationKafkaState:          replicationkafka.States(),
	} {
		var row string
		for _, line := range lines {
			if strings.HasPrefix(line, "| `"+family+"`") {
				row = line
				break
			}
		}
		if row == "" {
			t.Errorf("docs/metricas.md não tem a linha da tabela de %s", family)
			continue
		}
		for _, v := range values {
			if !strings.Contains(row, "`"+v+"`") {
				t.Errorf("a linha de %s em docs/metricas.md não lista o valor %q:\n%s", family, v, row)
			}
		}
	}
}

// O bloco `feed` do /v1/stats (e por ele as séries do feed em disco) leva o
// tamanho do log e as falhas de disco do feed. O teste força as duas: o
// diretório sem escrita faz o gravador falhar e tentar de novo (cada tentativa
// conta), e devolvida a escrita o lote vai ao disco. Um campo esquecido no
// replicationStats sairia zero aqui, e no teste de subida o zero é o valor
// certo, que não discrimina.
func TestReplicationStatsCarriesTheFeedDiskFigures(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root escreve em diretório sem permissão")
	}
	dir := filepath.Join(t.TempDir(), "feed")
	feed, err := replication.Open(dir, replication.Options{MaxMessages: 1, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 3 << 20, MaxDiskBytes: 8 << 20, Logger: logging.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Resume(replication.Fingerprint{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o755)
		_ = feed.Close(nil, 5*time.Second)
		feed.Wait()
	})
	d := &dataset{nome: "events", replication: feed}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	feed.Publish("events", replication.Insert, types.Record{RowID: 1, Payload: []byte("a")}, time.Unix(1, 0))
	waitUntil(t, "a falha de disco do feed", func() bool { return feed.Status().DiskErrors > 0 })
	if got, after := replicationStats(d).Feed.DiskErrors, feed.Status().DiskErrors; got == 0 || got > after {
		t.Fatalf("diskErrors = %d no /v1/stats, com o feed em %d: o campo não sai do retrato do feed", got, after)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "o lote no disco", func() bool { return feed.Status().DiskSegments > 0 })
	st, got := feed.Status(), replicationStats(d).Feed
	if got.DiskBytes == 0 || got.DiskBytes != st.DiskBytes || got.DiskSegments != st.DiskSegments {
		t.Fatalf("diskBytes/diskSegments = %d/%d no /v1/stats, %d/%d no feed", got.DiskBytes, got.DiskSegments, st.DiskBytes, st.DiskSegments)
	}
}

// waitUntil espera cond por até 10 s.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s não aconteceu em 10 s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func labelValue(t *testing.T, s metrics.Sample, name string) string {
	t.Helper()
	if len(s.Labels) != 3 || s.Labels[2].Name != name {
		t.Fatalf("%s sem o rótulo %q na terceira posição: %+v", s.Name, name, s.Labels)
	}
	return s.Labels[2].Value
}

func TestReplicationTargetsKeepAlarmKafkaEnvelopesOffTheCDCTopic(t *testing.T) {
	for _, tc := range []struct {
		name, broker, topic string
		used, reject        bool
	}{
		{"same topic and cluster", "broker:9092", "cdc", true, true},
		{"another cluster", "other:9092", "cdc", true, false},
		{"another topic", "broker:9092", "alarms", true, false},
		{"unused destination", "broker:9092", "cdc", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				KafkaTargets:  []config.KafkaTargetSpec{{Name: "cdc", BootstrapServers: []string{"Broker:9092"}, Topic: "cdc"}},
				AlarmDelivery: config.AlarmDeliverySpec{Kafka: []config.AlarmKafkaSpec{{Name: "notify", BootstrapServers: []string{tc.broker}, Topic: tc.topic}}},
			}
			specs := map[string]*schema.DatasetSpec{
				"events":   {Replication: schema.ReplicationSpec{Enabled: true, Role: "master", Targets: []string{"cdc"}}},
				"services": {},
			}
			if tc.used {
				specs["services"].Counters = []schema.CounterSpec{{Name: "c", Alarms: []schema.AlarmSpec{{Name: "a", Notify: []string{"notify"}}}}}
			}
			err := validateReplicationTargets(cfg, specs)
			if tc.reject {
				if err == nil || !strings.Contains(err.Error(), `alarm Kafka target "notify"`) {
					t.Fatalf("expected alarm topic collision, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
