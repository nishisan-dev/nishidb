package server_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestServerEmitsRowsToNamedKafkaTopicsAndExposesDeliveryStats(t *testing.T) {
	for _, file := range []bool{false, true} {
		t.Run(fmt.Sprint(file), func(t *testing.T) {
			cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "primary", "backup"))
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			a := newEnvWithStorage(t, blockPadrao, "")
			spec := specServicesWithWindowMetricsAndEmit
			targets := "[primary, backup]"
			if file {
				targets = "[file, primary, backup]"
			} else {
				spec = strings.Replace(spec, "      maxBytes: 1MiB\n      keep: 2\n", "", 1)
			}
			spec += "      target: " + targets + "\n"
			a.writeDataset("servicos", spec)
			applyWithCounters(t, a, "servicos", append(serviceEvents("OLT_A", 2, true), serviceEvents("OLT_B", 1, false)...))
			cfg := a.load()
			configBytes, err := os.ReadFile(a.cfgPath)
			if err != nil {
				t.Fatal(err)
			}
			configBytes = append(configBytes, []byte(fmt.Sprintf("\nseriesDelivery:\n  kafka:\n    - name: primary\n      bootstrapServers: [%q]\n      topic: primary\n    - name: backup\n      bootstrapServers: [%q]\n      topic: backup\n", cluster.ListenAddrs()[0], cluster.ListenAddrs()[0]))...)
			if err := os.WriteFile(a.cfgPath, configBytes, 0600); err != nil {
				t.Fatal(err)
			}
			srv, err := server.Novo(a.load(), server.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = srv.Close() })
			initial := srv.MetricsExpositionForTest()
			labels := `{dataset="servicos",counter="autenticados_por_olt",destination="primary"}`
			if !strings.Contains(initial, metrics.MetSeriesKafkaDeliveredTotal+labels+" 0") {
				t.Fatalf("missing initial zero: %s", initial)
			}
			if !file && strings.Contains(initial, metrics.MetSeriesRowsTotal+"{") {
				t.Fatal("Kafka-only exposed file metrics")
			}
			stop := startWebhookServer(t, srv)
			deadline := time.Now().Add(5 * time.Second)
			var delivery []protocol.SeriesDeliveryStats
			for time.Now().Before(deadline) {
				for _, ds := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
					if ds.Name == "servicos" {
						delivery = ds.SeriesDelivery
					}
				}
				if len(delivery) == 2 && delivery[0].Delivered == 2 && delivery[1].Delivered == 2 {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			srv.StopEmittersForTest("servicos")
			if len(delivery) != 2 || delivery[0].Delivered != 2 || delivery[1].Delivered != 2 {
				t.Fatalf("bad delivery stats: %+v", delivery)
			}
			if !strings.Contains(srv.MetricsExpositionForTest(), metrics.MetSeriesKafkaDeliveredTotal+labels+" 2") {
				t.Fatal("stats and metrics disagree")
			}
			reader, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics("primary", "backup"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rows := map[string]map[string]map[string]any{"primary": {}, "backup": {}}
			for len(rows["primary"])+len(rows["backup"]) < 4 {
				fetches := reader.PollFetches(ctx)
				if err := fetches.Err(); err != nil {
					t.Fatal(err)
				}
				fetches.EachRecord(func(r *kgo.Record) {
					var row map[string]any
					if err := json.Unmarshal(r.Value, &row); err != nil {
						t.Fatal(err)
					}
					if row["dataset"] != "servicos" || row["counter"] != "autenticados_por_olt" {
						t.Fatal(row)
					}
					group := row["groupKey"].(string)
					if string(r.Key) != fmt.Sprintf(`["servicos","autenticados_por_olt",%q]`, group) {
						t.Fatalf("bad key: %s", r.Key)
					}
					if _, ok := row["auth_5m_avg"]; !ok {
						t.Fatal("window column missing")
					}
					rows[r.Topic][group] = row
				})
			}
			if !reflect.DeepEqual(rows["primary"], rows["backup"]) {
				t.Fatal("destinations received different snapshots")
			}
			if rows["primary"]["OLT_B"]["total"] != float64(0) {
				t.Fatal("zero group missing")
			}
			if file {
				for _, line := range seriesLines(t, a, "servicos", "autenticados_por_olt") {
					var row map[string]any
					if err := json.Unmarshal([]byte(line), &row); err != nil {
						t.Fatal(err)
					}
					row["dataset"] = "servicos"
					if !reflect.DeepEqual(row, rows["primary"][row["groupKey"].(string)]) {
						t.Fatalf("file differs: %v", row)
					}
				}
			} else if _, err := os.Stat(filepath.Join(cfg.DatasetRootDir("servicos"), schema.EmitDirName)); !os.IsNotExist(err) {
				t.Fatalf("Kafka-only created series: %v", err)
			}
			stop()
		})
	}
}

func TestSeriesKafkaUsesSharedTLSAndAllSASLMechanisms(t *testing.T) {
	certPath, keyPath := generateCertTLSWithValidade(t, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SERIES_AUTH_USER", "series_user")
	t.Setenv("SERIES_AUTH_PASS", "series_secret")
	for _, mechanism := range []string{"", schema.SASLPlain, schema.SASLScramSHA256, schema.SASLScramSHA512} {
		t.Run(mechanism, func(t *testing.T) {
			opts := []kfake.Opt{kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"), kfake.TLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})}
			security := schema.SecuritySpec{Protocol: schema.ProtocolSSL, CAFile: certPath}
			if mechanism != "" {
				opts = append(opts, kfake.EnableSASL(), kfake.Superuser(mechanism, "series_user", "series_secret"))
				security.Protocol = schema.ProtocolSASLSSL
				security.SASLMechanism = mechanism
				security.UsernameEnv = "SERIES_AUTH_USER"
				security.PasswordEnv = "SERIES_AUTH_PASS"
			}
			cluster, err := kfake.NewCluster(opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			a := newEnvWithStorage(t, blockPadrao, "")
			spec := strings.Replace(specServicesWithWindowMetricsAndEmit, "      maxBytes: 1MiB\n      keep: 2\n", "      target: secure\n", 1)
			a.writeDataset("servicos", spec)
			applyWithCounters(t, a, "servicos", serviceEvents("OLT_A", 1, true))
			cfg := a.load()
			cfg.SeriesDelivery.Kafka = append(cfg.SeriesDelivery.Kafka, config.SeriesKafkaSpec{Name: "secure", BootstrapServers: cluster.ListenAddrs(), Topic: "samples", Security: security})
			srv, err := server.Novo(cfg, server.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = srv.Close() })
			stop := startWebhookServer(t, srv)
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				for _, ds := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
					if ds.Name == "servicos" && len(ds.SeriesDelivery) == 1 && ds.SeriesDelivery[0].Delivered == 1 {
						stop()
						return
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatal("TLS/SASL sample not delivered")
		})
	}
}

func TestFailedSeriesStartupClosesEarlierEmittersAndDatasets(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	spec := strings.Replace(specServicesWithWindowMetricsAndEmit, "      keep: 2\n", "      keep: 2\n      target: [file, samples]\n  - name: second\n    groupBy: [olt]\n    emit:\n      every: 30s\n", 1)
	a.writeDataset("servicos", spec)
	cfg := a.load()
	cfg.SeriesDelivery.Kafka = []config.SeriesKafkaSpec{{Name: "samples", BootstrapServers: []string{"127.0.0.1:1"}, Topic: "samples"}}
	root := filepath.Join(cfg.DatasetRootDir("servicos"), schema.EmitDirName)
	bad := filepath.Join(root, "second.ndjson")
	if err := os.MkdirAll(bad, 0755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if srv, err := server.Novo(cfg, server.Options{}); err == nil {
			_ = srv.Close()
			t.Fatal("unwritable file accepted")
		}
		// Linux lets a second writer open a still-open series file. Inspecting
		// descriptors catches leaks that reopening the dataset alone misses.
		if fds, err := os.ReadDir("/proc/self/fd"); err == nil {
			for _, fd := range fds {
				if target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name())); err == nil && strings.HasPrefix(target, root+string(os.PathSeparator)) {
					t.Fatalf("failed startup leaked %s", target)
				}
			}
		}
	}
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("failed startup retained dataset lock: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
}
