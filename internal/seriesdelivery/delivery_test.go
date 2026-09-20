package seriesdelivery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type heldRecord struct {
	record *kgo.Record
	done   func(*kgo.Record, error)
}
type heldProducer struct{ records []heldRecord }

func (p *heldProducer) TryProduce(_ context.Context, r *kgo.Record, done func(*kgo.Record, error)) {
	p.records = append(p.records, heldRecord{r, done})
}
func (p *heldProducer) Close() {
	for _, r := range p.records {
		r.done(r.record, kgo.ErrClientClosed)
	}
	p.records = nil
}

func testOutput(p producer) *output {
	ctx, cancel := context.WithCancel(context.Background())
	return &output{d: &destination{client: p, ctx: ctx, cancel: cancel, timeout: time.Minute}, stats: protocol.SeriesDeliveryStats{Dropped: make(map[string]uint64)}}
}

func TestCapacityIncludesInflightRecordsAndCopiesOnlyAdmittedSamples(t *testing.T) {
	for _, byteLimit := range []bool{false, true} {
		t.Run(fmt.Sprint(byteLimit), func(t *testing.T) {
			p := &heldProducer{}
			o := testOutput(p)
			defer o.d.cancel()
			key, body := []byte("k"), []byte("v")
			want := MaxRecords
			if byteLimit {
				body = []byte(strings.Repeat("x", 900_000))
				want = MaxBytes / (len(body) + len(key))
			}
			for i := 0; i < want+2; i++ {
				o.Publish(key, body, time.Now())
			}
			body[0], key[0] = 'z', 'z'
			s := o.snapshot()
			if len(p.records) != want || s.PendingRecords != int64(want) || s.PendingBytes > MaxBytes || s.Dropped["buffer_full"] != 2 {
				t.Fatalf("admission escaped limits: %+v records=%d", s, len(p.records))
			}
			if p.records[0].record.Key[0] != 'k' || p.records[0].record.Value[0] == 'z' {
				t.Fatal("emitter scratch retained")
			}
			p.Close()
			s = o.snapshot()
			if s.PendingBytes != 0 || s.PendingRecords != 0 || s.Dropped["shutdown"] != uint64(want) {
				t.Fatalf("capacity leak: %+v", s)
			}
		})
	}
}

func TestOversizedAndCompletedRecordsHaveSingleOutcome(t *testing.T) {
	p := &heldProducer{}
	o := testOutput(p)
	defer o.d.cancel()
	o.Publish(nil, make([]byte, maxRecordBytes+1), time.Now())
	if len(p.records) != 0 || o.snapshot().Dropped["message_too_large"] != 1 {
		t.Fatal("oversized record admitted")
	}
	for _, err := range []error{nil, context.DeadlineExceeded, kerr.TopicAuthorizationFailed, kerr.MessageTooLarge} {
		o.Publish([]byte("key"), []byte("value"), time.Now())
		r := p.records[len(p.records)-1]
		r.done(r.record, err)
	}
	s := o.snapshot()
	if s.Delivered != 1 || s.LastSuccessTimestamp == 0 || s.PendingRecords != 0 || s.PendingBytes != 0 || s.Dropped["timeout"] != 1 || s.Dropped["publish_error"] != 1 || s.Dropped["message_too_large"] != 2 {
		t.Fatalf("bad outcomes: %+v", s)
	}
}

func deliveryConfig(brokers []string, targets ...string) (*config.Config, map[string]*schema.DatasetSpec) {
	cfg := &config.Config{}
	for _, target := range targets {
		cfg.SeriesDelivery.Kafka = append(cfg.SeriesDelivery.Kafka, config.SeriesKafkaSpec{Name: target, BootstrapServers: brokers, Topic: target, Timeout: "500ms"})
	}
	specs := map[string]*schema.DatasetSpec{"d": {Name: "d", Counters: []schema.CounterSpec{{Name: "c", Emit: &schema.EmitSpec{Targets: targets}}}}}
	return cfg, specs
}

func waitStats(t *testing.T, m *Manager, match func([]protocol.SeriesDeliveryStats) bool) []protocol.SeriesDeliveryStats {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		s := m.Stats("d")
		if match(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("delivery did not settle: %+v", m.Stats("d"))
	return nil
}

func TestKafkaDestinationsConfirmIndependentlyAndPublishOneRecordPerSample(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.AllowAutoTopicCreation(), kfake.SeedTopics(3, "first", "second"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cfg, specs := deliveryConfig(c.ListenAddrs(), "first", "second", "absent")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, out := range m.Outputs("d", "c") {
		for i := 0; i < 3; i++ {
			out.Publish([]byte("same-key"), []byte(fmt.Sprintf(`{"total":%d}`, i)), time.Now())
		}
	}
	waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool {
		return s[0].Delivered == 3 && s[1].Delivered == 3 && s[2].PendingRecords == 0 && s[2].Dropped["publish_error"]+s[2].Dropped["timeout"] == 3
	})
	reader, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics("first", "second"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	counts := map[string]int{}
	partitions := map[string]int32{}
	for counts["first"]+counts["second"] < 6 {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Value) != fmt.Sprintf(`{"total":%d}`, counts[r.Topic]) || string(r.Key) != "same-key" {
				t.Errorf("bad order or body: %+v", r)
			}
			if counts[r.Topic] > 0 && partitions[r.Topic] != r.Partition {
				t.Error("stable key changed partition")
			}
			counts[r.Topic]++
			partitions[r.Topic] = r.Partition
		})
	}
}

func TestPublishFailuresAreNotRetriedAndNextSampleRecovers(t *testing.T) {
	for _, code := range []int16{kerr.NotEnoughReplicas.Code, kerr.RequestTimedOut.Code, kerr.NotEnoughReplicasAfterAppend.Code, -1} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var calls atomic.Int64
			var healthy atomic.Bool
			c.ControlKey(0, func(req kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				calls.Add(1)
				if req.(*kmsg.ProduceRequest).Acks != -1 {
					t.Error("producer did not request acknowledgements from all in-sync replicas")
				}
				if healthy.Load() {
					return nil, nil, false
				}
				if code == -1 {
					return nil, errors.New("connection lost"), true
				}
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
				return resp, nil, true
			})
			cfg, specs := deliveryConfig(c.ListenAddrs(), "samples")
			m, err := Open(cfg, specs, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			out := m.Outputs("d", "c")[0]
			out.Publish([]byte("k"), []byte("old"), time.Now())
			waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool {
				return s[0].PendingRecords == 0 && s[0].Delivered == 0 && s[0].Dropped["publish_error"]+s[0].Dropped["timeout"] == 1
			})
			if calls.Load() != 1 {
				t.Fatalf("retried failed sample %d times", calls.Load())
			}
			healthy.Store(true)
			out.Publish([]byte("k"), []byte("new"), time.Now())
			waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool { return s[0].Delivered == 1 })
			if calls.Load() != 2 {
				t.Fatalf("old sample replayed: %d requests", calls.Load())
			}
		})
	}
}

func TestUnavailableBrokerDoesNotPreventStartupOrBoundedShutdown(t *testing.T) {
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := m.Outputs("d", "c")[0]
	var offers sync.WaitGroup
	for i := 0; i < 4; i++ {
		offers.Add(1)
		go func() {
			defer offers.Done()
			for j := 0; j < 100; j++ {
				out.Publish([]byte("k"), []byte("v"), time.Now())
			}
		}()
	}
	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown waiting for Kafka")
	}
	offers.Wait()
	m.Close()
	s := m.Stats("d")[0]
	var dropped uint64
	for _, n := range s.Dropped {
		dropped += n
	}
	if dropped != 400 || s.PendingRecords != 0 || s.PendingBytes != 0 {
		t.Fatalf("shutdown lost accounting: %+v", s)
	}
}

func TestSlowBrokerExpiresInflightSampleWithoutResending(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "samples"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	release := make(chan struct{})
	defer close(release)
	var requests atomic.Int64
	c.ControlKey(0, func(_ kmsg.Request) (kmsg.Response, error, bool) {
		requests.Add(1)
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	cfg, specs := deliveryConfig(c.ListenAddrs(), "samples")
	m, err := Open(cfg, specs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	started := time.Now()
	m.Outputs("d", "c")[0].Publish([]byte("key"), []byte("sample"), started)
	waitStats(t, m, func(s []protocol.SeriesDeliveryStats) bool {
		return s[0].PendingRecords == 0 && s[0].Dropped["timeout"] == 1
	})
	if time.Since(started) > 3*time.Second || requests.Load() != 1 {
		t.Fatalf("timeout/retry contract broken: duration=%s requests=%d", time.Since(started), requests.Load())
	}
}

func TestStartupRejectsUnknownReferencesAndInvalidLocalCredentials(t *testing.T) {
	cfg, specs := deliveryConfig([]string{"127.0.0.1:1"}, "samples")
	specs["d"].Counters[0].Emit.Targets = []string{"unknown"}
	if m, err := Open(cfg, specs, nil); err == nil {
		m.Close()
		t.Fatal("unknown reference accepted")
	}
	specs["d"].Counters[0].Emit.Targets = []string{"samples"}
	t.Setenv("SERIES_TEST_USER", "user")
	t.Setenv("SERIES_TEST_PASS", "")
	cfg.SeriesDelivery.Kafka[0].Security = schema.SecuritySpec{Protocol: schema.ProtocolSASLSSL, SASLMechanism: schema.SASLPlain, UsernameEnv: "SERIES_TEST_USER", PasswordEnv: "SERIES_TEST_PASS"}
	if m, err := Open(cfg, specs, nil); err == nil {
		m.Close()
		t.Fatal("missing credential accepted")
	}
	cfg.SeriesDelivery.Kafka[0].Security = schema.SecuritySpec{Protocol: schema.ProtocolSSL, CAFile: "/nonexistent/ca.pem"}
	if m, err := Open(cfg, specs, nil); err == nil {
		m.Close()
		t.Fatal("invalid CA accepted")
	}
}
