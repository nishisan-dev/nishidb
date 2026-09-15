package countermetrics

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// Os testes vivem no pacote interno: o teste concorrente precisa contar as
// releituras do seqlock (Metric.read), os de ciclo de vida usam os seams de
// teste, e os de ordem de escrita conferem a identidade em disco pelo arquivo
// do próprio amostrador.

// baseMs é um instante alinhado à fronteira de segundo: com a janela de 1m
// (balde de 1 s) cada segundo inteiro somado ao relógio é exatamente uma época.
const baseMs int64 = 1_700_000_000_000

const (
	testDataset = "ds"
	testCounter = "by_olt"
	testMetric  = "auth_1m"
)

type fakeGroup struct {
	key   []byte
	total int64
	live  bool
}

// fakeCounter é o contador em memória. O mutex faz o papel do lock folha do
// dicionário, e a chave devolvida é estável, como a do contador real.
type fakeCounter struct {
	mu sync.Mutex
	// groups é indexado pelo id; o 0 nunca é usado.
	groups []fakeGroup
	// limitOverride, quando não zero, é o Limit devolvido no lugar do real:
	// leva o amostrador a um id que o dicionário real nunca emitiria.
	limitOverride uint32
}

func newFakeCounter() *fakeCounter {
	return &fakeCounter{groups: make([]fakeGroup, 1)}
}

func (f *fakeCounter) Limit() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.limitOverride != 0 {
		return f.limitOverride
	}
	return uint32(len(f.groups))
}

func (f *fakeCounter) Group(id uint32) (key, text []byte, total, refs int64, live bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == 0 || int(id) >= len(f.groups) {
		return nil, nil, 0, 0, false
	}
	g := f.groups[id]
	return g.key, g.key, g.total, 1, g.live
}

func (f *fakeCounter) add(key string, total int64) uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups = append(f.groups, fakeGroup{key: []byte(key), total: total, live: true})
	return uint32(len(f.groups) - 1)
}

func (f *fakeCounter) setTotal(id uint32, total int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups[id].total = total
}

func (f *fakeCounter) setLive(id uint32, live bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups[id].live = live
}

func (f *fakeCounter) setKey(id uint32, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups[id].key = []byte(key)
}

func (f *fakeCounter) setLimitOverride(limit uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limitOverride = limit
}

// addRetired emite n ids de grupos que o amostrador nunca vê vivos: a
// rotatividade do contador sem o custo de uma chave por id.
func (f *fakeCounter) addRetired(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups = append(f.groups, make([]fakeGroup, n)...)
}

// fakeClock é o relógio injetado; atômico porque o laço o lê de outra
// goroutine.
type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(baseMs)
	return c
}

func (c *fakeClock) Now() time.Time { return time.UnixMilli(c.ms.Load()) }

func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// epoch é a época corrente para a janela de 1m (balde de 1000 ms).
func (c *fakeClock) epoch() uint32 { return uint32(c.ms.Load() / 1000) }

// lockedBuffer recebe as linhas de log de mais de uma goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// events devolve as linhas JSON cujo campo event é o pedido.
func (b *lockedBuffer) events(t *testing.T, event string) []map[string]any {
	t.Helper()
	b.mu.Lock()
	raw := bytes.Clone(b.buf.Bytes())
	b.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		var line map[string]any
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatalf("linha de log não é JSON: %v\n%s", err, sc.Text())
		}
		if line["event"] == event {
			out = append(out, line)
		}
	}
	return out
}

func metricSpec(name string, window time.Duration) schema.CounterMetricSpec {
	return schema.CounterMetricSpec{
		Name:       name,
		Type:       schema.MetricTypeWindow,
		Source:     schema.MetricSourceTotal,
		Window:     window,
		Aggregates: []string{schema.AggregateAvg, schema.AggregateMin, schema.AggregateMax},
	}
}

// harness é um dataset com um contador (testCounter) e uma métrica de 1m
// (testMetric), com relógio e log capturados. Reabrir o amostrador sobre o
// mesmo harness é reiniciar o processo sobre o mesmo diretório.
type harness struct {
	dir     string
	counter *fakeCounter
	clock   *fakeClock
	logs    *lockedBuffer
	// maxGroups vai para Spec.MaxGroups: o teto de grupos VIVOS do schema, que
	// o amostrador não pode usar como teto de id.
	maxGroups uint32
}

func newHarness(t *testing.T) *harness {
	return &harness{
		dir:       filepath.Join(t.TempDir(), "counter_metrics"),
		counter:   newFakeCounter(),
		clock:     newFakeClock(),
		logs:      &lockedBuffer{},
		maxGroups: 1000,
	}
}

func (h *harness) options(readOnly bool) Options {
	return Options{
		Dataset: testDataset,
		Dir:     h.dir,
		Counters: []CounterInput{{
			Spec: schema.CounterSpec{
				Name:      testCounter,
				MaxGroups: h.maxGroups,
				Metrics:   []schema.CounterMetricSpec{metricSpec(testMetric, time.Minute)},
			},
			Reader: h.counter,
		}},
		Log:      logging.New(h.logs, logging.FormatJSON, slog.LevelDebug),
		Now:      h.clock.Now,
		ReadOnly: readOnly,
	}
}

func (h *harness) path() string {
	return filepath.Join(h.dir, testCounter, testMetric+fileExtension)
}

func (h *harness) open(t *testing.T) *Sampler {
	t.Helper()
	return openWith(t, h.options(false))
}

func (h *harness) openReadOnly(t *testing.T) *Sampler {
	t.Helper()
	return openWith(t, h.options(true))
}

func openWith(t *testing.T, opts Options) *Sampler {
	t.Helper()
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustMetric(t *testing.T, s *Sampler) *Metric {
	t.Helper()
	m, ok := s.Metric(testCounter, testMetric)
	if !ok {
		t.Fatalf("Metric(%q, %q) não achou a métrica declarada", testCounter, testMetric)
	}
	return m
}

func sampleOnce(t *testing.T, s *Sampler) {
	t.Helper()
	if err := s.SampleOnce(); err != nil {
		t.Fatalf("SampleOnce: %v", err)
	}
}

func closeSampler(t *testing.T, s *Sampler) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// wantAggregate confere o publicado do grupo, e que o avg de um grupo com
// amostra é finito — a garantia que o consumidor documenta como nossa.
func wantAggregate(t *testing.T, m *Metric, id uint32, samples uint64, avg float64, minimum, maximum int64) {
	t.Helper()
	gotSamples, gotAvg, gotMin, gotMax := m.Aggregate(id)
	if gotSamples > 0 && (math.IsNaN(gotAvg) || math.IsInf(gotAvg, 0)) {
		t.Fatalf("grupo %d: avg não finito (%v) com %d amostra(s)", id, gotAvg, gotSamples)
	}
	if gotSamples != samples || gotAvg != avg || gotMin != minimum || gotMax != maximum {
		t.Fatalf("grupo %d: publicado (amostras %d, avg %v, min %d, max %d), esperado (amostras %d, avg %v, min %d, max %d)",
			id, gotSamples, gotAvg, gotMin, gotMax, samples, avg, minimum, maximum)
	}
}

func wantNoValue(t *testing.T, m *Metric, id uint32) {
	t.Helper()
	if samples, avg, minimum, maximum := m.Aggregate(id); samples != 0 {
		t.Fatalf("grupo %d: esperado sem valor, publicado (amostras %d, avg %v, min %d, max %d)", id, samples, avg, minimum, maximum)
	}
}

func touch(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return err == nil
}

// traceLoops liga o seam loopTrace e devolve os laços vivos agora e o máximo
// já observado. Chame ANTES de abrir o amostrador: o Cleanup que desliga o
// seam tem de rodar depois do Close, com nenhum laço de pé.
func traceLoops(t *testing.T) (alive, peak *atomic.Int32) {
	alive, peak = new(atomic.Int32), new(atomic.Int32)
	loopTrace = func(delta int) {
		n := alive.Add(int32(delta))
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				return
			}
		}
	}
	t.Cleanup(func() { loopTrace = nil })
	return alive, peak
}
