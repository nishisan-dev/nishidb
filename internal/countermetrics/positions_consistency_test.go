package countermetrics

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

const secondTestMetric = "auth_2m"

// twoMetricOptions é o harness com uma segunda métrica (2m) no mesmo contador:
// as duas usam a mesma posição de cada grupo, cada uma no seu arquivo.
func twoMetricOptions(h *harness, readOnly bool) Options {
	o := h.options(readOnly)
	o.Counters[0].Spec.Metrics = append(o.Counters[0].Spec.Metrics, metricSpec(secondTestMetric, 2*time.Minute))
	return o
}

// seedFile escreve à mão um arquivo de métrica com as identidades dadas e, em
// cada posição, um balde fechado de 3 amostras de valor value na época
// seguinte à do relógio. Source 1 é o código de disco da fonte total.
func seedFile(t *testing.T, h *harness, path string, widthMs int64, identities map[uint32]uint64, value int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, _, err := metricwindow.Open(path, metricwindow.Header{BucketCount: 60, BucketWidthMs: widthMs, Source: 1}, 0, fileCapacityLimit)
	if err != nil {
		t.Fatal(err)
	}
	epoch := uint32(h.clock.ms.Load() / widthMs)
	for position, kh := range identities {
		f.SetIdentity(position, metricwindow.Identity{KeyHash: kh, ValidFromEpoch: uint64(epoch)})
		f.WriteBucket((epoch+1)%60, position, metricwindow.Bucket{
			Epoch: epoch + 1, Owner: metricwindow.OwnerTag(kh), Count: 3, Sum: 3 * value, Min: value, Max: value,
		})
	}
	if err := f.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// fileSnapshot guarda bytes e mtime de um arquivo para provar que nada o
// escreveu.
type fileSnapshot struct {
	content []byte
	modTime time.Time
}

func snapshotFile(t *testing.T, path string) fileSnapshot {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fileSnapshot{content: content, modTime: info.ModTime()}
}

func wantFileUnchanged(t *testing.T, path string, before fileSnapshot) {
	t.Helper()
	after := snapshotFile(t, path)
	if !bytes.Equal(before.content, after.content) {
		t.Errorf("%s: os bytes mudaram; o modo leitura nunca escreve", filepath.Base(path))
	}
	if !before.modTime.Equal(after.modTime) {
		t.Errorf("%s: o mtime mudou (%v → %v); o modo leitura nunca escreve", filepath.Base(path), before.modTime, after.modTime)
	}
}

// A restauração em modo leitura (CLI local) libera posições sem dono e com
// hash duplicado sem escrever nada: a região é somente-leitura, e uma escrita
// ali derrubaria a CLI em pânico em vez de responder a consulta.
func TestReadOnlyRestoreNeverWritesWhileFreeingPositions(t *testing.T) {
	t.Run("unclaimed identity", func(t *testing.T) {
		h := newHarness(t)
		a := h.counter.add("A", 10)
		b := h.counter.add("B", 20)
		s := h.open(t)
		sampleOnce(t, s)
		h.clock.advance(time.Second)
		sampleOnce(t, s)
		closeSampler(t, s)
		// B se aposenta sem que o amostrador veja: a identidade dele fica em
		// disco sem dono vivo.
		h.counter.setLive(b, false)
		before := snapshotFile(t, h.path())

		h.clock.advance(time.Second)
		ro := h.openReadOnly(t)
		m := mustMetric(t, ro)
		wantAggregate(t, m, a, 2, 10, 10, 10)
		wantNoValue(t, m, b)
		if !slices.Equal(m.state.free, []uint32{2}) {
			t.Errorf("lista livre = %v, esperado [2] (a posição de B)", m.state.free)
		}
		closeSampler(t, ro)
		wantFileUnchanged(t, h.path(), before)
	})

	t.Run("duplicated hash", func(t *testing.T) {
		h := newHarness(t)
		a := h.counter.add("A", 10)
		ha := keyHash([]byte("A"))
		seedFile(t, h, h.path(), 1000, map[uint32]uint64{1: ha, 2: ha}, 7)
		before := snapshotFile(t, h.path())

		h.clock.advance(4 * time.Second)
		ro := h.openReadOnly(t)
		m := mustMetric(t, ro)
		wantNoValue(t, m, a)
		if !slices.Equal(m.state.free, []uint32{2, 1}) {
			t.Errorf("lista livre = %v, esperado [2 1] (as duas posições do hash duplicado)", m.state.free)
		}
		closeSampler(t, ro)
		wantFileUnchanged(t, h.path(), before)
	})
}

// Uma falha de infra no acquire (aqui, o arquivo da SEGUNDA métrica fechado)
// deixa lista livre, próxima posição, posições ocupadas e mapa como estavam,
// nos dois ramos: posição nova e reuso da lista livre.
func TestAcquireFailureLeavesTheFreeListTheNextPositionAndTheMapUnchanged(t *testing.T) {
	for _, c := range []struct {
		name     string
		fromFree bool
	}{
		{name: "new position", fromFree: false},
		{name: "reused position", fromFree: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.counter.add("A", 1)
			x := h.counter.add("X", 3)
			s := openWith(t, twoMetricOptions(h, false))
			sampleOnce(t, s)
			if c.fromFree {
				h.counter.setLive(x, false)
				sampleOnce(t, s)
			}
			first, _ := s.Metric(testCounter, testMetric)
			second, _ := s.Metric(testCounter, secondTestMetric)
			state := first.state
			if c.fromFree && len(state.free) != 1 {
				t.Fatalf("lista livre = %v antes da falha, esperado uma posição para o ramo de reuso", state.free)
			}
			freeBefore := slices.Clone(state.free)
			nextBefore, occupiedBefore := state.next, state.occupied.Load()

			b := h.counter.add("B", 2)
			if err := second.file.Close(); err != nil {
				t.Fatal(err)
			}
			// Mesma época: os grupos já vistos não tocam o arquivo; só o acquire de
			// B pede capacidade, e a segunda métrica recusa.
			if err := s.SampleOnce(); !errors.Is(err, metricwindow.ErrClosed) {
				t.Fatalf("SampleOnce devolveu %v, esperado ErrClosed", err)
			}
			if !slices.Equal(state.free, freeBefore) {
				t.Errorf("lista livre = %v depois da falha, antes %v", state.free, freeBefore)
			}
			if state.next != nextBefore {
				t.Errorf("próxima posição = %d depois da falha, antes %d", state.next, nextBefore)
			}
			if got := state.occupied.Load(); got != occupiedBefore {
				t.Errorf("posições ocupadas = %d depois da falha, antes %d", got, occupiedBefore)
			}
			if got := state.positions.load(b); got != 0 {
				t.Errorf("B ficou com a posição %d depois da falha, esperado sem posição", got)
			}
			if samples, _, _, _ := first.Aggregate(b); samples != 0 {
				t.Errorf("B publicou %d amostra(s) na primeira métrica depois da falha no acquire", samples)
			}
		})
	}
}

// Métricas do mesmo contador que discordam do dono de uma posição a
// invalidam: a posição vai para a lista livre (zerada nos dois arquivos só no
// modo escrita), o grupo que concorda em outra posição a mantém, e o grupo sem
// posição recomeça do zero.
func TestMetricsDisagreeingOnAPositionInvalidateIt(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		name := "write"
		if readOnly {
			name = "read-only"
		}
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			a := h.counter.add("A", 10)
			b := h.counter.add("B", 20)
			ha, hb := keyHash([]byte("A")), keyHash([]byte("B"))
			firstPath := h.path()
			secondPath := filepath.Join(h.dir, testCounter, secondTestMetric+fileExtension)
			// Posição 1: A num arquivo, B no outro. Posição 2: B nos dois.
			seedFile(t, h, firstPath, 1000, map[uint32]uint64{1: ha, 2: hb}, 7)
			seedFile(t, h, secondPath, 2000, map[uint32]uint64{1: hb, 2: hb}, 8)
			firstBefore := snapshotFile(t, firstPath)
			secondBefore := snapshotFile(t, secondPath)

			h.clock.advance(4 * time.Second)
			s := openWith(t, twoMetricOptions(h, readOnly))
			first, _ := s.Metric(testCounter, testMetric)
			second, _ := s.Metric(testCounter, secondTestMetric)
			for _, m := range []*Metric{first, second} {
				if samples, _, _, _ := m.Aggregate(a); samples != 0 {
					t.Errorf("%s: A restaurado com %d amostra(s) de uma posição em que as métricas discordam", m.name, samples)
				}
			}
			wantAggregate(t, first, b, 3, 7, 7, 7)
			wantAggregate(t, second, b, 3, 8, 8, 8)
			if got := first.state.positions.load(b); got != 2 {
				t.Errorf("B ficou na posição %d, esperado 2 (a que as métricas concordam)", got)
			}
			if !slices.Equal(first.state.free, []uint32{1}) {
				t.Errorf("lista livre = %v, esperado [1] (a posição em conflito)", first.state.free)
			}

			if readOnly {
				closeSampler(t, s)
				wantFileUnchanged(t, firstPath, firstBefore)
				wantFileUnchanged(t, secondPath, secondBefore)
				return
			}
			for _, m := range []*Metric{first, second} {
				if got := m.file.Identity(1); got != (metricwindow.Identity{}) {
					t.Errorf("%s: identidade da posição em conflito = %+v, esperada zerada", m.name, got)
				}
			}
			sampleOnce(t, s)
			if got := first.state.positions.load(a); got != 1 {
				t.Errorf("A ficou na posição %d, esperado 1 (a liberada)", got)
			}
			wantAggregate(t, first, a, 1, 10, 10, 10)
			wantAggregate(t, second, a, 1, 10, 10, 10)
		})
	}
}
