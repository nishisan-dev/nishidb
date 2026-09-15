package countermetrics

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

// Rotatividade: 10 grupos vivos ao mesmo tempo e mais de 100.000 ids emitidos
// ao longo das passadas. Arquivo e entradas em heap ficam no tamanho dos 10
// vivos (piso de capacidade e um bloco); só o mapa id → posição cresce com os
// ids, a 4 B cada.
func TestRotationKeepsFileAndHeapAtThePeakOfLiveGroups(t *testing.T) {
	h := newHarness(t)
	const (
		live       = 10
		passes     = 110
		idsPerPass = 910
	)
	s := h.open(t)
	m := mustMetric(t, s)
	var current []uint32
	for p := range passes {
		for _, id := range current {
			h.counter.setLive(id, false)
		}
		h.counter.addRetired(idsPerPass - live)
		current = current[:0]
		for i := range live {
			current = append(current, h.counter.add(fmt.Sprintf("G%d_%d", p, i), int64(p*100+i)))
		}
		sampleOnce(t, s)
		h.clock.advance(time.Second)
	}

	emitted := h.counter.Limit() - 1
	if emitted != passes*idsPerPass {
		t.Fatalf("%d ids emitidos, esperado %d", emitted, passes*idsPerPass)
	}
	st := s.Stats()[0]
	t.Logf("ids emitidos %d, capacidade %d, blocos de entradas %d, heap %d B, mapa %d B",
		emitted, m.file.Capacity(), len(*m.chunks.Load()), st.HeapBytes, st.PositionMapBytes)
	if got := m.file.Capacity(); got != 1024 {
		t.Errorf("capacidade do arquivo = %d depois de %d ids com %d vivos, esperado o piso 1024", got, emitted, live)
	}
	if got := len(*m.chunks.Load()); got != 1 {
		t.Errorf("%d blocos de entradas em heap, esperado 1", got)
	}
	if st.Groups != live {
		t.Errorf("Groups = %d, esperado %d", st.Groups, live)
	}
	if want := int64(chunkSize) * entryBytes; st.HeapBytes != want {
		t.Errorf("HeapBytes = %d, esperado um bloco (%d)", st.HeapBytes, want)
	}
	mapChunks := (int64(emitted) + 1 + positionMapChunkSize - 1) / positionMapChunkSize
	if want := mapChunks * positionMapChunkSize * positionBytes; st.PositionMapBytes != want {
		t.Errorf("PositionMapBytes = %d, esperado %d (4 B por id)", st.PositionMapBytes, want)
	}
	for i, id := range current {
		v := int64((passes-1)*100 + i)
		wantAggregate(t, m, id, 1, float64(v), v, v)
	}
}

// B herda a posição de A, aposentado com baldes na janela, e começa sem as
// amostras de A. As chaves colidem na marca de dono de propósito: sem a troca
// de dono (validFromEpoch novo), só a marca separaria os baldes, e ela não
// separa.
func TestReusedPositionStartsWithoutTheRetiredOwnerSamples(t *testing.T) {
	k1, k2 := collidingKeys(t)
	h := newHarness(t)
	a := h.counter.add(k1, 1000)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s) // E
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+1: grava o balde E de A
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+2: grava o balde E+1 de A
	positionA := m.state.positions.load(a)

	h.counter.setLive(a, false)
	b := h.counter.add(k2, 5)
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+3: A sai e B assume a posição de A
	if got := m.state.positions.load(b); got != positionA {
		t.Fatalf("B ficou na posição %d, e não na %d de A: o cenário de reuso não se montou", got, positionA)
	}
	wantNoValue(t, m, a)
	wantAggregate(t, m, b, 1, 5, 5, 5)

	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+4: relê os slots da posição, e os baldes de A trazem a marca de B
	wantAggregate(t, m, b, 2, 5, 5, 5)
}

// Reinício depois de rotatividade, com posições livres e ocupadas misturadas:
// os vivos recuperam seus valores, a posição de um grupo aposentado enquanto o
// processo estava parado volta para a lista livre (com a identidade zerada), e
// os grupos novos reaproveitam as posições livres em vez de abrir outras.
func TestRestoreAfterRotationRecoversLiveGroupsAndFreesPositionsRetiredWhileStopped(t *testing.T) {
	h := newHarness(t)
	a := h.counter.add("A", 10)
	b := h.counter.add("B", 20)
	c := h.counter.add("C", 30)
	d := h.counter.add("D", 40)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s) // E
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+1

	h.counter.setLive(b, false)
	e := h.counter.add("E", 50)
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+2: B sai, E assume a posição de B
	if got := m.state.positions.load(e); got != 2 {
		t.Fatalf("E ficou na posição %d, esperado 2 (a de B)", got)
	}
	h.counter.setLive(c, false)
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+3: C sai, a posição 3 fica livre
	closeSampler(t, s)

	h.counter.setLive(d, false) // D se aposenta com o amostrador parado
	h.clock.advance(time.Second)
	reopened := h.open(t)
	rm := mustMetric(t, reopened)
	wantAggregate(t, rm, a, 4, 10, 10, 10)
	wantAggregate(t, rm, e, 2, 50, 50, 50)
	wantNoValue(t, rm, d)
	if groups, _, _, _ := rm.Usage(); groups != 2 {
		t.Errorf("Usage.groups = %d depois da restauração, esperado 2", groups)
	}
	if got := rm.file.Identity(4); got != (metricwindow.Identity{}) {
		t.Errorf("identidade da posição de D, aposentado com o processo parado = %+v, esperada zerada", got)
	}

	f := h.counter.add("F", 60)
	g := h.counter.add("G", 70)
	h.clock.advance(time.Second)
	sampleOnce(t, reopened)
	got := []uint32{rm.state.positions.load(f), rm.state.positions.load(g)}
	slices.Sort(got)
	if !slices.Equal(got, []uint32{3, 4}) {
		t.Errorf("F e G ficaram nas posições %v, esperado [3 4]: posição livre vazou", got)
	}
}

// Hash repetido em duas posições (lixo de crash) não tem dono confiável: as
// duas posições são liberadas, e o grupo recomeça como novo.
func TestRestoreFreesBothPositionsOfADuplicatedHash(t *testing.T) {
	h := newHarness(t)
	a := h.counter.add("OLT_A", 7)
	epoch := h.clock.epoch()
	if err := os.MkdirAll(filepath.Dir(h.path()), 0o755); err != nil {
		t.Fatal(err)
	}
	f, _, err := metricwindow.Open(h.path(), metricwindow.Header{BucketCount: 60, BucketWidthMs: 1000, Source: 1}, 0, fileCapacityLimit)
	if err != nil {
		t.Fatal(err)
	}
	ha := keyHash([]byte("OLT_A"))
	for _, position := range []uint32{1, 2} {
		f.SetIdentity(position, metricwindow.Identity{KeyHash: ha, ValidFromEpoch: uint64(epoch)})
		f.WriteBucket((epoch+1)%60, position, metricwindow.Bucket{
			Epoch: epoch + 1, Owner: metricwindow.OwnerTag(ha), Count: 3, Sum: 30, Min: 10, Max: 10,
		})
	}
	if err := f.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	h.clock.advance(2 * time.Second)
	s := h.open(t)
	m := mustMetric(t, s)
	wantNoValue(t, m, a)
	for _, position := range []uint32{1, 2} {
		if got := m.file.Identity(position); got != (metricwindow.Identity{}) {
			t.Errorf("identidade da posição %d com hash repetido = %+v, esperada zerada", position, got)
		}
	}
	sampleOnce(t, s)
	if got := m.state.positions.load(a); got != 1 {
		t.Errorf("o grupo ficou na posição %d, esperado 1 (a menor livre)", got)
	}
	wantAggregate(t, m, a, 1, 7, 7, 7)
}

// A posição que o leitor carregou do mapa é reatribuída a outro grupo antes da
// leitura da entrada — o seam readPositionHook põe a reatribuição exatamente
// nessa brecha. O leitor responde sem valor, e nunca o valor do grupo novo.
func TestAggregateNeverReturnsTheValueOfTheGroupThatReusedThePosition(t *testing.T) {
	t.Cleanup(func() { readPositionHook = nil })
	h := newHarness(t)
	a := h.counter.add("OLT_A", 111)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	positionA := m.state.positions.load(a)

	var b uint32
	readPositionHook = func(uint32, uint32) {
		readPositionHook = nil
		h.counter.setLive(a, false)
		b = h.counter.add("OLT_B", 222)
		h.clock.advance(time.Second)
		sampleOnce(t, s) // A sai, B assume a posição de A e publica 222
	}
	samples, avg, minimum, maximum := m.Aggregate(a)
	if b == 0 || m.state.positions.load(b) != positionA {
		t.Fatalf("cenário não montado: B não assumiu a posição %d de A", positionA)
	}
	if samples != 0 {
		t.Fatalf("Aggregate(A) devolveu (amostras %d, avg %v, min %d, max %d): o valor do grupo que reaproveitou a posição", samples, avg, minimum, maximum)
	}
	wantAggregate(t, m, b, 1, 222, 222, 222)
}

// Estresse: leitores de ids que se aposentam enquanto as posições são
// reaproveitadas por outros grupos, a cada passada. Cada grupo tem valor
// disjunto (id × 1000), então uma leitura com amostra e valor de outro id é o
// defeito, sem ambiguidade. Os seams cedem o processador na brecha do leitor e
// no meio da escrita, para que a intercalação não dependa de sorte.
func TestReadersNeverSeeAnotherGroupsValueWhilePositionsAreReused(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)
	}
	h := newHarness(t)
	const width = 16
	current := make([]uint32, 0, width)
	addGeneration := func() {
		current = current[:0]
		for range width {
			id := h.counter.add(fmt.Sprintf("G%d", h.counter.Limit()), 0)
			h.counter.setTotal(id, int64(id)*1000)
			current = append(current, id)
		}
	}
	addGeneration()
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)

	var highest atomic.Uint32
	highest.Store(current[width-1])
	var hookCalls atomic.Uint64
	readPositionHook = func(uint32, uint32) {
		if hookCalls.Add(1)%8 == 0 {
			runtime.Gosched()
		}
	}
	writes := 0
	publishYield = func() {
		writes++
		if writes%16 == 0 {
			runtime.Gosched()
		}
	}

	var (
		stop                 atomic.Bool
		reads, valued, wrong atomic.Int64
		firstWrong           atomic.Pointer[string]
		wg                   sync.WaitGroup
	)
	readers := max(2, runtime.GOMAXPROCS(0)-1)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				hi := highest.Load()
				lo := uint32(1)
				if hi > 3*width {
					lo = hi - 3*width
				}
				for id := lo; id <= hi; id++ {
					samples, avg, minimum, maximum, _ := m.read(id)
					reads.Add(1)
					if samples == 0 {
						continue
					}
					valued.Add(1)
					want := int64(id) * 1000
					if math.IsNaN(avg) || avg != float64(want) || minimum != want || maximum != want {
						if wrong.Add(1) == 1 {
							msg := fmt.Sprintf("id %d: amostras %d, avg %v, min %d, max %d", id, samples, avg, minimum, maximum)
							firstWrong.Store(&msg)
						}
					}
				}
			}
		}()
	}

	deadline := time.Now().Add(3 * time.Second)
	passes := 0
	for passes < 500 && time.Now().Before(deadline) {
		for _, id := range current {
			h.counter.setLive(id, false)
		}
		addGeneration()
		highest.Store(current[width-1])
		h.clock.advance(time.Second)
		if err := s.SampleOnce(); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("SampleOnce: %v", err)
		}
		passes++
	}
	stop.Store(true)
	wg.Wait()
	readPositionHook = nil
	publishYield = nil

	t.Logf("passadas %d, leitores %d, leituras %d, com valor %d, de outro grupo %d, capacidade %d",
		passes, readers, reads.Load(), valued.Load(), wrong.Load(), m.file.Capacity())
	if n := wrong.Load(); n > 0 {
		t.Errorf("%d leitura(s) devolveram o valor de outro grupo; a primeira: %s", n, *firstWrong.Load())
	}
	if valued.Load() == 0 {
		t.Errorf("nenhuma leitura com valor em %d leituras: o teste não provou nada", reads.Load())
	}
}

// Um Limit acima do espaço de ids do dicionário é recusado antes de o mapa id →
// posição crescer para ele.
func TestSampleRefusesALimitAboveTheDictionaryCeiling(t *testing.T) {
	h := newHarness(t)
	h.counter.add("OLT_A", 1)
	s := h.open(t)
	sampleOnce(t, s)
	before := s.Stats()[0].PositionMapBytes

	h.counter.setLimitOverride(fileCapacityLimit + 1)
	if err := s.SampleOnce(); err == nil {
		t.Fatal("SampleOnce aceitou um Limit acima do teto de ids do dicionário")
	}
	if after := s.Stats()[0].PositionMapBytes; after != before {
		t.Errorf("PositionMapBytes foi de %d para %d: o mapa cresceu antes da recusa", before, after)
	}
}
