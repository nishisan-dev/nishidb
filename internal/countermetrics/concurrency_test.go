package countermetrics

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// Leitores lendo Aggregate em laço enquanto o amostrador publica: nenhuma
// leitura pode sair rasgada. O escritor salta mais que a janela a cada passada,
// então o publicado de cada passada é uma amostra só, com avg = min = max, e os
// totais alternam entre extremos — uma leitura que misturasse o avg de uma
// passada com o min/max da outra quebra min <= avg <= max.
//
// O teste mede quantas vezes o leitor refez a leitura (seq ímpar ou mudado).
// Zero releituras quer dizer que leitor e escritor nunca se cruzaram, e aí o
// teste não provou nada: ele falha em vez de passar vazio. Para que a
// intercalação não dependa de sorte num núcleo só, o seam publishYield cede o
// processador no meio de uma a cada 16 escritas, com seq ímpar.
func TestConcurrentReadersNeverSeeATornAggregate(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)
	}
	h := newHarness(t)
	const groups = 8
	for i := range groups {
		h.counter.add(fmt.Sprintf("OLT_%d", i), 10)
	}
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)

	// Só o escritor (esta goroutine, via SampleOnce) chama o seam.
	writes := 0
	publishYield = func() {
		writes++
		if writes%16 == 0 {
			runtime.Gosched()
		}
	}
	defer func() { publishYield = nil }()

	var (
		stop                 atomic.Bool
		retries, reads, torn atomic.Int64
		firstTorn            atomic.Pointer[string]
		wg                   sync.WaitGroup
	)
	readers := max(2, runtime.GOMAXPROCS(0)-1)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				for id := uint32(1); id <= groups; id++ {
					samples, avg, minimum, maximum, r := m.read(id)
					retries.Add(int64(r))
					reads.Add(1)
					if samples == 0 || math.IsNaN(avg) || math.IsInf(avg, 0) || float64(minimum) > avg || avg > float64(maximum) {
						if torn.Add(1) == 1 {
							msg := fmt.Sprintf("grupo %d: amostras %d, avg %v, min %d, max %d", id, samples, avg, minimum, maximum)
							firstTorn.Store(&msg)
						}
					}
				}
			}
		}()
	}

	const minPasses = 2_000
	deadline := time.Now().Add(3 * time.Second)
	passes := 0
	for passes < minPasses || (retries.Load() < 1_000 && time.Now().Before(deadline)) {
		total := int64(10)
		if passes%2 == 0 {
			total = 1_000_000
		}
		for id := uint32(1); id <= groups; id++ {
			h.counter.setTotal(id, total)
		}
		h.clock.advance(time.Duration(schema.MetricWindowBuckets+1) * time.Second)
		if err := s.SampleOnce(); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("SampleOnce: %v", err)
		}
		passes++
	}
	stop.Store(true)
	wg.Wait()

	t.Logf("passadas %d, leitores %d, leituras %d, releituras %d", passes, readers, reads.Load(), retries.Load())
	if n := torn.Load(); n > 0 {
		t.Errorf("%d leitura(s) rasgada(s); a primeira: %s", n, *firstTorn.Load())
	}
	if retries.Load() == 0 {
		t.Errorf("nenhuma releitura em %d leituras: leitor e escritor não se intercalaram, e o teste não provou nada", reads.Load())
	}
}
