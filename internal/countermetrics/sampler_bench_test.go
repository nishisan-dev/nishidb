package countermetrics

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// pilotGroups é o inventário do piloto: um contador por OLT com 2.830 grupos
// vivos.
const pilotGroups = 2830

// BenchmarkSampleOncePilotShape mede a passada do amostrador no formato do
// piloto — um contador de 2.830 grupos com as duas métricas do exemplo do
// plano (`auth_5m`, 5m com avg; `auth_24h`, 24h com max) — e reporta o custo
// de memória e de disco que o aceite cita.
//
// Os três regimes separam o que a passada faz:
//
//   - sameEpoch: o relógio não anda, nenhum balde fecha. É o piso: ler cada
//     grupo, dobrar a amostra no balde aberto e publicar.
//   - pilotCadence: o relógio anda o intervalo real de amostragem (5m ÷ 60 =
//     5 s), então TODA passada fecha o balde da métrica de 5m (grava o balde e
//     relê os 60 slots de cada grupo) e a de 24h fecha a cada 288 passadas.
//   - bothClose: o relógio anda um balde da janela de 24h (24 min) por passada,
//     então as duas métricas fecham balde em toda passada. É o teto.
//
// O leitor é o contador em memória dos testes (um mutex por Group), e não o
// *counter.Counter real: o número é o custo do amostrador mais um lock por
// grupo, sem o do dicionário do storage.
//
// As métricas reportadas têm duas origens, e o nome diz qual:
//
//   - MEDIDAS: heap_measured_B é o crescimento de runtime.MemStats.HeapAlloc
//     entre antes do Open e depois das passadas de aquecimento, com
//     runtime.GC() antes de cada leitura — o heap vivo que o amostrador de
//     fato retém, temporários da restauração já coletados; win_B e
//     win_alloc_B saem do stat de um .win (tamanho e blocos alocados pelo
//     sistema de arquivos).
//   - CONTABILIZADAS: heap_accounted_B é a contabilidade de Sampler.Stats —
//     HeapBytes das duas métricas (blocos × 1.024 × tamanho da entrada) mais
//     o PositionMapBytes do contador, contado uma vez —, e map_accounted_B é
//     só o mapa. Não entram nela o diretório de blocos, as structs fixas e as
//     fatias de trabalho, e é a diferença entre as duas que a bancada expõe.
//
// As duas de heap também saem divididas por grupo × métrica (5.660).
func BenchmarkSampleOncePilotShape(b *testing.B) {
	regimes := []struct {
		name    string
		advance time.Duration
	}{
		{"sameEpoch", 0},
		{"pilotCadence", 5 * time.Second},
		{"bothClose", 24 * time.Minute},
	}
	for _, r := range regimes {
		b.Run(r.name, func(b *testing.B) {
			counter := newFakeCounter()
			for i := range pilotGroups {
				counter.add(fmt.Sprintf("OLT_%04d", i), int64(i%500))
			}
			clock := newFakeClock()
			dir := filepath.Join(b.TempDir(), "counter_metrics")
			// Tudo o que não é do amostrador nasce ANTES da primeira leitura do
			// heap: contador, relógio, opções e logger não entram na medida.
			opts := Options{
				Dataset: testDataset,
				Dir:     dir,
				Counters: []CounterInput{{
					Spec: schema.CounterSpec{
						Name: testCounter,
						Metrics: []schema.CounterMetricSpec{
							{Name: "auth_5m", Type: schema.MetricTypeWindow, Source: schema.MetricSourceTotal,
								Window: 5 * time.Minute, Aggregates: []string{schema.AggregateAvg}},
							{Name: "auth_24h", Type: schema.MetricTypeWindow, Source: schema.MetricSourceTotal,
								Window: 24 * time.Hour, Aggregates: []string{schema.AggregateMax}},
						},
					},
					Reader: counter,
				}},
				Log: logging.Discard(),
				Now: clock.Now,
			}
			heapBefore := liveHeap()
			s, err := Open(opts)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })
			// Duas passadas antes da medida: arquivos crescidos e blocos em heap
			// já alocados, que é o regime permanente.
			for range 2 {
				clock.advance(5 * time.Second)
				if err := s.SampleOnce(); err != nil {
					b.Fatal(err)
				}
			}
			heapMeasured := int64(liveHeap()) - int64(heapBefore)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				clock.advance(r.advance)
				if err := s.SampleOnce(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			st := s.Stats()
			if len(st) != 2 || st[0].Groups != pilotGroups {
				b.Fatalf("Stats = %+v, esperado 2 métricas com %d grupos", st, pilotGroups)
			}
			groupMetrics := float64(pilotGroups * len(st))
			heapAccounted := st[0].PositionMapBytes
			for _, m := range st {
				heapAccounted += m.HeapBytes
			}
			b.ReportMetric(float64(heapMeasured), "heap_measured_B")
			b.ReportMetric(float64(heapMeasured)/groupMetrics, "heap_measured_B/group-metric")
			b.ReportMetric(float64(heapAccounted), "heap_accounted_B")
			b.ReportMetric(float64(heapAccounted)/groupMetrics, "heap_accounted_B/group-metric")
			b.ReportMetric(float64(st[0].PositionMapBytes), "map_accounted_B")
			info, err := os.Stat(filepath.Join(dir, testCounter, "auth_5m"+fileExtension))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(info.Size()), "win_B")
			if raw, ok := info.Sys().(*syscall.Stat_t); ok {
				b.ReportMetric(float64(raw.Blocks*512), "win_alloc_B")
			}
		})
	}
}

// liveHeap devolve o heap vivo depois de uma coleta completa: HeapAlloc logo
// após runtime.GC() é o que sobrevive, sem o lixo ainda não coletado.
func liveHeap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}
