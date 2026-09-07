package metrics

import (
	"runtime"

	"github.com/nishisan-dev/nishidb/internal/memprof"
)

// RegisterProcess instala o coletor de memória do processo (§79, §92).
//
// É PULL: nada é amostrado em background. Ler /proc/self/status e
// smaps_rollup custa uma dezena de microssegundos, e fazê-lo num timer
// significaria publicar um número velho — pior, um número cuja idade o
// operador não conhece. No scrape, o valor é o do instante da pergunta.
//
// Fora do Linux o memprof.Read falha por construção (não há /proc). Neste
// caso as três séries de RSS somem do scrape em vez de sair zeradas: zero é
// uma medição, ausência é a ausência de medição, e um alerta de "RSS caiu para
// zero" é pior que nenhum dado. O heap Go continua saindo, porque esse vem do
// runtime e é portável.
func RegisterProcess(r *Registry) {
	r.Register(CollectorFunc(func() []Sample {
		out := make([]Sample, 0, 3)

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		out = append(out,
			Sample{
				Family: MetGoHeapBytes, Name: MetGoHeapBytes, Type: TypeGauge,
				Help:  "Bytes alocados no heap Go. NÃO inclui os índices mmap, que são a maior parte do estado.",
				Value: float64(ms.HeapAlloc),
			},
			Sample{
				Family: MetGoHeapObjects, Name: MetGoHeapObjects, Type: TypeGauge,
				Help:  "Objetos vivos no heap Go.",
				Value: float64(ms.HeapObjects),
			},
		)

		s, err := memprof.Read()
		if err != nil {
			return out
		}
		out = append(out, Sample{
			Family: MetProcessRSSBytes, Name: MetProcessRSSBytes, Type: TypeGauge,
			Help:  "RSS do processo em bytes. É o número do critério de aceite do §109 — o que o sistema operacional cobra.",
			Value: float64(s.VmRSSBytes),
		})
		return out
	}))
}
