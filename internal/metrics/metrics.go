// Package metrics é o registry e o formato de exposição do §92/§93.
//
// É escrito à mão, sem client_golang, por uma razão de produto: o §97 exige um
// binário único distribuível em scratch/distroless, e o projeto inteiro tem
// duas dependências. O formato de exposição textual do Prometheus é pequeno o
// bastante para caber aqui com teste próprio, e o que se ganha em troca de
// ~250 linhas é não arrastar a árvore de dependências de um cliente completo
// para expor três dezenas de séries.
//
// O registry é PULL. Os contadores que já existem no projeto (materializer,
// executor, planner, Store.Stats) NÃO são copiados para cá: são lidos na hora
// do scrape, por um Collector. Copiar o estado criaria um segundo contador com o
// mesmo nome de spec, e o próprio código já registra que isso é bug de
// observabilidade — em produção alguém lê o errado.
//
// Registry é seguro para uso concorrente. A ordem de emissão é estável entre
// scrapes (ordem de registro das famílias, ordem de primeira aparição das
// séries), porque diff de dois scrapes é ferramenta de diagnóstico e ordem de
// mapa a inutilizaria.
package metrics

import (
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Type é o tipo Prometheus da família.
type Type int

// Os três tipos que o formato de exposição do Prometheus reconhece e que este
// produto usa: contador monotônico, medidor e histograma de buckets fixos.
const (
	TypeCounter Type = iota
	TypeGauge
	TypeHistogram
)

// typeName é o que sai no `# TYPE`.
func typeName(t Type) string {
	switch t {
	case TypeCounter:
		return "counter"
	case TypeGauge:
		return "gauge"
	case TypeHistogram:
		return "histogram"
	}
	return "untyped"
}

// Label é um par nome/valor de uma série.
type Label struct {
	Name  string
	Value string
}

// Sample é uma linha de exposição já resolvida.
//
// Family e Name são separados por causa do histograma: as linhas se chamam
// `x_bucket`, `x_sum` e `x_count`, mas o `# HELP`/`# TYPE` sai UMA vez sob o
// nome-base `x`. Emitir cabeçalho por sufixo faz o Prometheus recusar o
// scrape inteiro, não só a linha.
type Sample struct {
	Family string
	Name   string
	Help   string
	Type   Type
	Labels []Label
	Value  float64
}

// Collector produz amostras no instante do scrape.
type Collector interface {
	Collect() []Sample
}

// CollectorFunc adapta uma função a Collector.
type CollectorFunc func() []Sample

// Collect implementa Collector.
func (f CollectorFunc) Collect() []Sample { return f() }

// Registry guarda as famílias e os coletores de um processo.
type Registry struct {
	mu     sync.Mutex
	order  []Collector
	byName map[string]any
}

// NewRegistry constrói um registry vazio.
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]any)}
}

// DefaultBuckets são os buckets de latência de consulta. Cobrem a faixa dos
// tetos do §89 (10 ms no point lookup) e do §90 (50 ms no indexado), com
// resolução suficiente abaixo deles para que a violação apareça antes de
// virar incidente — o p95 medido no M1 foi de 18 µs e 522 µs.
var DefaultBuckets = []float64{
	0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// Register registra uma fonte pull. É por aqui que os contadores existentes do
// projeto chegam ao /metrics sem serem duplicados.
func (r *Registry) Register(c Collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, c)
}

// register devolve a família já existente com esse nome, ou instala a nova.
// Name repetido com tipo diferente é bug de programação e entra em pânico:
// duas famílias com o mesmo nome produzem um scrape que o Prometheus recusa.
func register[F any](r *Registry, name string, create func() *F) *F {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byName[name]; ok {
		f, ok := existing.(*F)
		if !ok {
			panic("metrics: família " + name + " já registrada com outro tipo")
		}
		return f
	}
	f := create()
	r.byName[name] = f
	r.order = append(r.order, any(f).(Collector))
	return f
}

// Counter registra (ou recupera) uma família de contadores monotônicos.
func (r *Registry) Counter(name, help string, labels ...string) *CounterFamily {
	return register(r, name, func() *CounterFamily {
		return &CounterFamily{
			base:     base{name: name, help: help, labels: labels},
			children: make(map[string]*CounterSeries),
		}
	})
}

// Gauge registra (ou recupera) uma família de medidores.
func (r *Registry) Gauge(name, help string, labels ...string) *GaugeFamily {
	return register(r, name, func() *GaugeFamily {
		return &GaugeFamily{
			base:     base{name: name, help: help, labels: labels},
			children: make(map[string]*GaugeSeries),
		}
	})
}

// Histogram registra (ou recupera) uma família de histogramas. limites são
// os limites superiores dos buckets, em ordem crescente; o bucket +Inf é
// acrescentado sozinho.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *HistogramFamily {
	return register(r, name, func() *HistogramFamily {
		bucketsCopy := make([]float64, len(buckets))
		copy(bucketsCopy, buckets)
		return &HistogramFamily{
			base:     base{name: name, help: help, labels: labels},
			buckets:  bucketsCopy,
			children: make(map[string]*HistogramSeries),
		}
	})
}

// base é o que toda família tem.
type base struct {
	name   string
	help   string
	labels []string
}

// labelPairs casa os nomes declarados com os valores recebidos. Quantidade
// errada é panic e não erro: a lista de labels é literal no código, então
// errá-la é bug de programação, não estado de runtime que dê para tratar.
func (b *base) labelPairs(values []string) []Label {
	if len(values) != len(b.labels) {
		panic("metrics: " + b.name + " espera " + strconv.Itoa(len(b.labels)) +
			" label(s), recebeu " + strconv.Itoa(len(values)))
	}
	if len(values) == 0 {
		return nil
	}
	out := make([]Label, len(values))
	for i, v := range values {
		out[i] = Label{Name: b.labels[i], Value: v}
	}
	return out
}

// key identifica a série. \xff não aparece em UTF-8 válido, então não há
// como dois conjuntos de valores distintos colidirem.
func key(values []string) string { return strings.Join(values, "\xff") }

// --- contador ---

// CounterFamily é uma família de contadores monotônicos.
type CounterFamily struct {
	base
	mu       sync.RWMutex
	children map[string]*CounterSeries
	order    []*CounterSeries
}

// CounterSeries é um contador de uma combinação de labels.
type CounterSeries struct {
	labels []Label
	v      atomic.Uint64
}

// With devolve a série dos valores de label dados, criando-a se preciso.
func (f *CounterFamily) With(values ...string) *CounterSeries {
	k := key(values)
	f.mu.RLock()
	s, ok := f.children[k]
	f.mu.RUnlock()
	if ok {
		return s
	}
	labels := f.labelPairs(values)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.children[k]; ok {
		return s
	}
	s = &CounterSeries{labels: labels}
	f.children[k] = s
	f.order = append(f.order, s)
	return s
}

// Unlabeled é o açúcar para a família sem labels.
func (f *CounterFamily) Unlabeled() *CounterSeries { return f.With() }

// Inc soma 1.
func (s *CounterSeries) Inc() { s.v.Add(1) }

// Add soma n.
func (s *CounterSeries) Add(n uint64) { s.v.Add(n) }

// Value lê o acumulado.
func (s *CounterSeries) Value() uint64 { return s.v.Load() }

// Collect implementa Collector.
func (f *CounterFamily) Collect() []Sample {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Sample, 0, len(f.order))
	for _, s := range f.order {
		out = append(out, Sample{
			Family: f.name, Name: f.name, Help: f.help, Type: TypeCounter,
			Labels: s.labels, Value: float64(s.v.Load()),
		})
	}
	return out
}

// --- medidor ---

// GaugeFamily é uma família de medidores.
type GaugeFamily struct {
	base
	mu       sync.RWMutex
	children map[string]*GaugeSeries
	order    []*GaugeSeries
}

// GaugeSeries é um medidor de uma combinação de labels. O valor é float64
// guardado em bits para caber num atômico: lag, offset e RSS são inteiros
// grandes, mas taxa e timestamp são fracionários.
type GaugeSeries struct {
	labels []Label
	bits   atomic.Uint64
}

// With devolve a série dos valores de label dados, criando-a se preciso.
func (f *GaugeFamily) With(values ...string) *GaugeSeries {
	k := key(values)
	f.mu.RLock()
	s, ok := f.children[k]
	f.mu.RUnlock()
	if ok {
		return s
	}
	labels := f.labelPairs(values)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.children[k]; ok {
		return s
	}
	s = &GaugeSeries{labels: labels}
	f.children[k] = s
	f.order = append(f.order, s)
	return s
}

// Unlabeled é o açúcar para a família sem labels.
func (f *GaugeFamily) Unlabeled() *GaugeSeries { return f.With() }

// Set fixa o valor.
func (s *GaugeSeries) Set(v float64) { s.bits.Store(math.Float64bits(v)) }

// Add soma delta (negativo para subtrair).
//
// Existe para o medidor de "em voo": o incremento e o decremento acontecem em
// goroutines DIFERENTES — uma requisição entra enquanto outra sai —, e um Set
// calculado fora perderia atualizações concorrentes, deixando o gauge preso num
// valor que nunca mais desce. O CAS em laço é o preço de guardar float em bits,
// e é irrelevante aqui: observa-se duas vezes por consulta.
func (s *GaugeSeries) Add(delta float64) {
	for {
		old := s.bits.Load()
		next := math.Float64bits(math.Float64frombits(old) + delta)
		if s.bits.CompareAndSwap(old, next) {
			return
		}
	}
}

// Value lê o valor corrente.
func (s *GaugeSeries) Value() float64 { return math.Float64frombits(s.bits.Load()) }

// Collect implementa Collector.
func (f *GaugeFamily) Collect() []Sample {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Sample, 0, len(f.order))
	for _, s := range f.order {
		out = append(out, Sample{
			Family: f.name, Name: f.name, Help: f.help, Type: TypeGauge,
			Labels: s.labels, Value: s.Value(),
		})
	}
	return out
}

// --- histograma ---

// HistogramFamily é uma família de histogramas de buckets fixos.
type HistogramFamily struct {
	base
	buckets  []float64
	mu       sync.RWMutex
	children map[string]*HistogramSeries
	order    []*HistogramSeries
}

// HistogramSeries acumula contagem por bucket, soma e total.
//
// Usa mutex e não atômicos porque soma e buckets precisam avançar juntos: um
// scrape que pegasse a soma nova com a contagem velha produziria média errada.
// O custo é irrelevante — observa-se uma vez por consulta, não por evento.
type HistogramSeries struct {
	labels []Label
	// buckets é a fatia da família, compartilhada e só de leitura. Vive
	// também aqui para que Observe não precise da família — o chamador
	// guarda a série, não a família, no laço quente.
	buckets []float64
	mu      sync.Mutex
	counts  []uint64
	sum     float64
	total   uint64
}

// With devolve a série dos valores de label dados, criando-a se preciso.
func (f *HistogramFamily) With(values ...string) *HistogramSeries {
	k := key(values)
	f.mu.RLock()
	s, ok := f.children[k]
	f.mu.RUnlock()
	if ok {
		return s
	}
	labels := f.labelPairs(values)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.children[k]; ok {
		return s
	}
	s = &HistogramSeries{labels: labels, buckets: f.buckets, counts: make([]uint64, len(f.buckets))}
	f.children[k] = s
	f.order = append(f.order, s)
	return s
}

// Unlabeled é o açúcar para a família sem labels.
func (f *HistogramFamily) Unlabeled() *HistogramSeries { return f.With() }

// Observe registra uma observação na série.
func (s *HistogramSeries) Observe(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.counts {
		if v <= s.buckets[i] {
			s.counts[i]++
		}
	}
	s.sum += v
	s.total++
}

// Collect implementa Collector. Os buckets são CUMULATIVOS, como o formato exige.
func (f *HistogramFamily) Collect() []Sample {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Sample, 0, len(f.order)*(len(f.buckets)+3))
	for _, s := range f.order {
		s.mu.Lock()
		counts := make([]uint64, len(s.counts))
		copy(counts, s.counts)
		sum, total := s.sum, s.total
		s.mu.Unlock()

		for i, bound := range f.buckets {
			out = append(out, Sample{
				Family: f.name, Name: f.name + "_bucket", Help: f.help, Type: TypeHistogram,
				Labels: append(copyLabels(s.labels), Label{Name: "le", Value: formatValue(bound)}),
				Value:  float64(counts[i]),
			})
		}
		out = append(out, Sample{
			Family: f.name, Name: f.name + "_bucket", Help: f.help, Type: TypeHistogram,
			Labels: append(copyLabels(s.labels), Label{Name: "le", Value: "+Inf"}),
			Value:  float64(total),
		})
		out = append(out, Sample{
			Family: f.name, Name: f.name + "_sum", Help: f.help, Type: TypeHistogram,
			Labels: s.labels, Value: sum,
		})
		out = append(out, Sample{
			Family: f.name, Name: f.name + "_count", Help: f.help, Type: TypeHistogram,
			Labels: s.labels, Value: float64(total),
		})
	}
	return out
}

func copyLabels(in []Label) []Label {
	out := make([]Label, len(in), len(in)+1)
	copy(out, in)
	return out
}

// --- exposição ---

// WriteTo emite o formato textual do Prometheus (§93).
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	collectors := make([]Collector, len(r.order))
	copy(collectors, r.order)
	r.mu.Unlock()

	var sb strings.Builder
	seen := make(map[string]bool)
	for _, c := range collectors {
		for _, a := range c.Collect() {
			if !seen[a.Family] {
				seen[a.Family] = true
				if a.Help != "" {
					sb.WriteString("# HELP ")
					sb.WriteString(a.Family)
					sb.WriteByte(' ')
					sb.WriteString(escapeHelp(a.Help))
					sb.WriteByte('\n')
				}
				sb.WriteString("# TYPE ")
				sb.WriteString(a.Family)
				sb.WriteByte(' ')
				sb.WriteString(typeName(a.Type))
				sb.WriteByte('\n')
			}
			writeSample(&sb, a)
		}
	}
	n, err := io.WriteString(w, sb.String())
	return int64(n), err
}

func writeSample(sb *strings.Builder, a Sample) {
	sb.WriteString(a.Name)
	if len(a.Labels) > 0 {
		sb.WriteByte('{')
		for i, l := range a.Labels {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(l.Name)
			sb.WriteString(`="`)
			sb.WriteString(escapeLabelValue(l.Value))
			sb.WriteByte('"')
		}
		sb.WriteByte('}')
	}
	sb.WriteByte(' ')
	sb.WriteString(formatValue(a.Value))
	sb.WriteByte('\n')
}

// formatValue evita notação científica para inteiros. 1e7 é aceito pelo
// Prometheus, mas "1e+07" quebra o olho de quem lê o scrape na mão — e o
// scrape lido na mão é a primeira ferramenta de diagnóstico que existe.
func formatValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// escapeLabelValue escapa barra, aspa e quebra de linha. Sem isso, um valor
// vindo de config (nome de tópico, motivo de erro) derruba o scrape INTEIRO,
// não só a própria linha.
func escapeLabelValue(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for _, c := range s {
		switch c {
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteRune(c)
		}
	}
	return sb.String()
}

// escapeHelp escapa barra e quebra de linha (aspa não é especial no HELP).
func escapeHelp(s string) string {
	if !strings.ContainsAny(s, "\\\n") {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for _, c := range s {
		switch c {
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteRune(c)
		}
	}
	return sb.String()
}
