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
// do scrape, por um Coletor. Copiar o estado criaria um segundo contador com o
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

// Tipo é o tipo Prometheus da família.
type Tipo int

// Os três tipos que o formato de exposição do Prometheus reconhece e que este
// produto usa: contador monotônico, medidor e histograma de buckets fixos.
const (
	TipoContador Tipo = iota
	TipoMedidor
	TipoHistograma
)

// nomeDoTipo é o que sai no `# TYPE`.
func nomeDoTipo(t Tipo) string {
	switch t {
	case TipoContador:
		return "counter"
	case TipoMedidor:
		return "gauge"
	case TipoHistograma:
		return "histogram"
	}
	return "untyped"
}

// Label é um par nome/valor de uma série.
type Label struct {
	Nome  string
	Valor string
}

// Amostra é uma linha de exposição já resolvida.
//
// Familia e Nome são separados por causa do histograma: as linhas se chamam
// `x_bucket`, `x_sum` e `x_count`, mas o `# HELP`/`# TYPE` sai UMA vez sob o
// nome-base `x`. Emitir cabeçalho por sufixo faz o Prometheus recusar o
// scrape inteiro, não só a linha.
type Amostra struct {
	Familia string
	Nome    string
	Ajuda   string
	Tipo    Tipo
	Labels  []Label
	Valor   float64
}

// Coletor produz amostras no instante do scrape.
type Coletor interface {
	Collect() []Amostra
}

// ColetorFunc adapta uma função a Coletor.
type ColetorFunc func() []Amostra

// Collect implementa Coletor.
func (f ColetorFunc) Collect() []Amostra { return f() }

// Registry guarda as famílias e os coletores de um processo.
type Registry struct {
	mu      sync.Mutex
	ordem   []Coletor
	porNome map[string]any
}

// NewRegistry constrói um registry vazio.
func NewRegistry() *Registry {
	return &Registry{porNome: make(map[string]any)}
}

// LimitesPadrao são os buckets de latência de consulta. Cobrem a faixa dos
// tetos do §89 (10 ms no point lookup) e do §90 (50 ms no indexado), com
// resolução suficiente abaixo deles para que a violação apareça antes de
// virar incidente — o p95 medido no M1 foi de 18 µs e 522 µs.
var LimitesPadrao = []float64{
	0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// Coletor registra uma fonte pull. É por aqui que os contadores existentes do
// projeto chegam ao /metrics sem serem duplicados.
func (r *Registry) Coletor(c Coletor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ordem = append(r.ordem, c)
}

// registra devolve a família já existente com esse nome, ou instala a nova.
// Nome repetido com tipo diferente é bug de programação e entra em pânico:
// duas famílias com o mesmo nome produzem um scrape que o Prometheus recusa.
func registra[F any](r *Registry, nome string, nova func() *F) *F {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existente, ok := r.porNome[nome]; ok {
		f, ok := existente.(*F)
		if !ok {
			panic("metrics: família " + nome + " já registrada com outro tipo")
		}
		return f
	}
	f := nova()
	r.porNome[nome] = f
	r.ordem = append(r.ordem, any(f).(Coletor))
	return f
}

// Contador registra (ou recupera) uma família de contadores monotônicos.
func (r *Registry) Contador(nome, ajuda string, labels ...string) *FamiliaContador {
	return registra(r, nome, func() *FamiliaContador {
		return &FamiliaContador{
			base:   base{nome: nome, ajuda: ajuda, labels: labels},
			filhos: make(map[string]*SerieContador),
		}
	})
}

// Medidor registra (ou recupera) uma família de medidores.
func (r *Registry) Medidor(nome, ajuda string, labels ...string) *FamiliaMedidor {
	return registra(r, nome, func() *FamiliaMedidor {
		return &FamiliaMedidor{
			base:   base{nome: nome, ajuda: ajuda, labels: labels},
			filhos: make(map[string]*SerieMedidor),
		}
	})
}

// Histograma registra (ou recupera) uma família de histogramas. limites são
// os limites superiores dos buckets, em ordem crescente; o bucket +Inf é
// acrescentado sozinho.
func (r *Registry) Histograma(nome, ajuda string, limites []float64, labels ...string) *FamiliaHistograma {
	return registra(r, nome, func() *FamiliaHistograma {
		lim := make([]float64, len(limites))
		copy(lim, limites)
		return &FamiliaHistograma{
			base:    base{nome: nome, ajuda: ajuda, labels: labels},
			limites: lim,
			filhos:  make(map[string]*SerieHistograma),
		}
	})
}

// base é o que toda família tem.
type base struct {
	nome   string
	ajuda  string
	labels []string
}

// paresDeLabel casa os nomes declarados com os valores recebidos. Quantidade
// errada é panic e não erro: a lista de labels é literal no código, então
// errá-la é bug de programação, não estado de runtime que dê para tratar.
func (b *base) paresDeLabel(valores []string) []Label {
	if len(valores) != len(b.labels) {
		panic("metrics: " + b.nome + " espera " + strconv.Itoa(len(b.labels)) +
			" label(s), recebeu " + strconv.Itoa(len(valores)))
	}
	if len(valores) == 0 {
		return nil
	}
	out := make([]Label, len(valores))
	for i, v := range valores {
		out[i] = Label{Nome: b.labels[i], Valor: v}
	}
	return out
}

// chave identifica a série. \xff não aparece em UTF-8 válido, então não há
// como dois conjuntos de valores distintos colidirem.
func chave(valores []string) string { return strings.Join(valores, "\xff") }

// --- contador ---

// FamiliaContador é uma família de contadores monotônicos.
type FamiliaContador struct {
	base
	mu     sync.RWMutex
	filhos map[string]*SerieContador
	ordem  []*SerieContador
}

// SerieContador é um contador de uma combinação de labels.
type SerieContador struct {
	labels []Label
	v      atomic.Uint64
}

// Com devolve a série dos valores de label dados, criando-a se preciso.
func (f *FamiliaContador) Com(valores ...string) *SerieContador {
	k := chave(valores)
	f.mu.RLock()
	s, ok := f.filhos[k]
	f.mu.RUnlock()
	if ok {
		return s
	}
	labels := f.paresDeLabel(valores)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.filhos[k]; ok {
		return s
	}
	s = &SerieContador{labels: labels}
	f.filhos[k] = s
	f.ordem = append(f.ordem, s)
	return s
}

// Sozinha é o açúcar para a família sem labels.
func (f *FamiliaContador) Sozinha() *SerieContador { return f.Com() }

// Inc soma 1.
func (s *SerieContador) Inc() { s.v.Add(1) }

// Add soma n.
func (s *SerieContador) Add(n uint64) { s.v.Add(n) }

// Valor lê o acumulado.
func (s *SerieContador) Valor() uint64 { return s.v.Load() }

// Collect implementa Coletor.
func (f *FamiliaContador) Collect() []Amostra {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Amostra, 0, len(f.ordem))
	for _, s := range f.ordem {
		out = append(out, Amostra{
			Familia: f.nome, Nome: f.nome, Ajuda: f.ajuda, Tipo: TipoContador,
			Labels: s.labels, Valor: float64(s.v.Load()),
		})
	}
	return out
}

// --- medidor ---

// FamiliaMedidor é uma família de medidores.
type FamiliaMedidor struct {
	base
	mu     sync.RWMutex
	filhos map[string]*SerieMedidor
	ordem  []*SerieMedidor
}

// SerieMedidor é um medidor de uma combinação de labels. O valor é float64
// guardado em bits para caber num atômico: lag, offset e RSS são inteiros
// grandes, mas taxa e timestamp são fracionários.
type SerieMedidor struct {
	labels []Label
	bits   atomic.Uint64
}

// Com devolve a série dos valores de label dados, criando-a se preciso.
func (f *FamiliaMedidor) Com(valores ...string) *SerieMedidor {
	k := chave(valores)
	f.mu.RLock()
	s, ok := f.filhos[k]
	f.mu.RUnlock()
	if ok {
		return s
	}
	labels := f.paresDeLabel(valores)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.filhos[k]; ok {
		return s
	}
	s = &SerieMedidor{labels: labels}
	f.filhos[k] = s
	f.ordem = append(f.ordem, s)
	return s
}

// Sozinha é o açúcar para a família sem labels.
func (f *FamiliaMedidor) Sozinha() *SerieMedidor { return f.Com() }

// Set fixa o valor.
func (s *SerieMedidor) Set(v float64) { s.bits.Store(math.Float64bits(v)) }

// Add soma delta (negativo para subtrair).
//
// Existe para o medidor de "em voo": o incremento e o decremento acontecem em
// goroutines DIFERENTES — uma requisição entra enquanto outra sai —, e um Set
// calculado fora perderia atualizações concorrentes, deixando o gauge preso num
// valor que nunca mais desce. O CAS em laço é o preço de guardar float em bits,
// e é irrelevante aqui: observa-se duas vezes por consulta.
func (s *SerieMedidor) Add(delta float64) {
	for {
		velho := s.bits.Load()
		novo := math.Float64bits(math.Float64frombits(velho) + delta)
		if s.bits.CompareAndSwap(velho, novo) {
			return
		}
	}
}

// Valor lê o valor corrente.
func (s *SerieMedidor) Valor() float64 { return math.Float64frombits(s.bits.Load()) }

// Collect implementa Coletor.
func (f *FamiliaMedidor) Collect() []Amostra {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Amostra, 0, len(f.ordem))
	for _, s := range f.ordem {
		out = append(out, Amostra{
			Familia: f.nome, Nome: f.nome, Ajuda: f.ajuda, Tipo: TipoMedidor,
			Labels: s.labels, Valor: s.Valor(),
		})
	}
	return out
}

// --- histograma ---

// FamiliaHistograma é uma família de histogramas de buckets fixos.
type FamiliaHistograma struct {
	base
	limites []float64
	mu      sync.RWMutex
	filhos  map[string]*SerieHistograma
	ordem   []*SerieHistograma
}

// SerieHistograma acumula contagem por bucket, soma e total.
//
// Usa mutex e não atômicos porque soma e buckets precisam avançar juntos: um
// scrape que pegasse a soma nova com a contagem velha produziria média errada.
// O custo é irrelevante — observa-se uma vez por consulta, não por evento.
type SerieHistograma struct {
	labels []Label
	// limites é a fatia da família, compartilhada e só de leitura. Vive
	// também aqui para que Observa não precise da família — o chamador
	// guarda a série, não a família, no laço quente.
	limites   []float64
	mu        sync.Mutex
	contagens []uint64
	soma      float64
	total     uint64
}

// Com devolve a série dos valores de label dados, criando-a se preciso.
func (f *FamiliaHistograma) Com(valores ...string) *SerieHistograma {
	k := chave(valores)
	f.mu.RLock()
	s, ok := f.filhos[k]
	f.mu.RUnlock()
	if ok {
		return s
	}
	labels := f.paresDeLabel(valores)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.filhos[k]; ok {
		return s
	}
	s = &SerieHistograma{labels: labels, limites: f.limites, contagens: make([]uint64, len(f.limites))}
	f.filhos[k] = s
	f.ordem = append(f.ordem, s)
	return s
}

// Sozinha é o açúcar para a família sem labels.
func (f *FamiliaHistograma) Sozinha() *SerieHistograma { return f.Com() }

// Observa registra uma observação na série.
func (s *SerieHistograma) Observa(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.contagens {
		if v <= s.limites[i] {
			s.contagens[i]++
		}
	}
	s.soma += v
	s.total++
}

// Collect implementa Coletor. Os buckets são CUMULATIVOS, como o formato exige.
func (f *FamiliaHistograma) Collect() []Amostra {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Amostra, 0, len(f.ordem)*(len(f.limites)+3))
	for _, s := range f.ordem {
		s.mu.Lock()
		contagens := make([]uint64, len(s.contagens))
		copy(contagens, s.contagens)
		soma, total := s.soma, s.total
		s.mu.Unlock()

		for i, lim := range f.limites {
			out = append(out, Amostra{
				Familia: f.nome, Nome: f.nome + "_bucket", Ajuda: f.ajuda, Tipo: TipoHistograma,
				Labels: append(copiaLabels(s.labels), Label{Nome: "le", Valor: formataValor(lim)}),
				Valor:  float64(contagens[i]),
			})
		}
		out = append(out, Amostra{
			Familia: f.nome, Nome: f.nome + "_bucket", Ajuda: f.ajuda, Tipo: TipoHistograma,
			Labels: append(copiaLabels(s.labels), Label{Nome: "le", Valor: "+Inf"}),
			Valor:  float64(total),
		})
		out = append(out, Amostra{
			Familia: f.nome, Nome: f.nome + "_sum", Ajuda: f.ajuda, Tipo: TipoHistograma,
			Labels: s.labels, Valor: soma,
		})
		out = append(out, Amostra{
			Familia: f.nome, Nome: f.nome + "_count", Ajuda: f.ajuda, Tipo: TipoHistograma,
			Labels: s.labels, Valor: float64(total),
		})
	}
	return out
}

func copiaLabels(in []Label) []Label {
	out := make([]Label, len(in), len(in)+1)
	copy(out, in)
	return out
}

// --- exposição ---

// WriteTo emite o formato textual do Prometheus (§93).
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	coletores := make([]Coletor, len(r.ordem))
	copy(coletores, r.ordem)
	r.mu.Unlock()

	var sb strings.Builder
	vistas := make(map[string]bool)
	for _, c := range coletores {
		for _, a := range c.Collect() {
			if !vistas[a.Familia] {
				vistas[a.Familia] = true
				if a.Ajuda != "" {
					sb.WriteString("# HELP ")
					sb.WriteString(a.Familia)
					sb.WriteByte(' ')
					sb.WriteString(escapaAjuda(a.Ajuda))
					sb.WriteByte('\n')
				}
				sb.WriteString("# TYPE ")
				sb.WriteString(a.Familia)
				sb.WriteByte(' ')
				sb.WriteString(nomeDoTipo(a.Tipo))
				sb.WriteByte('\n')
			}
			escreveAmostra(&sb, a)
		}
	}
	n, err := io.WriteString(w, sb.String())
	return int64(n), err
}

func escreveAmostra(sb *strings.Builder, a Amostra) {
	sb.WriteString(a.Nome)
	if len(a.Labels) > 0 {
		sb.WriteByte('{')
		for i, l := range a.Labels {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(l.Nome)
			sb.WriteString(`="`)
			sb.WriteString(escapaValorDeLabel(l.Valor))
			sb.WriteByte('"')
		}
		sb.WriteByte('}')
	}
	sb.WriteByte(' ')
	sb.WriteString(formataValor(a.Valor))
	sb.WriteByte('\n')
}

// formataValor evita notação científica para inteiros. 1e7 é aceito pelo
// Prometheus, mas "1e+07" quebra o olho de quem lê o scrape na mão — e o
// scrape lido na mão é a primeira ferramenta de diagnóstico que existe.
func formataValor(v float64) string {
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

// escapaValorDeLabel escapa barra, aspa e quebra de linha. Sem isso, um valor
// vindo de config (nome de tópico, motivo de erro) derruba o scrape INTEIRO,
// não só a própria linha.
func escapaValorDeLabel(s string) string {
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

// escapaAjuda escapa barra e quebra de linha (aspa não é especial no HELP).
func escapaAjuda(s string) string {
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
