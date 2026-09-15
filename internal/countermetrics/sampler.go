// Package countermetrics é o amostrador das métricas de janela dos contadores:
// a cada poucos segundos lê o total de cada grupo vivo, dobra a amostra no
// balde aberto em heap, grava o balde quando ele fecha e publica, por grupo, o
// agregado da janela (quantidade de amostras, avg, min e max) para leitura
// concorrente sem lock e sem alocação.
//
// # Por que mora aqui
//
// Fora de internal/counters e de internal/storage porque tem RELÓGIO — o mesmo
// argumento de internal/alarm e internal/series. O storage não pode ter estado
// que dependa do relógio no caminho de aplicação: tudo o que o applyRecord
// alimenta tem de ser reconstruível a partir do payload, e uma média "dos
// últimos cinco minutos" não é. O amostrador lê os contadores só pelo lock
// FOLHA do dicionário (Limit/Group) e nunca toma o lock do writer.
//
// # Dono dos arquivos
//
// Cada métrica tem um arquivo metricwindow em <Dir>/<counter>/<metric>.win, e
// o amostrador é o ÚNICO dono dele: o *metricwindow.File não é thread-safe e
// só a goroutine do laço (ou, antes do Start e depois do Stop, quem abriu) o
// toca. Quem consulta pelo SQL ou pelo alarme lê o que o amostrador publica em
// heap (ver Metric.Aggregate), nunca o mmap.
//
// # Posição, e não id
//
// Arquivos e entradas em heap são endereçados pela POSIÇÃO de um grupo vivo,
// reaproveitada quando o grupo se aposenta, e não pelo id do grupo, que o
// dicionário nunca reaproveita. O custo fica limitado pelo máximo de grupos
// vivos simultâneos, mais 4 B por id já emitido no mapa id → posição de cada
// contador. As contas estão em counterState.
package countermetrics

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

const (
	// minSampleInterval é o piso do intervalo de amostragem. Amostra mais fina
	// que um segundo não muda nada que o operador enxergue — o menor balde
	// declarável (schema.MinMetricWindow / schema.MetricWindowBuckets) já tem
	// 1 s — e custa uma varredura inteira dos grupos a cada vez.
	//
	// É também o piso que faz a contagem de amostras de um balde caber nos 16
	// bits do formato em disco: o maior balde (schema.MaxMetricWindow /
	// schema.MetricWindowBuckets = 12 h) amostrado a cada 1 s dá 43.200
	// amostras, abaixo de 65.535. Baixar este piso exige refazer essa conta, e
	// há um teste que a refaz a partir das constantes.
	minSampleInterval = time.Second
	// maxSampleInterval é o teto do intervalo. Numa janela de 24 h o balde tem
	// 24 min, e amostrar uma vez por balde deixaria min e max cegos a qualquer
	// pico ou vale que durasse menos que isso; 10 s mantém a resolução do min e
	// do max sem que a janela longa dite a cadência.
	maxSampleInterval = 10 * time.Second

	// maxGroupID é o maior id de grupo que um contador emite. O teto é do
	// DICIONÁRIO, e não do maxGroups do schema: Dict.Intern
	// (internal/storage/counter/dict.go) recusa internar chave nova quando já
	// há math.MaxInt32 chaves, e os ids começam em 1. maxGroups limita os
	// grupos VIVOS; id de grupo aposentado nunca é reaproveitado. É o limite do
	// mapa id → posição (ver checkLimit).
	maxGroupID = math.MaxInt32
	// fileCapacityLimit é o teto de capacidade de cada arquivo metricwindow:
	// maxGroupID+1 = 1<<31, potência de 2 e a maior capacidade que o campo
	// capacity do formato guarda.
	//
	// É teto de formato, e não o tamanho esperado: o arquivo é endereçado pela
	// posição de grupo vivo, reaproveitada, e a posição nunca passa do máximo
	// de grupos vivos simultâneos, que nunca passa dos ids. A capacidade real é
	// a potência de 2 (piso 1.024) acima desse máximo.
	fileCapacityLimit uint32 = maxGroupID + 1

	// sourceCodeTotal é o código numérico da fonte schema.MetricSourceTotal no
	// campo Source do cabeçalho do arquivo. É FORMATO DE DISCO: mudar o número
	// faz toda abertura seguinte ver cabeçalho divergente e recriar o arquivo,
	// perdendo a janela. Fonte nova recebe número novo; este nunca muda.
	sourceCodeTotal uint32 = 1

	// fileExtension é a extensão do arquivo de uma métrica. A limpeza de
	// órfãos só apaga arquivo com ela (ou com ela seguida do sufixo do tmp de
	// crescimento do metricwindow).
	fileExtension = ".win"
	// tmpSuffix é o sufixo do arquivo temporário que o metricwindow usa ao
	// crescer ou recriar um arquivo (<metric>.win.tmp).
	tmpSuffix = ".tmp"
)

// ErrReadOnly marca uma operação de escrita num amostrador aberto com
// Options.ReadOnly: ele publica o que está em disco e nunca amostra.
var ErrReadOnly = errors.New("countermetrics: sampler opened read-only")

// ErrClosed marca o uso de um amostrador depois de Close.
var ErrClosed = errors.New("countermetrics: sampler is closed")

// stopWaitHook é o seam de teste do intervalo entre o cancelamento do laço e o
// Wait do Stop (nil em produção): é ali que um Start concorrente subiria um
// segundo laço se o Stop soltasse mu antes de o laço anterior sair. O custo é
// uma comparação com nil por Stop.
var stopWaitHook func()

// loopTrace é o seam de teste que conta laços vivos (nil em produção): +1 ao
// entrar em run e -1 ao sair. O custo é uma comparação com nil por subida do
// laço, não por passada.
var loopTrace func(delta int)

// Reader é o que o amostrador precisa de um contador: o limite dos ids e, por
// id, a chave, o total e se o grupo está vivo. Definido no consumidor, com só
// o que ele usa; é satisfeito por *counter.Counter, que responde sob o lock
// folha do dicionário, com chave estável e sem alocar.
//
// O Reader é chamado de dentro da passada, na goroutine do laço. Ele NÃO pode
// chamar Stop nem Close do amostrador: os dois esperam a goroutine do laço
// sair, e ela está parada dentro do Reader — trava para sempre.
type Reader interface {
	Limit() uint32
	Group(id uint32) (key, text []byte, total, refs int64, live bool)
}

// CounterInput é um contador com métricas declaradas.
type CounterInput struct {
	// Spec é o contador validado; Spec.Metrics não pode ser vazio.
	// Spec.MaxGroups não entra aqui: o tamanho vem dos grupos vivos que o
	// amostrador de fato vê (ver counterState).
	Spec schema.CounterSpec
	// Reader lê os grupos do contador.
	Reader Reader
}

// Options configura a abertura do amostrador de UM dataset.
type Options struct {
	Dataset string
	// Dir é o diretório base dos arquivos das métricas do dataset.
	Dir string
	// Counters são os contadores que declaram métricas, e só eles.
	Counters []CounterInput
	Log      *logging.Logger
	// Now é o relógio. Exclusivo de teste, no molde do series.Options.
	Now func() time.Time
	// ReadOnly abre os arquivos só para leitura (CLI local): publica o que
	// está em disco pelo mesmo código da restauração, nunca escreve e nunca
	// amostra. Arquivo ausente vira métrica sem valores e SEM log: é o estado
	// normal de um dataset cujo servidor ainda não amostrou. Arquivo que existe
	// e é ilegível, ou tem cabeçalho divergente, vira métrica sem valores com
	// WARN counter_metric.unreadable.
	ReadOnly bool
}

// Sampler mantém as métricas de janela de todos os contadores de um dataset.
//
// SampleOnce não é thread-safe: com o laço de pé, só o laço o chama. Os
// métodos de leitura (Metric, Stats, Active, LastSampleSeconds,
// RestoredGroups e os de *Metric) são seguros de qualquer goroutine.
type Sampler struct {
	dataset  string
	dir      string
	log      *logging.Logger
	now      func() time.Time
	readOnly bool
	// counters e metrics são imutáveis depois do Open, na ordem declarada;
	// metrics é a lista achatada, contador → métrica.
	counters []*counterState
	metrics  []*Metric
	interval time.Duration
	// restoredGroups é fixado no Open e não muda mais.
	restoredGroups int64

	passNanos atomic.Int64

	// mu serializa Start, Stop e Close INTEIROS: o Stop o segura através do
	// Wait e da gravação, então um Start concorrente só roda depois de o laço
	// anterior ter saído, e nunca há dois laços. Não há deadlock porque a
	// goroutine do laço nunca toma mu — SampleOnce consulta closed, que é
	// atômico, e o resto do laço usa só atômicos e o log. A exceção é um
	// Reader que chame Stop ou Close (ver Reader).
	mu      sync.Mutex
	cancel  context.CancelFunc
	closed  atomic.Bool
	active  atomic.Bool
	running sync.WaitGroup
}

// Open abre os arquivos de todas as métricas declaradas, remove os órfãos (só
// em modo escrita) e restaura, contador a contador, o mapa id → posição e o
// publicado a partir do disco, relativo a Options.Now. Não sobe o laço: isso é
// o Start.
//
// Erro de validação das opções, de criação de diretório, de remoção de órfão,
// de abertura de arquivo ou de restauração (em modo escrita) devolve Sampler
// nil, com os arquivos já abertos fechados.
func Open(opts Options) (*Sampler, error) {
	if opts.Dataset == "" {
		return nil, errors.New("countermetrics: dataset name is required")
	}
	if opts.Dir == "" {
		return nil, fmt.Errorf("countermetrics: dataset %s: directory is required", opts.Dataset)
	}
	if err := validateCounters(opts.Dataset, opts.Counters); err != nil {
		return nil, err
	}
	s := &Sampler{dataset: opts.Dataset, dir: opts.Dir, log: opts.Log, now: opts.Now, readOnly: opts.ReadOnly}
	if s.log == nil {
		s.log = logging.Discard()
	}
	if s.now == nil {
		s.now = time.Now
	}
	if !s.readOnly {
		if err := os.MkdirAll(s.dir, 0o755); err != nil {
			return nil, fmt.Errorf("countermetrics: dataset %s: %w", s.dataset, err)
		}
		if err := s.removeOrphans(opts.Counters); err != nil {
			return nil, err
		}
	}
	for _, in := range opts.Counters {
		c := &counterState{name: in.Spec.Name, reader: in.Reader, next: 1}
		for _, spec := range in.Spec.Metrics {
			m, err := s.openMetric(c, spec)
			if err != nil {
				_ = s.closeFiles()
				return nil, err
			}
			c.metrics = append(c.metrics, m)
			s.metrics = append(s.metrics, m)
		}
		c.epochs = make([]uint32, len(c.metrics))
		s.counters = append(s.counters, c)
	}
	now := s.now()
	for _, c := range s.counters {
		restored, err := c.restore(now, s.readOnly)
		if err != nil {
			_ = s.closeFiles()
			return nil, fmt.Errorf("countermetrics: dataset %s: counter %q: %w", s.dataset, c.name, err)
		}
		s.restoredGroups += restored
	}
	s.interval = sampleInterval(s.metrics)
	return s, nil
}

// validateCounters recusa a entrada que produziria caminho ambíguo ou arquivo
// sem leitura. O schema já validou o YAML; o que chega aqui fora disso é bug de
// quem monta as opções, e recusar alto é mais barato que descobrir depois dois
// contadores gravando no mesmo arquivo.
func validateCounters(dataset string, counters []CounterInput) error {
	seen := make(map[string]bool, len(counters))
	for _, c := range counters {
		name := c.Spec.Name
		if !isPathSegment(name) {
			return fmt.Errorf("countermetrics: dataset %s: counter name %q is not a plain path segment", dataset, name)
		}
		if seen[name] {
			return fmt.Errorf("countermetrics: dataset %s: counter %q passed twice", dataset, name)
		}
		seen[name] = true
		if len(c.Spec.Metrics) == 0 {
			return fmt.Errorf("countermetrics: dataset %s: counter %q declares no metrics", dataset, name)
		}
		if c.Reader == nil {
			return fmt.Errorf("countermetrics: dataset %s: counter %q has no reader", dataset, name)
		}
		metrics := make(map[string]bool, len(c.Spec.Metrics))
		for _, m := range c.Spec.Metrics {
			if !isPathSegment(m.Name) {
				return fmt.Errorf("countermetrics: dataset %s: counter %q: metric name %q is not a plain path segment", dataset, name, m.Name)
			}
			if metrics[m.Name] {
				return fmt.Errorf("countermetrics: dataset %s: counter %q: metric %q declared twice", dataset, name, m.Name)
			}
			metrics[m.Name] = true
			if _, err := metricHeader(m); err != nil {
				return fmt.Errorf("countermetrics: dataset %s: counter %q: %w", dataset, name, err)
			}
		}
	}
	return nil
}

// isPathSegment diz se o nome serve, sozinho, de um componente de caminho.
func isPathSegment(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name
}

// metricHeader é o cabeçalho do arquivo de uma métrica. A largura do balde é
// janela ÷ schema.MetricWindowBuckets em milissegundos, sem arredondamento: o
// schema só aceita janela múltipla de 1m, e uma divisão com resto aqui é bug.
func metricHeader(m schema.CounterMetricSpec) (metricwindow.Header, error) {
	if m.Type != schema.MetricTypeWindow {
		return metricwindow.Header{}, fmt.Errorf("metric %q: type %q unsupported", m.Name, m.Type)
	}
	if m.Source != schema.MetricSourceTotal {
		return metricwindow.Header{}, fmt.Errorf("metric %q: source %q unsupported", m.Name, m.Source)
	}
	ms := m.Window.Milliseconds()
	if ms <= 0 || ms%schema.MetricWindowBuckets != 0 || m.Window%time.Millisecond != 0 {
		return metricwindow.Header{}, fmt.Errorf("metric %q: window %s does not split into %d whole-millisecond buckets", m.Name, m.Window, schema.MetricWindowBuckets)
	}
	return metricwindow.Header{
		BucketCount:   schema.MetricWindowBuckets,
		BucketWidthMs: ms / schema.MetricWindowBuckets,
		Source:        sourceCodeTotal,
	}, nil
}

// openMetric abre o arquivo de uma métrica. Em modo escrita, reset do
// cabeçalho vira WARN e ResetsTotal; em modo leitura, arquivo ausente vira
// métrica sem arquivo (sem valores) e sem log, e arquivo ilegível ou de outra
// forma vira métrica sem arquivo com WARN.
//
// A capacidade pedida é o piso do formato: o arquivo cresce sob demanda, com
// as posições que a passada atribui, e um arquivo existente mantém a sua.
func (s *Sampler) openMetric(c *counterState, spec schema.CounterMetricSpec) (*Metric, error) {
	header, err := metricHeader(spec)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(s.dir, c.name)
	path := filepath.Join(dir, spec.Name+fileExtension)
	m := &Metric{
		counter: c.name,
		name:    spec.Name,
		window:  spec.Window,
		widthMs: header.BucketWidthMs,
		state:   c,
	}
	if s.readOnly {
		f, err := metricwindow.OpenReadOnly(path)
		if errors.Is(err, fs.ErrNotExist) {
			// Arquivo ausente não é anomalia e não se registra, no precedente do
			// estado dos alarmes (internal/alarm/runner.go): é o dataset cujo
			// servidor ainda não amostrou — quem só ingere e consulta, ou quem
			// acabou de declarar a métrica. Avisar aqui a cada consulta ensinaria
			// o operador a ignorar o WARN que importa, o de arquivo corrompido.
			return m, nil
		}
		if err == nil && f.Header() != header {
			err = fmt.Errorf("header %+v differs from the declared %+v", f.Header(), header)
			_ = f.Close()
		}
		if err != nil {
			s.log.Warn("counter_metric.unreadable",
				"the window metric file could not be read; the metric has no values in this read-only session",
				"dataset", s.dataset, "counter", m.counter, "metric", m.name, "path", path, "error", err.Error())
			return m, nil
		}
		m.file = f
		m.diskBytes.Store(f.SizeBytes())
		return m, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("countermetrics: dataset %s: %w", s.dataset, err)
	}
	f, reset, err := metricwindow.Open(path, header, 0, fileCapacityLimit)
	if err != nil {
		return nil, fmt.Errorf("countermetrics: dataset %s: counter %q: metric %q: %w", s.dataset, m.counter, m.name, err)
	}
	m.file = f
	m.diskBytes.Store(f.SizeBytes())
	if reset {
		m.resets.Add(1)
		s.log.Warn("counter_metric.reset",
			"the window metric file had an unreadable or different header and was recreated; the window history of this metric was lost",
			"dataset", s.dataset, "counter", m.counter, "metric", m.name, "path", path)
	}
	return m, nil
}

// sampleInterval é clamp(menor janela / schema.MetricWindowBuckets,
// minSampleInterval, maxSampleInterval): a cadência acompanha o menor balde
// declarado, e zero quando não há métrica — a resposta que faz o laço não
// subir.
func sampleInterval(metrics []*Metric) time.Duration {
	smallest := time.Duration(0)
	for _, m := range metrics {
		if smallest == 0 || m.window < smallest {
			smallest = m.window
		}
	}
	if smallest == 0 {
		return 0
	}
	return min(max(smallest/schema.MetricWindowBuckets, minSampleInterval), maxSampleInterval)
}

// SampleOnce faz uma passada sobre todos os contadores, com o relógio de
// Options.Now. Público para que o teste o chame direto, em vez de esperar um
// tique — o mesmo motivo do series.Emitter.EmitOnce.
//
// Para na primeira falha e a devolve; os contadores e os grupos anteriores ao
// que falhou já dobraram a amostra e publicaram, e o sampledAt das métricas do
// contador que falhou não avança. Toda falha daqui é de infra (crescimento do
// arquivo, teto de capacidade, arquivo fechado), nunca dado ruim.
func (s *Sampler) SampleOnce() error {
	if s.readOnly {
		return ErrReadOnly
	}
	if s.closed.Load() {
		return ErrClosed
	}
	started := time.Now()
	now := s.now()
	for _, c := range s.counters {
		if err := c.sample(now); err != nil {
			return fmt.Errorf("countermetrics: dataset %s: counter %q: %w", s.dataset, c.name, err)
		}
	}
	s.passNanos.Store(int64(time.Since(started)))
	return nil
}

// Start sobe o laço de amostragem. Idempotente. Não sobe nada em modo
// leitura, depois de Close ou sem métrica declarada: não se paga goroutine
// para não fazer nada. Chamado durante um Stop, espera o Stop terminar (ver
// Sampler.mu).
//
// Molde do series.Emitter e do alarm.Runner: primeira passada imediata,
// medidor atômico que sobe FORA da goroutine, cancelamento por contexto.
func (s *Sampler) Start(ctx context.Context) {
	if s.readOnly || s.interval == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil || s.closed.Load() {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.active.Store(true)
	s.running.Add(1)
	go func() {
		defer s.running.Done()
		// Declarado depois do Done e por isso roda antes dele: o Stop devolve o
		// controle com o medidor já em zero.
		defer s.active.Store(false)
		s.run(ctx)
	}()
}

// run é o laço.
func (s *Sampler) run(ctx context.Context) {
	if loopTrace != nil {
		loopTrace(1)
		defer loopTrace(-1)
	}
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := s.SampleOnce(); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Para na PRIMEIRA falha, e para de vez, como os laços irmãos: o que
			// chega aqui é arquivo que não cresce (disco cheio, permissão) ou
			// arquivo fechado por um crescimento que falhou, e a passada seguinte
			// reencontraria o mesmo estado. O publicado congela com o sampledAt
			// visível em Usage.
			s.log.Error("counter_metric.stopped",
				"window metric sampling failed and the loop stopped; the window metrics of this dataset are frozen",
				"dataset", s.dataset, "error", err.Error())
			return
		}
		t.Reset(s.interval)
	}
}

// Stop cancela o laço, AGUARDA a goroutine dele sair e grava os baldes
// abertos que mudaram desde a última gravação, com msync de cada arquivo.
// Idempotente de verdade: um segundo Stop sem amostra nova no meio não grava
// balde nenhum (ColumnWrites não muda), só refaz o msync. Pode ser chamado sem
// nunca ter havido laço, mas NUNCA de dentro do Reader (ver Reader).
//
// # Por que gravar aqui
//
// O balde aberto só vai ao disco quando fecha. Sem esta gravação, um reinício
// dentro do mesmo balde perderia as amostras dele — numa janela de 24 h, até
// 24 min de amostras a cada deploy. Com ela, a restauração recarrega o balde
// aberto e a contagem continua. Num crash sobra o que já tinha fechado: o
// arquivo é telemetria, fora da ordem de durabilidade do checkpoint.
//
// Falha na gravação vira WARN counter_metric.state_not_saved, e não erro: o
// encerramento segue, e o custo é o balde aberto, não o dataset.
func (s *Sampler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopLocked()
}

// stopLocked é o Stop com mu já tomado. mu fica tomado através do Wait: é o
// que impede um Start concorrente de subir um laço enquanto o anterior ainda
// não saiu (ver Sampler.mu).
func (s *Sampler) stopLocked() {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if stopWaitHook != nil {
		stopWaitHook()
	}
	s.running.Wait()
	// Depois do Wait não há passada em voo, e mu impede o Close de fechar os
	// arquivos no meio da gravação.
	if s.readOnly || s.closed.Load() {
		return
	}
	var errs []error
	for _, m := range s.metrics {
		if err := m.saveOpenBuckets(); err != nil {
			errs = append(errs, fmt.Errorf("counter %q: metric %q: %w", m.counter, m.name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		s.log.Warn("counter_metric.state_not_saved",
			"the open buckets were not saved on shutdown; a restart loses the samples of the current bucket",
			"dataset", s.dataset, "error", err.Error())
	}
}

// Close é Stop seguido do fechamento dos arquivos, sob o mesmo mu — sem
// brecha para um Start subir um laço entre a parada e o fechamento.
// Idempotente, e nunca de dentro do Reader. Depois dele, Metric continua
// respondendo o último publicado.
func (s *Sampler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopLocked()
	if s.closed.Swap(true) {
		return nil
	}
	return s.closeFiles()
}

// closeFiles fecha os arquivos de todas as métricas abertas até aqui.
func (s *Sampler) closeFiles() error {
	var errs []error
	for _, m := range s.metrics {
		if m.file == nil {
			continue
		}
		if err := m.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("countermetrics: dataset %s: counter %q: metric %q: %w", s.dataset, m.counter, m.name, err))
		}
	}
	return errors.Join(errs...)
}

// Active diz se o laço está de pé. Vai para a observabilidade: métrica
// congelada é indistinguível de grupo estável sem esta série.
func (s *Sampler) Active() bool { return s.active.Load() }

// LastSampleSeconds é a duração da última passada completa, em segundos (zero
// antes da primeira). Medida no relógio de parede do processo, não no de
// Options.Now.
func (s *Sampler) LastSampleSeconds() float64 {
	return time.Duration(s.passNanos.Load()).Seconds()
}

// RestoredGroups é quantas entradas saíram da abertura já com amostra na
// janela, somado sobre as métricas.
func (s *Sampler) RestoredGroups() int64 { return s.restoredGroups }

// Metric devolve a leitura de uma métrica declarada. É true para TODA métrica
// de todo contador passado ao Open — inclusive em modo leitura com arquivo
// ausente ou ilegível (a leitura responde sem valores) e depois de o laço
// parar por erro (responde o publicado congelado); false só para nome não
// declarado.
//
// Devolve o tipo concreto; quem consome pela interface do consumidor
// (counters.MetricSource) recebe o adaptador de Sampler.MetricSource. Não
// aloca.
func (s *Sampler) Metric(counter, metric string) (*Metric, bool) {
	for _, m := range s.metrics {
		if m.counter == counter && m.name == metric {
			return m, true
		}
	}
	return nil, false
}

// MetricStats é o retrato de uma métrica para a observabilidade.
type MetricStats struct {
	Dataset string
	Counter string
	Name    string
	// Groups é quantas posições do contador estão ocupadas por grupos vivos.
	Groups int64
	// HeapBytes é a estimativa das entradas em heap da métrica (ver
	// Metric.Usage), limitada pelo máximo de grupos vivos simultâneos.
	HeapBytes int64
	// PositionMapBytes é o mapa id → posição do CONTADOR, 4 B por id já
	// emitido. É o mesmo mapa em todas as métricas do contador: somar esta
	// coluna sobre as métricas de um contador conta o mapa mais de uma vez.
	PositionMapBytes int64
	// DiskBytes é o tamanho do arquivo mapeado.
	DiskBytes int64
	// ResetsTotal é quantas vezes o arquivo foi recriado por cabeçalho
	// divergente ou ilegível.
	ResetsTotal int64
	// ColumnWrites é quantos baldes foram gravados no arquivo: um por grupo
	// por balde fechado com amostra nova, mais os abertos alterados gravados
	// no Stop.
	ColumnWrites int64
	// NewGroups é quantas vezes uma entrada ganhou dono novo nesta métrica:
	// grupo novo, posição reaproveitada de um aposentado, ou id que o contador
	// reconstruído deu a outra chave. É a medida da rotatividade que o reuso de
	// posição absorve.
	NewGroups int64
	// SampledAtUnixMs é o instante (Options.Now) da última passada completa
	// sobre esta métrica; zero é "nunca amostrou".
	SampledAtUnixMs int64
}

// Stats devolve o retrato de todas as métricas, na ordem contador → métrica
// declarada.
func (s *Sampler) Stats() []MetricStats {
	out := make([]MetricStats, 0, len(s.metrics))
	for _, m := range s.metrics {
		groups, heap, disk, sampledAt := m.Usage()
		out = append(out, MetricStats{
			Dataset:          s.dataset,
			Counter:          m.counter,
			Name:             m.name,
			Groups:           groups,
			HeapBytes:        heap,
			PositionMapBytes: m.state.positions.bytes.Load(),
			DiskBytes:        disk,
			ResetsTotal:      m.resets.Load(),
			ColumnWrites:     m.columnWrites.Load(),
			NewGroups:        m.newGroups.Load(),
			SampledAtUnixMs:  sampledAt,
		})
	}
	return out
}
