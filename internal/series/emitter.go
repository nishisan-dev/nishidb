package series

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Reader é o que a emissão precisa de um contador.
//
// É menos do que o alarme pede, e de propósito: a emissão não avalia condição
// nenhuma, então não precisa da linha montada pela tabela virtual — a chave, o
// texto, o total e os refs (a coluna `rows`) bastam, e são exatamente o que o
// dicionário do contador entrega sob o seu lock folha, o par numa carga só.
type Reader interface {
	Limit() uint32
	Group(id uint32) (key, text []byte, total, refs int64, live bool)
}

// MetricReader lê o valor publicado de UMA métrica de janela.
//
// A interface é declarada aqui, no consumidor, pelo mesmo motivo do Reader
// acima: a emissão só precisa do agregado do grupo, e pede menos do que a
// tabela do contador — o Usage dela alimenta `_meta.counter_metrics`, e a
// série não tem tabela nenhuma para alimentar. Um counters.MetricReader é
// atribuível a este sem conversão, que é o que mantém esta aresta inexistente.
//
// samples == 0 é "sem valor": ou o grupo não teve amostra na janela, ou nunca
// foi amostrado. Com samples > 0, avg é FINITO — a garantia é de quem
// implementa, e não da emissão (ver counters.MetricReader, que explica por que
// a conferência por linha não se paga).
type MetricReader interface {
	Aggregate(id uint32) (samples uint64, avg float64, minimum, maximum int64)
}

// Metric é uma métrica de janela declarada e os agregados que ela gera na
// linha, na ordem declarada.
//
// Reader nil é válido, e é o que o dataset sem amostrador entrega: as colunas
// continuam na linha, todas null. O formato da linha é função do YAML, e não
// de o laço de amostragem estar de pé — é a mesma regra que faz o schema de
// `_counters.<nome>` não depender da fonte (ver counters.MetricSource).
type Metric struct {
	Scale      float64
	Name       string
	Aggregates []string
	Reader     MetricReader
}

// Options configura a emissão de UM contador.
type Options struct {
	DisableTotal bool
	Measures     []schema.CounterMeasureSpec
	// Outputs are asynchronous destinations. Publish must copy retained bytes
	// before returning; the emitter reuses its buffers for the next group.
	Outputs []Output
	// DisableFile selects Kafka-only output; zero preserves legacy callers.
	DisableFile bool
	Dataset     string
	Counter     string
	// Columns são os nomes das colunas do groupBy, na ordem declarada.
	Columns []string
	// Metrics são as métricas de janela do contador, na ordem declarada.
	// Vazio é o contador que não declara `metrics:`, e a linha dele não leva
	// coluna de métrica nenhuma — só as fixas: `total` e `rows` (que entrou
	// para todo contador, logo depois de `total`), entre as colunas do groupBy
	// e as medidas.
	Metrics []Metric
	Reader  Reader
	// Dir é o diretório da série dentro do dataset, e File o nome do arquivo.
	Dir, File string
	Every     time.Duration
	MaxBytes  int64
	Keep      int
	Log       *logging.Logger
	// Now é o relógio. Exclusivo de teste, no molde do UsaRelogio do storage.
	Now func() time.Time
}

// Output accepts a self-contained Kafka record without waiting for network I/O.
type Output interface {
	Publish(key, body []byte, at time.Time)
}

// Emitter escreve periodicamente a série de um contador: uma linha por grupo
// vivo, a cada intervalo.
//
// Não é thread-safe: o dono é o laço.
type Emitter struct {
	measureReader interface {
		MeasureCount(uint32, int, int64) int64
	}
	measurePrefixes                          [][]byte
	atMillis                                 int64
	opts                                     Options
	log                                      *logging.Logger
	now                                      func() time.Time
	w                                        *Writer
	fileFailed                               bool
	kafkaPrefix, kafkaBuf, keyPrefix, keyBuf []byte

	// buf é o scratch da linha, vals o da decomposição da chave e atBuf o do
	// instante. Existem para que a passada não aloque por grupo — num contador
	// por PON são milhares de linhas por intervalo.
	buf   []byte
	vals  []types.Value
	atBuf []byte

	// prefix e colPrefix são as partes FIXAS da linha, já escapadas na
	// construção: o nome do contador e o de cada coluna não mudam pela vida do
	// emissor, e convertê-los de string a cada linha alocaria por grupo.
	prefix    []byte
	colPrefix [][]byte
	// metrics são as métricas ligadas às colunas que geram, com o prefixo de
	// cada uma já escapado pelo mesmo motivo do colPrefix.
	metrics []metricBinding

	// rows, errs e lastSuccess são os acumulados da observabilidade. Atômicos
	// porque o scrape os lê de outra goroutine enquanto a passada os escreve.
	rows        atomic.Uint64
	errs        atomic.Uint64
	lastSuccess atomic.Int64

	mu     sync.Mutex
	cancel context.CancelFunc
	active atomic.Bool
	inKeep sync.WaitGroup
}

// aggregate identifica o agregado de uma coluna de métrica, para a linha ser
// montada por um switch em vez de por comparação de texto por grupo.
//
// O enum é local, e não importado de internal/counters, que tem o seu igual:
// trazê-lo de lá criaria a aresta series -> counters que este pacote foi
// desenhado para não ter (ver Reader). O que NÃO é duplicado é o nome da
// coluna, cujo dono único é schema.MetricColumnName — o nome é contrato, e o
// enum é detalhe de montagem.
type aggregate uint8

const (
	aggregateAvg aggregate = iota
	aggregateMin
	aggregateMax
)

// aggregateOf traduz o agregado declarado. O YAML já recusou o desconhecido;
// a recusa aqui é para a Metric montada sem ele.
func aggregateOf(name string) (aggregate, error) {
	switch name {
	case schema.AggregateAvg:
		return aggregateAvg, nil
	case schema.AggregateMin:
		return aggregateMin, nil
	case schema.AggregateMax:
		return aggregateMax, nil
	default:
		return 0, fmt.Errorf("series: aggregate %q has no column in the emitted line (known: %s, %s, %s)",
			name, schema.AggregateAvg, schema.AggregateMin, schema.AggregateMax)
	}
}

// metricColumn é uma coluna gerada por uma métrica: o prefixo `,"<nome>":` já
// escapado e o agregado que preenche o valor.
type metricColumn struct {
	prefix []byte
	agg    aggregate
}

// metricBinding é uma métrica ligada às colunas que ela gera.
type metricBinding struct {
	scale   float64
	reader  MetricReader
	columns []metricColumn
}

// bindMetrics resolve UMA vez, na construção, o agregado e o prefixo escapado
// de cada coluna gerada. Um contador por PON emite milhares de linhas por
// intervalo, e compor o nome da coluna por linha alocaria em todas elas.
func bindMetrics(metrics []Metric) ([]metricBinding, error) {
	if len(metrics) == 0 {
		return nil, nil
	}
	out := make([]metricBinding, 0, len(metrics))
	for _, m := range metrics {
		b := metricBinding{scale: m.Scale, reader: m.Reader, columns: make([]metricColumn, 0, len(m.Aggregates))}
		for _, a := range m.Aggregates {
			agg, err := aggregateOf(a)
			if err != nil {
				return nil, fmt.Errorf("series: metric %q: %w", m.Name, err)
			}
			p := []byte{','}
			p = executor.AppendJSONString(p, []byte(schema.MetricColumnName(m.Name, a)))
			b.columns = append(b.columns, metricColumn{prefix: append(p, ':'), agg: agg})
		}
		out = append(out, b)
	}
	return out, nil
}

// NewEmitter binds the selected outputs and opens the optional series file.
func NewEmitter(opts Options) (*Emitter, error) {
	if opts.Reader == nil {
		return nil, errors.New("series: Reader obrigatório")
	}
	if opts.Every <= 0 {
		return nil, fmt.Errorf("series: intervalo %v inválido", opts.Every)
	}
	e := &Emitter{opts: opts, log: opts.Log, now: opts.Now}
	if e.log == nil {
		e.log = logging.Discard()
	}
	if e.now == nil {
		e.now = time.Now
	}
	e.prefix = append(e.prefix, `{"counter":`...)
	e.prefix = executor.AppendJSONString(e.prefix, []byte(opts.Counter))
	e.prefix = append(e.prefix, `,"groupKey":`...)
	for _, name := range opts.Columns {
		p := []byte{','}
		p = executor.AppendJSONString(p, []byte(name))
		e.colPrefix = append(e.colPrefix, append(p, ':'))
	}
	metrics, err := bindMetrics(opts.Metrics)
	if err != nil {
		return nil, err
	}
	e.metrics = metrics
	if len(opts.Measures) > 0 {
		var ok bool
		e.measureReader, ok = opts.Reader.(interface {
			MeasureCount(uint32, int, int64) int64
		})
		if !ok {
			return nil, errors.New("series: measure reader required")
		}
		for _, m := range opts.Measures {
			p := executor.AppendJSONString([]byte{','}, []byte(m.Name))
			e.measurePrefixes = append(e.measurePrefixes, append(p, ':'))
		}
	}
	if !opts.DisableFile {
		w, err := OpenWriter(opts.Dir, opts.File, opts.MaxBytes, opts.Keep)
		if err != nil {
			return nil, err
		}
		e.w = w
	}
	if len(opts.Outputs) > 0 {
		e.kafkaPrefix = executor.AppendJSONString([]byte(`{"dataset":`), []byte(opts.Dataset))
		e.kafkaPrefix = append(e.kafkaPrefix, ',')
		e.keyPrefix = executor.AppendJSONString([]byte{'['}, []byte(opts.Dataset))
		e.keyPrefix = executor.AppendJSONString(append(e.keyPrefix, ','), []byte(opts.Counter))
	}
	if e.w == nil && len(opts.Outputs) == 0 {
		return nil, errors.New("series: no output selected")
	}
	return e, nil
}

// EmitOnce escreve uma linha por grupo vivo do contador.
//
// Público para que o teste o chame direto, em vez de esperar um tique — o mesmo
// motivo do AposentaGroupsVazios no storage.
//
// A varredura toma apenas o lock FOLHA do dicionário do contador, e nada do
// Store: é isso que atende, por construção, a exigência da issue de que a
// emissão não segure o writer.
func (e *Emitter) EmitOnce() error {
	// AppendFormat no scratch, e não Format: o segundo devolve uma string
	// nova, e uma alocação por passada num laço que roda para sempre é lixo
	// que nunca para de crescer.
	at := e.now().UTC()
	e.atMillis = at.UnixMilli()
	e.atBuf = at.AppendFormat(e.atBuf[:0], timeLayout)
	limit := e.opts.Reader.Limit()
	for id := uint32(1); id < limit; id++ {
		key, text, total, rows, live := e.opts.Reader.Group(id)
		if !live {
			// Grupo aposentado: saiu do inventário, e continuar a publicá-lo
			// faria o gráfico mostrar um equipamento que não existe.
			continue
		}
		line, err := e.appendLine(e.buf[:0], id, key, text, total, rows)
		e.buf = line
		if err != nil {
			e.errs.Add(1)
			return err
		}
		if len(e.opts.Outputs) > 0 {
			e.kafkaBuf = append(e.kafkaBuf[:0], e.kafkaPrefix...)
			e.kafkaBuf = append(e.kafkaBuf, line[1:]...)
			e.keyBuf = append(e.keyBuf[:0], e.keyPrefix...)
			for _, value := range e.vals {
				e.keyBuf = executor.AppendValue(append(e.keyBuf, ','), value)
			}
			e.keyBuf = append(e.keyBuf, ']')
			for _, output := range e.opts.Outputs {
				output.Publish(e.keyBuf, e.kafkaBuf, at)
			}
		}
		if e.w != nil && !e.fileFailed {
			if err := e.w.Write(line); err != nil {
				if err := e.failFile(err); err != nil {
					return err
				}
			} else {
				e.rows.Add(1)
			}
		}
	}
	if e.w != nil && !e.fileFailed {
		if err := e.w.Flush(); err != nil {
			return e.failFile(err)
		}
		e.lastSuccess.Store(e.now().UnixMilli())
	}
	return nil
}

func (e *Emitter) failFile(err error) error {
	e.errs.Add(1)
	e.fileFailed = true
	if len(e.opts.Outputs) == 0 {
		return err
	}
	e.log.Error("series.file_stopped", "local series output failed; other destinations remain active", "dataset", e.opts.Dataset, "counter", e.opts.Counter)
	return nil
}

// appendLine monta a linha NDJSON de um grupo.
//
// Os valores usam o serializador CANÔNICO das respostas de consulta
// (executor.AppendValue), e não uma renderização própria: o slot sai como o
// número 8, e não como a string "8". A série vai para uma ferramenta de
// gráfico, e um número que chega como texto obriga quem consome a converter —
// ou, pior, a ordenar lexicograficamente. Uma segunda renderização também
// divergiria da primeira nos casos de ponta.
//
// id é o id do grupo, e não só o índice do laço: é a chave com que a métrica
// de janela publica o agregado dele, a mesma que a tabela do contador usa.
//
// A ordem das colunas fixas é a do `SELECT *` da tabela: `total`, `rows`, as
// medidas e as métricas. `rows` nunca é null — também com `total: false`.
func (e *Emitter) appendLine(dst []byte, id uint32, key, text []byte, total, rows int64) ([]byte, error) {
	vals, err := valuekey.DecodeComposite(key, e.vals[:0])
	e.vals = vals
	if err != nil {
		return dst, fmt.Errorf("series: chave do grupo em %q: %w", e.opts.Counter, err)
	}
	dst = append(dst, e.prefix...)
	dst = executor.AppendJSONString(dst, text)
	for i, p := range e.colPrefix {
		if i >= len(vals) {
			break
		}
		dst = append(dst, p...)
		dst = executor.AppendValue(dst, vals[i])
	}
	dst = append(dst, `,"total":`...)
	if e.opts.DisableTotal {
		dst = append(dst, "null"...)
	} else {
		dst = executor.AppendValue(dst, types.Value{Type: types.TypeInt64, I64: total})
	}
	dst = append(dst, `,"rows":`...)
	dst = executor.AppendValue(dst, types.Value{Type: types.TypeInt64, I64: rows})
	for i, m := range e.opts.Measures {
		dst = append(dst, e.measurePrefixes[i]...)
		v := float64(e.measureReader.MeasureCount(id, i, e.atMillis)) * m.Scale()
		dst = executor.AppendValue(dst, types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(v))})
	}
	dst = e.appendMetrics(dst, id)
	dst = append(dst, `,"at":`...)
	dst = executor.AppendJSONString(dst, e.atBuf)
	return append(dst, '}'), nil
}

// appendMetrics acrescenta as colunas das métricas de janela do grupo, na
// ordem declarada, entre o `total` e o `at`.
//
// A chave sai SEMPRE, e o que falta vira null — nunca zero, que é um valor
// legítimo de contador, e nunca chave ausente. É o que executor.AppendRow faz
// com coluna nula, e é o que faz esta linha casar campo a campo com o que um
// `SELECT * FROM _counters.<nome>` devolve para o mesmo grupo. Quem consome a
// série tem esquema de linha fixo, que é o que uma ferramenta de gráfico pede.
//
// Sem fonte (reader nil) todas as colunas saem null: o formato da linha é
// função do YAML, e não de o amostrador estar de pé.
func (e *Emitter) appendMetrics(dst []byte, id uint32) []byte {
	for _, m := range e.metrics {
		var samples uint64
		var avg float64
		var lowest, highest int64
		if m.reader != nil {
			samples, avg, lowest, highest = m.reader.Aggregate(id)
		}
		for _, c := range m.columns {
			dst = append(dst, c.prefix...)
			if samples == 0 {
				dst = append(dst, "null"...)
				continue
			}
			if m.scale != 0 {
				value := avg
				switch c.agg {
				case aggregateMin:
					value = float64(lowest)
				case aggregateMax:
					value = float64(highest)
				}
				dst = executor.AppendValue(dst, types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(value * m.scale))})
				continue
			}
			switch c.agg {
			case aggregateAvg:
				// O double vai pelo mesmo AppendValue do resto da linha, que
				// já devolve null para não-finito. Conferir aqui pagaria por
				// grupo o que é propriedade de quem publica.
				dst = executor.AppendValue(dst, types.Value{
					Type: types.TypeDouble, I64: int64(math.Float64bits(avg)),
				})
			case aggregateMin:
				dst = executor.AppendValue(dst, types.Value{Type: types.TypeInt64, I64: lowest})
			case aggregateMax:
				dst = executor.AppendValue(dst, types.Value{Type: types.TypeInt64, I64: highest})
			}
		}
	}
	return dst
}

// Flush esvazia o buffer do escritor.
func (e *Emitter) Flush() error {
	if e.w == nil {
		return nil
	}
	return e.w.Flush()
}

// Close stops the loop and closes the optional file. The owner closes shared outputs.
func (e *Emitter) Close() error {
	e.Stop()
	if e.w == nil {
		return nil
	}
	return e.w.Close()
}

// Start sobe o laço de emissão. Idempotente.
//
// Molde do StartRetention do storage: primeira passada imediata, medidor
// atômico que sobe FORA da goroutine, cancelamento por contexto, e o Stop
// cancela e AGUARDA.
func (e *Emitter) Start(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.active.Store(true)
	e.inKeep.Add(1)
	go func() {
		defer e.inKeep.Done()
		defer e.active.Store(false)
		e.run(ctx)
	}()
}

// Stop cancela o laço e AGUARDA a passada em voo — cancelar sozinho devolveria
// o controle com uma varredura ainda escrevendo no arquivo. Idempotente.
func (e *Emitter) Stop() {
	e.mu.Lock()
	cancel := e.cancel
	e.cancel = nil
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.inKeep.Wait()
}

// Active diz se o laço está de pé.
func (e *Emitter) Active() bool { return e.active.Load() }

func (e *Emitter) run(ctx context.Context) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := e.EmitOnce(); err != nil {
			if ctx.Err() != nil {
				return
			}
			// Para na PRIMEIRA falha, e para de vez, como os laços irmãos: o
			// que chega aqui é disco cheio ou arquivo inacessível, e a passada
			// seguinte reencontraria o mesmo estado. Um laço que insistisse
			// transformaria uma falha em enxurrada de log.
			e.log.Error("series.stopped",
				"a emissão periódica falhou e o laço parou; a série deste contador não é mais escrita",
				"dataset", e.opts.Dataset, "counter", e.opts.Counter, "error", err.Error())
			return
		}
		t.Reset(e.opts.Every)
	}
}

// Stats é o retrato da emissão de um contador para a observabilidade.
type Stats struct {
	File    bool
	Dataset string
	Counter string
	// Rows são as linhas escritas desde a subida, e Rotations quantas vezes o
	// arquivo foi girado — o primeiro é o volume da série, o segundo diz
	// quanto histórico está sendo descartado por intervalo.
	Rows      uint64
	Rotations uint64
	// Errors distingue "a série parou porque nada mudou" de "a série parou
	// porque a escrita falha".
	Errors uint64
	// Bytes é o tamanho do arquivo corrente.
	Bytes int64
	// LastSuccessUnixMs é a última passada que escreveu sem erro. É a série
	// que um alerta de "a emissão parou" observa.
	LastSuccessUnixMs int64
}

// Stats devolve o retrato corrente.
func (e *Emitter) Stats() Stats {
	st := Stats{
		File:              e.w != nil,
		Dataset:           e.opts.Dataset,
		Counter:           e.opts.Counter,
		Rows:              e.rows.Load(),
		Errors:            e.errs.Load(),
		LastSuccessUnixMs: e.lastSuccess.Load(),
	}
	if e.w != nil {
		st.Rotations, st.Bytes = e.w.Rotations(), e.w.Size()
	}
	return st
}

// timeLayout é o formato do instante da linha: RFC3339 com milissegundos em
// UTC, o mesmo das chaves de instante do evento de alarme, para que série e
// alarme sejam comparáveis sem conversão.
const timeLayout = "2006-01-02T15:04:05.000Z"
