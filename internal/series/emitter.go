package series

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Reader é o que a emissão precisa de um contador.
//
// É menos do que o alarme pede, e de propósito: a emissão não avalia condição
// nenhuma, então não precisa da linha montada pela tabela virtual — a chave, o
// texto e o total bastam, e são exatamente o que o dicionário do contador
// entrega sob o seu lock folha.
type Reader interface {
	Limit() uint32
	Group(id uint32) (key, text []byte, total, refs int64, live bool)
}

// Options configura a emissão de UM contador.
type Options struct {
	Dataset string
	Counter string
	// Columns são os nomes das colunas do groupBy, na ordem declarada.
	Columns []string
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

// Emitter escreve periodicamente a série de um contador: uma linha por grupo
// vivo, a cada intervalo.
//
// Não é thread-safe: o dono é o laço.
type Emitter struct {
	opts Options
	log  *logging.Logger
	now  func() time.Time
	w    *Writer

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

	mu     sync.Mutex
	cancel context.CancelFunc
	active atomic.Bool
	inKeep sync.WaitGroup
}

// NewEmitter abre o arquivo da série e devolve o emissor.
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
	w, err := OpenWriter(opts.Dir, opts.File, opts.MaxBytes, opts.Keep)
	if err != nil {
		return nil, err
	}
	e.w = w
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
	e.atBuf = e.now().UTC().AppendFormat(e.atBuf[:0], timeLayout)
	limit := e.opts.Reader.Limit()
	for id := uint32(1); id < limit; id++ {
		key, text, total, _, live := e.opts.Reader.Group(id)
		if !live {
			// Grupo aposentado: saiu do inventário, e continuar a publicá-lo
			// faria o gráfico mostrar um equipamento que não existe.
			continue
		}
		line, err := e.appendLine(e.buf[:0], key, text, total)
		e.buf = line
		if err != nil {
			return err
		}
		if err := e.w.Write(line); err != nil {
			return err
		}
	}
	return e.w.Flush()
}

// appendLine monta a linha NDJSON de um grupo.
//
// Os valores usam o serializador CANÔNICO das respostas de consulta
// (executor.AppendValue), e não uma renderização própria: o slot sai como o
// número 8, e não como a string "8". A série vai para uma ferramenta de
// gráfico, e um número que chega como texto obriga quem consome a converter —
// ou, pior, a ordenar lexicograficamente. Uma segunda renderização também
// divergiria da primeira nos casos de ponta.
func (e *Emitter) appendLine(dst, key, text []byte, total int64) ([]byte, error) {
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
	dst = executor.AppendValue(dst, types.Value{Type: types.TypeInt64, I64: total})
	dst = append(dst, `,"at":`...)
	dst = executor.AppendJSONString(dst, e.atBuf)
	return append(dst, '}'), nil
}

// Flush esvazia o buffer do escritor.
func (e *Emitter) Flush() error { return e.w.Flush() }

// Close para o laço e fecha o arquivo.
func (e *Emitter) Close() error {
	e.Stop()
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

// timeLayout é o formato do instante da linha: RFC3339 com milissegundos em
// UTC, o mesmo das chaves de instante do evento de alarme, para que série e
// alarme sejam comparáveis sem conversão.
const timeLayout = "2006-01-02T15:04:05.000Z"
