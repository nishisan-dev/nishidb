package materializer

import (
	"errors"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Store é a superfície do storage engine consumida pelo materializer
// (§2.2/§15A). Declarada AQUI, no consumidor, e não no storage: é o que
// permite ao dublê de teste satisfazer o contrato sem importar a
// implementação que dubla.
//
// O Put orquestra TODO o write path — append, validação, diff dos campos
// indexados prev↔novo, publicação atômica da location. O materializer nunca
// toca em índices: a ordem de aplicação precisa viver em um lugar só, porque
// o recovery a re-executa sem passar por aqui.
type Store interface {
	// GetByKey lê o record corrente da key sobre buf (que pode crescer);
	// Record.Key e Record.Payload são janelas zero-copy sobre o buf devolvido.
	//
	// Contrato do buffer: o caller passa buf[:0] e REATRIBUI o slice
	// devolvido; o provedor pode substituí-lo por um maior, mas NUNCA pode
	// preservar o conteúdo anterior (append sobre o que recebeu) — isso faria
	// o buffer do writer crescer sem limite, um evento por vez, sem que
	// nenhum teste de comportamento percebesse.
	GetByKey(key, buf []byte) (types.Record, []byte, bool, error)

	// Put publica o estado novo. prev != nil evita o re-lookup e entrega o
	// payload anterior para o diff dos índices; deve ser o record corrente
	// DESTA key.
	Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error)
}

// Action é o desfecho de um Apply.
type Action uint8

const (
	// ActionCreated: a key não existia; nasceu uma row.
	ActionCreated Action = iota
	// ActionUpdated: merge sobre o estado corrente (mesmo rowId).
	ActionUpdated
	// ActionReplaced: o evento substituiu o estado corrente (mode replace).
	ActionReplaced
	// ActionStale: sequence menor ou igual à corrente — descartado sem
	// nenhuma escrita (§25).
	ActionStale
	// ActionSkipped: evento ruim (uma das 5 categorias §101). A ingestão
	// segue no próximo evento.
	ActionSkipped
	// ActionFailed: falha que NÃO é do evento — writer em modo falho, erro do
	// store ou inconsistência interna. Exige decisão do caller.
	ActionFailed
)

var actionNames = [...]string{"created", "updated", "replaced", "stale", "skipped", "failed"}

func (a Action) String() string {
	if int(a) < len(actionNames) {
		return actionNames[a]
	}
	return "action(?)"
}

// ErrorCategory classifica a falha de um Apply. As cinco primeiras são as
// categorias de evento ruim da spec §101; as três últimas separam o que NÃO é
// culpa do evento — a distinção que impede um bug nosso de desaparecer na
// métrica de fonte ruidosa.
type ErrorCategory uint8

const (
	CategoryNone ErrorCategory = iota
	CategoryInvalidJSON
	CategorySchemaValidation
	CategoryInvalidKey
	CategoryInvalidSequence
	CategoryTypeConversion
	// CategoryWriterFailed: writer em modo falho (sticky, §14.14). Terminal:
	// reprocessar o evento é inútil até o dataset ser reaberto.
	CategoryWriterFailed
	// CategoryStorage: erro do store que não é modo falho (argumento
	// inválido, leitura corrompida, falha do dicionário). Indica bug nosso ou
	// dataset avariado, não evento ruim.
	CategoryStorage
	// CategoryInternal: inconsistência detectada pelo próprio materializer
	// (ErrInternalState).
	CategoryInternal

	numCategories
)

var categoryNames = [...]string{
	CategoryNone:             "none",
	CategoryInvalidJSON:      "INVALID_JSON",
	CategorySchemaValidation: "SCHEMA_VALIDATION",
	CategoryInvalidKey:       "INVALID_KEY",
	CategoryInvalidSequence:  "INVALID_SEQUENCE",
	CategoryTypeConversion:   "TYPE_CONVERSION",
	CategoryWriterFailed:     "WRITER_FAILED",
	CategoryStorage:          "STORAGE_ERROR",
	CategoryInternal:         "INTERNAL_STATE",
}

func (c ErrorCategory) String() string {
	if int(c) < len(categoryNames) {
		return categoryNames[c]
	}
	return "category(?)"
}

// badEvent reporta se a categoria é de evento ruim (§101) — o balde que a
// política log-and-skip cobre.
func (c ErrorCategory) badEvent() bool {
	return c >= CategoryInvalidJSON && c <= CategoryTypeConversion
}

// ApplyResult é o desfecho completo de um evento. Err é nil para Created,
// Updated, Replaced e Stale.
type ApplyResult struct {
	Action   Action
	RowID    types.RowID
	Category ErrorCategory
	Err      error
}

// Fatal reporta que a ingestão precisa PARAR: o writer está em modo falho e
// nenhum evento vai ser aceito até o dataset reabrir. Sem essa distinção, a
// política log-and-skip vira um skip infinito e a ingestão descarta tudo em
// silêncio.
func (r ApplyResult) Fatal() bool { return r.Category == CategoryWriterFailed }

// Metrics são os contadores de ingestão do milestone (§2.9). São atomic
// porque quem os LÊ é outra goroutine (exporter/harness), não o writer.
type Metrics struct {
	events   atomic.Int64
	created  atomic.Int64
	updated  atomic.Int64
	replaced atomic.Int64
	stale    atomic.Int64
	invalid  atomic.Int64
	failed   atomic.Int64
	byCat    [numCategories]atomic.Int64
}

// MetricsSnapshot é a leitura consistente-por-contador dos contadores. Não é
// um instantâneo atômico do conjunto: os campos são lidos em sequência, o que
// basta para observabilidade e para os testes (que leem com o writer parado).
type MetricsSnapshot struct {
	EventsTotal   int64
	CreatedTotal  int64
	UpdatedTotal  int64
	ReplacedTotal int64
	StaleTotal    int64
	// InvalidTotal soma as 5 categorias §101 (eventos ruins, Skipped).
	InvalidTotal int64
	// FailedTotal soma o que não é culpa do evento (Failed).
	FailedTotal int64
	ByCategory  [numCategories]int64
}

// Materializer aplica eventos JSON sobre o estado materializado de um
// dataset.
//
// NÃO é thread-safe: pertence à goroutine do writer único (spec §28) e
// reutiliza todos os buffers entre eventos. Duas goroutines chamando Apply
// corrompem o patch e o payload em construção.
type Materializer struct {
	schema *schema.Compiled
	store  Store

	patch   PatchBuffer
	prevRec types.Record // estado corrente lido; janela sobre prevBuf
	prevBuf []byte       // destino do GetByKey
	outBuf  []byte       // payload de saída

	metrics Metrics
}

// New monta o materializer do dataset sobre o store.
func New(c *schema.Compiled, s Store) *Materializer {
	return &Materializer{schema: c, store: s}
}

// Metrics devolve os contadores acumulados.
//
// A leitura é contador a contador, NÃO um instantâneo atômico do conjunto:
// com o writer ativo, EventsTotal pode já contar um evento cujo desfecho
// ainda não entrou em Created/Stale/Invalid. Serve a observabilidade (onde a
// defasagem de um evento não muda nada) e aos testes, que leem com o writer
// parado — não use como base de decisão de controle de fluxo.
func (m *Materializer) Metrics() MetricsSnapshot {
	s := MetricsSnapshot{
		EventsTotal:   m.metrics.events.Load(),
		CreatedTotal:  m.metrics.created.Load(),
		UpdatedTotal:  m.metrics.updated.Load(),
		ReplacedTotal: m.metrics.replaced.Load(),
		StaleTotal:    m.metrics.stale.Load(),
		InvalidTotal:  m.metrics.invalid.Load(),
		FailedTotal:   m.metrics.failed.Load(),
	}
	for i := range s.ByCategory {
		s.ByCategory[i] = m.metrics.byCat[i].Load()
	}
	return s
}

// Apply materializa um evento (§2.9): parse tipado, leitura do estado
// corrente, sequence check, merge ou replace, e publicação pelo Put.
//
// O event só precisa viver até o retorno: tudo o que sobrevive foi copiado
// para os buffers internos ou já foi gravado pelo store.
//
// Erros nunca escapam como panic — a política de ingestão da spec §101 é por
// evento, e um panic derrubaria o loop inteiro por causa de um único evento
// malformado. O desfecho vem no ApplyResult: Skipped para evento ruim,
// Failed para o que não é culpa do evento (com Fatal marcando o writer em
// modo falho, quando o caller precisa parar de vez).
func (m *Materializer) Apply(event []byte) ApplyResult {
	m.metrics.events.Add(1)

	if err := m.patch.Parse(event, m.schema); err != nil {
		return m.failure(err)
	}

	var (
		found bool
		err   error
	)
	// O buf devolvido é reatribuído SEMPRE (pode ter crescido), inclusive no
	// erro — descartá-lo faria o próximo evento realocar do zero.
	m.prevRec, m.prevBuf, found, err = m.store.GetByKey(m.patch.key, m.prevBuf[:0])
	if err != nil {
		return m.failure(err)
	}

	// §25: sequence menor ou igual à corrente é descarte SEM escrita — sem
	// merge, sem append, sem índices. O record já veio do GetByKey (a
	// validação de colisão da primary obriga a lê-lo de qualquer jeito), então
	// o stale check não custa I/O algum.
	if found && m.schema.Seq != nil && m.patch.seq <= m.prevRec.Sequence {
		m.metrics.stale.Add(1)
		return ApplyResult{Action: ActionStale, RowID: m.prevRec.RowID}
	}

	// prevRec.Payload aponta para prevBuf e outBuf é um buffer DISTINTO: o
	// merge lê de um e escreve no outro sem risco de sobrescrita.
	replace := m.schema.Mode == schema.ModeReplace
	var payload []byte
	if found && !replace {
		payload, err = mergeRecords(m.prevRec.Payload, m.patch.entries, m.outBuf[:0])
	} else {
		// Create (não há estado anterior) e replace (§23, o estado anterior é
		// descartado) chegam ao mesmo lugar: o payload é só o patch.
		payload, err = encodePatch(m.patch.entries, m.outBuf[:0])
	}
	m.outBuf = payload
	if err != nil {
		return m.failure(err)
	}

	// prev entrega ao Put o rowId e o payload anterior — sem ele o Put refaz o
	// lookup e o diff de índices perde a base de comparação. O endereço é o de
	// um campo do materializer, não de uma local: tomar o endereço de uma
	// variável de pilha aqui a faria escapar para o heap a cada evento.
	var prev *types.Record
	if found {
		prev = &m.prevRec
	}
	rowID, created, err := m.store.Put(m.patch.key, m.patch.seq, 0, payload, prev)
	if err != nil {
		return m.failure(err)
	}

	switch {
	case created:
		m.metrics.created.Add(1)
		return ApplyResult{Action: ActionCreated, RowID: rowID}
	case replace:
		m.metrics.replaced.Add(1)
		return ApplyResult{Action: ActionReplaced, RowID: rowID}
	default:
		m.metrics.updated.Add(1)
		return ApplyResult{Action: ActionUpdated, RowID: rowID}
	}
}

// failure classifica o erro, contabiliza na categoria e escolhe entre Skipped
// (evento ruim, a ingestão segue) e Failed (não é culpa do evento).
func (m *Materializer) failure(err error) ApplyResult {
	cat := classify(err)
	m.metrics.byCat[cat].Add(1)
	if cat.badEvent() {
		m.metrics.invalid.Add(1)
		return ApplyResult{Action: ActionSkipped, Category: cat, Err: err}
	}
	m.metrics.failed.Add(1)
	return ApplyResult{Action: ActionFailed, Category: cat, Err: err}
}

// classify mapeia o erro para a categoria §101 (mais as três que a spec não
// nomeia). O default é CategoryStorage porque todo erro que chega aqui sem
// sentinela conhecido veio do store ou do dicionário — tratá-lo como evento
// ruim faria a ingestão pular em silêncio um dataset avariado.
func classify(err error) ErrorCategory {
	switch {
	case errors.Is(err, jsonp.ErrInvalidJSON):
		return CategoryInvalidJSON
	case errors.Is(err, schema.ErrUnknownField):
		return CategorySchemaValidation
	case errors.Is(err, ErrInvalidKey):
		return CategoryInvalidKey
	case errors.Is(err, ErrInvalidSequence):
		return CategoryInvalidSequence
	case errors.Is(err, ErrTypeConversion):
		return CategoryTypeConversion
	case errors.Is(err, types.ErrWriterFailed):
		return CategoryWriterFailed
	case errors.Is(err, ErrInternalState):
		return CategoryInternal
	default:
		return CategoryStorage
	}
}
