package materializer

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/predicado"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
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
	//
	// O buffer precisa voltar em TODOS os caminhos, inclusive key ausente e
	// erro: devolver nil ali faz o materializer perder o buffer e realocar a
	// cada evento, e isso não aparece em nenhum teste de comportamento — só
	// no gate de alocação.
	// Estado devolve os TRÊS estados da key (§58). São três e não dois
	// porque o stale check precisa rodar também sobre a entidade EXPIRADA:
	// tratá-la como ausente a faria ser recriada por um evento antigo, que é
	// a ressurreição que o tombstone existe para impedir.
	Estado(key, buf []byte) (types.Record, []byte, types.Presenca, error)

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
//
// STALE_EVENT, que a §101 lista, NÃO é categoria de erro aqui: descarte por
// sequence não é falha, é o funcionamento normal do §25. Ele aparece como
// ActionStale e StaleTotal, sem Err.
type ErrorCategory uint8

const (
	CategoryNone ErrorCategory = iota
	CategoryInvalidJSON
	CategorySchemaValidation
	CategoryInvalidKey
	CategoryInvalidSequence
	CategoryTypeConversion
	// CategoryEventTooLarge: alguma parte do evento excedeu um teto de
	// FORMATO do storage — valor acima de types.MaxIndexedValueBytes (64 KiB)
	// num campo com bitmap, key acima de MaxKeySize, record acima de
	// MaxRecordSize.
	//
	// É a 6ª categoria de evento ruim, que a §101 não nomeia e que o
	// comentário desta constante pedia desde o M1. Até então esses casos
	// caíam em CategoryStorage, o balde do "bug nosso ou dataset avariado", e
	// o efeito era concreto: um produtor emitindo um atributo RADIUS de 70
	// KiB num campo indexado fazia o `ingest` sair com exit 7 (perda de dado)
	// a cada evento. Num daemon, sem exit code, a escolha seria entre avançar
	// o offset (perda definitiva e invisível) e não avançar (livelock por
	// evento envenenado). Aqui a política do §101 governa, como em qualquer
	// outro evento ruim.
	CategoryEventTooLarge
	// CategoryWriterFailed: writer em modo falho (sticky, §14.14). Terminal:
	// reprocessar o evento é inútil até o dataset ser reaberto.
	CategoryWriterFailed
	// CategoryStorage: erro do store que não é modo falho e não é teto de
	// formato — argumento inválido, leitura corrompida, falha do dicionário.
	// Indica bug nosso ou dataset avariado, e por isso NÃO é governado pela
	// política de evento: quem consome o resultado tem de parar a ingestão,
	// nunca descartar e seguir.
	CategoryStorage
	// CategoryInternal: inconsistência detectada pelo próprio materializer
	// (ErrInternalState).
	CategoryInternal
	// CategoryFieldsExhausted é o dataset que alcançou schema.maxCols tendo
	// declarado a política `stop`.
	//
	// Fica FORA da faixa de BadEvent(), e é isso que faz o `stop` parar: quem
	// consulta a política de evento inválido é o ramo de BadEvent() do writer,
	// cujo default é log-and-skip — dentro da faixa, o writer descartaria o
	// evento e seguiria, entregando o oposto da chave. Fora dela o resultado
	// cai no ramo de erro e a parada é observável, com o offset congelado.
	//
	// Fica no FIM da lista de propósito: inserir no meio mudaria o valor das
	// constantes seguintes. Elas não são serializadas hoje (as métricas usam o
	// nome), mas depender disso seria fragilidade de graça.
	CategoryFieldsExhausted

	// NumCategories é o limite superior EXCLUSIVO das categorias. Exportado
	// porque quem reporta contagem por categoria (o CLI) precisa varrer o
	// conjunto inteiro: um laço com o limite escrito à mão para de reportar a
	// última categoria no dia em que uma nova for acrescentada — foi o que
	// aconteceu com CategoryInternal, que existia e não aparecia no resumo.
	NumCategories = numCategories
)

const numCategories = CategoryFieldsExhausted + 1

var categoryNames = [...]string{
	CategoryNone:             "none",
	CategoryInvalidJSON:      "INVALID_JSON",
	CategorySchemaValidation: "SCHEMA_VALIDATION",
	CategoryInvalidKey:       "INVALID_KEY",
	CategoryInvalidSequence:  "INVALID_SEQUENCE",
	CategoryTypeConversion:   "TYPE_CONVERSION",
	CategoryEventTooLarge:    "EVENT_TOO_LARGE",
	CategoryWriterFailed:     "WRITER_FAILED",
	CategoryStorage:          "STORAGE_ERROR",
	CategoryInternal:         "INTERNAL_STATE",
	CategoryFieldsExhausted:  "FIELDS_EXHAUSTED",
}

func (c ErrorCategory) String() string {
	if int(c) < len(categoryNames) {
		return categoryNames[c]
	}
	return "category(?)"
}

// BadEvent reporta se a categoria é de evento ruim (§101) — o balde que a
// política log-and-skip cobre, e o ÚNICO em que descartar é a decisão certa.
// Fora dele o evento não entrou por falha nossa ou do dataset, e quem chamou
// precisa tratar isso como perda, não como descarte.
func (c ErrorCategory) BadEvent() bool {
	return c >= CategoryInvalidJSON && c <= CategoryEventTooLarge
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

	// porRegra conta quantas escritas cada regra de TTL decidiu, e semRegra
	// as que não casaram com nenhuma. São a resposta para "qual regra está
	// pegando o quê" — sem eles, um `when` que nunca casa é indistinguível de
	// um que casa sempre, e os dois são erros de configuração comuns.
	porRegra []atomic.Int64
	semRegra atomic.Int64
	// congeladasCapturadas conta a CAPTURA (o valor entrou no patch); a
	// PRESERVAÇÃO (o merge segurou o anterior) e os efeitos da soma vivem em
	// efeitos, porque quem os enxerga é o merge. Captura sem preservação é
	// coluna que nunca congelou.
	congeladasCapturadas atomic.Int64
	efeitos              efeitosDoMerge

	// histLinhas conta as linhas gravadas na tabela irmã e histDescartadas os
	// eventos que não viraram linha nenhuma. Os dois juntos são o que diz se
	// o histórico está COMPLETO — e é a pergunta que importa numa tabela cuja
	// razão de existir é servir de evidência.
	histLinhas      atomic.Int64
	histDescartadas atomic.Int64
	histDegradado   atomic.Bool
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

	// RegrasCasadas é indexada pela ORDEM das regras no YAML, e SemRegra são
	// as escritas que não casaram com nenhuma. Vazia num dataset sem regras.
	RegrasCasadas []int64
	SemRegra      int64
	// CongeladasCapturadas e CongeladasPreservadas contam as duas metades do
	// congelamento: a entry emitida na ingestão e o valor anterior segurado
	// pelo merge.
	CongeladasCapturadas  int64
	CongeladasPreservadas int64
	// Acumuladas conta as somas que o merge fez, e Saturadas as que bateram no
	// teto do int64 — a partir dali o valor para de crescer e deixa de ser
	// verdade, então ele precisa ser visível sem ninguém ir procurar.
	Acumuladas int64
	Saturadas  int64

	// HistoricoLinhas e HistoricoDescartadas contam as duas metades da tabela
	// irmã; HistoricoDegradado diz se ela parou de receber. Zeradas no dataset
	// que não declara `history`.
	HistoricoLinhas      int64
	HistoricoDescartadas int64
	HistoricoDegradado   bool
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

	// hist é a tabela irmã append-only, ou nil no dataset sem `history`.
	// histRaw é a coluna _raw, ou nil quando `rawPayload` está desligado.
	// histBuf é reutilizado entre eventos como os demais buffers — o payload
	// do histórico é montado uma vez por evento, e alocá-lo a cada um
	// dobraria o custo de GC do caminho quente para a tabela de debug.
	hist    EscritorDeHistorico
	histRaw *schema.CompiledField
	histBuf []byte
	// histCausa é a degradação STICKY do histórico. Não é atômico porque
	// pertence à goroutine do writer, como todo o resto daqui; quem observa de
	// fora lê o espelho em metrics.histDegradado.
	histCausa error

	// agora é o relógio da injeção do campo de sistema. Campo, e não uma
	// chamada direta a time.Now, para que um teste de expiração possa avançar
	// o tempo sem dormir — a alternativa seria um teste que leva minutos ou
	// que só prova a expiração com prazos irrealistas.
	agora func() time.Time

	// regras são as de TTL já compiladas, e slots são os arrays de avaliação
	// de cada uma — um por regra, alocados UMA vez aqui: a avaliação roda por
	// evento e não pode alocar. fr é o leitor reusado da coleta do estado
	// anterior, como todo buffer deste pacote.
	regras []predicado.RegraCompilada
	slots  [][]predicado.Slot
	fr     encoding.FieldReader

	metrics Metrics
}

// New monta o materializer do dataset sobre o store.
//
// Devolve erro quando uma regra de TTL não compila: a condição vem do YAML e
// só pode ser checada com o schema compilado na mão, então este é o primeiro
// ponto capaz de recusá-la. Falhar aqui é o que faz o dataset não subir com
// uma regra que nunca casaria — a alternativa seria descobrir isso no primeiro
// evento, com a ingestão já rodando.
func New(c *schema.Compiled, s Store) (*Materializer, error) {
	m := &Materializer{schema: c, store: s, agora: time.Now}
	m.patch.congeladasCapturadas = &m.metrics.congeladasCapturadas
	if len(c.RegrasTTL) > 0 {
		regras, err := predicado.CompilaRegras(c.RegrasTTL, c)
		if err != nil {
			return nil, err
		}
		m.regras = regras
		m.metrics.porRegra = make([]atomic.Int64, len(regras))
		m.slots = make([][]predicado.Slot, len(regras))
		for i := range regras {
			m.slots[i] = predicado.NovosSlots(regras[i].Slots.IDs)
		}
	}
	return m, nil
}

// UsaRelogio troca o relógio da injeção do campo de sistema. Exclusivo de
// teste: em produção o relógio é o do processo.
func (m *Materializer) UsaRelogio(f func() time.Time) { m.agora = f }

// Metrics devolve os contadores acumulados.
//
// A leitura é contador a contador, NÃO um instantâneo atômico do conjunto:
// com o writer ativo, EventsTotal pode já contar um evento cujo desfecho
// ainda não entrou em Created/Stale/Invalid. Serve a observabilidade (onde a
// defasagem de um evento não muda nada) e aos testes, que leem com o writer
// parado — não use como base de decisão de controle de fluxo.
func (m *Materializer) Metrics() MetricsSnapshot {
	s := MetricsSnapshot{
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
	if n := len(m.metrics.porRegra); n > 0 {
		s.RegrasCasadas = make([]int64, n)
		for i := range m.metrics.porRegra {
			s.RegrasCasadas[i] = m.metrics.porRegra[i].Load()
		}
	}
	s.SemRegra = m.metrics.semRegra.Load()
	s.CongeladasCapturadas = m.metrics.congeladasCapturadas.Load()
	s.CongeladasPreservadas = m.metrics.efeitos.congeladas.Load()
	s.Acumuladas = m.metrics.efeitos.acumuladas.Load()
	s.Saturadas = m.metrics.efeitos.saturadas.Load()
	s.HistoricoLinhas = m.metrics.histLinhas.Load()
	s.HistoricoDescartadas = m.metrics.histDescartadas.Load()
	s.HistoricoDegradado = m.metrics.histDegradado.Load()
	// events é lido por ÚLTIMO, de propósito: o Apply o incrementa na ENTRADA
	// e o desfecho na SAÍDA, então ler events primeiro deixaria o writer
	// avançar entre as leituras e produziria um snapshot com
	// soma(desfechos) > EventsTotal — um exporter que publique "eventos em
	// voo" mostraria valor negativo. Lendo por último, o desvio é monotônico e
	// sempre no mesmo sentido: o déficit são os eventos em processamento.
	s.EventsTotal = m.metrics.events.Load()
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

	if err := m.patch.Parse(event, m.schema, m.agora()); err != nil {
		res := m.failure(err)
		// Evento RUIM vai para o histórico com `_raw`, se ele existir: é o
		// ponto cego que a coluna existe para fechar. Falha de INFRA não vai —
		// não é o evento que está errado, e gravá-lo faria o histórico
		// registrar como dado ruim o que é problema de disco nosso.
		if res.Category.BadEvent() {
			m.apendaEventoRuimNoHistorico(event)
		}
		return res
	}
	m.apendaNoHistorico(event)

	var (
		presenca types.Presenca
		err      error
	)
	// O buf devolvido é reatribuído SEMPRE (pode ter crescido), inclusive no
	// erro — descartá-lo faria o próximo evento realocar do zero.
	m.prevRec, m.prevBuf, presenca, err = m.store.Estado(m.patch.key, m.prevBuf[:0])
	if err != nil {
		return m.failure(err)
	}

	// §25: sequence menor ou igual à corrente é descarte SEM escrita — sem
	// merge, sem append, sem índices. O record já veio do GetByKey (a
	// validação de colisão da primary obriga a lê-lo de qualquer jeito), então
	// o stale check não custa I/O algum.
	// presenca.Existe(), e não "está viva": é AQUI que o débito 24 se fecha.
	// A entidade expirada mantém RowID e Sequence, então o stale check tem
	// contra o que comparar — e um evento antigo é descartado em vez de
	// recriar a entidade que acabou de expirar.
	if presenca.Existe() && m.schema.Seq != nil && m.patch.seq <= m.prevRec.Sequence {
		m.metrics.stale.Add(1)
		return ApplyResult{Action: ActionStale, RowID: m.prevRec.RowID}
	}

	// O prazo é decidido DEPOIS do Parse e ANTES do merge: ele depende do
	// estado anterior, que o Parse não tem, e a entry precisa entrar no patch
	// antes de o payload novo ser montado.
	if m.schema.Prazo != nil {
		d, err := m.duracaoDe(m.prevRec.Payload, m.patch.entries)
		if err != nil {
			return m.failure(err)
		}
		// Emitida em TODA escrita, e não só quando o resultado muda. O merge
		// preserva campo que o evento não menciona, então a emissão
		// condicional deixaria o prazo curto de quando a condição valia
		// sobreviver ao evento que a quebra — e a entidade morreria logo
		// depois de o alarme reabrir. São 8 bytes por versão de record para
		// tornar impossível a única falha que arruinaria a feature.
		m.patch.emitePrazo(m.schema.Prazo, d)
	}

	// prevRec.Payload aponta para prevBuf e outBuf é um buffer DISTINTO: o
	// merge lê de um e escreve no outro sem risco de sobrescrita.
	replace := m.schema.Mode == schema.ModeReplace
	var payload []byte
	// O merge exige estado VIVO. Sobre uma lápide ele arrastaria os campos da
	// encarnação anterior para a entidade que está reabrindo — um alarme novo
	// herdaria a severidade do que foi fechado.
	if presenca == types.PresencaViva && !replace {
		payload, err = mergeRecords(m.prevRec.Payload, m.patch.entries, m.outBuf[:0], &m.metrics.efeitos)
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
	// prev vale também para a entidade expirada: é ele que faz o Put reusar o
	// mesmo rowId (a primary ainda nomeia a key) e dá ao diff de índices a
	// base certa para limpar as postings da encarnação anterior.
	var prev *types.Record
	if presenca.Existe() {
		prev = &m.prevRec
	}
	rowID, created, err := m.store.Put(m.patch.key, m.patch.seq, 0, payload, prev)
	if err != nil {
		// O rowID devolvido junto com erro é descartado de propósito. Com
		// SyncEveryPut, uma falha de fsync devolve (rowID, created, err) com o
		// estado JÁ commitado e visível — mas o writer ficou sticky, então o
		// desfecho correto é Failed+Fatal e a ingestão para. Reportar Created
		// aqui faria o caller contabilizar sucesso num writer morto; o evento
		// "perdido" volta pelo reprocessamento e o stale check o descarta se
		// já tiver sido aplicado.
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
	if cat.BadEvent() {
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
	// Depois do writer falho, que é terminal e vence qualquer outra leitura,
	// e antes do default, que é onde o teto de formato caía até o M1 —
	// contando como perda de dado o que é culpa do evento.
	case errors.Is(err, types.ErrTooLarge):
		return CategoryEventTooLarge
	case errors.Is(err, ErrInternalState):
		return CategoryInternal
	// O teto de campos alcançado não é evento ruim nem dataset avariado: é um
	// limite declarado sendo atingido. Sem este ramo ele caía no default e o
	// operador o lia como STORAGE_ERROR — "bug nosso" — enquanto o writer
	// parava para sempre sem explicação acionável.
	case errors.Is(err, schema.ErrTetoDeCampos):
		return CategoryFieldsExhausted
	default:
		return CategoryStorage
	}
}
