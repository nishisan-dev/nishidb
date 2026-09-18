package alarm

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Options configura o runner.
type Options struct {
	// Sink é para onde os eventos vão. Obrigatório.
	Sink Sink
	// Log é o logger; nil vira Discard, para que nenhum caminho checar nil.
	Log *logging.Logger
	// StatePath é o arquivo de estado durável. Vazio DESLIGA a persistência —
	// é o que o teste de lógica de passada usa, e não um modo de produção.
	StatePath string
	// Engine é o motor onde as consultas de enriquecimento correm. nil deixa
	// os blocos declarados vazios, com o motivo no evento — é o que o teste da
	// lógica de passada usa, e não um modo de produção.
	Engine Executor
	// Now é o relógio. Exclusivo de teste, no molde do UsaRelogio do storage:
	// um teste de prazo precisa avançar o tempo sem dormir, e a alternativa
	// seria uma suíte que leva minutos ou que só prova a sustentação com
	// prazos irrealistas.
	Now func() time.Time
	// Metrics é a fonte dos valores publicados das métricas de janela, lidos
	// pela tabela PRÓPRIA do laço. nil é aceito: as colunas continuam no schema
	// e saem NULL — e NULL compara falso, então um `qualifyWhen` sobre métrica
	// nunca qualifica sem fonte. É o que o teste de lógica de passada usa
	// quando não exercita métricas.
	Metrics counters.MetricSource
}

// Runner observa os alarmes de um dataset.
//
// Não é thread-safe: a passada é de um dono, o laço. O que ela toca no storage
// é só a leitura de grupos, que toma o lock FOLHA do dicionário do contador e
// nada do Store — é por isso que a varredura não disputa nada com a ingestão.
type Runner struct {
	dataset   string
	counters  []*runnerCounter
	sink      Sink
	log       *logging.Logger
	statePath string
	now       func() time.Time

	// savedAtUnixMs é o instante gravado no arquivo de estado lido na subida;
	// zero quando não havia estado. É o que mede a lacuna do processo parado.
	savedAtUnixMs int64

	// pass numera as passadas, para saber quais grupos a varredura VIU. Um
	// grupo aposentado não aparece na varredura, e é a ausência que resolve o
	// alarme dele.
	pass uint64

	// snapshot é o retrato dos grupos em observação, republicado a cada
	// passada que muda algo. É como a view `<schema>.alarms` (atalho de
	// `_counters.alarms`) lê o estado sem disputar o mapa de trackers com a
	// varredura.
	snapshot snapshotHolder

	// lastSweep é a duração da última passada, em nanossegundos, e
	// restoredGroups quantos cronômetros vieram do disco na subida.
	lastSweep      atomic.Int64
	restoredGroups int64

	// dirty marca que houve transição e o estado precisa ir ao disco. A
	// gravação é por TRANSIÇÃO, e não por passada: o estado só muda quando um
	// alarme entra em observação, dispara ou resolve.
	dirty bool

	// buf é reusado pela leitura da linha de cada grupo.
	buf []byte
	// cols é reusado ao montar as colunas de um evento.
	cols []Column
	// vals é o scratch da decomposição da chave canônica.
	vals []types.Value

	engine Executor
	// enrichBudget é quantas consultas ainda podem correr NESTA passada.
	enrichBudget int
	// enrichBuf e params são scratch das consultas de enriquecimento.
	enrichBuf []byte
	params    []types.Value

	// loop é o ciclo de vida da goroutine de varredura.
	loop loop
}

// definitions devolve os alarmes compilados, para quem precisa decidir a
// cadência.
func (r *Runner) definitions() []Definition {
	out := make([]Definition, 0, len(r.counters))
	for _, rc := range r.counters {
		out = append(out, rc.def)
	}
	return out
}

// runnerCounter é um contador observado, com a tabela virtual pela qual as
// linhas são lidas.
type runnerCounter struct {
	def    Definition
	reader counters.Reader
	// store é um Store virtual PRÓPRIO do alarme, e não o que as consultas
	// usam: ele tem buffer e scratch, e compartilhá-lo poria dois donos no
	// mesmo estado interno.
	store query.Store
	names []string
	// format é o molde do groupKey, para renderizar o texto de um grupo que a
	// varredura não viu mais.
	format *keyformat.Format
	// metricColumns são as colunas das métricas de janela, na ordem declarada,
	// com o FieldID que a tabela PRÓPRIA do laço lhes deu — é o payload dela
	// que o evento lê.
	metricColumns []metricColumn
	alarms        []*runnerAlarm
}

// metricColumn é uma coluna gerada por métrica de janela na linha do contador.
type metricColumn struct {
	id   types.FieldID
	name string
}

// runnerAlarm é um alarme com o avaliador e o estado por grupo.
type runnerAlarm struct {
	compiled *compiledAlarm
	eval     counter.Evaluator
	// qualifier avalia o `qualifyWhen`; nil quando o alarme não o declara.
	qualifier counter.Evaluator
	// trackers guarda estado SÓ dos grupos em observação, indexados pela chave
	// canônica. Um contador por PON tem milhares de grupos saudáveis, e um
	// tracker para cada faria a memória crescer com o inventário em vez de com
	// o problema.
	trackers map[string]*tracker
	stats    alarmCounters
}

// New monta o runner a partir dos alarmes compilados e dos contadores abertos.
//
// readers casa nome de contador → leitor. Um alarme compilado sem leitor é
// erro: significa que o contador fechou entre a compilação e aqui, e um alarme
// que não observa nada ficaria calado para sempre.
func New(defs []Definition, readers map[string]counters.Reader, opts Options) (*Runner, error) {
	if opts.Sink == nil {
		return nil, errors.New("alarm: Sink obrigatório")
	}
	r := &Runner{
		engine:    opts.Engine,
		sink:      opts.Sink,
		log:       opts.Log,
		statePath: opts.StatePath,
		now:       opts.Now,
	}
	if r.log == nil {
		r.log = logging.Discard()
	}
	if r.now == nil {
		r.now = time.Now
	}
	for _, d := range defs {
		reader, ok := readers[d.Counter]
		if !ok {
			return nil, fmt.Errorf("alarm: contador %q sem leitor", d.Counter)
		}
		rc := &runnerCounter{def: d, reader: reader}
		// Tabela virtual PRÓPRIA do alarme: ela tem buffer e scratch, e
		// compartilhar a das consultas poria dois donos no mesmo estado
		// interno. Montá-la custa uma compilação de schema na subida. Sem
		// MetricSource as colunas das métricas existem e saem NULL.
		sc, store, err := counters.Table(d.def, reader, d.metrics, opts.Metrics)
		if err != nil {
			return nil, fmt.Errorf("alarm: tabela do contador %q: %w", d.Counter, err)
		}
		rc.store = store
		if rc.metricColumns, err = metricColumnsOf(sc, d.metrics); err != nil {
			return nil, fmt.Errorf("alarm: counter %q: %w", d.Counter, err)
		}
		rc.names, rc.format = d.def.Names, d.def.Format
		for i := range d.alarms {
			a := &d.alarms[i]
			ra := &runnerAlarm{
				compiled: a,
				eval:     a.newEvaluator(),
				trackers: make(map[string]*tracker),
			}
			if a.newQualifier != nil {
				ra.qualifier = a.newQualifier()
			}
			rc.alarms = append(rc.alarms, ra)
		}
		r.dataset = d.Dataset
		r.counters = append(r.counters, rc)
	}
	r.restore()
	// O retrato nasce com o que veio do disco: um processo que volta com
	// alarmes em curso tem de responder a `<schema>.alarms` antes da primeira
	// passada, senão a view diria que não há nada acontecendo durante a
	// janela de subida — justamente quando alguém está olhando.
	r.publishSnapshot()
	return r, nil
}

// metricColumnsOf resolve, no schema da tabela própria do laço, o FieldID de
// cada coluna gerada pelas métricas, na ordem declarada.
func metricColumnsOf(sc *schema.Compiled, metrics []schema.CounterMetricSpec) ([]metricColumn, error) {
	var out []metricColumn
	for _, m := range metrics {
		for _, col := range m.Columns() {
			f, ok := sc.FieldByName([]byte(col))
			if !ok {
				return nil, fmt.Errorf("column %q of metric %q has no FieldID in the counter table", col, m.Name)
			}
			out = append(out, metricColumn{id: f.ID, name: col})
		}
	}
	return out, nil
}

// restore instala o estado durável lido do disco.
//
// Nenhum modo de falha aqui impede a subida, pela razão do §51: cronômetro
// ilegível não torna dado inacessível. O que se perde é a contagem em curso, e
// cada alarme ativo recomeça a contar — degradação que sai no log em vez de
// recusa de abertura.
func (r *Runner) restore() {
	if r.statePath == "" {
		return
	}
	st, err := checkpoint.LoadAlarmState(r.statePath)
	switch {
	case errors.Is(err, checkpoint.ErrInAlarmState):
		// Primeira subida, ou nenhum alarme ativo quando o processo saiu. Não
		// é anomalia e não se registra.
		return
	case err != nil:
		r.log.Warn("alarm.state_unreadable",
			"o estado de sustentação não pôde ser lido; cada alarme ativo recomeça a contar o prazo",
			"path", r.statePath, "error", err.Error())
		return
	}
	r.savedAtUnixMs = st.SavedAtUnixMs
	restored, dropped := 0, 0
	for _, e := range st.Alarms {
		ra := r.findAlarm(e.Counter, e.Alarm)
		if ra == nil {
			dropped += len(e.Groups)
			continue
		}
		// A assinatura da regra é o que decide se o cronômetro ainda vale. Um
		// prazo, uma condição, uma qualificação ou a janela de uma métrica
		// citada trocados no YAML invalidam a medida: disparar com o cronômetro
		// antigo anunciaria uma sustentação que ninguém mediu.
		if e.When != ra.compiled.when || e.QualifyWhen != ra.compiled.qualifyWhen ||
			e.SustainedFor != ra.compiled.sustainedFor || e.MetricsFingerprint != ra.compiled.metricsFingerprint {
			dropped += len(e.Groups)
			continue
		}
		for _, g := range e.Groups {
			tr := &tracker{}
			tr.restore(time.UnixMilli(g.SinceUnixMs))
			ra.trackers[string(g.Group)] = tr
			ra.stats.tracked.Add(1)
			restored++
		}
	}
	r.restoredGroups = int64(restored)
	if restored > 0 || dropped > 0 {
		r.log.Info("alarm.state_restored",
			"estado de sustentação lido do disco",
			"restored", restored, "dropped", dropped, "savedAt", st.SavedAtUnixMs)
	}
}

func (r *Runner) findAlarm(counterName, alarmName string) *runnerAlarm {
	for _, rc := range r.counters {
		if rc.def.Counter != counterName {
			continue
		}
		for _, ra := range rc.alarms {
			if ra.compiled.name == alarmName {
				return ra
			}
		}
	}
	return nil
}

// EvaluateOnce faz UMA passada por todos os alarmes de todos os contadores.
//
// É público para que o teste a chame direto, em vez de esperar um tique — é o
// mesmo motivo do AposentaGroupsVazios no storage.
func (r *Runner) EvaluateOnce() error {
	inicio := time.Now()
	defer func() { r.lastSweep.Store(int64(time.Since(inicio))) }()
	now := r.now()
	r.pass++
	r.dirty = false
	r.enrichBudget = maxEnrichmentsPerSweep
	for _, rc := range r.counters {
		if err := r.sweep(rc, now); err != nil {
			return err
		}
	}
	if r.dirty {
		r.publishSnapshot()
		return r.persist(now)
	}
	return nil
}

// sweep varre os grupos de um contador e decide cada alarme.
//
// Duas fases, e a segunda é o que impede alarme eterno: a varredura só enxerga
// grupo VIVO, então um grupo aposentado simplesmente não aparece. É a ausência
// dele na passada — e não um evento do storage — que resolve o alarme.
func (r *Runner) sweep(rc *runnerCounter, now time.Time) error {
	snap := rc.store.OpenSnapshot()
	for id := types.RowID(1); id <= snap.LimitRowID; id++ {
		key, text, total, _, live := rc.reader.Group(uint32(id))
		if !live {
			continue
		}
		rec, buf, ok, err := rc.store.GetByRowID(snap, id, r.buf)
		r.buf = buf
		if err != nil {
			return fmt.Errorf("alarm: lendo o grupo %d de %q: %w", id, rc.def.Counter, err)
		}
		if !ok {
			// Grupo que saiu entre a leitura da chave e a da linha. A próxima
			// passada o trata como ausente, que é o desfecho certo.
			continue
		}
		for _, ra := range rc.alarms {
			holds, err := ra.eval.Evaluate(rec.Payload)
			if err != nil {
				return fmt.Errorf("alarm: avaliando %q sobre o grupo %q: %w", ra.compiled.name, text, err)
			}
			// Leitura de mapa com string(chave) não copia a chave (otimização
			// do compilador), e é o caminho de TODO grupo saudável.
			tr, existed := ra.trackers[string(key)]
			if !existed && !holds {
				// O caso dominante: milhares de grupos em ordem, e nada a
				// lembrar de nenhum deles. Criar tracker aqui seria lixo
				// proporcional ao inventário, não ao problema.
				continue
			}
			// A qualificação só é avaliada com a condição valendo. Com ela
			// falsa o desfecho já está decidido — resolução por condição, ou
			// nada —, e avaliar o qualifyWhen de todo grupo saudável seria
			// pagar uma segunda avaliação por grupo do inventário para não
			// mudar decisão nenhuma.
			qualified := true
			if holds && ra.qualifier != nil {
				if qualified, err = ra.qualifier.Evaluate(rec.Payload); err != nil {
					return fmt.Errorf("alarm: evaluating the qualifyWhen of %q on group %q: %w", ra.compiled.name, text, err)
				}
			}
			if !existed {
				if !qualified {
					// Condição valendo sobre grupo inelegível — a OLT que
					// nunca teve ninguém. É o caso dominante do alarme
					// qualificado, e pela mesma razão do anterior não ganha
					// tracker.
					continue
				}
				tr = &tracker{}
				ra.trackers[string(key)] = tr
				ra.stats.tracked.Add(1)
			}
			tr.seen = r.pass
			r.decide(rc, ra, tr, key, text, total, rec.Payload,
				observation{present: true, conditionHolds: holds, qualified: qualified}, now)
		}
	}
	// Fase dois: os grupos com estado que a varredura NÃO viu desapareceram.
	// O laço é sobre os trackers, que são poucos por construção — varrer o
	// universo de grupos aqui seria pagar o inventário de novo.
	for _, ra := range rc.alarms {
		for k, tr := range ra.trackers {
			if tr.seen == r.pass {
				continue
			}
			r.decide(rc, ra, tr, []byte(k), nil, 0, nil, observation{}, now)
		}
	}
	return nil
}

// decide aplica a observação ao tracker do grupo e emite o que sair.
//
// O tracker chega resolvido: quem varre já o procurou no mapa, e procurá-lo
// aqui de novo custaria uma segunda busca por grupo.
//
// payload é a linha do grupo que a observação avaliou; nil para o grupo que a
// varredura não viu.
func (r *Runner) decide(rc *runnerCounter, ra *runnerAlarm, tr *tracker, key, text []byte, total int64,
	payload []byte, o observation, now time.Time) {
	before := tr.phase
	d := tr.evaluate(o, ra.compiled.sustainedFor, now)
	if tr.phase != before {
		r.dirty = true
	}
	if d.verdict == verdictFiring {
		// A captura mora AQUI, na transição, e não em tracker.evaluate: é em
		// decide que payload e now existem. Lê do MESMO payload que ra.eval
		// acabou de avaliar — nunca do `total` que o sweep tem em mãos
		// (rc.reader.Group vem de uma leitura diferente de
		// counters/table.go, (*source).record, e pode divergir dele — ver
		// firedValueOf). Só roda NESTA transição, e não em toda passada sobre
		// um grupo já disparado ou ainda pending, por dois motivos: custo — um
		// contador por PON tem milhares de grupos, e varrer o payload de cada
		// um a cada passada para nada seria proporcional ao inventário, não ao
		// problema — e CONGELAMENTO — firedAt e firingValue descrevem O
		// disparo, e reler a cada passada faria um grupo disparado que oscila
		// (a condição continua valendo, mas o número muda) anunciar um valor
		// que não foi o que decidiu.
		tr.firedAt = now
		tr.firingValue, tr.hasFiringValue = firedValueOf(payload, ra.compiled)
	}
	if !tr.active() {
		// delete com string(chave) também não copia a chave.
		delete(ra.trackers, string(key))
		ra.stats.tracked.Add(-1)
	}
	if d.verdict == verdictNone {
		return
	}
	switch d.verdict {
	case verdictFiring:
		ra.stats.fired.Add(1)
		ra.stats.firing.Add(1)
	case verdictCleared, verdictRetired:
		ra.stats.resolved.Add(1)
		ra.stats.firing.Add(-1)
	case verdictDisqualified:
		ra.stats.resolved.Add(1)
		ra.stats.disqualified.Add(1)
		ra.stats.firing.Add(-1)
	}
	e := r.event(rc, ra, key, text, total, payload, d, now)
	// Só o DISPARO enriquece e carrega o valor capturado. O enriquecimento
	// existe para dar contexto ao problema; na resolução o fato relevante é
	// que ele passou, e executar as consultas outra vez dobraria o custo
	// para contar isso — e o valor que decidiu o disparo não é um valor novo
	// que a resolução esteja anunciando.
	if d.verdict == verdictFiring {
		e.ValueColumn = ra.compiled.valueColumn
		e.FiringValue = tr.firingValue
		e.HasFiringValue = tr.hasFiringValue
		e.Enrichment = r.enrich(ra, key, text, total)
	}
	r.sink.Emit(e)
}

// event monta o evento de uma decisão.
func (r *Runner) event(rc *runnerCounter, ra *runnerAlarm, key, text []byte, total int64,
	payload []byte, d decision, now time.Time) Event {
	e := Event{
		Dataset:          rc.def.Dataset,
		Counter:          rc.def.Counter,
		Alarm:            ra.compiled.name,
		GroupKey:         string(text),
		Total:            total,
		Condition:        ra.compiled.when,
		SustainedFor:     ra.compiled.sustainedFor,
		QualifyCondition: ra.compiled.qualifyWhen,
		Metrics:          r.metricsOf(rc, payload),
		Since:            d.since,
		At:               now,
		Restored:         d.restored,
	}
	switch d.verdict {
	case verdictFiring:
		e.State = StateFiring
	case verdictCleared:
		e.State, e.Reason = StateResolved, ReasonConditionCleared
	case verdictRetired:
		e.State, e.Reason = StateResolved, ReasonGroupRetired
	case verdictDisqualified:
		e.State, e.Reason = StateResolved, ReasonDisqualified
	}
	if d.restored && r.savedAtUnixMs > 0 {
		e.DowntimeMs = now.UnixMilli() - r.savedAtUnixMs
	}
	// O grupo aposentado não tem texto nem colunas para dar: a varredura não o
	// viu. A chave canônica é o que se tem, e é dela que o texto sai.
	if text == nil {
		e.GroupKey = r.groupKeyOf(rc, key)
	}
	e.Columns = r.columnsOf(rc, key)
	return e
}

// columnsOf abre a chave canônica nas colunas do groupBy, na ordem declarada.
func (r *Runner) columnsOf(rc *runnerCounter, key []byte) []Column {
	vals, err := valuesOf(key, r.vals)
	r.vals = vals
	if err != nil || len(vals) != len(rc.names) {
		return nil
	}
	r.cols = r.cols[:0]
	for i, n := range rc.names {
		r.cols = append(r.cols, Column{Name: n, Value: textOfValue(vals[i])})
	}
	// A cópia existe porque o scratch é reusado na próxima decisão, e o evento
	// sobrevive à passada — devolver a fatia interna faria o consumidor ler as
	// colunas de outro grupo.
	out := make([]Column, len(r.cols))
	copy(out, r.cols)
	return out
}

// metricsOf lê do payload da linha as colunas das métricas de janela, na ordem
// declarada.
//
// Lê o MESMO payload que a decisão avaliou, e não uma releitura da tabela: entre
// as duas o amostrador pode publicar outro valor, e o evento mostraria um número
// que não foi o que decidiu. Só os FieldIDs das métricas são aproveitados;
// coluna ausente é NULL e não entra.
//
// Aloca, e está fora da passada saudável: só roda quando há evento a emitir.
// Erro de leitura aqui não segura o evento — a mesma linha acabou de passar pelo
// avaliador —, e o que se perde são as colunas a partir do defeito.
func (r *Runner) metricsOf(rc *runnerCounter, payload []byte) []Column {
	if payload == nil || len(rc.metricColumns) == 0 {
		return nil
	}
	values := make([]string, len(rc.metricColumns))
	present := make([]bool, len(rc.metricColumns))
	found := 0
	var fr encoding.FieldReader
	fr.Reset(payload)
	for id, v, ok := fr.Next(); ok; id, v, ok = fr.Next() {
		for i, c := range rc.metricColumns {
			if c.id == id {
				values[i], present[i] = textOfValue(v), true
				found++
				break
			}
		}
	}
	if found == 0 {
		return nil
	}
	out := make([]Column, 0, found)
	for i, c := range rc.metricColumns {
		if present[i] {
			out = append(out, Column{Name: c.name, Value: values[i]})
		}
	}
	return out
}

// firedValueOf lê, do MESMO payload que a decisão avaliou, o valor da coluna
// declarada em `value:` de um alarme — inclusive quando ela é `total`: o
// total que o sweep tem em mãos (rc.reader.Group, uma leitura por texto/refs)
// vem de uma leitura diferente da que produziu ESTE payload
// (internal/counters/table.go, (*source).record), e os dois podem divergir
// entre a leitura da chave e a da linha. Ler do payload garante que o número
// do evento é o número que decidiu.
//
// Coluna ausente do payload (amostra de métrica de janela ainda inexistente)
// devolve hasValue=false — nunca um zero que se pareceria com um valor
// observado.
//
// Só é chamada na transição para o disparo (ver Runner.decide), nunca sobre
// um grupo pending sem transição nem sobre o caminho saudável — não por
// alocação (o FieldReader é valor de pilha e não escapa; `go build
// -gcflags=-m` confirma), mas por CUSTO: percorrer o payload inteiro a cada
// passada, para milhares de grupos saudáveis ou pending do inventário, seria
// trabalho proporcional ao tamanho do contador, não ao problema. A mesma
// guarda também é o que CONGELA firedAt e firingValue no instante do disparo
// — ver o comentário da chamada em Runner.decide.
func firedValueOf(payload []byte, a *compiledAlarm) (value float64, hasValue bool) {
	if payload == nil {
		return 0, false
	}
	var fr encoding.FieldReader
	fr.Reset(payload)
	for id, v, ok := fr.Next(); ok; id, v, ok = fr.Next() {
		if id == a.valueFieldID {
			return floatOfFieldValue(a.valueType, v), true
		}
	}
	return 0, false
}

// floatOfFieldValue converte o valor de uma coluna da linha do contador para
// o float64 do firingValue: direto para types.TypeDouble (a média de uma
// métrica de janela, que já chega como os 8 bytes do float64) e por conversão
// de inteiro para types.TypeInt64 (total, min e max). A conversão perde
// precisão acima de 2^53 — o maior inteiro que um float64 representa
// exatamente —, limite aceito porque o dado nasce como contagem de linhas
// materializadas, ordens de grandeza abaixo disso.
func floatOfFieldValue(t types.FieldType, v types.Value) float64 {
	if t == types.TypeDouble {
		return math.Float64frombits(uint64(v.I64))
	}
	return float64(v.I64)
}

// groupKeyOf renderiza o texto do grupo a partir da chave canônica, pelo MESMO
// molde que o contador usa — é o caso do grupo que a varredura não viu mais, e
// cujo texto ninguém tem em mão.
func (r *Runner) groupKeyOf(rc *runnerCounter, key []byte) string {
	text, scratch, err := counter.KeyText(nil, rc.format, key, r.vals)
	r.vals = scratch
	if err != nil {
		return ""
	}
	return string(text)
}

// persist grava o estado dos alarmes em observação.
//
// Só os ATIVOS vão ao disco, e é o que mantém o arquivo proporcional ao
// problema. Estado vazio é gravado igual: sem isso o arquivo da passada
// anterior continuaria afirmando alarmes ativos depois de todos resolverem.
func (r *Runner) persist(now time.Time) error {
	if r.statePath == "" {
		return nil
	}
	st := checkpoint.AlarmState{SavedAtUnixMs: now.UnixMilli()}
	for _, rc := range r.counters {
		for _, ra := range rc.alarms {
			if len(ra.trackers) == 0 {
				continue
			}
			e := checkpoint.AlarmEntry{
				Counter:            rc.def.Counter,
				Alarm:              ra.compiled.name,
				When:               ra.compiled.when,
				QualifyWhen:        ra.compiled.qualifyWhen,
				MetricsFingerprint: ra.compiled.metricsFingerprint,
				SustainedFor:       ra.compiled.sustainedFor,
				Groups:             make([]checkpoint.GroupSince, 0, len(ra.trackers)),
			}
			for k, tr := range ra.trackers {
				e.Groups = append(e.Groups, checkpoint.GroupSince{
					Group:       []byte(k),
					SinceUnixMs: tr.observedSince().UnixMilli(),
				})
			}
			st.Alarms = append(st.Alarms, e)
		}
	}
	if err := checkpoint.WriteAlarmState(r.statePath, st); err != nil {
		return fmt.Errorf("alarm: gravando o estado de sustentação: %w", err)
	}
	return nil
}

// trackedGroups é quantos pares (alarme, grupo) têm estado em memória. Serve à
// observabilidade e ao teste que fixa a proporcionalidade: o número acompanha o
// problema, nunca o tamanho do inventário.
func (r *Runner) trackedGroups() int {
	n := 0
	for _, rc := range r.counters {
		for _, ra := range rc.alarms {
			n += len(ra.trackers)
		}
	}
	return n
}

// valuesOf decompõe a chave canônica do grupo nos valores das colunas do
// groupBy, reusando o scratch do chamador.
func valuesOf(key []byte, scratch []types.Value) ([]types.Value, error) {
	return valuekey.DecodeComposite(key, scratch[:0])
}

// textOfValue rende um valor de coluna da linha do contador — do groupBy ou de
// uma métrica de janela — como texto, no mesmo formato que a coluna teria num
// SELECT.
func textOfValue(v types.Value) string {
	switch v.Type {
	case types.TypeString:
		return string(v.Bytes)
	case types.TypeInt64:
		return strconv.FormatInt(v.I64, 10)
	case types.TypeBool:
		if v.I64 == 1 {
			return "true"
		}
		return "false"
	case types.TypeDouble:
		// A média de uma métrica de janela. A grafia é a do NDJSON
		// (executor.AppendValue): 'g', a forma mais curta que faz round-trip.
		// O contrato do MetricReader garante valor finito com amostra, e o
		// guarda cobre quem o quebrar: não-finito sai vazio, e não como NaN.
		f := math.Float64frombits(uint64(v.I64))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return ""
		}
		return strconv.FormatFloat(f, 'g', -1, 64)
	default:
		// groupBy aceita só string, int64 e boolean (validateGroupingColumn), e
		// as métricas geram só int64 e double; um tipo novo aqui exige decidir a
		// renderização antes.
		return ""
	}
}
