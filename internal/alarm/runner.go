package alarm

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/keyformat"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
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
	// Now é o relógio. Exclusivo de teste, no molde do UsaRelogio do storage:
	// um teste de prazo precisa avançar o tempo sem dormir, e a alternativa
	// seria uma suíte que leva minutos ou que só prova a sustentação com
	// prazos irrealistas.
	Now func() time.Time
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
	alarms []*runnerAlarm
}

// runnerAlarm é um alarme com o avaliador e o estado por grupo.
type runnerAlarm struct {
	compiled *compiledAlarm
	eval     counter.Evaluator
	// trackers guarda estado SÓ dos grupos em observação, indexados pela chave
	// canônica. Um contador por PON tem milhares de grupos saudáveis, e um
	// tracker para cada faria a memória crescer com o inventário em vez de com
	// o problema.
	trackers map[string]*tracker
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
		var err error
		// Tabela virtual PRÓPRIA do alarme: ela tem buffer e scratch, e
		// compartilhar a das consultas poria dois donos no mesmo estado
		// interno. Montá-la custa uma compilação de schema na subida.
		if _, rc.store, err = counters.Table(d.def, reader); err != nil {
			return nil, fmt.Errorf("alarm: tabela do contador %q: %w", d.Counter, err)
		}
		rc.names, rc.format = d.def.Names, d.def.Format
		for i := range d.alarms {
			a := &d.alarms[i]
			rc.alarms = append(rc.alarms, &runnerAlarm{
				compiled: a,
				eval:     a.newEvaluator(),
				trackers: make(map[string]*tracker),
			})
		}
		r.dataset = d.Dataset
		r.counters = append(r.counters, rc)
	}
	r.restore()
	return r, nil
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
		// prazo trocado no YAML invalida a medida: disparar com o cronômetro
		// antigo anunciaria uma sustentação que ninguém mediu.
		if e.When != ra.compiled.when || e.SustainedFor != ra.compiled.sustainedFor {
			dropped += len(e.Groups)
			continue
		}
		for _, g := range e.Groups {
			tr := &tracker{}
			tr.restore(time.UnixMilli(g.SinceUnixMs))
			ra.trackers[string(g.Group)] = tr
			restored++
		}
	}
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
	now := r.now()
	r.pass++
	r.dirty = false
	for _, rc := range r.counters {
		if err := r.sweep(rc, now); err != nil {
			return err
		}
	}
	if r.dirty {
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
			if !existed {
				if !holds {
					// O caso dominante: milhares de grupos em ordem, e nada a
					// lembrar de nenhum deles. Criar tracker aqui seria lixo
					// proporcional ao inventário, não ao problema.
					continue
				}
				tr = &tracker{}
				ra.trackers[string(key)] = tr
			}
			tr.seen = r.pass
			r.decide(rc, ra, tr, key, text, total, observation{present: true, conditionHolds: holds}, now)
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
			r.decide(rc, ra, tr, []byte(k), nil, 0, observation{}, now)
		}
	}
	return nil
}

// decide aplica a observação ao tracker do grupo e emite o que sair.
//
// O tracker chega resolvido: quem varre já o procurou no mapa, e procurá-lo
// aqui de novo custaria uma segunda busca por grupo.
func (r *Runner) decide(rc *runnerCounter, ra *runnerAlarm, tr *tracker, key, text []byte, total int64, o observation, now time.Time) {
	before := tr.phase
	d := tr.evaluate(o, ra.compiled.sustainedFor, now)
	if tr.phase != before {
		r.dirty = true
	}
	if !tr.active() {
		// delete com string(chave) também não copia a chave.
		delete(ra.trackers, string(key))
	}
	if d.verdict == verdictNone {
		return
	}
	r.sink.Emit(r.event(rc, ra, key, text, total, tr, d, now))
}

// event monta o evento de uma decisão.
func (r *Runner) event(rc *runnerCounter, ra *runnerAlarm, key, text []byte, total int64,
	tr *tracker, d decision, now time.Time) Event {
	e := Event{
		Dataset:      rc.def.Dataset,
		Counter:      rc.def.Counter,
		Alarm:        ra.compiled.name,
		GroupKey:     string(text),
		Total:        total,
		Condition:    ra.compiled.when,
		SustainedFor: ra.compiled.sustainedFor,
		Since:        tr.observedSince(),
		At:           now,
		Restored:     d.restored,
	}
	switch d.verdict {
	case verdictFiring:
		e.State = StateFiring
		e.Since = tr.observedSince()
	case verdictCleared:
		e.State, e.Reason = StateResolved, ReasonConditionCleared
	case verdictRetired:
		e.State, e.Reason = StateResolved, ReasonGroupRetired
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
				Counter:      rc.def.Counter,
				Alarm:        ra.compiled.name,
				When:         ra.compiled.when,
				SustainedFor: ra.compiled.sustainedFor,
				Groups:       make([]checkpoint.GroupSince, 0, len(ra.trackers)),
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

// textOfValue rende um valor de coluna do groupBy como texto, no mesmo formato
// que a coluna teria num SELECT.
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
	default:
		// groupBy aceita só os três tipos acima (validateGroupingColumn), e um
		// tipo novo aqui exige decidir a renderização antes.
		return ""
	}
}
