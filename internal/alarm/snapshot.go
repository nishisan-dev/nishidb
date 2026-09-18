package alarm

import (
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// ActiveGroup é um par (alarme, grupo) em observação, como a view
// `<schema>.alarms` (atalho de `_counters.alarms`) o mostra.
//
// Pendente e disparado são estados DISTINTOS e os dois aparecem: "vai alarmar
// em quarenta segundos" é informação que só existe antes do disparo, e é
// exatamente o que alguém quer ver ao investigar um incidente em curso.
type ActiveGroup struct {
	Counter  string
	Alarm    string
	GroupKey string
	// Firing distingue o alarme já anunciado do que ainda conta o prazo.
	Firing bool
	// Since é quando a condição passou a valer.
	Since time.Time
	// Condition e SustainedFor são a regra, porque quem consulta a tabela não
	// tem o YAML à mão.
	Condition    string
	SustainedFor time.Duration
	// QualifyCondition é o `qualifyWhen` declarado; vazio quando não há.
	QualifyCondition string
	// ValueColumn é a coluna de `_counters.<contador>` cujo valor vira
	// FiringValue ao disparar — `total` por padrão, ou a coluna declarada em
	// `value:`. SEMPRE preenchida, pendente ou disparado: é propriedade da
	// REGRA, como Condition, e não da transição.
	ValueColumn string
	// FiredAt é o instante da transição para o disparo; ZERO enquanto
	// pending — não há disparo a descrever ainda.
	FiredAt time.Time
	// FiringValue é o valor de ValueColumn capturado NESSA transição, e
	// HasFiringValue diz se ele existia no payload que decidiu o disparo. Os
	// dois são zero/false enquanto pending, pela mesma razão de FiredAt.
	FiringValue    float64
	HasFiringValue bool
}

// ActiveGroups devolve o retrato publicado pela última passada.
//
// A fatia é IMUTÁVEL e compartilhada: quem a recebe pode lê-la sem lock, e a
// passada seguinte publica outra em vez de mexer nesta. É o padrão de retrato
// publicado, e existe porque a alternativa — a tabela percorrer o mapa de
// trackers — é corrida de dados com a passada que o escreve.
func (r *Runner) ActiveGroups() []ActiveGroup {
	if p := r.snapshot.Load(); p != nil {
		return *p
	}
	return nil
}

// publishSnapshot monta e publica o retrato dos grupos em observação.
//
// Chamado SÓ quando houve transição na passada, e é o que mantém o gate de
// alocação em zero: uma passada sobre milhares de grupos saudáveis não muda
// nada, não publica nada e não aloca nada. Em troca, o retrato pode estar uma
// passada atrás de si mesmo — o que não é problema para uma tabela que descreve
// um estado cuja menor granularidade de mudança é a própria passada.
//
// A ordem é canônica (contador, alarme, grupo) pela razão de sempre: a tabela é
// consultada por SQL, e uma ordem que variasse entre passadas faria o mesmo
// SELECT sem ORDER BY devolver as linhas em ordem diferente a cada consulta.
func (r *Runner) publishSnapshot() {
	total := 0
	for _, rc := range r.counters {
		for _, ra := range rc.alarms {
			total += len(ra.trackers)
		}
	}
	if total == 0 {
		var empty []ActiveGroup
		r.snapshot.Store(&empty)
		return
	}
	out := make([]ActiveGroup, 0, total)
	for _, rc := range r.counters {
		for _, ra := range rc.alarms {
			for k, tr := range ra.trackers {
				text, scratch, err := counter.KeyText(nil, rc.format, []byte(k), r.vals)
				r.vals = scratch
				if err != nil {
					// Chave que não renderiza não é motivo para esconder a
					// linha: o estado existe, e o grupo sai identificado pelo
					// que se tem.
					text = nil
				}
				out = append(out, ActiveGroup{
					Counter:          rc.def.Counter,
					Alarm:            ra.compiled.name,
					GroupKey:         string(text),
					Firing:           tr.phase == phaseFiring,
					Since:            tr.observedSince(),
					Condition:        ra.compiled.when,
					SustainedFor:     ra.compiled.sustainedFor,
					QualifyCondition: ra.compiled.qualifyWhen,
					ValueColumn:      ra.compiled.valueColumn,
					FiredAt:          tr.firedAt,
					FiringValue:      tr.firingValue,
					HasFiringValue:   tr.hasFiringValue,
				})
			}
		}
	}
	slices.SortFunc(out, func(a, b ActiveGroup) int {
		if c := strings.Compare(a.Counter, b.Counter); c != 0 {
			return c
		}
		if c := strings.Compare(a.Alarm, b.Alarm); c != 0 {
			return c
		}
		return strings.Compare(a.GroupKey, b.GroupKey)
	})
	r.snapshot.Store(&out)
}

// snapshotHolder é o tipo do ponteiro publicado. Declarado à parte para que o
// campo do Runner leia como o que é.
type snapshotHolder = atomic.Pointer[[]ActiveGroup]
