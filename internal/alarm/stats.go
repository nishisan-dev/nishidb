package alarm

import (
	"sync/atomic"
	"time"
)

// Stats é o retrato de UM alarme para a observabilidade.
//
// Um por alarme declarado, sempre — mesmo o que nunca disparou. É o que faz
// "nenhum disparo" ser distinguível de "ninguém está observando": série que
// pode alarmar não pode nascer no primeiro alarme.
type Stats struct {
	Dataset string
	Counter string
	Alarm   string
	// Tracked são os grupos em observação: a condição vale para eles, tenham
	// ou não cumprido o prazo. Acompanha o PROBLEMA, nunca o tamanho do
	// inventário.
	Tracked int64
	// Firing são os grupos com o alarme disparado agora.
	Firing int64
	// Fired e Resolved são acumulados desde a subida.
	Fired    uint64
	Resolved uint64
	// EnrichFailed e EnrichSkipped são contados SEPARADOS porque pedem ações
	// diferentes: falha manda investigar a consulta, pulo diz que houve
	// avalanche e o teto da passada agiu. Um contador só faria os dois
	// parecerem o mesmo problema.
	EnrichFailed  uint64
	EnrichSkipped uint64
}

// alarmCounters são os acumulados de um alarme. Atômicos porque o scrape os lê
// de outra goroutine enquanto a passada os escreve.
type alarmCounters struct {
	fired         atomic.Uint64
	resolved      atomic.Uint64
	enrichFailed  atomic.Uint64
	enrichSkipped atomic.Uint64
	firing        atomic.Int64
}

// Stats devolve um retrato por alarme declarado, na ordem da declaração.
func (r *Runner) Stats() []Stats {
	out := make([]Stats, 0, len(r.counters))
	for _, rc := range r.counters {
		for _, ra := range rc.alarms {
			out = append(out, Stats{
				Dataset:       rc.def.Dataset,
				Counter:       rc.def.Counter,
				Alarm:         ra.compiled.name,
				Tracked:       int64(len(ra.trackers)),
				Firing:        ra.stats.firing.Load(),
				Fired:         ra.stats.fired.Load(),
				Resolved:      ra.stats.resolved.Load(),
				EnrichFailed:  ra.stats.enrichFailed.Load(),
				EnrichSkipped: ra.stats.enrichSkipped.Load(),
			})
		}
	}
	return out
}

// LastSweep é quanto durou a última passada.
//
// É o número que diz se a varredura está acompanhando a cadência. Sem ele, uma
// passada que passou a durar mais que o intervalo apareceria apenas como série
// atrasada, sem causa nomeada.
func (r *Runner) LastSweep() time.Duration { return time.Duration(r.lastSweep.Load()) }

// RestoredGroups são os grupos cujo cronômetro veio do disco na subida.
func (r *Runner) RestoredGroups() int64 { return r.restoredGroups }
