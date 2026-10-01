package alarm

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// minSweepInterval é o piso da cadência da varredura.
//
// O mesmo piso de um segundo do watchdogInterval do Kafka, e pela mesma razão:
// a cadência é derivada do prazo que se quer medir, e um prazo curto não pode
// virar varredura contínua sobre milhares de grupos.
const minSweepInterval = time.Second

// sweepDivisor é quantas vezes a varredura amostra dentro do menor prazo
// declarado. Quatro, como o watchdog: com passada igual ao prazo, um alarme de
// dois minutos levaria até quatro para sair, porque a sustentação só é
// reconhecida entre DUAS observações.
const sweepDivisor = 4

// sweepInterval devolve a cadência da varredura para os alarmes declarados, ou
// zero quando não há nenhum — e zero é a resposta que faz o laço não subir.
func sweepInterval(defs []Definition) time.Duration {
	menor := time.Duration(0)
	for _, d := range defs {
		for _, a := range d.alarms {
			if menor == 0 || a.sustainedFor < menor {
				menor = a.sustainedFor
			}
		}
	}
	if menor == 0 {
		return 0
	}
	if d := menor / sweepDivisor; d > minSweepInterval {
		return d
	}
	return minSweepInterval
}

// Ciclo de vida do laço, no molde do StartRetention do storage: primeira
// passada imediata, medidor atômico observável, cancelamento por contexto, e o
// Stop cancela e AGUARDA.
type loop struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	active atomic.Bool
	inKeep sync.WaitGroup
}

// Start sobe o laço de varredura. Não sobe nada quando nenhum alarme foi
// declarado: não se paga thread para não fazer nada. É idempotente — duas
// chamadas não são duas goroutines.
func (r *Runner) Start(ctx context.Context) {
	interval := sweepInterval(r.definitions())
	if interval == 0 {
		return
	}
	r.loop.mu.Lock()
	defer r.loop.mu.Unlock()
	if r.loop.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	r.loop.cancel = cancel
	// O medidor sobe AQUI, e não dentro da goroutine: quem chamou tem direito
	// de ver o estado que acabou de instalar, e um Active() lido entre a
	// chamada e o primeiro escalonamento não pode responder que o laço não
	// subiu.
	r.loop.active.Store(true)
	r.loop.inKeep.Add(1)
	go func() {
		defer r.loop.inKeep.Done()
		// Declarado DEPOIS do Done e por isso roda ANTES dele: o Stop devolve
		// o controle já com o medidor em zero, e não com uma janela em que a
		// goroutine acabou e a série ainda diz 1.
		defer r.loop.active.Store(false)
		r.run(ctx, interval)
	}()
}

// Stop cancela o laço, AGUARDA a passada em voo e grava o estado.
//
// Cancelar sozinho devolveria o controle com uma varredura ainda lendo grupos e
// ainda gravando o arquivo de estado. A passada em voo não vai até o fim: o
// cancelamento a interrompe antes do próximo grupo (ver evaluate), então a
// espera é a do grupo corrente, e não a de uma rajada inteira. É idempotente e pode ser chamado sem
// nunca ter havido laço — é o que permite a quem nos usa chamá-lo sem perguntar
// se a configuração o ligou.
//
// # Por que gravar aqui, e não só nas transições
//
// O estado é gravado quando algo muda, e é o suficiente para o cronômetro
// sobreviver. Mas o arquivo carrega TAMBÉM o savedAt, que é o que o evento
// restaurado usa para dizer quanto tempo ninguém observou — e com gravação só
// em transições esse instante fica na última mudança, não na parada.
//
// O defeito apareceu no piloto, num reinício de verdade: nove minutos sem
// transição, parada de segundos, e o downtimeMs do evento anunciou nove
// minutos de lacuna. O campo promete "quanto tempo ninguém observou" e
// entregava "quanto tempo desde a última mudança de estado" — enganoso para
// quem usa o número para decidir se confia no alarme.
//
// Gravar no encerramento gracioso põe o savedAt no instante da parada. Num
// crash sobra o último save, e aí a lacuna é de fato desconhecida: um número
// grande passa a ser a resposta honesta, em vez de um artefato da cadência de
// transições.
func (r *Runner) Stop() {
	r.loop.mu.Lock()
	cancel := r.loop.cancel
	r.loop.cancel = nil
	r.loop.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.loop.inKeep.Wait()
	// Depois do Wait não há mais passada em voo, então isto não disputa o
	// estado com ninguém.
	if err := r.persist(r.now()); err != nil {
		r.log.Warn("alarm.state_not_saved",
			"o estado de sustentação não foi gravado no encerramento; os cronômetros voltam do último save e a lacuna do próximo evento será maior que a parada",
			"dataset", r.dataset, "error", err.Error())
	}
}

// Active diz se o laço está de pé. Vai para a observabilidade: "nenhum alarme
// disparou" é indistinguível de "ninguém está observando" sem esta série.
func (r *Runner) Active() bool { return r.loop.active.Load() }

// run é o laço. Primeira passada imediata — um alarme que já deveria estar
// valendo na subida não espera uma cadência inteira para ser notado.
func (r *Runner) run(ctx context.Context, interval time.Duration) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// O cancelamento do Stop interrompe a passada entre grupos.
		if err := r.evaluate(ctx.Done()); err != nil {
			// Cancelamento não é falha.
			if ctx.Err() != nil {
				return
			}
			// Para na PRIMEIRA falha, e para de vez, como os demais laços do
			// produto: o que chega aqui é leitura de grupo que falhou ou
			// escrita de estado que falhou, e a passada seguinte reencontraria
			// o mesmo estado e o mesmo erro. Um laço que insistisse
			// transformaria uma falha em enxurrada de log.
			r.log.Error("alarm.stopped",
				"a varredura de alarmes falhou e o laço parou; os alarmes deste dataset não são mais observados",
				"dataset", r.dataset, "error", err.Error())
			return
		}
		t.Reset(interval)
	}
}
