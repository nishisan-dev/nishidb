package kafka

import (
	"testing"
	"time"
)

const windowOfTest = time.Minute

func newVigilante() (*vigilante, time.Time) {
	t0 := time.Date(2026, 8, 27, 8, 0, 0, 0, time.UTC)
	return &vigilante{window: windowOfTest, ultimoProgresso: t0, eventos: 100}, t0
}

func exige(t *testing.T, got, quer veredito, caso string) {
	t.Helper()
	nomes := map[veredito]string{vereditoFollow: "segue", vereditoBackpressure: "backpressure", vereditoRecria: "recria"}
	if got != quer {
		t.Fatalf("%s: veredito %s, quer %s", caso, nomes[got], nomes[quer])
	}
}

// O caso que o watchdog existe para pegar: parado, o broker declarando
// mensagens, e a fila vazia — o writer não tem o que fazer, então quem não
// entrega é o poller.
func TestParadoComLagEFilaVaziaRecria(t *testing.T) {
	v, t0 := newVigilante()
	l := leitura{eventos: 100, lagPendente: true, lagConhecido: true, fila: 0}
	exige(t, v.evaluate(l, t0.Add(30*time.Second)), vereditoFollow, "dentro da janela")
	exige(t, v.evaluate(l, t0.Add(61*time.Second)), vereditoRecria, "além da janela")
}

// A guarda que o operador pediu: backpressure da persistência NÃO pode
// reiniciar a conexão. Fila com itens significa que o writer é o gargalo, e
// recriar abortaria trabalho em voo por um problema que não é de conexão.
func TestBackpressureNaPersistenciaNaoRecria(t *testing.T) {
	v, t0 := newVigilante()
	l := leitura{eventos: 100, lagPendente: true, lagConhecido: true, fila: 4999}
	exige(t, v.evaluate(l, t0.Add(61*time.Second)), vereditoBackpressure, "fila cheia")
	// E não vira recriação por insistência: mesmo depois de muitas janelas
	// com a fila ocupada, o veredito continua o mesmo.
	agora := t0.Add(61 * time.Second)
	for range 10 {
		agora = agora.Add(61 * time.Second)
		exige(t, v.evaluate(l, agora), vereditoBackpressure, "insistindo com fila cheia")
	}
}

// Um item só na fila já basta para se abster: a asserção é sobre "o writer
// tem o que fazer", não sobre a fila estar cheia.
func TestUmItemNaFilaJaImpedeARecriacao(t *testing.T) {
	v, t0 := newVigilante()
	l := leitura{eventos: 100, lagPendente: true, lagConhecido: true, fila: 1}
	exige(t, v.evaluate(l, t0.Add(61*time.Second)), vereditoBackpressure, "um item")
}

// Tópico legitimamente parado é o falso positivo clássico, e é por isso que o
// gatilho não pode ser "sem eventos por N segundos".
func TestTopicoEmDiaNuncaRecria(t *testing.T) {
	v, t0 := newVigilante()
	l := leitura{eventos: 100, lagPendente: false, lagConhecido: true, fila: 0}
	agora := t0
	for range 20 {
		agora = agora.Add(61 * time.Second)
		exige(t, v.evaluate(l, agora), vereditoFollow, "tópico sem tráfego")
	}
}

// Admin que não respondeu é "não sei", e agir sobre o que não se sabe seria
// recriar o consumidor por causa de uma consulta que falhou.
func TestLagDesconhecidoFazOVigiaSeAbster(t *testing.T) {
	v, t0 := newVigilante()
	l := leitura{eventos: 100, lagConhecido: false, fila: 0}
	exige(t, v.evaluate(l, t0.Add(61*time.Second)), vereditoFollow, "lag desconhecido")
}

func TestProgressoReiniciaARelogio(t *testing.T) {
	v, t0 := newVigilante()
	parado := leitura{eventos: 100, lagPendente: true, lagConhecido: true}
	exige(t, v.evaluate(parado, t0.Add(59*time.Second)), vereditoFollow, "quase vencendo")
	// Um evento passou: a janela recomeça daqui.
	exige(t, v.evaluate(leitura{eventos: 101, lagPendente: true, lagConhecido: true}, t0.Add(60*time.Second)),
		vereditoFollow, "houve progresso")
	parado.eventos = 101
	exige(t, v.evaluate(parado, t0.Add(119*time.Second)), vereditoFollow, "ainda dentro da nova janela")
	exige(t, v.evaluate(parado, t0.Add(121*time.Second)), vereditoRecria, "nova janela vencida")
}

// Sem o reinício, um tópico que ficou uma hora parado entraria na primeira
// leitura com backpressure já com a janela vencida — e recriaria na leitura
// seguinte, que é exatamente o que a guarda existe para impedir.
func TestOReinicioImpedeQueAJanelaVelhaContamineOProximoVeredito(t *testing.T) {
	v, t0 := newVigilante()
	inDia := leitura{eventos: 100, lagPendente: false, lagConhecido: true}
	exige(t, v.evaluate(inDia, t0.Add(1*time.Hour)), vereditoFollow, "uma hora em dia")
	// Agora chega tráfego e o writer engasga: a fila enche.
	withFila := leitura{eventos: 100, lagPendente: true, lagConhecido: true, fila: 10}
	exige(t, v.evaluate(withFila, t0.Add(1*time.Hour).Add(time.Second)), vereditoFollow,
		"a janela recomeçou depois do tópico em dia")
}

func TestIntervaloDaVigiaNuncaEhMenorQueUmSegundo(t *testing.T) {
	if got := watchdogInterval(10 * time.Second); got != 2500*time.Millisecond {
		t.Errorf("janela de 10s -> passo %s, quer 2.5s", got)
	}
	if got := watchdogInterval(2 * time.Second); got != time.Second {
		t.Errorf("janela curta -> passo %s, quer 1s", got)
	}
}
