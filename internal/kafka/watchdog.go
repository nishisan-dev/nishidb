package kafka

import (
	"context"
	"errors"
	"time"
)

// ErrSemProgresso é a saída do watchdog: o consumidor parou de receber com o
// broker declarando mensagens, e quem o criou deve recriá-lo.
//
// É sentinela e não erro genérico porque o desfecho é diferente de qualquer
// outra falha de ingestão: os demais erros PARAM a ingestão do dataset, e
// este manda tentar de novo pelo mesmo caminho da subida.
var ErrSemProgresso = errors.New("kafka: consumidor sem progresso com lag pendente")

// A CONSULTA DO VIGIA MUDOU JUNTO COM A MÉTRICA, e pela mesma razão.
//
// Ela usava kadm.Lag, que devolve o lag do offset COMMITADO no group — e o
// commit só sai depois do checkpoint durável (§48). Com watchdogTimeout no
// piso declarável de 10 s e storage.checkpoint.interval em 30 s, existia uma
// janela real em que o tópico silencia, os eventos param, o commit ainda não
// saiu e o broker responde "há o que consumir" sobre um consumidor
// perfeitamente em dia — com a fila vazia, que é exatamente a conjunção de
// vereditoRecria. Isso é RECRIAÇÃO ESPÚRIA de consumidor saudável, e o godoc
// abaixo a descartava apoiado justamente na leitura que não valia.
//
// Agora a pergunta é "o broker tem offset além do que eu APLIQUEI", que é a
// que o vigia sempre quis fazer, e ela não depende da cadência de checkpoint.
// De quebra, uma requisição em vez de três.
//
// prazoDaConsultaDeLag limita a pergunta ao broker. Ela roda no laço do
// watchdog, e um admin que também travasse não pode travar o vigia junto.
const prazoDaConsultaDeLag = 10 * time.Second

// intervaloDaVigia é de quanto em quanto tempo o watchdog olha. A janela de
// decisão é o WatchdogTimeout; este é só o passo da amostragem, e um quarto da
// janela dá quatro chances de ver o progresso antes de concluir que não houve.
func watchdogInterval(janela time.Duration) time.Duration {
	if p := janela / 4; p > time.Second {
		return p
	}
	return time.Second
}

// vigia observa a ingestão e devolve ErrSemProgresso quando ela para de andar
// com mensagens pendentes no broker.
//
// A DECISÃO É UMA CONJUNÇÃO DE TRÊS FATOS, e são três porque dois não bastam.
//
//  1. Os eventos não avançaram na janela. Sozinho, isto não diz nada: um
//     tópico legitimamente parado produz exatamente o mesmo sintoma, e um
//     watchdog que disparasse por aqui recriaria o consumidor toda madrugada
//     de baixo movimento.
//
//  2. O broker declara mensagens pendentes (lag > 0). Com o fato 1, já é
//     forte: há o que consumir e não estamos consumindo. Mas ainda é
//     ambíguo quanto à CAUSA.
//
//  3. A fila poller→writer está VAZIA. Este é o que separa as duas causas, e
//     é a razão de o watchdog não brigar com backpressure de persistência:
//
//     fila VAZIA  = o writer não tem o que fazer  → quem não entrega é o
//     poller, e recriar o cliente é o remédio certo.
//
//     fila COM ITENS = o writer não está dando conta (disco lento,
//     checkpoint longo, storage em modo falho) → recriar a conexão seria
//     ERRADO e destrutivo: abortaria trabalho em voo por um problema que
//     não é de conexão, e a fila voltaria a encher do mesmo jeito.
//
// A leitura da fila é a do instante do olhar, e não uma média: um writer que
// pega um item, some por um minuto e devolve a fila a zero é indistinguível
// de um poller travado por qualquer instrumento — mas nesse caso o próprio
// evento teria avançado, e o fato 1 não valeria.
func (c *Consumidor) vigia(ctx context.Context, janela time.Duration) error {
	if janela <= 0 {
		<-ctx.Done()
		return nil
	}
	t := time.NewTicker(watchdogInterval(janela))
	defer t.Stop()

	v := &vigilante{janela: janela, ultimoProgresso: time.Now(), eventos: c.met.eventos.Value()}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		l := leitura{eventos: c.met.eventos.Value()}
		// O lag e a fila só são consultados quando os eventos NÃO andaram:
		// perguntar ao broker a cada tique de um consumidor saudável seria
		// tráfego administrativo por nada.
		if l.eventos == v.eventos && time.Since(v.ultimoProgresso) >= janela {
			l.lagPendente, l.lagConhecido = c.lagPendente(ctx)
			l.fila, _ = c.fila.Profundidade()
		}
		switch v.avalia(l, time.Now()) {
		case vereditoRecria:
			return ErrSemProgresso
		case vereditoBackpressure:
			c.log.Warn("ingestao.sem_progresso",
				"a ingestão não avança e a fila tem itens: o gargalo é a PERSISTÊNCIA, não o Kafka; o watchdog não age",
				"janela", janela.String(), "fila", l.fila)
		}
	}
}

// leitura é o que o vigia enxerga num instante.
type leitura struct {
	eventos uint64
	// lagPendente só é verdadeiro com lagConhecido: "não perguntei" e
	// "perguntei e deu zero" são afirmações diferentes, e tratá-las igual
	// faria o watchdog agir sobre o que não sabe.
	lagPendente, lagConhecido bool
	fila                      int
}

type veredito int

const (
	// vereditoSegue: há progresso, ou não há o que decidir.
	vereditoSegue veredito = iota
	// vereditoBackpressure: parado, mas com fila — o gargalo é depois dela.
	vereditoBackpressure
	// vereditoRecria: parado, com lag e sem fila — o poller não entrega.
	vereditoRecria
)

// vigilante guarda o estado entre duas leituras.
type vigilante struct {
	janela          time.Duration
	ultimoProgresso time.Time
	eventos         uint64
}

// avalia devolve o veredito de uma leitura.
//
// O relógio de progresso é REINICIADO em tudo que não seja recriar, e isso é
// deliberado: a janela mede ausência de progresso desde a última vez em que
// houve motivo para não agir, não desde sempre. Sem esse reinício, um tópico
// que ficou uma hora parado e depois encheu de backpressure entraria na
// primeira leitura seguinte já com a janela vencida.
func (v *vigilante) avalia(l leitura, agora time.Time) veredito {
	if l.eventos != v.eventos {
		v.eventos = l.eventos
		v.ultimoProgresso = agora
		return vereditoSegue
	}
	if agora.Sub(v.ultimoProgresso) < v.janela {
		return vereditoSegue
	}
	// Lag desconhecido: o watchdog se abstém. É a resposta certa para "não
	// sei" — agir aqui seria recriar o consumidor por causa de um admin que
	// não respondeu.
	if !l.lagConhecido {
		v.ultimoProgresso = agora
		return vereditoSegue
	}
	// Tópico em dia. O silêncio é o esperado, e um watchdog que disparasse
	// aqui recriaria o consumidor toda madrugada de baixo movimento.
	if !l.lagPendente {
		v.ultimoProgresso = agora
		return vereditoSegue
	}
	// Fila com itens: o writer não está dando conta. Recriar a conexão seria
	// ERRADO e destrutivo — abortaria trabalho em voo por um problema que não
	// é de conexão, e a fila voltaria a encher do mesmo jeito.
	if l.fila > 0 {
		v.ultimoProgresso = agora
		return vereditoBackpressure
	}
	return vereditoRecria
}

// lagPendente pergunta ao BROKER se há mensagens não consumidas.
//
// Pergunta, e não lê a série de lag do /metrics: aquela é atualizada pelo
// SCRAPE (ver AtualizaLag), então num processo que ninguém raspa ela fica
// velha, e o watchdog decidiria por um número de idade desconhecida.
//
// A consulta vai pelo cliente ADMIN, que é uma conexão distinta da do
// consumo — e é exatamente por isso que este watchdog é construível: no
// incidente de 27/08/2026 o admin seguiu respondendo o tempo todo em que o
// PollRecords estava travado. Se um dia os dois travarem juntos, ok=false e o
// watchdog se abstém, que é a resposta certa para "não sei".
func (c *Consumidor) lagPendente(ctx context.Context) (pendente, ok bool) {
	ctx, cancela := context.WithTimeout(ctx, prazoDaConsultaDeLag)
	defer cancela()
	fins, err := c.adm.ListEndOffsets(ctx, c.cfg.Topic)
	if err != nil {
		c.met.errosKafka.Inc()
		return false, false
	}
	var viu bool
	for _, ps := range fins {
		for _, o := range ps {
			if o.Err != nil || o.Topic != c.cfg.Topic || o.Offset < 0 {
				continue
			}
			viu = true
			if o.Offset > c.met.posicaoDe(o.Partition).Load() {
				return true, true
			}
		}
	}
	if !viu {
		return false, false
	}
	return false, true
}
