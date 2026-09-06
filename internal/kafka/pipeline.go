package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/materializer"
)

// esteira é o anel que reordena o que N decodificadores produzem fora de ordem
// e entrega ao writer na ordem de `seq`.
//
// TRÊS MECANISMOS VIRAM UM. O anel indexado por `seq & mask` faz reordenação
// (o writer só consome o slot de `proxima`), reciclagem de buffer (o slot de
// `seq` é o mesmo de `seq+janela`, e o decode escreve direto nele) e
// backpressure (reservar `seq` espera até `seq - proxima < janela`) com a mesma
// aritmética. Uma fila de saída ordenada precisaria dos três separados.
//
// A AFIRMAÇÃO DE CORREÇÃO é mais forte que "preserva a ordem por partição": o
// `seq` é atribuído no ENVIO, que é sequencial, então consumir por `seq`
// crescente reproduz EXATAMENTE a sequência que a goroutine única veria — a
// mesma ordem, os mesmos vizinhos, o mesmo estado anterior em cada Put. O §26
// sai de graça como caso particular.
//
// MUTEX E COND, não lock-free. A 2.000 ev/s o orçamento por evento é 500 µs e
// sinalizar uma cond custa ~100 ns: o lock-free não compraria nada e custaria
// uma prova.
//
// PROGRESSO É GARANTIDO, e a razão precisa ficar escrita porque um anel com
// backpressure é onde deadlock mora. Os `seq` são atribuídos em ordem no envio
// e os decodificadores tiram da FIFO, então o conjunto em voo é sempre
// `[proxima, proxima+K-1]` para K decodificadores. Quem segura `proxima` nunca
// espera vaga — `0 < janela` —, então ele sempre publica, o writer sempre
// avança, e as vagas sempre saem. É por isso que a janela precisa ser
// confortavelmente maior que o número de workers, nunca o contrário.
type esteira struct {
	mu     sync.Mutex
	slots  []slot
	mask   int64
	janela int64

	// proxima é o seq que o writer quer consumir agora. Tudo abaixo dela já
	// foi aplicado; o slot de proxima-janela já pode ser reusado.
	proxima int64

	// livre acorda quem espera vaga. Cond e não channel porque a espera é de
	// MUITOS (K decodificadores) por um evento de UM (o writer avançou), que é
	// exatamente o que Broadcast faz.
	livre *sync.Cond

	// pronto acorda o writer. Channel e não cond porque o writer espera em
	// select junto do despertador do checkpoint — o débito 56 — e cond não
	// entra em select. Buffer 1 pela mesma razão que o `espera` da Fila: o
	// sinal não pode se perder entre a checagem e o bloqueio.
	pronto chan struct{}

	// ocioso acumula NANOSSEGUNDOS que o WRITER passou esperando trabalho, e
	// desdeOcio marca o início da espera em curso.
	//
	// A contabilidade migrou da Fila para cá junto com o writer. Enquanto ele
	// consumia a fila, `nishidb_kafka_writer_idle_seconds_total` saía de lá; com
	// os decodificadores no meio, quem espera na fila são ELES, e continuar
	// publicando aquele número como ociosidade do writer seria uma métrica
	// mentindo o nome — pior, mentindo por um FATOR: com K decodificadores
	// parados, o acumulado sobe K segundos por segundo, e a tabela de decisão de
	// docs/operacao-kafka.md compara `rate()` com 1.
	//
	// O trecho em curso é somado na leitura pela mesma razão que na Fila: o caso
	// que mais importa é aquele em que a espera NÃO termina, e um contador que
	// só soma no fim fica imóvel exatamente aí.
	ocioso    atomic.Int64
	desdeOcio atomic.Int64

	// semMais marca que nenhum decodificador vai publicar de novo. O writer
	// drena o que sobrou, em ordem, e só então encerra.
	semMais bool
	// abandonada marca que o writer foi embora. Quem espera vaga desiste em
	// vez de esperar para sempre — o evento não aplicado não avança offset,
	// então o reinício o reprocessa.
	abandonada bool
}

// slot é uma posição do anel: o Decodificado que o worker preenche e o que o
// writer precisa saber sobre ele.
type slot struct {
	dec      materializer.Decodificado
	rec      *kgo.Record
	barreira *barreira
	pronto   bool

	// retidos espelha dec.RetainedBytes() para quem raspa a métrica.
	//
	// ESPELHO, e não leitura direta, porque o gauge é lido pela goroutine do
	// exportador enquanto um decodificador escreve no arena do slot: somar
	// dec.RetainedBytes() ali seria corrida de dado sobre os cabeçalhos dos
	// slices, e o -race a pegaria — depois de a série já existir em produção.
	// Quem escreve é sempre o dono corrente do slot, no instante em que ele
	// termina.
	retidos atomic.Int64
}

// NovaEsteira monta o anel com a janela pedida, arredondada para a potência de
// dois imediatamente superior.
//
// Potência de dois porque o índice é `seq & mask`: com módulo, cada evento
// pagaria uma divisão inteira no caminho quente para nada.
//
// A JANELA É maxPollRecords, e não ingestionQueueSize. Cada slot segura os
// buffers do decode até a marca d'água e nunca encolhe sozinho, então a janela
// é um multiplicador de memória — 5.000 slots dimensionariam o anel pela fila
// de ENTRADA, que existe para absorver rajada, e não pelo trabalho em voo, que
// é o que o anel de fato guarda. Um lote de poll é o que o pipeline processa de
// uma vez.
func novaEsteira(janela int) *esteira {
	n := potenciaDeDois(janela)
	e := &esteira{
		slots:  make([]slot, n),
		mask:   n - 1,
		janela: n,
		pronto: make(chan struct{}, 1),
	}
	e.livre = sync.NewCond(&e.mu)
	return e
}

// tamanho é a janela efetiva do anel, já arredondada.
func (e *esteira) tamanho() int { return int(e.janela) }

// Reserva espera a vaga do seq e devolve o Decodificado daquele slot para o
// worker preencher.
//
// O decode escreve DIRETO no slot, sem cópia intermediária, e é isso que faz o
// arena viver no anel: copiar um Decodificado copiaria o cabeçalho do slice e
// os dois passariam a dividir o mesmo array, de modo que o decode seguinte do
// worker corromperia o evento já publicado.
//
// Devolve nil quando a esteira foi abandonada: o writer não está mais lá para
// consumir, e insistir seria esperar para sempre.
func (e *esteira) reserva(seq int64) *materializer.Decodificado {
	e.mu.Lock()
	defer e.mu.Unlock()
	for seq-e.proxima >= e.janela && !e.abandonada {
		e.livre.Wait()
	}
	if e.abandonada {
		return nil
	}
	return &e.slots[seq&e.mask].dec
}

// Publica marca o slot pronto para o writer.
//
// Acorda o writer SÓ quando o que ficou pronto é o slot que ele espera. Um
// decodificador adiantado — o caso comum com K workers — não tem por que tirar
// o writer do lugar: quando `proxima` chegar, o slot já estará pronto e o
// caminho rápido do Consome o pega sem bloquear.
func (e *esteira) publica(seq int64, rec *kgo.Record, b *barreira) {
	s := &e.slots[seq&e.mask]
	// Fora do lock: o slot ainda é do decodificador neste instante, e a soma
	// percorre os buffers dele.
	s.retidos.Store(int64(s.dec.BytesRetidos()))

	e.mu.Lock()
	s.rec, s.barreira, s.pronto = rec, b, true
	acorda := seq == e.proxima
	e.mu.Unlock()
	if acorda {
		select {
		case e.pronto <- struct{}{}:
		default:
		}
	}
}

// Consome entrega o próximo slot em ordem de seq.
//
// A assinatura espelha a de Fila.recebeOuAcorda de propósito, inclusive o
// terceiro retorno: o writer precisa distinguir "a esteira acabou" de "o
// despertador do checkpoint tocou" (débito 56), e tratar os dois como fim do
// laço faria o writer encerrar a cada intervalo.
//
// O slot devolvido continua sendo do anel: só é liberado por Avanca, o que dá
// ao writer o tempo do Aplica inteiro sobre buffers que ninguém mais toca.
func (e *esteira) consome(ctx context.Context, acorda <-chan time.Time) (*slot, bool, bool) {
	for {
		e.mu.Lock()
		s := &e.slots[e.proxima&e.mask]
		if s.pronto {
			e.mu.Unlock()
			return s, true, false
		}
		fim := e.semMais
		e.mu.Unlock()
		if fim {
			// Os seq são contíguos e todo worker publica o que reservou, então
			// slot de proxima vazio com semMais ligado só pode significar que
			// não há mais nada. Não existe buraco a pular.
			return nil, false, false
		}
		// Daqui em diante o writer está ocioso de verdade: o caminho rápido
		// acima já não achou trabalho. O relógio só é lido QUANDO HOUVE espera,
		// como na Fila — um time.Now() por evento no caminho rápido seria
		// instrumentação mudando o que ela mede.
		fecha := marcaEspera(&e.desdeOcio, &e.ocioso)
		select {
		case <-e.pronto:
			fecha()
		case <-acorda:
			fecha()
			return nil, false, true
		case <-ctx.Done():
			fecha()
			return nil, false, false
		}
	}
}

// Avanca libera o slot que o writer acabou de aplicar.
//
// Encolhe é chamado AQUI, e não no decode, porque este é o único instante em
// que o slot está comprovadamente ocioso: o writer terminou e o próximo dono
// dele só chega daqui a `janela` eventos. Encolher no decode devolveria a
// memória e a retomaria no evento seguinte, que é churn, não economia.
func (e *esteira) avanca(limiarDeEncolhimento int) bool {
	e.mu.Lock()
	s := &e.slots[e.proxima&e.mask]
	s.pronto, s.rec, s.barreira = false, nil, nil
	// Libera SEMPRE, Encolhe só acima do limiar. São coisas diferentes: a
	// primeira solta memória de outro dono (o buffer de fetch de que o evento é
	// janela), e segurá-la num slot ocioso mantém vivo o lote inteiro; a segunda
	// devolve os buffers que são nossos, ao preço de o próximo evento realocar.
	s.dec.Libera()
	encolheu := s.dec.Encolhe(limiarDeEncolhimento)
	s.retidos.Store(int64(s.dec.BytesRetidos()))
	e.proxima++
	e.mu.Unlock()
	e.livre.Broadcast()
	return encolheu
}

// BytesRetidos soma a memória que os slots seguram — a de regime e a em voo.
//
// É o número que o gauge publica, e ele existe porque a esteira é o TERCEIRO
// consumidor de memória da ingestão: os dois gates da fila contam registros
// enfileirados e bytes de payload cru, e nenhum dos dois enxerga o que o slot
// segura. O de registros porque o record já saiu da fila; o de bytes porque ele
// devolveu o orçamento no recebimento, de propósito e documentado lá.
//
// São as DUAS parcelas de propósito. A de regime é a marca d'água dos buffers
// que o slot carrega de um evento para o próximo, que o limiar de encolhimento
// governa. A em voo é o payload do evento decodificado e ainda não aplicado, que
// nenhum limiar governa — ver o débito 101. Publicar só a primeira faria o gauge
// ficar plano exatamente quando o pico acontece.
func (e *esteira) bytesRetidos() int64 {
	var n int64
	for i := range e.slots {
		n += e.slots[i].retidos.Load()
	}
	return n
}

// Ocioso é o tempo que o writer passou esperando trabalho, INCLUINDO a espera
// que ainda está em curso.
func (e *esteira) Ocioso() time.Duration {
	return time.Duration(e.ocioso.Load() + emCurso(&e.desdeOcio))
}

// SemMaisEventos declara que nenhum decodificador vai publicar de novo.
//
// Chamado depois que TODOS os workers saíram — antes disso, um slot vazio no
// meio do anel seria indistinguível de um decode em curso, e o writer
// encerraria com evento por aplicar.
func (e *esteira) semMaisEventos() {
	e.mu.Lock()
	e.semMais = true
	e.mu.Unlock()
	select {
	case e.pronto <- struct{}{}:
	default:
	}
}

// Abandona destrava quem espera vaga porque o writer foi embora.
//
// O evento reservado e não aplicado se perde, e isso é correto: o offset só
// avança para evento DECIDIDO (§48), então o reinício o reprocessa. A
// alternativa — deixar os workers esperando — trava o encerramento e o
// supervisor mata o processo no meio de um checkpoint.
func (e *esteira) abandona() {
	e.mu.Lock()
	e.abandonada = true
	e.mu.Unlock()
	e.livre.Broadcast()
}

// janelaMaxima é o teto do anel, em slots.
//
// Existe porque `maxPollRecords` é um inteiro positivo sem teto declarado, e o
// arredondamento para potência de dois é um laço de deslocamento: com um valor
// perto de 2^62 o deslocamento estoura para negativo, depois para zero, e o
// laço NUNCA converge — o processo trava na subida, sem log, sem erro e sem
// diagnóstico. Um teto transforma "trava" em "degrada".
//
// 65.536 slots é ordem de grandeza acima de qualquer lote de poll plausível
// (o padrão é 1.000) e já custa alguns MiB só em estruturas de slot. Acima
// disso o anel deixa de comprar sobreposição — o trabalho em voo é limitado
// pela velocidade dos decodificadores, não pelo tamanho do anel — e passa a ser
// só memória.
const janelaMaxima = 1 << 16

// potenciaDeDois arredonda a janela para cima até a potência de dois, limitada
// a janelaMaxima.
func potenciaDeDois(janela int) int64 {
	if janela >= janelaMaxima {
		return janelaMaxima
	}
	n := int64(1)
	for n < int64(janela) {
		n <<= 1
	}
	return n
}

// paginaDoSO é o piso do limiar de encolhimento.
//
// Abaixo de uma página, soltar o buffer não devolve memória ao sistema
// operacional — devolve ao alocador do Go, que a guarda —, então o limiar
// pequeno só compraria realocação. É o mesmo raciocínio do débito 28: o que
// ocupa memória é o array, e devolvê-lo só vale quando ele é grande.
const paginaDoSO = 4096

// limiarDeSlot é quanta memória um slot pode reter antes de ser encolhido.
//
// DERIVADO, e não inventado. O operador já declarou quanto a ingestão pode
// segurar de eventos em voo: `ingestionQueueBytes`, o segundo gate da fila. A
// esteira é um consumidor da mesma natureza — memória presa por evento que
// ainda não virou linha —, e cada um dos seus slots pode reter, no máximo, a
// parte média desse orçamento.
//
// O QUE ESTE LIMIAR GOVERNA, E O QUE ELE NÃO GOVERNA. Ele é aplicado ao LIBERAR
// o slot, então o que ele limita é o que os slots OCIOSOS carregam de um evento
// para o próximo — a marca d'água que nunca encolhe, que é o vazamento lento
// que a Fase 4 podia introduzir. Ele NÃO limita o pico: um anel cheio de
// eventos decodificados e ainda não aplicados segura os arenas de todos eles,
// cada um do tamanho do seu evento, e o gate de bytes da fila já devolveu esse
// orçamento no dequeue. O teto do pico é a janela vezes o maior evento em voo.
//
// Não há gate para o pico, e não há um de propósito: recusar um evento por
// causa da soma dos vizinhos seria descartar dado por pressão de memória, que é
// exatamente o que o §101 não autoriza. O que existe é a MEDIDA —
// `nishidb_ingest_decode_arena_bytes`, lida da estrutura viva no scrape —, e é
// dela que sai a decisão de baixar `maxPollRecords`. Registrado como débito.
//
// Nos padrões do produto — 64 MiB de fila e janela de 1.024 — dá 64 KiB por
// slot, o que é folgado para um evento RADIUS (~2,4 KiB medidos no piloto) e
// aparado para o evento de megabytes que a esteira existe para não segurar.
//
// O piso de uma página impede que uma fila declarada pequena transforme cada
// evento numa realocação.
func limiarDeSlot(cfg *Config) int {
	janela := potenciaDeDois(cfg.MaxPollRecords)
	limiar := cfg.QueueBytes / janela
	if limiar < paginaDoSO {
		return paginaDoSO
	}
	return int(limiar)
}
