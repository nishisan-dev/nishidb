package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Item é o que trafega na fila.
//
// Marca != nil identifica o MARCADOR DE FIM DE LOTE, que não carrega evento.
// Ele existe porque o poller precisa saber quando o writer terminou o lote
// para liberar o rebalance (kgo.AllowRebalance) — e a fila é FIFO com um
// consumidor só, então um marcador no fim do lote é sinal exato e sem
// contador compartilhado.
//
// É exportado para que o harness de benchmark possa usar a fila DE VERDADE em
// vez de uma imitação: o §108 exige medir a rodada de 8M com ingestão Kafka
// real, e uma fila de mentira mediria o orçamento de memória errado.
type Item struct {
	Rec   *kgo.Record
	Marca chan struct{}
	bytes int64
}

// Fila é o bounded channel do §27 com os DOIS gates que o §29 e o §128
// exigem.
//
// O gate de REGISTROS é a capacidade do channel (`ingestionQueueSize`). O
// gate de BYTES é o segundo, e existe porque contar registros não é unidade
// de memória: 5000 slots com payload grande estouram o orçamento de 1 GiB do
// §109 sem que a contagem reclame, e o §79 exige expor "Kafka buffers".
//
// Um produtor (o poller) e um consumidor (o writer único). Sob saturação o
// Envia bloqueia, o poller para de fazer fetch, e o franz-go segue mandando
// heartbeat na goroutine dele — que é literalmente o que o §29 pede: reduzir
// intake, preservar membership, manter heartbeats, não perder mensagem.
type Fila struct {
	ch       chan Item
	maxBytes int64

	mu    sync.Mutex
	bytes int64
	// espera é sinalizado (não bloqueante) quando bytes diminuem. Buffer 1
	// basta porque há um único produtor à espera.
	espera chan struct{}
}

// NovaFila constrói a fila com os dois tetos.
func NovaFila(registros int, maxBytes int64) *Fila {
	return &Fila{
		ch:       make(chan Item, registros),
		maxBytes: maxBytes,
		espera:   make(chan struct{}, 1),
	}
}

// Envia bloqueia até haver espaço nos dois gates, ou até o contexto acabar.
//
// O contexto é conferido ANTES de qualquer tentativa: com fila vazia, um
// select entre "cabe" e "contexto cancelado" teria os dois casos prontos e o
// Go escolheria aleatoriamente, tornando o encerramento não determinístico.
func (f *Fila) Envia(ctx context.Context, r *kgo.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n := int64(len(r.Value) + len(r.Key))
	if err := f.reserva(ctx, n); err != nil {
		return err
	}
	select {
	case f.ch <- Item{Rec: r, bytes: n}:
		return nil
	case <-ctx.Done():
		f.libera(n)
		return ctx.Err()
	}
}

// EnviaMarca põe o marcador de fim de lote. Não consome orçamento de bytes.
func (f *Fila) EnviaMarca(ctx context.Context, marca chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case f.ch <- Item{Marca: marca}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reserva toma n bytes do orçamento.
func (f *Fila) reserva(ctx context.Context, n int64) error {
	for {
		f.mu.Lock()
		// A segunda condição é o escape para o evento maior que o teto
		// inteiro: sem ela, um único record acima de ingestionQueueBytes
		// nunca caberia e a ingestão travaria para sempre, num deadlock que
		// nenhuma métrica explicaria. Com a fila vazia, o record passa
		// sozinho — o teto é de FILA, não de evento (o teto de evento é o
		// do §31, e vira EVENT_TOO_LARGE lá no materializer).
		if f.bytes+n <= f.maxBytes || f.bytes == 0 {
			f.bytes += n
			f.mu.Unlock()
			return nil
		}
		f.mu.Unlock()
		select {
		case <-f.espera:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *Fila) libera(n int64) {
	if n == 0 {
		return
	}
	f.mu.Lock()
	f.bytes -= n
	f.mu.Unlock()
	select {
	case f.espera <- struct{}{}:
	default:
	}
}

// Recebe entrega o próximo item, esperando por ele o quanto for preciso.
func (f *Fila) Recebe(ctx context.Context) (Item, bool) {
	it, ok, _ := f.recebeOuAcorda(ctx, nil)
	return it, ok
}

// recebeOuAcorda é o Recebe com uma saída a mais: o canal `acorda`.
//
// Existe pelo débito 56. O checkpoint por TEMPO tem de disparar mesmo sem
// evento novo, e o Recebe puro deixa o writer dormindo no channel até chegar
// um — com o tópico em dia, isso é para sempre. `acordou` distingue o
// despertar do FIM da fila, que também devolve ok=false: tratar os dois igual
// faria o writer encerrar o laço a cada intervalo.
//
// Com `acorda` nil o comportamento é exatamente o de antes, porque channel
// nil nunca fica pronto num select.
//
// O orçamento de bytes é liberado AQUI, e não depois que o writer aplica o
// evento: o que se está limitando é a memória retida pela FILA, e o record
// que o writer está processando não está mais nela. Contar o evento em voo
// tornaria a contabilidade mais conservadora e a API mais fácil de errar (o
// chamador teria de devolver os bytes em todos os caminhos de erro).
func (f *Fila) recebeOuAcorda(ctx context.Context, acorda <-chan time.Time) (Item, bool, bool) {
	if ctx.Err() != nil {
		return Item{}, false, false
	}
	select {
	case it, ok := <-f.ch:
		if !ok {
			return Item{}, false, false
		}
		f.libera(it.bytes)
		return it, true, false
	case <-acorda:
		return Item{}, false, true
	case <-ctx.Done():
		return Item{}, false, false
	}
}

// Fecha encerra a fila. Só o produtor pode chamar.
func (f *Fila) Fecha() { close(f.ch) }

// Profundidade devolve o ocupado nos dois gates, para o §79.
func (f *Fila) Profundidade() (registros int, bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ch), f.bytes
}
