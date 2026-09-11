package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A esteira entrega em ordem de seq, publiquem os decodificadores na ordem que
// publicarem.
//
// É a propriedade inteira do anel, e a afirmação é mais forte que "preserva a
// ordem por partição": o seq é atribuído no envio, que é sequencial, então
// consumir por seq crescente reproduz EXATAMENTE a sequência que a goroutine
// única veria — os mesmos vizinhos e o mesmo estado anterior em cada Put. A
// ordem por partição (§26) é um caso particular disso.
//
// A publicação é feita de trás para frente de propósito: é o pior caso, e o que
// um anel quebrado entregaria invertido.
func TestAEsteiraEntregaEmOrdemDeSeq(t *testing.T) {
	e := newPipeline(8)
	ctx := context.Background()

	const n = 8
	for seq := int64(n - 1); seq >= 0; seq-- {
		if d := e.reserva(seq); d == nil {
			t.Fatalf("reserva(%d) devolveu nil", seq)
		}
		e.publica(seq, &kgo.Record{Offset: seq}, nil)
	}

	for quero := int64(0); quero < n; quero++ {
		s, ok, acordou := e.consome(ctx, nil)
		if !ok || acordou {
			t.Fatalf("consome na posição %d: ok=%v acordou=%v", quero, ok, acordou)
		}
		if s.rec.Offset != quero {
			t.Fatalf("consome devolveu offset %d, quero %d", s.rec.Offset, quero)
		}
		e.advance(0)
	}
}

// A janela é backpressure: reservar além dela ESPERA, e é o writer que
// destrava.
//
// Sem isso o anel não seria limitado — um decodificador adiantado sobrescreveria
// o slot de um evento ainda não aplicado, e o writer aplicaria o evento errado
// sem que nada acusasse.
func TestAJanelaSeguraQuemSeAdianta(t *testing.T) {
	e := newPipeline(2) // janela 2: seq 2 só cabe depois de o writer passar do 0

	for seq := int64(0); seq < 2; seq++ {
		e.reserva(seq)
		e.publica(seq, &kgo.Record{Offset: seq}, nil)
	}

	entrou := make(chan struct{})
	go func() {
		e.reserva(2)
		close(entrou)
	}()
	select {
	case <-entrou:
		t.Fatal("reserva(2) passou com a janela cheia: o anel não é limitado")
	case <-time.After(50 * time.Millisecond):
	}

	s, ok, _ := e.consome(context.Background(), nil)
	if !ok {
		t.Fatal("consome não devolveu o slot 0")
	}
	_ = s
	e.advance(0)

	select {
	case <-entrou:
	case <-time.After(time.Second):
		t.Fatal("reserva(2) continuou presa depois de o writer avançar")
	}
}

// K decodificadores concorrentes, ordem preservada.
//
// O teste anterior prova a reordenação com publicações sequenciais e
// controladas; este a prova sob escalonamento real, que é onde um anel com
// aritmética errada falha de vez em quando em vez de sempre. As goroutinas
// pegam seq fora de ordem de propósito e dormem tempos diferentes.
func TestEsteiraComKDecodificadoresPreservaAOrdem(t *testing.T) {
	const n = 512
	e := newPipeline(64)
	ctx := context.Background()

	vistos := make([]int64, 0, n)
	writerPronto := make(chan struct{})
	go func() {
		defer close(writerPronto)
		for i := 0; i < n; i++ {
			s, ok, _ := e.consome(ctx, nil)
			if !ok {
				return
			}
			vistos = append(vistos, s.rec.Offset)
			e.advance(0)
		}
	}()

	// Um "poller" único distribui os seq em ordem para K workers, que é
	// exatamente o que a FIFO faz: o conjunto em voo é sempre contíguo.
	const k = 8
	entry := make(chan int64)
	var wg sync.WaitGroup
	for w := 0; w < k; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for seq := range entry {
				if d := e.reserva(seq); d == nil {
					return
				}
				// Tempos de decode diferentes por worker: é o que faz as
				// publicações chegarem fora de ordem.
				time.Sleep(time.Duration(w%3) * 50 * time.Microsecond)
				e.publica(seq, &kgo.Record{Offset: seq}, nil)
			}
		}(w)
	}
	for seq := int64(0); seq < n; seq++ {
		entry <- seq
	}
	close(entry)
	wg.Wait()

	select {
	case <-writerPronto:
	case <-time.After(10 * time.Second):
		t.Fatal("o writer não consumiu tudo: o anel travou")
	}
	if len(vistos) != n {
		t.Fatalf("o writer viu %d eventos, quero %d", len(vistos), n)
	}
	for i, v := range vistos {
		if v != int64(i) {
			t.Fatalf("posição %d entregou o seq %d: a esteira não reordenou", i, v)
		}
	}
}

// O encerramento não perde evento e não trava.
//
// São as duas metades do mesmo desenho: a esteira só é declarada sem mais
// eventos DEPOIS de todo decodificador ter saído — antes disso, um slot vazio é
// indistinguível de um decode em curso —, e quem espera vaga precisa ser
// destravado quando o writer vai embora, ou o encerramento nunca termina.
func TestEncerramentoDrenaTudoENaoTrava(t *testing.T) {
	e := newPipeline(16)
	ctx := context.Background()

	for seq := int64(0); seq < 8; seq++ {
		e.reserva(seq)
		e.publica(seq, &kgo.Record{Offset: seq}, nil)
	}
	e.withoutMaisEventos()

	var vistos int
	for {
		_, ok, _ := e.consome(ctx, nil)
		if !ok {
			break
		}
		vistos++
		e.advance(0)
	}
	if vistos != 8 {
		t.Fatalf("drenou %d de 8 eventos publicados antes do encerramento", vistos)
	}

	// E o outro lado: quem espera vaga com o writer já fora.
	e2 := newPipeline(1)
	e2.reserva(0)
	e2.publica(0, &kgo.Record{}, nil)
	saiu := make(chan bool, 1)
	go func() {
		d := e2.reserva(1) // janela 1, slot 0 ocupado: espera
		saiu <- d == nil
	}()
	time.Sleep(20 * time.Millisecond)
	e2.abandona()
	select {
	case nulo := <-saiu:
		if !nulo {
			t.Error("reserva devolveu slot numa esteira abandonada")
		}
	case <-time.After(time.Second):
		t.Fatal("reserva ficou presa depois do abandono: o encerramento trava aqui")
	}
}

// A ociosidade do WRITER sai da esteira, e não da fila.
//
// É uma armadilha de instrumentação que a Fase 4 abriu e que passaria calada.
// Até aqui o writer consumia a fila, e `nishidb_kafka_writer_idle_seconds_total`
// saía do acumulador dela. Com os decodificadores no meio, quem espera na fila
// são ELES — e são K. Continuar publicando aquele número como ociosidade do
// writer mentiria por um FATOR: com oito decodificadores parados o acumulado
// sobe oito segundos por segundo, e a tabela de decisão de
// docs/operacao-kafka.md compara `rate()` com 1. Pior: a espera do writer, que é
// a metade que decide se ele é o gargalo, deixaria de ser medida.
func TestAOciosidadeDoWriterSaiDaEsteiraENaoDaFila(t *testing.T) {
	e := newPipeline(4)
	f := NewQueue(4, 1<<20)

	if d := e.IDle(); d != 0 {
		t.Fatalf("esteira nasceu com %v de ociosidade", d)
	}

	// O writer espera na esteira, sem nada publicado.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, ok, _ := e.consome(ctx, nil); ok {
		t.Fatal("consome devolveu slot de uma esteira vazia")
	}
	if d := e.IDle(); d <= 0 {
		t.Fatal("a esteira não contou a espera do writer")
	}

	// E a fila, que ninguém consumiu, não contou ociosidade nenhuma: com a
	// contabilidade no lugar errado, ela é que teria o número.
	if _, idle := f.Tempos(); idle != 0 {
		t.Fatalf("a fila contou %v de ociosidade sem ninguém a consumir", idle)
	}
}
