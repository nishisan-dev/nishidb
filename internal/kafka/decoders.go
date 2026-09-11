package kafka

import (
	"context"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/materializer"
)

// decodificadores é o pool que tira eventos da fila, decodifica em paralelo e
// publica na esteira, que os reordena para o writer.
//
// POR QUE O DECODE É A METADE QUE PARALELIZA. Medido no piloto em 2026-08-26,
// dos 902 µs de CPU por evento, 298 são parse e resolução de campos — trabalho
// puro sobre o payload, sem tocar disco nem estado compartilhado depois que a
// descoberta saiu dele (Fase 4.2). O resto escreve, e escrever tem de continuar
// numa goroutine só: o `wmu` do storage serializa Put/Apenda/Sync/Checkpoint, e
// o Materializer reusa prevBuf/outBuf/histBuf entre eventos.
//
// UM CAMINHO DE CÓDIGO, DIMENSIONADO DIFERENTE. Com `decodeWorkers: 1` — o
// default — existe exatamente um decodificador, e ele passa pela esteira como
// os outros. A alternativa, um atalho serial quando N==1, faria a configuração
// de produção exercitar um caminho que a suíte quase não visita, e o defeito
// apareceria só sob carga.
type decodificadores struct {
	fila    *Fila
	est     *esteira
	mat     *materializer.Materializer
	met     *metricas
	n       int
	limiar  int
	esperar sync.WaitGroup
}

// novoPoolDeDecode monta o pool. limiar é o teto de memória retida por slot
// acima do qual o writer encolhe os buffers ao liberá-lo.
func newDecodePool(n int, fila *Fila, est *esteira, mat *materializer.Materializer, met *metricas, limiar int) *decodificadores {
	return &decodificadores{fila: fila, est: est, mat: mat, met: met, n: n, limiar: limiar}
}

// Rises lança as goroutines e devolve a função que espera todas encerrarem e
// declara a esteira sem mais eventos.
//
// A DECLARAÇÃO VEM DEPOIS DO WaitGroup, e a ordem é a correção: enquanto um
// decodificador estiver vivo, um slot vazio no meio do anel é indistinguível de
// um decode em curso, e o writer encerraria com evento por aplicar — perda
// silenciosa exatamente na janela em que o produto promete não perder nada.
func (d *decodificadores) Rises(ctx context.Context) func() {
	for i := 0; i < d.n; i++ {
		d.esperar.Add(1)
		go func() {
			defer d.esperar.Done()
			d.run(ctx)
		}()
	}
	return func() {
		d.esperar.Wait()
		d.est.withoutMaisEventos()
	}
}

// run é o laço de um decodificador.
//
// A ORDEM RESERVA→DECODE→PUBLICA é o que faz o arena morar no anel: reservar
// primeiro devolve o buffer do próprio slot, e o decode escreve direto nele.
// Decodificar num buffer local e copiar para o slot copiaria o cabeçalho do
// slice, os dois passariam a dividir o mesmo array, e o decode seguinte deste
// worker corromperia o evento já publicado — um valor de outro evento aparecendo
// numa linha, sem erro nenhum.
//
// TODO ITEM RESERVADO É PUBLICADO, inclusive a barreira e inclusive sob
// cancelamento. O writer consome por seq contíguo: um seq reservado e nunca
// publicado o deixaria esperando para sempre, e o encerramento morreria no meio
// de um checkpoint.
func (d *decodificadores) run(ctx context.Context) {
	for {
		it, ok := d.fila.Recebe(ctx)
		if !ok {
			return
		}
		dec := d.est.reserva(it.seq)
		if dec == nil {
			// Esteira abandonada: o writer foi embora. O evento não aplicado
			// não avança offset (§48), então o reinício o reprocessa.
			return
		}
		if it.Barreira == nil {
			d.mat.Decode(dec, it.Rec.Value)
		}
		d.est.publica(it.seq, it.Rec, it.Barreira)
	}
}
