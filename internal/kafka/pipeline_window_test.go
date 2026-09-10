package kafka

import "testing"

// A janela do anel vem do LOTE DE POLL, não do tamanho da fila.
//
// A distinção é de memória, e ela é grande: cada slot retém os buffers do
// decode até a marca d'água e não encolhe sozinho, então a janela multiplica
// tudo. Dimensionar pela `ingestionQueueSize` (5.000 no padrão) usaria o número
// que existe para ABSORVER RAJADA na entrada para medir o TRABALHO EM VOO, e o
// anel ficaria cinco vezes maior do que o pipeline processa de uma vez — 5.000
// arenas vivas dentro de um orçamento de 1 GiB (§109).
//
// O teste separa as duas grandezas por um fator grande justamente para que
// trocar uma pela outra não passe.
func TestAJanelaVemDoLoteDePollNaoDaFila(t *testing.T) {
	cfg := &Config{
		Dataset: "eventos", Topic: "eventos", GroupID: "g",
		Brokers:        []string{"127.0.0.1:9092"},
		MaxPollRecords: 64,
		QueueSize:      4096, // muito maior, de propósito
		QueueBytes:     64 << 20,
	}
	w := NewWriter(cfg, nil, storeGolden{}, NewQueue(cfg.QueueSize, cfg.QueueBytes),
		nil, registerMetrics(nil, "eventos"), nil, Cadencia{}, nil)

	if got := w.est.tamanho(); got != 64 {
		t.Errorf("janela da esteira = %d com maxPollRecords=64 e ingestionQueueSize=4096; "+
			"quero 64 — a janela é o trabalho em voo, não a rajada de entrada", got)
	}
}

// A janela é arredondada para cima, para potência de dois.
//
// O índice do anel é `seq & mask`, e a máscara só existe com potência de dois.
// Arredondar para BAIXO reduziria a janela abaixo do que o operador declarou,
// que é a direção errada num número que também é backpressure.
func TestAJanelaArredondaParaCima(t *testing.T) {
	casos := map[int]int{1: 1, 2: 2, 3: 4, 1000: 1024, 1024: 1024, 1025: 2048}
	for pedido, quero := range casos {
		if got := newPipeline(pedido).tamanho(); got != quero {
			t.Errorf("novaEsteira(%d).tamanho() = %d, quero %d", pedido, got, quero)
		}
	}
}

// A janela tem TETO, e o teto existe para trocar "trava" por "degrada".
//
// `maxPollRecords` é um inteiro positivo sem limite superior declarado, e o
// arredondamento é um laço de deslocamento: perto de 2^62 o deslocamento estoura
// para negativo, depois para zero, e o laço nunca converge. O processo travaria
// na subida — sem log, sem erro, sem diagnóstico —, que é o pior desfecho
// possível para um valor de configuração absurdo.
func TestAJanelaTemTetoENaoTravaComValorAbsurdo(t *testing.T) {
	for _, pedido := range []int{janelaMaxima, janelaMaxima + 1, 1 << 40, int(^uint(0) >> 1)} {
		got := powerOfTwo(pedido)
		if got != janelaMaxima {
			t.Errorf("potenciaDeDois(%d) = %d, quero o teto de %d", pedido, got, janelaMaxima)
		}
	}
	// E o teto não pode ter engolido a faixa útil.
	if got := powerOfTwo(1000); got != 1024 {
		t.Errorf("potenciaDeDois(1000) = %d, quero 1024", got)
	}
}
