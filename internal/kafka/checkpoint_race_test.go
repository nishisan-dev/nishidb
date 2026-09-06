package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// storeLento é o dublê que ALARGA a janela da corrida.
//
// O Checkpoint real leva centenas de milissegundos no piloto (fsync mais os
// msync de todos os índices), e é durante ele que a goroutine do group e a do
// writer se encontram. Um dublê instantâneo fecharia a janela e faria o teste
// passar por sorte de escalonamento — que é exatamente o modo de falha que
// este arquivo existe para não ter.
type storeLento struct {
	mu       sync.Mutex
	offsets  checkpoint.SourceOffsets
	demora   time.Duration
	chamadas int
}

func (s *storeLento) Checkpoint() error {
	time.Sleep(s.demora)
	s.mu.Lock()
	s.chamadas++
	s.mu.Unlock()
	return nil
}

func (s *storeLento) SetSourceOffsets(o checkpoint.SourceOffsets) error {
	// Guarda a referência de propósito, como o store real faz: é isto que
	// torna a CÓPIA do mapa em checkpoint() obrigatória. Sem ela, o writer
	// seguiria mutando por avanca o mesmo mapa que está aqui dentro, e o lock
	// do writer não alcançaria este lado da fronteira.
	s.mu.Lock()
	s.offsets = o
	s.mu.Unlock()
	return nil
}

func (s *storeLento) SourceOffsets() (checkpoint.SourceOffsets, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsets, true, nil
}

func (s *storeLento) RowCount() uint64 { return 0 }

// leTudo percorre o mapa guardado pelo store, imitando o que o gravador do
// manifest faz com ele. É aqui que a referência compartilhada apareceria como
// leitura concorrente de mapa.
func (s *storeLento) leTudo() {
	s.mu.Lock()
	o := s.offsets
	s.mu.Unlock()
	for p, v := range o.Offsets {
		_, _ = p, v
	}
}

// TestCheckpointDaRevogacaoNaoCorreComOWriter reproduz a corrida entre
// aoRevogar e a goroutine do writer.
//
// aoRevogar chama w.checkpoint DA GOROUTINE DO GROUP do franz-go
// (consumer.go), e ao mesmo tempo o writer pode estar em avanca ou em
// talvezCheckpoint pelo despertador. Os três tocam offsets, desdeCP e
// ultimoCP. O comentário do campo dizia "só a goroutine do writer o toca", e
// isso nunca foi verdade: a barreira de fim de lote esvazia a FILA, não para
// a GOROUTINE.
//
// Sem o posMu, este teste reprova sob -race com "race detected" — verificado
// removendo o lock antes de escrevê-lo.
//
// O -race é o oráculo aqui, e não o throw do runtime, de propósito: a detecção
// de escrita concorrente em mapa é best-effort e NÃO dispara a cada ocorrência
// (com o lock removido, este mesmo teste passa sem -race). Um teste que
// esperasse o `fatal error` seria intermitente. O que está em jogo em produção
// é justamente essa loteria: quando o runtime pega, derruba o processo; quando
// não pega, corrompe o mapa de offsets em silêncio.
func TestCheckpointDaRevogacaoNaoCorreComOWriter(t *testing.T) {
	st := &storeLento{demora: 2 * time.Millisecond}
	cfg := &Config{Dataset: "eventos", Topic: "eventos", GroupID: "g", Brokers: []string{"127.0.0.1:9092"}}
	met := registraMetricas(nil, "eventos")

	commit := func(_ context.Context, off map[int32]int64) error {
		// Percorre o mapa recebido, como o franz-go faz ao montar a requisição
		// de commit. Segunda fronteira que a cópia protege.
		for p, v := range off {
			_, _ = p, v
		}
		return nil
	}

	w := NovoWriter(cfg, nil, st, nil, nil, met, map[int32]int64{0: 0},
		Cadencia{Eventos: 5, Intervalo: time.Millisecond}, commit)

	ctx := context.Background()
	const voltas = 300

	var wg sync.WaitGroup
	wg.Add(3)

	// (1) A goroutine do writer: avança offsets a cada evento decidido.
	go func() {
		defer wg.Done()
		for i := 0; i < voltas; i++ {
			w.avanca(&kgo.Record{Partition: int32(i % 4), Offset: int64(i)})
		}
	}()

	// (2) A goroutine do writer de novo: o despertador reavaliando a cadência.
	go func() {
		defer wg.Done()
		for i := 0; i < voltas; i++ {
			if err := w.talvezCheckpoint(ctx); err != nil {
				t.Errorf("talvezCheckpoint: %v", err)
				return
			}
		}
	}()

	// (3) A goroutine do GROUP: é o aoRevogar, e é ela que não deveria estar
	// aqui sem sincronização.
	go func() {
		defer wg.Done()
		for i := 0; i < voltas/10; i++ {
			if err := w.checkpoint(ctx); err != nil {
				t.Errorf("checkpoint da revogação: %v", err)
				return
			}
			st.leTudo()
		}
	}()

	wg.Wait()

	// O writer decidiu `voltas` eventos sobre 4 partições, então a última
	// posição de cada uma é conhecida: o maior i daquela partição, mais um.
	w.posMu.Lock()
	defer w.posMu.Unlock()
	for p := int32(0); p < 4; p++ {
		var ultimo int64 = -1
		for i := 0; i < voltas; i++ {
			if int32(i%4) == p {
				ultimo = int64(i)
			}
		}
		if got := w.offsets[p]; got != ultimo+1 {
			t.Fatalf("partição %d: offset %d, quer %d", p, got, ultimo+1)
		}
	}
}
