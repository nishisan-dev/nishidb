//go:build !race

package seriesdelivery

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// instantProducer conclui cada registro dentro do próprio TryProduce, sem
// reter nada: o gate abaixo mede o Publish, e não o produtor.
type instantProducer struct{}

func (instantProducer) TryProduce(_ context.Context, r *kgo.Record, done func(*kgo.Record, error)) {
	done(r, nil)
}
func (instantProducer) Close() {}

// publishAllocsWithoutWait é o que o Publish SEM espera aloca, medido: o
// contexto do prazo do registro (com o timer e o cancel), o record, as cópias
// da chave e do corpo e o callback. É o necessário — o registro sai do scratch
// do emissor e vive até o callback — e é o MESMO número do Publish anterior à
// contrapressão (medido em 02/10/2026 sobre o 1cba0a3: 8). O número é fixo, e
// não um teto, para que um context.AfterFunc (ou qualquer alocação) que vaze do
// caminho de espera para o caminho rápido reprove aqui.
const publishAllocsWithoutWait = 8

func TestPublishWithoutWaitKeepsItsMeasuredAllocations(t *testing.T) {
	o := testOutput(instantProducer{})
	defer o.d.cancel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key, body, at := []byte("key"), []byte(`{"total":1}`), time.Now()
	got := testing.AllocsPerRun(200, func() {
		if err := o.Publish(ctx, key, body, at); err != nil {
			t.Fatal(err)
		}
	})
	if got != publishAllocsWithoutWait {
		t.Fatalf("o Publish sem espera alocou %v vez(es) por registro, quero %d", got, publishAllocsWithoutWait)
	}
	if s := o.snapshot(); s.Delivered != 201 || s.AdmissionWaitSeconds != 0 {
		t.Fatalf("o gate não exerceu o caminho sem espera: %+v", s)
	}
}
