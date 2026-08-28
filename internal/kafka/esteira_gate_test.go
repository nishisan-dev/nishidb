//go:build !race

package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A esteira não aloca por evento. Nada, nunca.
//
// Ela entra no caminho quente de TODO evento — reservar, publicar, consumir,
// liberar —, e a ingestão do piloto roda 24/7 a centenas de eventos por
// segundo: alocação aqui é pressão de GC permanente, e num pipeline que existe
// para ganhar CPU seria ganhar de um lado e pagar do outro.
//
// O gate nasce COM a estrutura, e não depois. Foi a ausência de um gate
// equivalente sobre o writer Kafka que deixou a alocação por evento do
// `avanca` — uma string nova por partição por evento — passar despercebida até
// alguém ir procurar com profile na mão.
//
// O limiar de encolhimento entra generoso de propósito: encolher SOLTA memória
// e o próximo decode realoca, então medir com um limiar apertado mediria o
// churn do trim, não o caminho quente.
func TestEsteiraNaoAlocaNoCaminhoQuente(t *testing.T) {
	e := novaEsteira(64)
	ctx := context.Background()
	rec := &kgo.Record{Partition: 0, Offset: 0, Value: []byte(`{"id":"A"}`)}
	seq := int64(0)

	volta := func() {
		if d := e.reserva(seq); d == nil {
			t.Fatal("reserva devolveu nil")
		}
		e.publica(seq, rec, nil)
		s, ok, _ := e.consome(ctx, nil)
		if !ok {
			t.Fatal("consome não devolveu o slot recém-publicado")
		}
		_ = s
		e.avanca(1 << 30)
		seq++
	}
	// Aquece: a primeira volta paga o que as seguintes reusam.
	for i := 0; i < 3; i++ {
		volta()
	}
	if n := testing.AllocsPerRun(500, volta); n != 0 {
		t.Errorf("allocs/evento na esteira = %.1f, quer 0", n)
	}
}
