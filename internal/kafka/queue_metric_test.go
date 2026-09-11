package kafka

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// scrape rende o /metrics do registry.
func scrape(t *testing.T, r *metrics.Registry) string {
	t.Helper()
	var sb strings.Builder
	if _, err := r.WriteTo(&sb); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return sb.String()
}

// A profundidade da fila é lida no INSTANTE DO SCRAPE, e é isso que impede a
// série de congelar quando quem a atualizava para.
//
// O defeito que este teste tranca é de produção, medido em 27/08/2026: os dois
// medidores eram gravados pelo laço de poll, o poller travou dentro do
// PollRecords e eles pararam no último valor amostrado. O writer seguiu
// drenando, a fila esvaziou, e o painel continuou afirmando "479 registros
// esperando" — apontando a investigação para o componente errado, justamente
// no minuto em que a pergunta era se o gargalo estava antes ou depois da fila.
//
// A asserção é a que separa as duas implementações: o valor muda entre dois
// scrapes SEM que nada do consumidor rode.
func TestProfundidadeDaFilaNaoCongelaQuandoOPollerPara(t *testing.T) {
	r := metrics.NewRegistry()
	met := registerMetrics(r, "events")

	// Nenhuma fonte instalada: o dataset sem consumidor Kafka não tem fila, e
	// publicar zero afirmaria que existe uma e está vazia.
	if s := scrape(t, r); strings.Contains(s, metrics.MetKafkaQueueDepth) {
		t.Fatalf("a série saiu sem fila instalada:\n%s", s)
	}

	records, bytes := 479, int64(1_039_285)
	met.OlhaAFila(func() (int, int64) { return records, bytes })

	if s := scrape(t, r); !strings.Contains(s, `nishidb_kafka_queue_depth{dataset="events"} 479`) {
		t.Fatalf("primeira leitura não reflete a fila:\n%s", s)
	}

	// O writer drena e o poller NÃO roda mais — que é exatamente o estado do
	// incidente. Na implementação antiga, nada reescreveria o medidor.
	records, bytes = 0, 0
	s := scrape(t, r)
	if !strings.Contains(s, `nishidb_kafka_queue_depth{dataset="events"} 0`) {
		t.Fatalf("a profundidade congelou depois de a fila esvaziar:\n%s", s)
	}
	if !strings.Contains(s, `nishidb_kafka_queue_bytes{dataset="events"} 0`) {
		t.Fatalf("os bytes congelaram depois de a fila esvaziar:\n%s", s)
	}
}

// A fonte é a Fila de verdade, e não um dublê: é o que prova que o coletor
// está ligado ao objeto que o writer drena.
func TestOColetorLeAFilaDeVerdade(t *testing.T) {
	r := metrics.NewRegistry()
	met := registerMetrics(r, "events")
	f := NewQueue(8, 1<<20)
	met.OlhaAFila(f.Profundidade)

	if s := scrape(t, r); !strings.Contains(s, `nishidb_kafka_queue_depth{dataset="events"} 0`) {
		t.Fatalf("fila nova devia sair vazia:\n%s", s)
	}
}
