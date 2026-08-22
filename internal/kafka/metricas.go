package kafka

import (
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// metricas são as séries do §92 que só o consumer pode alimentar, mais as
// cinco que o M2 precisou inventar (ver internal/metrics/nomes.go).
//
// Todas carregam o label `dataset`: o §82 permite vários datasets por
// instância, e uma série sem esse label somaria consumos independentes num
// número que não descreve nenhum deles.
//
// Nunca é nil. Quando o chamador não passa registry, as séries nascem num
// registry descartável que ninguém raspa — o que custa alguns átomos e evita
// espalhar `if met != nil` por todo ponto de instrumentação, que é onde a
// guarda esquecida vira panic em produção.
type metricas struct {
	eventos    *metrics.SerieContador
	stale      *metrics.SerieContador
	falhas     *metrics.SerieContador
	errosKafka *metrics.SerieContador

	// famInvalidos carrega o LabelMotivo (Task A5, issue #15): cada categoria
	// §101 de evento ruim tem sua própria série, e não uma soma cega — é o
	// que permite ao painel e a um alerta nomear EVENT_TOO_LARGE em vez de
	// enxergar só um total misturado com INVALID_JSON e as outras quatro.
	famInvalidos *metrics.FamiliaContador

	bloqueado      *metrics.SerieMedidor
	filaRegistros  *metrics.SerieMedidor
	filaBytes      *metrics.SerieMedidor
	partAtribuidas *metrics.SerieMedidor
	partTotal      *metrics.SerieMedidor

	famLag    *metrics.FamiliaMedidor
	famOffset *metrics.FamiliaMedidor
	dataset   string
}

// registraMetricas instala as séries do dataset.
func registraMetricas(r *metrics.Registry, dataset string) *metricas {
	if r == nil {
		r = metrics.NewRegistry()
	}
	contador := func(nome, ajuda string) *metrics.SerieContador {
		return r.Contador(nome, ajuda, metrics.LabelDataset).Com(dataset)
	}
	medidor := func(nome, ajuda string) *metrics.SerieMedidor {
		return r.Medidor(nome, ajuda, metrics.LabelDataset).Com(dataset)
	}
	return &metricas{
		eventos: contador(metrics.MetIngestEventsTotal, "Eventos oferecidos à ingestão."),
		stale:   contador(metrics.MetIngestStaleTotal, "Eventos descartados por sequence (§25). Não é erro."),
		famInvalidos: r.Contador(metrics.MetIngestInvalidTotal,
			"Eventos descartados por serem ruins (§101), por motivo (categoria §101; Task A5/issue #15).",
			metrics.LabelDataset, metrics.LabelMotivo),
		falhas: contador(metrics.MetIngestFailedTotal,
			"Eventos que NÃO entraram por falha que não é do evento. O §92 não prevê esta série, e sem ela a perda de dado seria invisível num daemon, onde não há exit code para carregá-la."),
		errosKafka: contador(metrics.MetKafkaErrorsTotal,
			"Erros de broker ou rede. Balde próprio: o §101 só cataloga erro de EVENTO, e contar erro de rede como perda de dado é diagnóstico errado."),

		bloqueado: medidor(metrics.MetIngestBlocked,
			"1 quando a ingestão do dataset está PARADA por política de erro. Sem ela, a pausa do `invalidEvent: stop` seria um consumer silenciosamente parado."),
		filaRegistros: medidor(metrics.MetKafkaQueueDepth, "Registros na fila limitada poller→writer."),
		filaBytes:     medidor(metrics.MetKafkaQueueBytes, "Bytes retidos na fila limitada (§79, Kafka buffers)."),
		partAtribuidas: medidor(metrics.MetKafkaPartitionsAssigned,
			"Partições atribuídas a esta instância. Comparar com o total é o que torna visível o assignment parcial."),
		partTotal: medidor(metrics.MetKafkaPartitionsTotal, "Partições do tópico."),

		famLag:    r.Medidor(metrics.MetKafkaLag, "Lag por partição.", metrics.LabelDataset, metrics.LabelPartition),
		famOffset: r.Medidor(metrics.MetKafkaOffset, "Próximo offset a consumir, por partição.", metrics.LabelDataset, metrics.LabelPartition),
		dataset:   dataset,
	}
}

func (m *metricas) lag(p int32) *metrics.SerieMedidor {
	return m.famLag.Com(m.dataset, strconv.Itoa(int(p)))
}

func (m *metricas) offset(p int32) *metrics.SerieMedidor {
	return m.famOffset.Com(m.dataset, strconv.Itoa(int(p)))
}

// invalido devolve a série do motivo dado — o nome da categoria §101
// (ErrorCategory.String() do materializer, ex.: "EVENT_TOO_LARGE").
func (m *metricas) invalido(motivo string) *metrics.SerieContador {
	return m.famInvalidos.Com(m.dataset, motivo)
}
