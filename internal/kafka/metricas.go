package kafka

import (
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/materializer"
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

	bloqueado *metrics.SerieMedidor
	// filaViva é lida no instante do SCRAPE, e não gravada pelo laço de poll.
	//
	// Ela era um par de medidores que o poller atualizava depois de enfileirar
	// o lote (consumidor.go). O defeito disso não aparece em regime: aparece
	// exatamente quando alguém precisa dela. Com o poller travado dentro do
	// PollRecords, os dois medidores CONGELAM no último valor amostrado — e o
	// writer, que segue drenando, esvazia a fila sem que ninguém reescreva o
	// número. O painel passa a afirmar "479 registros esperando" sobre uma
	// fila vazia, no minuto em que a pergunta é justamente se o gargalo está
	// antes ou depois dela.
	//
	// Isso não é hipótese: foi o que aconteceu no piloto em 27/08/2026, e a
	// série congelada apontou a investigação para o componente errado.
	//
	// Como coletor, o valor sai de len(ch) no momento do scrape. Ele não pode
	// mentir por travamento de ninguém — no máximo não é emitido, e ausência
	// é uma resposta honesta que a série congelada não dava.
	filaViva       func() (int, int64)
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
	famInvalidos := r.Contador(metrics.MetIngestInvalidTotal,
		"Eventos descartados por serem ruins (§101), por motivo (categoria §101; Task A5/issue #15).",
		metrics.LabelDataset, metrics.LabelMotivo)
	// Pré-registra as seis categorias de evento ruim em zero: sem isto, cada
	// série só nasce no primeiro Apply que descartar um evento daquela
	// categoria, e nishidb_ingest_invalid_total{motivo="..."} deixa de
	// aparecer em /metrics desde a subida — mudança observável de superfície
	// que o piloto em produção já emite. O enum §101 é fechado (por isso
	// BadEvent(), não uma lista escrita à mão), então o custo de cardinalidade
	// é fixo: sempre seis séries por dataset.
	for cat := materializer.ErrorCategory(0); cat < materializer.NumCategories; cat++ {
		if cat.BadEvent() {
			famInvalidos.Com(dataset, cat.String())
		}
	}
	m := &metricas{
		eventos:      contador(metrics.MetIngestEventsTotal, "Eventos oferecidos à ingestão."),
		stale:        contador(metrics.MetIngestStaleTotal, "Eventos descartados por sequence (§25). Não é erro."),
		famInvalidos: famInvalidos,
		falhas: contador(metrics.MetIngestFailedTotal,
			"Eventos que NÃO entraram por falha que não é do evento. O §92 não prevê esta série, e sem ela a perda de dado seria invisível num daemon, onde não há exit code para carregá-la."),
		errosKafka: contador(metrics.MetKafkaErrorsTotal,
			"Erros de broker ou rede. Balde próprio: o §101 só cataloga erro de EVENTO, e contar erro de rede como perda de dado é diagnóstico errado."),

		bloqueado: medidor(metrics.MetIngestBlocked,
			"1 quando a ingestão do dataset está PARADA por política de erro. Sem ela, a pausa do `invalidEvent: stop` seria um consumer silenciosamente parado."),
		partAtribuidas: medidor(metrics.MetKafkaPartitionsAssigned,
			"Partições atribuídas a esta instância. Comparar com o total é o que torna visível o assignment parcial."),
		partTotal: medidor(metrics.MetKafkaPartitionsTotal, "Partições do tópico."),

		famLag:    r.Medidor(metrics.MetKafkaLag, "Lag por partição.", metrics.LabelDataset, metrics.LabelPartition),
		famOffset: r.Medidor(metrics.MetKafkaOffset, "Próximo offset a consumir, por partição.", metrics.LabelDataset, metrics.LabelPartition),
		dataset:   dataset,
	}

	// O coletor é registrado AQUI, junto das famílias, e não onde a fila
	// nasce: a fonte é instalada depois (OlhaAFila), e até lá o coletor não
	// emite nada. Registrar cedo mantém a ordem de exposição estável entre
	// um consumidor que subiu e um que não subiu.
	r.Coletor(metrics.ColetorFunc(m.amostrasDaFila))
	return m
}

// OlhaAFila instala a fonte de profundidade. Chamado uma vez, quando a fila
// existe.
func (m *metricas) OlhaAFila(f func() (int, int64)) { m.filaViva = f }

// amostrasDaFila lê a fila no instante do scrape.
//
// Sem fonte instalada não emite nada, e isso é deliberado: o dataset sem
// consumidor Kafka não tem fila, e publicar zero ali afirmaria que existe uma
// e está vazia. É a mesma distinção entre "medi e deu zero" e "não há o que
// medir" que o resto do produto faz.
func (m *metricas) amostrasDaFila() []metrics.Amostra {
	if m.filaViva == nil {
		return nil
	}
	reg, bytes := m.filaViva()
	rotulo := []metrics.Label{{Nome: metrics.LabelDataset, Valor: m.dataset}}
	return []metrics.Amostra{
		{
			Nome: metrics.MetKafkaQueueDepth, Tipo: metrics.TipoMedidor, Valor: float64(reg),
			Ajuda:  "Registros na fila limitada poller→writer, lidos no instante do scrape.",
			Labels: rotulo,
		},
		{
			Nome: metrics.MetKafkaQueueBytes, Tipo: metrics.TipoMedidor, Valor: float64(bytes),
			Ajuda:  "Bytes retidos na fila limitada (§79), lidos no instante do scrape.",
			Labels: rotulo,
		},
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
