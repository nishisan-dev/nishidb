package kafka

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	filaViva func() (int, int64)
	// temposDaFila é a integral de espera dos dois lados, lida no scrape pela
	// mesma razão da profundidade.
	temposDaFila   func() (time.Duration, time.Duration)
	partAtribuidas *metrics.SerieMedidor
	partTotal      *metrics.SerieMedidor

	// posicoes é a posição APLICADA por partição — o próximo offset a
	// consumir —, publicada pelo writer a cada evento decidido.
	//
	// Mapa IMUTÁVEL trocado por ponteiro atômico, e não uma família de
	// medidores. O caminho quente só faz Store no *atomic.Int64 que já existe;
	// nada é criado por evento. Antes, avanca chamava
	// famOffset.Com(dataset, strconv.Itoa(p)), e Com monta a chave com
	// strings.Join(valores, "\xff") — uma string nova A CADA EVENTO, mais o
	// slice variádico, no caminho quente de um projeto cuja doutrina é zero
	// alocação ali. Nenhum gate cobria esse trecho: os de alocação medem
	// PatchBuffer.Parse e Materializer.Apply, e o writer Kafka está fora dos
	// dois.
	posicoes atomic.Pointer[map[int32]*atomic.Int64]
	// novasPart serializa a criação do mapa; fora do caminho quente.
	novasPart sync.Mutex

	// marca é o fim do log por partição, como o broker respondeu, com o
	// instante da resposta. Só o scrape a escreve.
	marca atomic.Pointer[marcaDagua]

	// aplicaSeg é a latência de um evento pelo Apply.
	aplicaSeg *metrics.SerieHistograma

	dataset string
}

// marcaDagua é um instantâneo do fim do log, com a hora em que ele foi colhido.
//
// O instante viaja junto porque um valor velho não pode ser publicado como se
// fosse de agora: ele erraria para BAIXO, que é o lado que ESCONDE incidente.
type marcaDagua struct {
	fim map[int32]int64
	em  time.Time
}

// limitesDoApply são os buckets do histograma do Apply.
//
// Próprios, e não os LimitesPadrao: aqueles começam em 50 µs, e a mediana
// medida no piloto é da ordem de 8 µs — o primeiro bucket engoliria quase
// tudo e o histograma não distinguiria nada abaixo dele. O topo em 1 s é o que
// torna os stalls de segundos ATRIBUÍVEIS ao Apply, em vez de a "algo".
var limitesDoApply = []float64{
	0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.005, 0.01, 0.05, 0.1, 1,
}

// validadeDaMarcaDagua é por quanto tempo a última resposta do broker ainda
// descreve o presente.
//
// Um minuto é mais que um intervalo de scrape típico e menos que qualquer
// janela em que o operador consiga nomear o instante. Vencida sem leitura
// nova, as séries que dependem do broker somem — ausência é resposta honesta,
// número velho não é.
const validadeDaMarcaDagua = time.Minute

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

		aplicaSeg: r.Histograma(metrics.MetIngestApplySeconds,
			"Latência de um evento pelo Apply: parse, estado, merge, Put e o Apenda do histórico. O checkpoint fica fora — ele tem histograma próprio.",
			limitesDoApply, metrics.LabelDataset).Com(dataset),
		dataset: dataset,
	}

	// O coletor é registrado AQUI, junto das famílias, e não onde a fila
	// nasce: a fonte é instalada depois (OlhaAFila), e até lá o coletor não
	// emite nada. Registrar cedo mantém a ordem de exposição estável entre
	// um consumidor que subiu e um que não subiu.
	r.Coletor(metrics.ColetorFunc(m.amostrasDaFila))
	r.Coletor(metrics.ColetorFunc(m.amostrasDaPosicao))
	return m
}

// OlhaAFila instala a fonte de profundidade. Chamado uma vez, quando a fila
// existe.
func (m *metricas) OlhaAFila(f func() (int, int64)) { m.filaViva = f }

// OlhaOsTempos instala a fonte da integral de espera. Mesma vida da OlhaAFila.
func (m *metricas) OlhaOsTempos(f func() (time.Duration, time.Duration)) { m.temposDaFila = f }

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
	out := []metrics.Amostra{
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
	if m.temposDaFila == nil {
		return out
	}
	bloqueado, ocioso := m.temposDaFila()
	return append(out,
		metrics.Amostra{
			Nome: metrics.MetKafkaPollBlockedSeconds, Tipo: metrics.TipoContador,
			Valor:  bloqueado.Seconds(),
			Ajuda:  "Segundos que o poller passou esperando espaço na fila. rate() é a fração do tempo em que o writer foi o gargalo.",
			Labels: rotulo,
		},
		metrics.Amostra{
			Nome: metrics.MetKafkaWriterIdleSeconds, Tipo: metrics.TipoContador,
			Valor:  ocioso.Seconds(),
			Ajuda:  "Segundos que o writer passou esperando evento. rate() alto com lag alto significa que quem não entrega é o poller.",
			Labels: rotulo,
		},
	)
}

// posicaoDe devolve o slot atômico da partição, criando-o se for a primeira
// vez.
//
// O caminho quente é o ramo de cima: uma carga atômica e um lookup num mapa de
// int32, sem alocar. A criação é rara — uma vez por partição por assignment —
// e paga um mutex e a cópia do mapa, que é o preço de o leitor nunca precisar
// de lock.
func (m *metricas) posicaoDe(p int32) *atomic.Int64 {
	if mp := m.posicoes.Load(); mp != nil {
		if s, ok := (*mp)[p]; ok {
			return s
		}
	}
	return m.registraParticao(p)
}

func (m *metricas) registraParticao(p int32) *atomic.Int64 {
	m.novasPart.Lock()
	defer m.novasPart.Unlock()
	// Recheca sob o lock: duas goroutines podem ter falhado o ramo rápido.
	if mp := m.posicoes.Load(); mp != nil {
		if s, ok := (*mp)[p]; ok {
			return s
		}
	}
	novo := make(map[int32]*atomic.Int64)
	if mp := m.posicoes.Load(); mp != nil {
		for k, v := range *mp {
			novo[k] = v
		}
	}
	s := &atomic.Int64{}
	novo[p] = s
	m.posicoes.Store(&novo)
	return s
}

// publicaMarca guarda o instantâneo do broker, carimbado com o agora.
func (m *metricas) publicaMarca(fim map[int32]int64, em time.Time) {
	m.marca.Store(&marcaDagua{fim: fim, em: em})
}

// amostrasDaPosicao emite offset, high watermark e lag DO MESMO INSTANTÂNEO.
//
// É o coletor que torna `lag == high_watermark - offset` um invariante dentro
// de uma raspagem, e portanto testável.
//
// O DEFEITO QUE ISTO FECHA. nishidb_kafka_lag vinha de kadm.Lag, que devolve o
// lag do offset COMMITADO no consumer group — e o commit só sai depois do
// checkpoint durável (§48), a cada 30s no piloto. Entre dois commits o offset
// do group fica parado e a série cresce à taxa de PRODUÇÃO, com o consumidor
// perfeitamente em dia. Medido no piloto em 28/08/2026: a série subia até
// 14.167 e caía para 515 num único segundo em que 478 eventos foram
// consumidos, e o crescimento entre commits era de 450 ev/s contra 452 ev/s de
// produção. O painel mandou a investigação atrás de um gargalo de vazão que
// não era o problema.
//
// Agora o lag é fim-do-log menos posição APLICADA. A posição aplicada, e não a
// pollada: escolher a pollada faria offset + lag != high watermark e
// reintroduziria, num terceiro lugar, a divergência que já quebrou o painel.
//
// Sob falha do broker, as séries que dependem dele NÃO SAEM. Zero é medição;
// ausência é ausência de medição, e `absent(nishidb_kafka_lag)` é um alerta
// que o operador consegue escrever. É a mesma doutrina do coletor da fila.
func (m *metricas) amostrasDaPosicao() []metrics.Amostra {
	mp := m.posicoes.Load()
	if mp == nil || len(*mp) == 0 {
		return nil
	}
	// Ordem estável: a iteração de mapa em Go é aleatória, e um /metrics que
	// muda a ordem das linhas a cada raspagem é ruído em qualquer diff.
	parts := make([]int32, 0, len(*mp))
	for p := range *mp {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })

	md := m.marca.Load()
	valida := md != nil && time.Since(md.em) <= validadeDaMarcaDagua

	out := make([]metrics.Amostra, 0, len(parts)*3)
	for _, p := range parts {
		rot := []metrics.Label{
			{Nome: metrics.LabelDataset, Valor: m.dataset},
			{Nome: metrics.LabelPartition, Valor: strconv.Itoa(int(p))},
		}
		aplicado := (*mp)[p].Load()
		out = append(out, metrics.Amostra{
			Nome: metrics.MetKafkaOffset, Tipo: metrics.TipoMedidor, Valor: float64(aplicado),
			Ajuda:  "Próximo offset a consumir, por partição: a posição APLICADA pelo writer.",
			Labels: rot,
		})
		if !valida {
			continue
		}
		fim, ok := md.fim[p]
		if !ok {
			// Partição que o broker não respondeu nesta leitura: omite as duas
			// que dependem dela, e não a partição inteira.
			continue
		}
		atraso := fim - aplicado
		if atraso < 0 {
			// O fim do log é anterior à posição aplicada: leitura do broker
			// mais velha que o consumo. Não é lag negativo, é zero.
			atraso = 0
		}
		out = append(out,
			metrics.Amostra{
				Nome: metrics.MetKafkaHighWatermark, Tipo: metrics.TipoMedidor, Valor: float64(fim),
				Ajuda:  "Fim do log por partição, como o broker respondeu no instante desta raspagem.",
				Labels: rot,
			},
			metrics.Amostra{
				Nome: metrics.MetKafkaLag, Tipo: metrics.TipoMedidor, Valor: float64(atraso),
				Ajuda:  "Mensagens entre a posição aplicada e o fim do log. Não depende do commit no group.",
				Labels: rot,
			},
		)
	}
	return out
}

// invalido devolve a série do motivo dado — o nome da categoria §101
// (ErrorCategory.String() do materializer, ex.: "EVENT_TOO_LARGE").
func (m *metricas) invalido(motivo string) *metrics.SerieContador {
	return m.famInvalidos.Com(m.dataset, motivo)
}
