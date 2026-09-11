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
// cinco que o M2 precisou inventar (ver internal/metrics/names.go).
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
	eventos    *metrics.CounterSeries
	stale      *metrics.CounterSeries
	falhas     *metrics.CounterSeries
	errosKafka *metrics.CounterSeries

	// famInvalidos carrega o LabelInvalidReason (Task A5, issue #15): cada categoria
	// §101 de evento ruim tem sua própria série, e não uma soma cega — é o
	// que permite ao painel e a um alerta nomear EVENT_TOO_LARGE em vez de
	// enxergar só um total misturado com INVALID_JSON e as outras quatro.
	famInvalidos *metrics.CounterFamily

	bloqueado *metrics.GaugeSeries
	// filaViva é lida no instante do SCRAPE, e não gravada pelo laço de poll.
	//
	// Ela era um par de medidores que o poller atualizava depois de enfileirar
	// o lote (consumer.go). O defeito disso não aparece em regime: aparece
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
	// esteiraViva é a memória retida pelos slots de decodificação, e
	// esteiraOciosa é o tempo que o WRITER passou esperando trabalho nela. As
	// duas são lidas no scrape pela mesma razão da filaViva.
	esteiraViva   func() int64
	esteiraOciosa func() time.Duration
	// temposOfFila é a integral de espera dos dois lados, lida no scrape pela
	// mesma razão da profundidade.
	temposOfFila   func() (time.Duration, time.Duration)
	partAtribuidas *metrics.GaugeSeries
	partTotal      *metrics.GaugeSeries
	// slotsEncolhidos e decodeWorkers são as duas séries da esteira de
	// decodificação: quantas vezes um slot devolveu memória, e com quantos
	// decodificadores o dataset subiu.
	slotsEncolhidos *metrics.CounterSeries
	decodeWorkers   *metrics.GaugeSeries

	// posicoes é a posição APLICADA por partição — o próximo offset a
	// consumir —, publicada pelo writer a cada evento decidido.
	//
	// Mapa IMUTÁVEL trocado por ponteiro atômico, e não uma família de
	// medidores. O caminho quente só faz Store no *atomic.Int64 que já existe;
	// nada é criado por evento. Antes, avanca chamava
	// famOffset.With(dataset, strconv.Itoa(p)), e Com monta a chave com
	// strings.Join(valores, "\xff") — uma string nova A CADA EVENTO, mais o
	// slice variádico, no caminho quente de um projeto cuja doutrina é zero
	// alocação ali. Nenhum gate cobria esse trecho: os de alocação medem
	// PatchBuffer.Parse e Materializer.Apply, e o writer Kafka está fora dos
	// dois.
	posicoes atomic.Pointer[map[int32]*atomic.Int64]
	// novasPart serializa a criação do mapa; fora do caminho quente.
	novasPart sync.Mutex

	// ultimoLote é o instante, em nanossegundos de Unix, do último lote NÃO
	// VAZIO que o poller entregou. Zero enquanto nenhum chegou. Não vira
	// série: é uma coluna de `_meta.kafka`, e o /metrics já responde a mesma
	// pergunta pela taxa de nishidb_ingest_events_total.
	ultimoLote atomic.Int64

	// mark é o fim do log por partição, como o broker respondeu, com o
	// instante da resposta. Só o scrape a escreve.
	mark atomic.Pointer[markDagua]

	// aplicaSec é a latência de um evento pelo Apply.
	aplicaSec *metrics.HistogramSeries

	dataset string
}

// markDagua é um instantâneo do fim do log, com a hora em que ele foi colhido.
//
// O instante viaja junto porque um valor velho não pode ser publicado como se
// fosse de agora: ele erraria para BAIXO, que é o lado que ESCONDE incidente.
type markDagua struct {
	end map[int32]int64
	in  time.Time
}

// limitsDoApply são os buckets do histograma do Apply.
//
// Próprios, e não os DefaultBuckets: aqueles começam em 50 µs, e a mediana
// medida no piloto é da ordem de 8 µs — o primeiro bucket engoliria quase
// tudo e o histograma não distinguiria nada abaixo dele. O topo em 1 s é o que
// torna os stalls de segundos ATRIBUÍVEIS ao Apply, em vez de a "algo".
var limitsDoApply = []float64{
	0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.005, 0.01, 0.05, 0.1, 1,
}

// validadeOfMarkDagua é por quanto tempo a última resposta do broker ainda
// descreve o presente.
//
// Um minuto é mais que um intervalo de scrape típico e menos que qualquer
// janela em que o operador consiga nomear o instante. Vencida sem leitura
// nova, as séries que dependem do broker somem — ausência é resposta honesta,
// número velho não é.
const validadeOfMarkDagua = time.Minute

// registraMetricas instala as séries do dataset.
func registerMetrics(r *metrics.Registry, dataset string) *metricas {
	if r == nil {
		r = metrics.NewRegistry()
	}
	counter := func(name, ajuda string) *metrics.CounterSeries {
		return r.Counter(name, ajuda, metrics.LabelDataset).With(dataset)
	}
	medidor := func(name, ajuda string) *metrics.GaugeSeries {
		return r.Gauge(name, ajuda, metrics.LabelDataset).With(dataset)
	}
	famInvalidos := r.Counter(metrics.MetIngestInvalidTotal,
		"Eventos descartados por serem ruins (§101), por motivo (categoria §101; Task A5/issue #15).",
		metrics.LabelDataset, metrics.LabelInvalidReason)
	// Pré-registra as seis categorias de evento ruim em zero: sem isto, cada
	// série só nasce no primeiro Apply que descartar um evento daquela
	// categoria, e nishidb_ingest_invalid_total{motivo="..."} deixa de
	// aparecer em /metrics desde a subida — mudança observável de superfície
	// que o piloto em produção já emite. O enum §101 é fechado (por isso
	// BadEvent(), não uma lista escrita à mão), então o custo de cardinalidade
	// é fixo: sempre seis séries por dataset.
	for cat := materializer.ErrorCategory(0); cat < materializer.NumCategories; cat++ {
		if cat.BadEvent() {
			famInvalidos.With(dataset, cat.String())
		}
	}
	m := &metricas{
		eventos:      counter(metrics.MetIngestEventsTotal, "Eventos oferecidos à ingestão."),
		stale:        counter(metrics.MetIngestStaleTotal, "Eventos descartados por sequence (§25). Não é erro."),
		famInvalidos: famInvalidos,
		falhas: counter(metrics.MetIngestFailedTotal,
			"Eventos que NÃO entraram por falha que não é do evento. O §92 não prevê esta série, e sem ela a perda de dado seria invisível num daemon, onde não há exit code para carregá-la."),
		errosKafka: counter(metrics.MetKafkaErrorsTotal,
			"Erros de broker ou rede. Balde próprio: o §101 só cataloga erro de EVENTO, e contar erro de rede como perda de dado é diagnóstico errado."),

		bloqueado: medidor(metrics.MetIngestBlocked,
			"1 quando a ingestão do dataset está PARADA por política de erro. Sem ela, a pausa do `invalidEvent: stop` seria um consumer silenciosamente parado."),
		partAtribuidas: medidor(metrics.MetKafkaPartitionsAssigned,
			"Partições atribuídas a esta instância. Comparar com o total é o que torna visível o assignment parcial."),
		partTotal: medidor(metrics.MetKafkaPartitionsTotal, "Partições do tópico."),

		slotsEncolhidos: counter(metrics.MetIngestDecodeSlotsShrunk,
			"Slots da esteira de decodificação cujos buffers foram soltos por passarem do limiar de memória retida. Crescimento constante é churn de alocação; zero com arena_bytes alto é memória presa."),
		decodeWorkers: medidor(metrics.MetIngestDecodeWorkers,
			"Decodificadores configurados para este dataset (source.kafka.consumer.decodeWorkers). É o denominador de qualquer leitura de vazão."),

		aplicaSec: r.Histogram(metrics.MetIngestApplySeconds,
			"Latência de um evento pelo Apply: parse, estado, merge, Put e o Apenda do histórico. O checkpoint fica fora — ele tem histograma próprio.",
			limitsDoApply, metrics.LabelDataset).With(dataset),
		dataset: dataset,
	}

	// O coletor é registrado AQUI, junto das famílias, e não onde a fila
	// nasce: a fonte é instalada depois (OlhaAFila), e até lá o coletor não
	// emite nada. Registrar cedo mantém a ordem de exposição estável entre
	// um consumidor que subiu e um que não subiu.
	r.Register(metrics.CollectorFunc(m.samplesOfFila))
	r.Register(metrics.CollectorFunc(m.samplesOfEsteira))
	r.Register(metrics.CollectorFunc(m.samplesOfPosition))
	return m
}

// OlhaAFila instala a fonte de profundidade. Chamado uma vez, quando a fila
// existe.
func (m *metricas) OlhaAFila(f func() (int, int64)) { m.filaViva = f }

// OlhaOsTempos instala a fonte da integral de espera. Mesma vida da OlhaAFila.
func (m *metricas) OlhaOsTempos(f func() (time.Duration, time.Duration)) { m.temposOfFila = f }

// OlhaAEsteira instala as fontes da esteira: a memória retida pelos slots e a
// ociosidade do writer.
func (m *metricas) OlhaAEsteira(bytes func() int64, idle func() time.Duration) {
	m.esteiraViva, m.esteiraOciosa = bytes, idle
}

// samplesOfEsteira lê a memória dos slots no instante do scrape.
//
// Lida da estrutura VIVA, e não de um medidor escrito pelo writer, pela mesma
// razão que a profundidade da fila: um medidor escrito pelo laço congela
// exatamente quando o laço trava, e foi assim que a profundidade mentiu no
// travamento de 27/08/2026.
//
// Sem fonte instalada não emite nada: o dataset sem consumidor Kafka não tem
// esteira, e zero ali afirmaria que existe uma e está vazia.
func (m *metricas) samplesOfEsteira() []metrics.Sample {
	if m.esteiraViva == nil {
		return nil
	}
	rotulo := []metrics.Label{{Name: metrics.LabelDataset, Value: m.dataset}}
	return []metrics.Sample{
		{
			Family: metrics.MetIngestDecodeArenaBytes, Name: metrics.MetIngestDecodeArenaBytes, Type: metrics.TypeGauge,
			Value:  float64(m.esteiraViva()),
			Help:   "Bytes que os slots da esteira de decodificação seguram entre eventos. Nenhum dos dois gates da fila conta esta memória.",
			Labels: rotulo,
		},
		{
			Family: metrics.MetKafkaWriterIDleSeconds, Name: metrics.MetKafkaWriterIDleSeconds, Type: metrics.TypeCounter,
			Value:  m.esteiraOciosa().Seconds(),
			Help:   "Segundos que o writer passou esperando trabalho na esteira. rate() alto com lag alto significa que quem não entrega é o poller ou os decodificadores.",
			Labels: rotulo,
		},
	}
}

// samplesOfFila lê a fila no instante do scrape.
//
// Sem fonte instalada não emite nada, e isso é deliberado: o dataset sem
// consumidor Kafka não tem fila, e publicar zero ali afirmaria que existe uma
// e está vazia. É a mesma distinção entre "medi e deu zero" e "não há o que
// medir" que o resto do produto faz.
func (m *metricas) samplesOfFila() []metrics.Sample {
	if m.filaViva == nil {
		return nil
	}
	reg, bytes := m.filaViva()
	rotulo := []metrics.Label{{Name: metrics.LabelDataset, Value: m.dataset}}
	out := []metrics.Sample{
		{
			Family: metrics.MetKafkaQueueDepth, Name: metrics.MetKafkaQueueDepth, Type: metrics.TypeGauge, Value: float64(reg),
			Help:   "Registros na fila limitada poller→writer, lidos no instante do scrape.",
			Labels: rotulo,
		},
		{
			Family: metrics.MetKafkaQueueBytes, Name: metrics.MetKafkaQueueBytes, Type: metrics.TypeGauge, Value: float64(bytes),
			Help:   "Bytes retidos na fila limitada (§79), lidos no instante do scrape.",
			Labels: rotulo,
		},
	}
	if m.temposOfFila == nil {
		return out
	}
	bloqueado, idle := m.temposOfFila()
	return append(out,
		metrics.Sample{
			Family: metrics.MetKafkaPollBlockedSeconds, Name: metrics.MetKafkaPollBlockedSeconds, Type: metrics.TypeCounter,
			Value:  bloqueado.Seconds(),
			Help:   "Segundos que o poller passou esperando espaço na fila. rate() é a fração do tempo em que o writer foi o gargalo.",
			Labels: rotulo,
		},
		metrics.Sample{
			Family: metrics.MetKafkaDecodeIDleSeconds, Name: metrics.MetKafkaDecodeIDleSeconds, Type: metrics.TypeCounter,
			Value:  idle.Seconds(),
			Help:   "Segundos SOMADOS que os decodificadores passaram esperando evento na fila. Divida por nishidb_ingest_decode_workers para ler como fração.",
			Labels: rotulo,
		},
	)
}

// positionOf devolve o slot atômico da partição, criando-o se for a primeira
// vez.
//
// O caminho quente é o ramo de cima: uma carga atômica e um lookup num mapa de
// int32, sem alocar. A criação é rara — uma vez por partição por assignment —
// e paga um mutex e a cópia do mapa, que é o preço de o leitor nunca precisar
// de lock.
func (m *metricas) positionOf(p int32) *atomic.Int64 {
	if mp := m.posicoes.Load(); mp != nil {
		if s, ok := (*mp)[p]; ok {
			return s
		}
	}
	return m.registerPartition(p)
}

func (m *metricas) registerPartition(p int32) *atomic.Int64 {
	m.novasPart.Lock()
	defer m.novasPart.Unlock()
	// Recheca sob o lock: duas goroutines podem ter falhado o ramo rápido.
	if mp := m.posicoes.Load(); mp != nil {
		if s, ok := (*mp)[p]; ok {
			return s
		}
	}
	nw := make(map[int32]*atomic.Int64)
	if mp := m.posicoes.Load(); mp != nil {
		for k, v := range *mp {
			nw[k] = v
		}
	}
	s := &atomic.Int64{}
	nw[p] = s
	m.posicoes.Store(&nw)
	return s
}

// publicaMark guarda o instantâneo do broker, carimbado com o agora.
func (m *metricas) publicaMark(end map[int32]int64, in time.Time) {
	m.mark.Store(&markDagua{end: end, in: in})
}

// samplesOfPosition emite offset, high watermark e lag DO MESMO INSTANTÂNEO.
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
func (m *metricas) samplesOfPosition() []metrics.Sample {
	ps, _ := m.posicoesOfInstant()
	if len(ps) == 0 {
		return nil
	}
	out := make([]metrics.Sample, 0, len(ps)*3)
	for _, p := range ps {
		rot := []metrics.Label{
			{Name: metrics.LabelDataset, Value: m.dataset},
			{Name: metrics.LabelPartition, Value: strconv.Itoa(int(p.Partition))},
		}
		out = append(out, metrics.Sample{
			Family: metrics.MetKafkaOffset, Name: metrics.MetKafkaOffset, Type: metrics.TypeGauge, Value: float64(p.Aplicado),
			Help:   "Próximo offset a consumir, por partição: a posição APLICADA pelo writer.",
			Labels: rot,
		})
		if !p.TemEnd {
			continue
		}
		out = append(out,
			metrics.Sample{
				Family: metrics.MetKafkaHighWatermark, Name: metrics.MetKafkaHighWatermark, Type: metrics.TypeGauge, Value: float64(p.EndDoLog),
				Help:   "Fim do log por partição, como o broker respondeu no instante desta raspagem.",
				Labels: rot,
			},
			metrics.Sample{
				Family: metrics.MetKafkaLag, Name: metrics.MetKafkaLag, Type: metrics.TypeGauge, Value: float64(p.Atraso),
				Help:   "Mensagens entre a posição aplicada e o fim do log. Não depende do commit no group.",
				Labels: rot,
			},
		)
	}
	return out
}

// PositionOfPartition é a posição de UMA partição num instante: a APLICADA e —
// quando a marca d'água do broker ainda vale — o fim do log e o atraso.
//
// HasEnd separa "o broker não respondeu por esta partição" de "o fim do log é
// zero". Sem ele, HWM e lag sairiam ZERADOS numa marca vencida, e zero é o
// lado que ESCONDE incidente — é a mesma distinção que a validade da marca
// existe para manter.
type PositionOfPartition struct {
	Partition int32
	Aplicado  int64
	EndDoLog  int64
	Atraso    int64
	TemEnd    bool
}

// posicoesOfInstant colhe TODAS as partições de UMA leitura da marca d'água.
//
// É a fonte única do samplesOfPosition acima e do Instantaneo que a
// `_meta.kafka` consome: a ordem estável, a validade da marca, a omissão da
// partição que o broker não respondeu e o piso do lag em zero moram AQUI, e
// não em dois lugares que divergem no dia em que um deles mudar. O godoc do
// samplesOfPosition explica POR QUE cada uma dessas regras é o que é.
//
// marcaEm é o instante da leitura do broker, ou o zero quando não há marca
// válida — é ele que carimba a idade das colunas que dependem do broker.
func (m *metricas) posicoesOfInstant() (ps []PositionOfPartition, markIn time.Time) {
	mp := m.posicoes.Load()
	if mp == nil || len(*mp) == 0 {
		return nil, time.Time{}
	}
	// Ordem estável: a iteração de mapa em Go é aleatória, e um /metrics que
	// muda a ordem das linhas a cada raspagem é ruído em qualquer diff.
	parts := make([]int32, 0, len(*mp))
	for p := range *mp {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })

	md := m.mark.Load()
	validate := md != nil && time.Since(md.in) <= validadeOfMarkDagua
	if validate {
		markIn = md.in
	}

	ps = make([]PositionOfPartition, 0, len(parts))
	for _, p := range parts {
		pos := PositionOfPartition{Partition: p, Aplicado: (*mp)[p].Load()}
		if validate {
			if end, ok := md.end[p]; ok {
				pos.EndDoLog, pos.TemEnd = end, true
				if pos.Atraso = end - pos.Aplicado; pos.Atraso < 0 {
					// O fim do log é anterior à posição aplicada: leitura do
					// broker mais velha que o consumo. Não é lag negativo, é
					// zero.
					pos.Atraso = 0
				}
			}
		}
		ps = append(ps, pos)
	}
	return ps, markIn
}

// markVencida diz se a última resposta do broker já não descreve o presente.
// Usa a MESMA validadeOfMarkDagua do coletor: um segundo prazo faria o
// /metrics e a `_meta.kafka` discordarem sobre quando o número morre.
func (m *metricas) markVencida() bool {
	md := m.mark.Load()
	return md == nil || time.Since(md.in) > validadeOfMarkDagua
}

// markLote registra o instante de um lote NÃO VAZIO entregue pelo poller, e
// ultimoLoteIn o devolve (zero = nenhum lote desde a subida deste consumidor).
//
// Mora no poller e é escrito uma vez por LOTE: quem responde "o Kafka ainda
// entrega?" é ele, e o custo é um relógio por PollRecords em vez de um por
// registro. O que o writer fez com o lote já sai no Desfecho.
func (m *metricas) markLote(t time.Time) { m.ultimoLote.Store(t.UnixNano()) }

func (m *metricas) ultimoLoteIn() time.Time {
	if n := m.ultimoLote.Load(); n != 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

// invalido devolve a série do motivo dado — o nome da categoria §101
// (ErrorCategory.String() do materializer, ex.: "EVENT_TOO_LARGE").
func (m *metricas) invalido(motivo string) *metrics.CounterSeries {
	return m.famInvalidos.With(m.dataset, motivo)
}
