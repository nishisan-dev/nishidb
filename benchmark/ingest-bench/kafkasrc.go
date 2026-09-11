package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
)

// source é de onde a fase tira eventos.
//
// A abstração existe porque o §108 exige que a rodada de 8M sob 1 GiB valide
// `Kafka ingestion`, e até o M1 o harness ingeria de um gerador em memória: o
// custo do consumidor — buffers do cliente, fila limitada, decompressão —
// ficava inteiramente FORA do número que o critério de aceite publicava. Era o
// débito M2 #2.
type source interface {
	// Next devolve o próximo evento, reusando dst. Devolve nil quando o
	// contexto acaba ou a fonte encerra.
	//
	// O ctx é o conserto do débito 67. Sem ele, sourceKafka.Next bloqueava em
	// Recebe(context.Background()) — contexto SEM prazo — e, como a checagem
	// de duração do runPhase acontece ANTES do Next, ela nunca era reavaliada:
	// tópico que esvaziava dentro da janela travava a fase para sempre. A
	// rodada de aceite do M4 publicou duration_sec de 1.327,6 s para 600 s de
	// ingestão, e com ela events_per_sec, allocs_per_event e gc_per_min
	// contaminados.
	Next(ctx context.Context, dst []byte) []byte
}

// sourceMemoria é o gerador do M1, que continua servindo às rodadas rápidas.
type sourceMemoria struct{ gen *radiusgen.Generator }

// O gerador nunca esvazia e nunca bloqueia, então o contexto não tem o que
// fazer aqui — ele existe na interface por causa da fonte Kafka.
func (f sourceMemoria) Next(_ context.Context, dst []byte) []byte { return f.gen.Next(dst) }

// sourceKafka lê de um tópico real pelo MESMO caminho que a produção usa: o
// cliente franz-go e a fila limitada do internal/kafka, com os dois gates.
//
// O que fica de fora é só o kafka.Writer, cujas ramificações de política são
// cobertas por teste de unidade; o que entra é justamente o que pesa na
// memória e o §79 manda expor — "Kafka buffers".
type sourceKafka struct {
	cl   *kgo.Client
	adm  *kadm.Client
	fila *kafka.Fila

	topic string
	grupo string

	cancel context.CancelFunc
	pronto sync.WaitGroup
	err    atomic.Value

	// Picos, para o relatório. São máximos e não instantâneos porque o que
	// interessa ao orçamento de memória é o pior momento, não o último.
	picoRecords atomic.Int64
	picoBytes   atomic.Int64
}

type optionsKafka struct {
	Brokers   []string
	Topic     string
	Grupo     string
	FilaTam   int
	FilaBytes int64
	MaxPoll   int
	OfComeco  bool
}

func openSourceKafka(ctx context.Context, o optionsKafka) (*sourceKafka, error) {
	start := kgo.NewOffset().AtEnd()
	if o.OfComeco {
		start = kgo.NewOffset().AtStart()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(o.Brokers...),
		kgo.ConsumeTopics(o.Topic),
		kgo.ConsumerGroup(o.Grupo),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(start),
	)
	if err != nil {
		return nil, fmt.Errorf("ingest-bench: cliente kafka: %w", err)
	}
	f := &sourceKafka{
		cl:    cl,
		adm:   kadm.NewClient(cl),
		fila:  kafka.NewQueue(o.FilaTam, o.FilaBytes),
		topic: o.Topic,
		grupo: o.Grupo,
	}
	pctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.pronto.Add(1)
	go f.poll(pctx, o.MaxPoll)
	return f, nil
}

func (f *sourceKafka) poll(ctx context.Context, maxPoll int) {
	defer f.pronto.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := f.cl.PollRecords(ctx, maxPoll)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			if ctx.Err() == nil {
				f.err.Store(fmt.Errorf("ingest-bench: fetch %s/%d: %w", t, p, err))
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if err := f.fila.Envia(ctx, r); err != nil {
				return
			}
			reg, bytes := f.fila.Profundidade()
			if int64(reg) > f.picoRecords.Load() {
				f.picoRecords.Store(int64(reg))
			}
			if bytes > f.picoBytes.Load() {
				f.picoBytes.Store(bytes)
			}
		})
	}
}

// Next bloqueia até haver evento, o contexto acabar, ou a fonte encerrar.
func (f *sourceKafka) Next(ctx context.Context, dst []byte) []byte {
	it, ok := f.fila.Recebe(ctx)
	if !ok || it.Rec == nil {
		return nil
	}
	return append(dst, it.Rec.Value...)
}

// Picos devolve os máximos observados nos dois gates da fila (§79).
func (f *sourceKafka) Picos() (records int, bytes int64) {
	return int(f.picoRecords.Load()), f.picoBytes.Load()
}

// Lag consulta o lag do group no broker (§92, §125).
//
// Commita antes de perguntar, senão o número seria sempre o total produzido:
// o lag é a distância entre o fim do log e o offset COMMITADO, e esta fonte
// (que é a bancada, não o motor) não commita durante a rodada.
//
// Ressalva registrada no próprio relatório: o que se commita aqui é o que foi
// POLLADO, que pode estar alguns registros à frente do que foi aplicado. O
// consumer de produção não faz isso — ele só commita depois do checkpoint
// durável (§48). A diferença é irrelevante para a ordem de grandeza que o
// §125 quer medir, e seria enganosa se fosse o contrário.
func (f *sourceKafka) Lag(ctx context.Context) int64 {
	if err := f.cl.CommitUncommittedOffsets(ctx); err != nil {
		return -1
	}
	lags, err := f.adm.Lag(ctx, f.grupo)
	if err != nil {
		return -1
	}
	l, ok := lags[f.grupo]
	if !ok {
		return -1
	}
	var total int64
	for _, p := range l.Lag[f.topic] {
		total += p.Lag
	}
	return total
}

func (f *sourceKafka) Err() error {
	if v := f.err.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (f *sourceKafka) Close() {
	f.cancel()
	f.cl.Close()
	f.pronto.Wait()
}

// produzCorpus publica N eventos no tópico, opcionalmente a uma taxa fixa.
//
// A produção é do harness e não do motor: o §125 pede a rodada a 500 ev/s, e
// num pipeline Kafka quem determina a taxa é o PRODUTOR — o consumidor apenas
// acompanha. Medir "500 ev/s" limitando o consumidor mediria o limitador.
func produzCorpus(ctx context.Context, brokers []string, topic string,
	gen *radiusgen.Generator, n int64, taxa int,
) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("ingest-bench: produtor kafka: %w", err)
	}
	defer cl.Close()

	var limiter *rateLimiter
	if taxa > 0 {
		limiter = newRateLimiter(taxa)
	}
	var buf []byte
	var fail atomic.Value
	for i := int64(0); i < n; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if limiter != nil {
			limiter.wait()
		}
		buf = gen.Next(buf[:0])
		value := append([]byte(nil), buf...)
		cl.Produce(ctx, &kgo.Record{Topic: topic, Value: value},
			func(_ *kgo.Record, err error) {
				if err != nil {
					fail.Store(err)
				}
			})
	}
	if err := cl.Flush(ctx); err != nil {
		return fmt.Errorf("ingest-bench: flush do produtor: %w", err)
	}
	if v := fail.Load(); v != nil {
		return fmt.Errorf("ingest-bench: produzir: %w", v.(error))
	}
	return nil
}

// waitBrokerPronto confirma que o broker responde antes de a medição
// começar: incluir o tempo de subida do cluster na vazão mediria o docker.
func waitBrokerPronto(ctx context.Context, brokers []string, prazo time.Duration) error {
	end := time.Now().Add(prazo)
	for time.Now().Before(end) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err == nil {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := cl.Ping(pctx)
			cancel()
			cl.Close()
			if err == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("ingest-bench: broker %v não respondeu em %s", brokers, prazo)
}

// buildSource devolve a fonte da fase.
//
// Sem -kafka, é o gerador em memória do M1 — que continua servindo às rodadas
// rápidas de regressão. Com -kafka, o corpus é PRODUZIDO no tópico e depois
// consumido pelo caminho real; é assim que a medição do §108 passa a incluir
// o custo do consumidor, que era o débito M2 #2.
func buildSource(cfg Config, gen *radiusgen.Generator, eventos int64, taxa int) (source, *sourceKafka, error) {
	if len(cfg.KafkaBrokers) == 0 {
		return sourceMemoria{gen: gen}, nil, nil
	}
	ctx := context.Background()
	if err := waitBrokerPronto(ctx, cfg.KafkaBrokers, 60*time.Second); err != nil {
		return nil, nil, err
	}
	fmt.Fprintf(cfg.Out, "KAFKA: produzindo %d eventos em %s...\n", eventos, cfg.KafkaTopic)
	if err := produzCorpus(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, gen, eventos, taxa); err != nil {
		return nil, nil, err
	}
	f, err := openSourceKafka(ctx, optionsKafka{
		Brokers:   cfg.KafkaBrokers,
		Topic:     cfg.KafkaTopic,
		Grupo:     cfg.KafkaGroup,
		FilaTam:   cfg.KafkaQueueSize,
		FilaBytes: cfg.KafkaQueueBytes,
		MaxPoll:   cfg.KafkaMaxPoll,
		OfComeco:  true,
	})
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

// registerKafka move para o relatório o que só a fonte Kafka sabe.
func registerKafka(rep *Report, f *sourceKafka) {
	reg, bytes := f.Picos()
	if reg > rep.KafkaQueueDepthMax {
		rep.KafkaQueueDepthMax = reg
	}
	if bytes > rep.KafkaQueueBytesMax {
		rep.KafkaQueueBytesMax = bytes
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rep.KafkaLagFinal = f.Lag(ctx)
}
