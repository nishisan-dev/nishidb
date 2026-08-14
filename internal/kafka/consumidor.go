package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

var (
	// ErrPosicaoDesconhecida indica dataset COM linhas e SEM offsets locais.
	//
	// É o buraco que o `autoOffsetReset` sozinho não cobre: "não há offset
	// local" também é verdade quando o kafka.offsets sumiu, ficou ilegível ou
	// veio de um restore parcial. Nesse estado, `earliest` reprocessaria o
	// tópico retido inteiro e `latest` pularia tudo — os dois em silêncio. O
	// sinal para detectar já é persistido (RowCount no manifest), então a
	// recusa é barata e a alternativa é corrupção lógica silenciosa.
	ErrPosicaoDesconhecida = errors.New("kafka: dataset tem linhas e não tem offsets locais")

	// ErrOrigemDivergente indica que o YAML aponta para um fluxo diferente do
	// que gerou os offsets persistidos.
	ErrOrigemDivergente = errors.New("kafka: a origem mudou desde o último checkpoint")

	// ErrTopicoAusente indica tópico inexistente ou sem partições no cluster.
	ErrTopicoAusente = errors.New("kafka: tópico ausente")

	// ErrAssignmentParcial indica que o group não entregou todas as partições
	// do tópico a esta instância.
	//
	// Single-writer (§27) somado a checkpoint local autoritativo (§49)
	// implica UMA instância ativa por group, mas a spec nunca diz isso. Sem
	// esta guarda o modo de falha é silencioso: duas instâncias dividem as
	// partições, cada dataset materializa estado parcial, e nenhuma métrica
	// do §92 acusa — o group consome tudo e o lag fica normal.
	ErrAssignmentParcial = errors.New("kafka: assignment parcial")
)

// Consumidor é o processo de ingestão de UM dataset.
type Consumidor struct {
	cfg    *Config
	cl     *kgo.Client
	adm    *kadm.Client
	fila   *Fila
	w      *Writer
	log    *logging.Logger
	met    *metricas
	origem checkpoint.Origem

	// erroAssign carrega a falha detectada dentro do callback de assignment,
	// que não tem por onde devolver erro.
	erroAssign atomic.Value
	// cancelaPoll interrompe o PollRecords em curso. Sem ele, uma recusa de
	// assignment só seria vista quando o poll voltasse — e num assignment
	// parcial o poll pode não voltar nunca, porque as partições que sobraram
	// para esta instância podem estar sem tráfego nenhum.
	cancelaPoll atomic.Value

	// offsetsIniciais é a posição de onde retomar, por partição.
	offsetsIniciais map[int32]int64

	fecha sync.Once
}

// Opcoes reúne o que o consumidor precisa de fora.
type Opcoes struct {
	Store    Store
	Mat      *materializer.Materializer
	Log      *logging.Logger
	Metrics  *metrics.Registry
	Cadencia Cadencia
	// ClienteKgo permite ao teste injetar opções extras (kfake usa isto para
	// apontar para o broker em memória).
	ClienteKgo []kgo.Opt
}

// Abre monta o consumidor e valida tudo o que pode ser validado ANTES de
// consumir o primeiro evento.
//
// A ordem das checagens importa: identidade e posição são conferidas antes de
// qualquer fetch, porque um seek num offset de outro fluxo não tem sintoma —
// o consumo simplesmente começa no meio.
func Abre(ctx context.Context, cfg *Config, o Opcoes) (*Consumidor, error) {
	log := o.Log
	if log == nil {
		log = logging.Descartado()
	}
	log = log.Component("kafka").Dataset(cfg.Dataset)
	met := registraMetricas(o.Metrics, cfg.Dataset)

	c := &Consumidor{cfg: cfg, log: log, met: met, origem: cfg.Origem()}

	opts := append(cfg.opcoesDoCliente(),
		kgo.OnPartitionsAssigned(c.aoAtribuir),
		kgo.OnPartitionsRevoked(c.aoRevogar),
		kgo.AdjustFetchOffsetsFn(c.ajustaOffsets),
	)
	opts = append(opts, o.ClienteKgo...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: criar cliente do dataset %q: %w", cfg.Dataset, err)
	}
	c.cl = cl
	c.adm = kadm.NewClient(cl)

	if err := c.resolveOrigemEPosicao(ctx, o.Store); err != nil {
		cl.Close()
		return nil, err
	}

	c.fila = NovaFila(cfg.QueueSize, cfg.QueueBytes)
	c.w = NovoWriter(cfg, o.Mat, o.Store, c.fila, log, met, c.offsetsIniciais, o.Cadencia, c.commitNoBroker)
	return c, nil
}

// resolveOrigemEPosicao completa a identidade com o cluster id, confere a
// divergência (D14) e resolve de onde retomar (D11).
func (c *Consumidor) resolveOrigemEPosicao(ctx context.Context, st Store) error {
	md, err := c.adm.Metadata(ctx, c.cfg.Topic)
	if err != nil {
		c.met.errosKafka.Inc()
		return fmt.Errorf("kafka: metadados do tópico %q: %w", c.cfg.Topic, err)
	}
	c.origem.ClusterID = md.Cluster

	persistidos, existe, err := st.SourceOffsets()
	switch {
	case err != nil:
		// Arquivo existia e estava ilegível. Tratar como "sem posição" faria
		// exatamente o que ErrPosicaoDesconhecida existe para impedir.
		return fmt.Errorf("%w: %w", ErrPosicaoDesconhecida, err)

	case existe:
		if campo := persistidos.Origem.Divergencia(c.origem); campo != "" {
			return fmt.Errorf("%w: %s mudou (persistido %q, configurado %q); "+
				"use `nishidb offsets` para reposicionar deliberadamente",
				ErrOrigemDivergente, campo,
				valorDoCampo(persistidos.Origem, campo), valorDoCampo(c.origem, campo))
		}
		c.offsetsIniciais = persistidos.Offsets

	default:
		// Sem posição local. Só é seguro aplicar o autoOffsetReset num
		// dataset VAZIO: com linhas, "sem offsets" é estado anômalo.
		if n := st.RowCount(); n > 0 {
			return fmt.Errorf("%w: %d linhas materializadas sem posição conhecida; "+
				"autoOffsetReset=%s reprocessaria ou pularia uma janela arbitrária em silêncio. "+
				"Use `nishidb offsets` para decidir explicitamente",
				ErrPosicaoDesconhecida, n, c.cfg.AutoOffsetReset)
		}
		c.offsetsIniciais = map[int32]int64{}
		c.log.Info("posicao.inicial", "dataset vazio e sem offsets locais; vale o autoOffsetReset",
			"autoOffsetReset", c.cfg.AutoOffsetReset)
	}

	// A existência do tópico é conferida DEPOIS da divergência de origem, e a
	// ordem é deliberada: quando o operador aponta o dataset para um tópico
	// que não existe, as duas coisas são verdade, mas "a origem mudou" é a
	// CAUSA e "o tópico não existe" é a consequência. Reportar a consequência
	// mandaria criar o tópico errado.
	//
	// A checagem em si existe porque, sem ela, o consumer sobe, não recebe
	// assignment nenhum e fica sentado: o health reporta UP, as métricas
	// mostram 0 partições, e nada é consumido — o operador vê tudo verde com o
	// dataset parado. A guarda de assignment não pega o caso, porque compara
	// atribuídas com total e 0 == 0.
	//
	// Recusar torna a ordem de subida relevante (o tópico precisa existir
	// antes do NishiDB), e é a troca certa: um consumer silenciosamente ocioso
	// é pior que uma recusa com o nome do tópico na mensagem.
	d, ok := md.Topics[c.cfg.Topic]
	switch {
	case !ok:
		return fmt.Errorf("%w: o tópico %q não existe no cluster", ErrTopicoAusente, c.cfg.Topic)
	case d.Err != nil:
		return fmt.Errorf("%w: tópico %q: %w", ErrTopicoAusente, c.cfg.Topic, d.Err)
	case len(d.Partitions) == 0:
		return fmt.Errorf("%w: o tópico %q não tem partição nenhuma", ErrTopicoAusente, c.cfg.Topic)
	}
	c.met.partTotal.Set(float64(len(d.Partitions)))
	return nil
}

func valorDoCampo(o checkpoint.Origem, campo string) string {
	switch campo {
	case "topic":
		return o.Topic
	case "consumer.groupId":
		return o.GroupID
	case "bootstrapServers":
		return o.Bootstrap
	case "cluster":
		return o.ClusterID
	}
	return ""
}

// aoAtribuir faz o seek pela posição LOCAL (§49) e aplica a guarda de
// instância única (D10).
//
// O seek é explícito e incondicional para toda partição com posição
// conhecida: o offset commitado no group é secundário, e deixá-lo posicionar
// o consumer faria o dataset divergir do que o Kafka acha que já foi
// entregue.
func (c *Consumidor) aoAtribuir(ctx context.Context, _ *kgo.Client, atribuidas map[string][]int32) {
	partes := atribuidas[c.cfg.Topic]
	c.met.partAtribuidas.Set(float64(len(partes)))

	total, err := c.totalDeParticoes(ctx)
	if err != nil {
		c.met.errosKafka.Inc()
		c.log.Warn("particoes.desconhecidas",
			"não foi possível confirmar o total de partições do tópico; a guarda de instância única não pôde ser aplicada",
			"erro", err.Error())
	} else if len(partes) != total {
		c.erroAssign.Store(fmt.Errorf("%w: o group %q entregou %d de %d partições de %q — "+
			"o v1 é single-writer (§27) com checkpoint local autoritativo (§49), o que exige "+
			"UMA instância ativa por group; com duas, cada dataset materializa estado parcial e "+
			"nenhuma métrica acusa",
			ErrAssignmentParcial, c.cfg.GroupID, len(partes), total, c.cfg.Topic))
		c.interrompePoll()
		return
	}

	c.log.Info("particoes.atribuidas", "partições atribuídas pelo group", "particoes", len(partes))
}

// ajustaOffsets é o SEEK do §49: a posição persistida localmente sobrepõe o
// offset commitado no group, em toda partição que já tenha posição conhecida.
//
// Tem de ser aqui, e não no callback de assignment, por uma limitação
// documentada do cliente: SetOffsets "só ajusta partições previamente
// consumidas", então chamá-lo num join novo é silenciosamente inócuo — o
// consumo seguiria pelo offset do broker, e o sintoma seria um dataset que
// nunca reprocessa o que deveria. Este hook roda depois de o cliente buscar
// os offsets do group e ANTES de a primeira fetch começar, que é exatamente a
// janela que o §49 descreve.
//
// WithEpoch(-1) desliga a detecção de data loss do cliente: reposicionar para
// trás é a operação PRETENDIDA aqui, e com epoch não negativo o cliente a
// trataria como truncamento do log.
func (c *Consumidor) ajustaOffsets(_ context.Context, doGroup map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	if len(c.offsetsIniciais) == 0 {
		return doGroup, nil
	}
	porParticao, ok := doGroup[c.cfg.Topic]
	if !ok {
		return doGroup, nil
	}
	ajustado := make(map[int32]kgo.Offset, len(porParticao))
	usados := make([]int32, 0, len(porParticao))
	for p, o := range porParticao {
		local, temLocal := c.offsetsIniciais[p]
		if !temLocal {
			// Partição sem posição local (tópico cresceu depois do último
			// checkpoint): vale o que o group diz, ou o autoOffsetReset.
			ajustado[p] = o
			continue
		}
		ajustado[p] = kgo.NewOffset().At(local).WithEpoch(-1)
		usados = append(usados, p)
	}
	saida := make(map[string]map[int32]kgo.Offset, len(doGroup))
	for t, v := range doGroup {
		saida[t] = v
	}
	saida[c.cfg.Topic] = ajustado

	sort.Slice(usados, func(i, j int) bool { return usados[i] < usados[j] })
	for _, p := range usados {
		c.met.offset(p).Set(float64(c.offsetsIniciais[p]))
	}
	c.log.Info("offsets.seek", "retomando pela posição do checkpoint local (§49)",
		"particoes", len(usados))
	return saida, nil
}

// aoRevogar torna durável o que já entrou antes de perder as partições.
//
// Com BlockRebalanceOnPoll, este callback só roda quando o lote corrente já
// foi processado, então não há evento em voo na fila. O checkpoint aqui é o
// que evita reprocessar desde o último ponto durável a cada rebalance.
func (c *Consumidor) aoRevogar(ctx context.Context, _ *kgo.Client, revogadas map[string][]int32) {
	if c.w == nil {
		return
	}
	if err := c.w.checkpoint(ctx); err != nil {
		c.log.Error("checkpoint.falhou", "checkpoint na revogação falhou; o próximo dono reprocessa desde o último ponto durável",
			"erro", err.Error())
	}
	c.met.partAtribuidas.Set(0)
	c.log.Info("particoes.revogadas", "partições revogadas pelo group",
		"particoes", len(revogadas[c.cfg.Topic]))
}

func (c *Consumidor) totalDeParticoes(ctx context.Context) (int, error) {
	d, err := c.adm.ListTopics(ctx, c.cfg.Topic)
	if err != nil {
		return 0, err
	}
	t, ok := d[c.cfg.Topic]
	if !ok {
		return 0, fmt.Errorf("tópico %q não existe no cluster", c.cfg.Topic)
	}
	if t.Err != nil {
		return 0, t.Err
	}
	return len(t.Partitions), nil
}

// commitNoBroker publica a posição no group DEPOIS do checkpoint durável.
//
// O commit existe para lag e para ferramentas de terceiros; ele NUNCA
// posiciona o consumer no boot (§49). Por isso a falha aqui é registrada e
// não interrompe nada.
func (c *Consumidor) commitNoBroker(ctx context.Context, offsets map[int32]int64) error {
	if len(offsets) == 0 {
		return nil
	}
	m := make(map[int32]kgo.EpochOffset, len(offsets))
	for p, off := range offsets {
		m[p] = kgo.EpochOffset{Epoch: -1, Offset: off}
	}
	// CommitOffsetsSync não devolve erro: o resultado chega no callback. É
	// síncrono, então ler a variável depois do retorno é seguro.
	var erroCommit error
	c.cl.CommitOffsetsSync(ctx,
		map[string]map[int32]kgo.EpochOffset{c.cfg.Topic: m},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				erroCommit = err
				return
			}
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if p.ErrorCode != 0 {
						erroCommit = fmt.Errorf("partição %d: código de erro %d", p.Partition, p.ErrorCode)
						return
					}
				}
			}
		})
	return erroCommit
}

// Roda executa o pipeline até o contexto acabar ou a ingestão parar.
func (c *Consumidor) Roda(ctx context.Context) error {
	ctx, cancela := context.WithCancel(ctx)
	defer cancela()
	c.cancelaPoll.Store(cancela)

	erroWriter := make(chan error, 1)
	go func() {
		err := c.w.Roda(ctx)
		erroWriter <- err
		if err != nil {
			// Acorda o poll: a pausa do writer só seria vista quando o
			// PollRecords voltasse, e ele pode não voltar tão cedo — um
			// tópico sem tráfego novo deixaria o consumer "parado" sem
			// ninguém saber por um prazo inteiro de fetch.
			cancela()
		}
	}()

	erro := c.poll(ctx)

	// Fecha a fila para o writer drenar o que sobrou e checkpointar.
	c.fila.Fecha()
	if e := <-erroWriter; e != nil && erro == nil {
		erro = e
	}
	return erro
}

// poll é o laço do §27: um fetch por vez, tudo para a fila limitada.
func (c *Consumidor) poll(ctx context.Context) error {
	for {
		// A ordem importa: o cancelamento pode ter vindo justamente de uma
		// recusa de assignment, e devolver nil por causa do contexto
		// esconderia a causa real.
		if err := c.assignmentInvalido(); err != nil {
			return err
		}
		if p := c.w.Pausa(); p != nil {
			return p
		}
		if ctx.Err() != nil {
			return nil // encerramento pedido; não é falha
		}

		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := c.registraErrosDeFetch(fetches); err != nil {
			c.cl.AllowRebalance()
			return err
		}
		if err := c.assignmentInvalido(); err != nil {
			c.cl.AllowRebalance()
			return err
		}

		enfileirados := 0
		fetches.EachRecord(func(r *kgo.Record) {
			if ctx.Err() != nil {
				return
			}
			if err := c.fila.Envia(ctx, r); err == nil {
				enfileirados++
			}
		})
		reg, bytes := c.fila.Profundidade()
		c.met.filaRegistros.Set(float64(reg))
		c.met.filaBytes.Set(float64(bytes))

		if enfileirados > 0 {
			// Espera o writer decidir o lote inteiro antes de liberar o
			// rebalance: é isso que garante que o commit seguinte só toque
			// partições que ainda são nossas.
			marca := make(chan struct{})
			if err := c.fila.EnviaMarca(ctx, marca); err == nil {
				select {
				case <-marca:
				case <-ctx.Done():
				}
			}
		}
		c.cl.AllowRebalance()
	}
}

// interrompePoll acorda o PollRecords para que a falha seja vista agora.
func (c *Consumidor) interrompePoll() {
	if v := c.cancelaPoll.Load(); v != nil {
		v.(context.CancelFunc)()
	}
}

func (c *Consumidor) assignmentInvalido() error {
	if v := c.erroAssign.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// registraErrosDeFetch trata erro de BROKER no balde próprio (D8).
//
// Fica antes e fora do Apply de propósito: o classify do materializer tem
// `default: CategoryStorage`, então um erro de rede que passasse por lá seria
// contado como perda de dado — diagnóstico errado que levaria o operador a
// procurar corrupção onde há indisponibilidade.
func (c *Consumidor) registraErrosDeFetch(f kgo.Fetches) error {
	var fatal error
	f.EachError(func(topico string, particao int32, err error) {
		c.met.errosKafka.Inc()
		if errors.Is(err, context.Canceled) {
			return
		}
		c.log.Warn("fetch.erro", "erro de fetch; o cliente tenta de novo com backoff próprio",
			"topico", topico, "partition", particao, "erro", err.Error())
		if errors.Is(err, kgo.ErrClientClosed) {
			fatal = err
		}
	})
	return fatal
}

// AtualizaLag publica o lag por partição (§92). É chamado pelo scrape, e não
// por um timer: consultar o broker num timer publicaria um número velho cuja
// idade o operador não conhece.
func (c *Consumidor) AtualizaLag(ctx context.Context) {
	lags, err := c.adm.Lag(ctx, c.cfg.GroupID)
	if err != nil {
		c.met.errosKafka.Inc()
		return
	}
	l, ok := lags[c.cfg.GroupID]
	if !ok {
		return
	}
	for _, porParticao := range l.Lag[c.cfg.Topic] {
		c.met.lag(porParticao.Partition).Set(float64(porParticao.Lag))
	}
}

// Pausa devolve o motivo da parada da ingestão, ou nil.
func (c *Consumidor) Pausa() error { return c.w.Pausa() }

// Desfecho devolve os contadores da ingestão.
func (c *Consumidor) Desfecho() Desfecho { return c.w.Desfecho() }

// Fecha encerra o cliente. Idempotente.
func (c *Consumidor) Fecha() {
	c.fecha.Do(func() {
		if c.cl != nil {
			c.cl.Close()
		}
	})
}
