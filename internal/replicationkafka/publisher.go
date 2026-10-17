package replicationkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Defaults do publicador. Os campos correspondentes de Options com zero valem
// estes.
const (
	// DefaultChunkRows é o tamanho do bloco do snapshot: quantas linhas o
	// exportador copia por snapshot curto, e quantos registros o publicador
	// junta antes de entregar um bloco ao produtor.
	DefaultChunkRows = 4096
	// DefaultPartitionCheckInterval é o intervalo da conferência do número de
	// partições do tópico.
	DefaultPartitionCheckInterval = time.Minute
	// DefaultMinBackoff e DefaultMaxBackoff limitam a espera entre duas
	// tentativas de entrega que falharam por infraestrutura.
	DefaultMinBackoff = 200 * time.Millisecond
	DefaultMaxBackoff = 5 * time.Second
)

const (
	// maxBufferedRecords e minBufferedBytes limitam o buffer do produtor: com
	// o buffer cheio, o Produce bloqueia, e é isso que dá ritmo ao snapshot e
	// ao tail quando o broker é mais lento que o feed.
	maxBufferedRecords = 10_000
	minBufferedBytes   = 32 << 20
	// recordOverheadBytes é a folga para o enquadramento do Kafka na
	// conferência de tamanho (a mesma do seriesdelivery).
	recordOverheadBytes = 128
	// snapshotChunkBytes fecha um bloco do snapshot antes das DefaultChunkRows
	// linhas quando as linhas são grandes: é o que limita o buffer do bloco.
	snapshotChunkBytes = 4 << 20
	// retryWarnInterval é o intervalo mínimo entre dois WARN de retentativa.
	retryWarnInterval = 5 * time.Second
	// blockedKeyPreview é quantos bytes da key o ERROR de mensagem grande
	// mostra.
	blockedKeyPreview = 128
	// metadataWait limita a espera do metadado do produtor depois de o
	// tópico mudar de número de partições: o refresh forçado do kgo respeita
	// o MetadataMinAge (5 s por padrão).
	metadataWait = 15 * time.Second
	// checkpointInterval é o intervalo mínimo entre duas gravações do .ckpt
	// no tail. O checkpoint pode ficar atrás do confirmado: a retomada
	// reentrega o intervalo, e a entrega já é at-least-once.
	checkpointInterval = time.Second
	// topicReadFetchWait é a espera máxima de um fetch da leitura do topo do
	// tópico na subida; o prazo da leitura inteira é o timeout do destino.
	topicReadFetchWait = 500 * time.Millisecond
)

// Estados do publicador, na grafia de Stats.State.
const (
	// StateSnapshotting é o snapshot em curso (reset, linhas, snapshot_end).
	StateSnapshotting = "snapshotting"
	// StateFollowing é o tail acompanhando o feed.
	StateFollowing = "following"
	// StateRetrying é a entrega falhando por infraestrutura; retenta para
	// sempre.
	StateRetrying = "retrying"
	// StateBlocked é o registro maior que maxMessageBytes: o publicador para
	// sem pular nada, até o reinício com o limite corrigido.
	StateBlocked = "blocked"
	// StateFailed é o bug (erro de renderização): o publicador para, e a
	// ingestão segue.
	StateFailed = "failed"
	// StateStopped é o publicador parado pelo encerramento.
	StateStopped = "stopped"
)

var states = [...]string{StateSnapshotting, StateFollowing, StateRetrying, StateBlocked, StateFailed, StateStopped}

// States devolve a lista fechada de estados, para as métricas.
func States() []string { return append([]string(nil), states[:]...) }

// snapshotReasons é a lista fechada dos motivos que o publicador emite e que
// viram série em Stats.Snapshots e nas métricas: os dez ResetReason, na ordem
// da declaração. Todo motivo daqui tem emissor no laço (a decisão da subida,
// publisher_resume.go, e as transições de run): uma série que nascesse antes
// do emissor seria uma série eternamente em zero, que um alarme sobre ela
// confundiria com "não aconteceu".
var snapshotReasons = [...]ResetReason{
	ResetStartup, ResetGap, ResetSnapshotFailed, ResetPartitionsChanged,
	ResetCrash, ResetRewind, ResetRetention, ResetConfigChanged, ResetRequested, ResetPeriodic,
}

// SnapshotReasons devolve a lista fechada de motivos de snapshot, na grafia
// de Stats.Snapshots.
func SnapshotReasons() []string {
	out := make([]string, len(snapshotReasons))
	for i, r := range snapshotReasons {
		out[i] = r.String()
	}
	return out
}

// Motivos de erro, na grafia de Stats.Errors.
const (
	ErrorTimeout         = "timeout"
	ErrorUnavailable     = "unavailable"
	ErrorMessageTooLarge = "message_too_large"
	ErrorPublish         = "publish_error"
	ErrorRender          = "render_error"
	// ErrorSnapshotGap é o snapshot abortado no meio porque o feed perdeu o
	// cursor do reset: o feed não retém as mudanças de um snapshot inteiro.
	ErrorSnapshotGap = "snapshot_gap"
	// ErrorCheckpointWrite é a gravação do .ckpt que falhou. Antes do reset
	// ela impede o snapshot de começar (retrying); depois do snapshot_end e
	// no tail ela só é contada, e o publicador segue.
	ErrorCheckpointWrite = "checkpoint_write"
	// ErrorTopicRead é a leitura do topo do tópico na subida que falhou ou
	// não conseguiu ler o último registro de alguma partição: o publicador
	// não retoma nem publica reset até a leitura dar certo.
	ErrorTopicRead = "topic_read"
)

var errorReasons = [...]string{
	ErrorTimeout, ErrorUnavailable, ErrorMessageTooLarge, ErrorPublish, ErrorRender, ErrorSnapshotGap,
	ErrorCheckpointWrite, ErrorTopicRead,
}

// ErrorReasons devolve a lista fechada de motivos de erro, para as métricas.
func ErrorReasons() []string { return append([]string(nil), errorReasons[:]...) }

// ErrInvalidPublisher é a configuração do publicador recusada pelo New.
var ErrInvalidPublisher = errors.New("replicationkafka: invalid publisher configuration")

// Stats é o retrato do publicador. É a fonte comum do /v1/stats e das
// métricas: as duas leem a MESMA estrutura.
type Stats struct {
	// Destination é o nome do destino em `kafkaTargets:`.
	Destination string `json:"destination"`
	// Topic é o tópico do destino, como a configuração o declara.
	Topic string `json:"topic"`
	State string `json:"state"`
	// Epoch e Cursor são o ponto do feed até onde tudo foi confirmado pelo
	// broker. Durante um snapshot, Cursor continua o do último ponto
	// confirmado; ele salta para o cursor do reset quando o snapshot termina.
	Epoch  string `json:"epoch"`
	Cursor uint64 `json:"cursor"`
	// LagSequences é o que o feed produziu além do cursor (durante o
	// snapshot, além do cursor do reset).
	LagSequences uint64 `json:"lagSequences"`
	// Delivered conta os registros Kafka confirmados pelo broker, de qualquer
	// op. A reprodução de um sufixo conta de novo o que foi confirmado de
	// novo.
	Delivered uint64 `json:"delivered"`
	// Snapshots conta os snapshots CONCLUÍDOS por motivo; todos os motivos
	// existem desde a construção, em zero.
	Snapshots map[string]uint64 `json:"snapshots"`
	// Errors conta as tentativas de entrega que falharam, por motivo (uma por
	// tentativa, com o motivo da primeira falha dela), os erros terminais
	// (render_error, message_too_large do bloqueio) e os snapshots abortados
	// porque o feed perdeu o cursor do reset (snapshot_gap); todos os motivos
	// existem desde a construção, em zero.
	Errors map[string]uint64 `json:"errors"`
	// LastSuccessTimestamp é o instante, em segundos Unix, da última entrega
	// completa (lote do tail ou snapshot); zero se nunca houve.
	LastSuccessTimestamp float64 `json:"lastSuccessTimestamp"`
	LastError            string  `json:"lastError"`
	// CheckpointCursor é o cursor do último .ckpt gravado com sucesso. Fica
	// atrás de Cursor (a gravação no tail é no máximo uma por segundo), e
	// durante um snapshot continua o do último ponto confirmado.
	CheckpointCursor uint64 `json:"checkpointCursor"`
	// CheckpointFound diz se há um .ckpt LEGÍVEL deste destino: lido na
	// subida (CRC e versão conferidos) ou gravado nesta vida. Um .ckpt que
	// existe mas não pode ser lido vale ausente, com WARN, e deixa o campo
	// falso. Falso até lá (inclusive antes do Start), e aí CheckpointCursor
	// zero não é um ponto gravado, é ausência.
	CheckpointFound bool `json:"checkpointFound"`
	// LagBytes é o atraso em bytes de quadro do feed em disco: a posição do
	// fim do último lote fechado menos a do cursor (durante o snapshot, a do
	// cursor do reset). Zero quando o feed já está noutra geração, ou quando
	// a posição do feed ficou atrás da do cursor.
	LagBytes uint64 `json:"lagBytes"`
	// LastResetReason é o motivo do último reset publicado ("" se nenhum).
	LastResetReason string `json:"lastResetReason"`
	// LastSnapshotAt é o instante, em segundos Unix, do último snapshot_end
	// confirmado, persistido no .ckpt (sobrevive ao reinício); zero se nunca
	// houve.
	LastSnapshotAt float64 `json:"lastSnapshotAt"`
}

func (s Stats) clone() Stats {
	out := s
	out.Snapshots = make(map[string]uint64, len(s.Snapshots))
	for k, v := range s.Snapshots {
		out.Snapshots[k] = v
	}
	out.Errors = make(map[string]uint64, len(s.Errors))
	for k, v := range s.Errors {
		out.Errors[k] = v
	}
	return out
}

// Options monta um Publisher.
type Options struct {
	// Dataset é o nome do dataset base.
	Dataset string
	// Target é o destino, já validado pelo config.
	Target config.KafkaTargetSpec
	// Feed é o feed de replicação do dataset.
	Feed *replication.Stream
	// Tables são as tabelas do snapshot: o dataset base e, quando há, a
	// histórica.
	Tables []replication.SnapshotTable
	// Renderer é exclusivo deste publicador: ele não é seguro para uso
	// concorrente.
	Renderer *Renderer
	Logger   *logging.Logger
	// CheckpointDir é o diretório do .ckpt e do .resync do destino: o
	// CheckpointDir(DatasetRootDir) do dataset. Obrigatório.
	CheckpointDir string
	// RenderHash é a impressão da renderização declarada (RenderHash de um
	// RenderSpec): divergente da gravada no .ckpt, a subida publica reset com
	// config_changed em vez de retomar.
	RenderHash [32]byte
	// PeriodicSnapshot é o intervalo entre snapshots periódicos, contado do
	// último snapshot_end confirmado e gravado no .ckpt; zero desliga.
	PeriodicSnapshot time.Duration
	// Now é o relógio de Stats e dos logs; nil vale time.Now.
	Now func() time.Time

	// ChunkRows, PartitionCheckInterval, MinBackoff e MaxBackoff: zero vale o
	// Default correspondente.
	ChunkRows              int
	PartitionCheckInterval time.Duration
	MinBackoff             time.Duration
	MaxBackoff             time.Duration

	// Ganchos de teste (export_test.go).
	extraOpts     []kgo.Opt
	wrapProducer  func(producer) producer
	afterSnapshot func()
	// beforeTopicRead roda antes de cada leitura do topo do tópico; um erro
	// conta como falha da leitura.
	beforeTopicRead func() error
	// checkpointFault roda antes de cada gravação do .ckpt; um erro conta
	// como falha da gravação.
	checkpointFault func() error
	// onVerificationRelease roda logo antes de a pendência de conferência ser
	// solta, uma vez: é onde o teste lê a geração do feed nesse instante.
	onVerificationRelease func()
}

// producer é o que o publicador usa do kgo.Client. Existe para o teste
// injetar falha por registro sem depender do tempo do broker.
type producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
}

// Publisher publica o feed de UM dataset num destino Kafka. Uma goroutine
// só, iniciada por Start e parada por Close; Stats é seguro de qualquer
// goroutine.
type Publisher struct {
	dataset      string
	destination  string
	topic        string
	feed         *replication.Stream
	tables       []replication.SnapshotTable
	renderer     *Renderer
	log          *logging.Logger
	now          func() time.Time
	client       producer
	admin        *kadm.Client
	timeout      time.Duration
	messageBytes int64
	chunkRows    int
	checkEvery   time.Duration
	minBackoff   time.Duration
	maxBackoff   time.Duration
	afterSnap    func()

	// ckptPath e resyncPath são os arquivos de estado do destino; renderHash,
	// periodic e bootstrap são o que a subida compara com o .ckpt.
	ckptPath   string
	resyncPath string
	renderHash [32]byte
	periodic   time.Duration
	bootstrap  []string
	// readerOpts são as opções de conexão (brokers, TLS, SASL) do cliente
	// consumidor da leitura do topo do tópico.
	readerOpts      []kgo.Opt
	beforeTopicRead func() error
	checkpointFault func() error

	// kcl é o cliente real, para o refresh de metadado (client pode ser o
	// embrulho do teste).
	kcl *kgo.Client
	// pinned é o número de partições fixado no último reset; staleMetadata é
	// ligado pelo particionador quando o metadado do kgo ainda não conhece
	// uma partição fixada. O particionador roda na goroutine do kgo.
	pinned        atomic.Int32
	staleMetadata atomic.Bool

	// Estado da goroutine do publicador; nada daqui é tocado por outra.
	hasher          kgo.TopicPartitioner
	hashRec         kgo.Record
	epoch           string
	cursor          uint64
	counts          []uint64
	tail            outBatch
	chunks          [2]outBatch
	chunk           int
	idx             []int
	firstFail       []int
	justSnapshotted bool
	lastSnapshot    time.Duration
	lastWarn        time.Time
	lastLivelock    time.Time
	snapshotStarted time.Time
	nextCheck       time.Time
	// ckpt é o checkpoint que o publicador quer no disco: o último ponto
	// confirmado, com Snapshotting durante um reset. ckptFound diz se a
	// subida achou um .ckpt legível; ckptDirty, que ckpt mudou desde a última
	// gravação bem-sucedida; lastCkptWrite é o instante dela.
	ckpt          checkpointFile
	ckptFound     bool
	ckptDirty     bool
	lastCkptWrite time.Time
	lastCkptWarn  time.Time
	// workEpoch é a geração em que o publicador trabalha: a do reset em curso
	// durante o snapshot, a do cursor no tail.
	workEpoch string
	// ctrl é o lote do resume, enviado sem barreira de ack: ele é apurado
	// pelo tail (reapControl) e na saída.
	ctrl outBatch
	// releaseVerification solta a pendência de conferência de rewind que o
	// New registrou no feed (replication.Stream.HoldForVerification). É
	// idempotente e nunca nil; fixada no New, é chamada pela goroutine do
	// publicador (decide, startup) e pelo Close.
	releaseVerification func()

	mu      sync.Mutex
	stats   Stats
	lagBase uint64
	// lagEpoch e lagPos são a geração e a posição em bytes do cursor, base do
	// LagBytes.
	lagEpoch string
	lagPos   uint64
	// acquireCancel cancela o Acquire do tail em curso; wakeRequested pede ao
	// laço a conferência dos gatilhos (pedido de snapshot) na próxima volta.
	acquireCancel context.CancelFunc
	wakeRequested bool

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	closeOnce sync.Once
	done      chan struct{}
	closeErr  error
}

// New valida as opções e monta o produtor, sem falar com o broker: um broker
// fora do ar não impede a subida.
//
// Com o feed retomado de um fechamento limpo (OriginClean), registra nele a
// pendência de conferência de rewind (replication.Stream.HoldForVerification):
// enquanto ela vale, o servidor não serve o feed aos followers HTTP. Fica no
// New, e não no Start, porque o servidor monta os publicadores ANTES de subir
// o HTTP, e o Start vem depois dele: registrada no Start, a pendência
// deixaria uma janela em que o follower retomaria o tail de um backup
// restaurado. Por isso o New vem DEPOIS do Resume do feed. A pendência é
// solta quando a conferência termina (decide), em qualquer saída da subida e
// no Close, inclusive o Close sem Start.
func New(o Options) (*Publisher, error) {
	switch {
	case o.Dataset == "":
		return nil, fmt.Errorf("%w: dataset name is empty", ErrInvalidPublisher)
	case o.Feed == nil:
		return nil, fmt.Errorf("%w: dataset %q has no replication feed", ErrInvalidPublisher, o.Dataset)
	case o.Renderer == nil:
		return nil, fmt.Errorf("%w: dataset %q has no envelope renderer", ErrInvalidPublisher, o.Dataset)
	case o.CheckpointDir == "":
		return nil, fmt.Errorf("%w: dataset %q has no checkpoint directory", ErrInvalidPublisher, o.Dataset)
	case o.ChunkRows < 0 || o.PartitionCheckInterval < 0 || o.MinBackoff < 0 || o.MaxBackoff < 0 || o.PeriodicSnapshot < 0:
		return nil, fmt.Errorf("%w: negative tuning value", ErrInvalidPublisher)
	}
	t := o.Target
	timeout, err := t.DeliveryTimeout()
	if err != nil {
		return nil, err
	}
	messageBytes, err := t.MessageBytes()
	if err != nil {
		return nil, err
	}
	tlsConfig, mechanism, err := kafka.ResolveSecurity(t.Security)
	if err != nil {
		return nil, fmt.Errorf("%w: destination %q: %v", config.ErrKafkaTarget, t.Name, err)
	}
	log := o.Logger
	if log == nil {
		log = logging.Discard()
	}
	p := &Publisher{
		dataset:      o.Dataset,
		destination:  t.Name,
		topic:        t.Topic,
		feed:         o.Feed,
		tables:       o.Tables,
		renderer:     o.Renderer,
		log:          log.Component("replication").Dataset(o.Dataset).With("destination", t.Name, "topic", t.Topic),
		now:          o.Now,
		timeout:      timeout,
		messageBytes: messageBytes,
		chunkRows:    orDefault(o.ChunkRows, DefaultChunkRows),
		checkEvery:   orDefault(o.PartitionCheckInterval, DefaultPartitionCheckInterval),
		minBackoff:   orDefault(o.MinBackoff, DefaultMinBackoff),
		maxBackoff:   orDefault(o.MaxBackoff, DefaultMaxBackoff),
		afterSnap:    o.afterSnapshot,
		hasher:       kgo.StickyKeyPartitioner(nil).ForTopic(t.Topic),
		done:         make(chan struct{}),
		renderHash:   o.RenderHash,
		periodic:     o.PeriodicSnapshot,
		bootstrap:    append([]string(nil), t.BootstrapServers...),

		beforeTopicRead: o.beforeTopicRead,
		checkpointFault: o.checkpointFault,
	}
	p.ckptPath, p.resyncPath = checkpointFiles(o.CheckpointDir, t.Name)
	if p.now == nil {
		p.now = time.Now
	}
	p.maxBackoff = max(p.maxBackoff, p.minBackoff)
	conn := []kgo.Opt{kgo.SeedBrokers(t.BootstrapServers...), kgo.DialTimeout(timeout)}
	if tlsConfig != nil {
		conn = append(conn, kgo.DialTLSConfig(tlsConfig))
	}
	if mechanism != nil {
		conn = append(conn, kgo.SASL(mechanism))
	}
	p.readerOpts = conn
	opts := []kgo.Opt{
		kgo.SeedBrokers(t.BootstrapServers...),
		kgo.DefaultProduceTopic(t.Topic),
		// Idempotente (o default do kgo, que NÃO é desligado), acks de todas
		// as réplicas em sincronia e retentativas internas padrão, limitadas
		// pelo RecordDeliveryTimeout. A cancelação liberada é o que limita o
		// encerramento com o broker fora; o preço é a duplicata possível, que
		// o contrato já assume (at-least-once).
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowIdempotentProduceCancellation(),
		kgo.RecordDeliveryTimeout(timeout),
		kgo.ProduceRequestTimeout(timeout),
		kgo.DialTimeout(timeout),
		kgo.ProducerBatchMaxBytes(int32(messageBytes)),
		kgo.MaxBufferedRecords(maxBufferedRecords),
		kgo.MaxBufferedBytes(int(max(minBufferedBytes, 2*messageBytes))),
		kgo.RecordPartitioner(kgo.BasicConsistentPartitioner(p.partitionerFor)),
	}
	if tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	opts = append(opts, o.extraOpts...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: destination %q: %v", config.ErrKafkaTarget, t.Name, err)
	}
	p.admin = kadm.NewClient(client)
	p.kcl = client
	p.client = client
	if o.wrapProducer != nil {
		p.client = o.wrapProducer(client)
	}
	p.stats = Stats{
		Destination: t.Name,
		Topic:       t.Topic,
		State:       StateSnapshotting,
		Snapshots:   make(map[string]uint64, len(snapshotReasons)),
		Errors:      make(map[string]uint64, len(errorReasons)),
	}
	// Toda série nasce em zero: o alarme sobre `errors` precisa da série
	// antes do primeiro erro.
	for _, r := range snapshotReasons {
		p.stats.Snapshots[r.String()] = 0
	}
	for _, r := range errorReasons {
		p.stats.Errors[r] = 0
	}
	// O contexto de vida é PRÓPRIO, e não o do servidor: o encerramento
	// fecha o feed e deixa o publicador drenar a cauda antes de cancelar.
	p.ctx, p.cancel = context.WithCancel(context.Background())
	// Por último, depois de toda recusa: o New que falha não deixa pendência
	// que ninguém soltaria.
	p.releaseVerification = func() {}
	if o.Feed.Origin().Kind == replication.OriginClean {
		release := o.Feed.HoldForVerification()
		p.releaseVerification = release
		if hook := o.onVerificationRelease; hook != nil {
			var once sync.Once
			p.releaseVerification = func() { once.Do(func() { hook(); release() }) }
		}
	}
	return p, nil
}

func orDefault[T int | time.Duration](v, def T) T {
	if v == 0 {
		return def
	}
	return v
}

// Start inicia a goroutine do publicador. Chamadas repetidas não fazem nada.
//
// Não recebe contexto de propósito: o laço vive até o Close, e não até o
// cancelamento do contexto do servidor, que viria antes da drenagem.
func (p *Publisher) Start() {
	p.startOnce.Do(func() {
		go p.run(p.ctx)
	})
}

// Close drena e para o publicador. O chamador fecha o feed ANTES: o laço
// entrega os lotes retidos, recebe ErrClosed e sai. Se ctx vencer antes, o
// laço é cancelado (um snapshot em curso é abortado sem snapshot_end) e os
// registros pendentes falham. Devolve ctx.Err() quando a drenagem não
// terminou dentro do prazo.
//
// É limitado por ctx mesmo com o broker inalcançável: nada aqui espera o
// broker sem ctx.
func (p *Publisher) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		// Sem Start, o done nunca fecharia.
		p.startOnce.Do(func() { close(p.done) })
		select {
		case <-p.done:
		case <-ctx.Done():
			p.closeErr = ctx.Err()
		}
		p.cancel()
		<-p.done
		// O laço solta a pendência ao sair da subida; sem Start, só o Close
		// a solta.
		p.releaseVerification()
		// O laço só sai depois de ter todas as promises de volta, ou por
		// cancelamento; o Flush aqui cobre o resto, limitado pelo mesmo ctx.
		if err := p.client.Flush(ctx); err != nil && p.closeErr == nil {
			p.closeErr = err
		}
		p.client.Close()
		p.mu.Lock()
		if p.stats.State != StateBlocked && p.stats.State != StateFailed {
			p.stats.State = StateStopped
		}
		p.mu.Unlock()
	})
	return p.closeErr
}

// Stats devolve uma cópia do retrato.
func (p *Publisher) Stats() Stats {
	epoch, produced, endPos := p.feed.Position()
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats.clone()
	if produced > p.lagBase {
		s.LagSequences = produced - p.lagBase
	}
	// A posição em bytes recomeça do zero a cada geração: comparada com a de
	// outra geração, ou atrás da do cursor, ela não mede atraso nenhum.
	//
	// Sem atraso em sequences também não há atraso em bytes. O snapshot lê o
	// cursor COM o intervalo ativo (as alterações já publicadas e ainda não
	// fechadas em lote, que ele cobre) e a posição em bytes SEM ele, que só
	// existe no lote fechado: quando esse lote fecha, a posição do feed passa
	// da do cursor sem nada a entregar, e o atraso ficaria parado acima de
	// zero com o feed ocioso.
	if produced > p.lagBase && epoch == p.lagEpoch && endPos >= p.lagPos {
		s.LagBytes = endPos - p.lagPos
	}
	return s
}

// RequestSnapshot registra um pedido de snapshot do destino, de forma durável
// (o contador .resync), e acorda o laço para conferi-lo já, sem esperar a
// cadência da conferência de partições. O pedido sobrevive a um reinício que
// aconteça antes do snapshot, e um pedido feito durante um snapshot continua
// pendente para o próximo. Devolve o erro da gravação do contador.
func (p *Publisher) RequestSnapshot() error {
	if _, err := requestResync(p.resyncPath); err != nil {
		return err
	}
	p.mu.Lock()
	p.wakeRequested = true
	if p.acquireCancel != nil {
		p.acquireCancel()
	}
	p.mu.Unlock()
	return nil
}
