package replicationkafka

import "github.com/twmb/franz-go/pkg/kgo"

// Producer expõe ao teste externo o que o publicador usa do kgo.Client, para
// o teste injetar falha por registro.
type Producer = producer

// WithProducerWrapper embrulha o produtor do publicador.
func WithProducerWrapper(o *Options, wrap func(Producer) Producer) { o.wrapProducer = wrap }

// WithAfterSnapshot roda fn na goroutine do publicador, logo depois do
// snapshot_end confirmado e antes do primeiro Acquire do tail.
func WithAfterSnapshot(o *Options, fn func()) { o.afterSnapshot = fn }

// WithKgoOpts acrescenta opções ao cliente kgo do publicador.
func WithKgoOpts(o *Options, opts ...kgo.Opt) { o.extraOpts = append(o.extraOpts, opts...) }

// WithTopicReadHook roda fn antes de cada leitura do topo do tópico na
// subida; um erro de fn conta como falha da leitura.
func WithTopicReadHook(o *Options, fn func() error) { o.beforeTopicRead = fn }

// WithCheckpointFault roda fn antes de cada gravação do .ckpt; um erro de fn
// conta como falha da gravação.
func WithCheckpointFault(o *Options, fn func() error) { o.checkpointFault = fn }

// CheckpointFile expõe o conteúdo do .ckpt ao teste externo, para montar o
// cenário de cada linha da decisão da subida.
type CheckpointFile = checkpointFile

// LoadCheckpoint e StoreCheckpoint leem e gravam o .ckpt.
var (
	LoadCheckpoint  = loadCheckpoint
	StoreCheckpoint = storeCheckpoint
)
