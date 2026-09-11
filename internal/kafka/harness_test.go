package kafka_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// A camada de UNIDADE dos testes do §105 roda contra o kfake, que é o broker
// em memória do próprio franz-go. A escolha está acoplada à do cliente (§6) e
// foi registrada no plano do M1: kfake para unidade, Testcontainers/Redpanda
// para integração. O que se ganha é poder exercitar crash, rebalance e
// reposicionamento em milissegundos e sem daemon externo.

const specYAML = `version: 1
name: radius_sessions
source:
  type: kafka
  kafka:
    enabled: true
    bootstrapServers:
      - PLACEHOLDER:9092
    topic: radius-accounting
    consumer:
      groupId: nishidb-radius
      maxPollRecords: 100
      ingestionQueueSize: 200
materialization:
  key:
    field: id
  mode: merge
  sequence:
    field: _SEQ
    type: int64
    strategy: greater
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    _SEQ:
      type: int64
    status:
      type: string
      index: true
`

// bench é um dataset real (storage + materializer) ligado a um broker
// kfake. Nada é dublê aqui além do broker: o objetivo dos testes do §105 é
// justamente exercitar a durabilidade de verdade.
type bench struct {
	t        *testing.T
	cluster  *kfake.Cluster // nil na camada de integração
	endereco string
	dir      string
	spec     *schema.DatasetSpec
	cfg      *kafka.Config
	reg      *metrics.Registry

	st  *storage.Store
	mat *materializer.Materializer
}

func newBench(t *testing.T, partitions int32) *bench {
	t.Helper()
	cluster, err := kfake.NewCluster(
		kfake.SeedTopics(partitions, "radius-accounting"),
		kfake.NumBrokers(1),
		kfake.ClusterID("bancada-de-teste"),
		kfake.SyncWrites(),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)

	b := newBenchIn(t, cluster.ListenAddrs()[0])
	b.cluster = cluster
	return b
}

// newBenchIn monta a bancada contra um broker JÁ existente. É o que
// permite a mesma bateria rodar contra o kfake (unidade) e contra um broker
// de verdade em container (integração), sem duplicar o harness.
func newBenchIn(t *testing.T, endereco string) *bench {
	t.Helper()
	b := &bench{t: t, endereco: endereco, dir: t.TempDir(), reg: metrics.NewRegistry()}
	b.loadSpec()
	b.open()
	return b
}

func (b *bench) loadSpec() {
	b.t.Helper()
	yml := replaceUm(specYAML, "PLACEHOLDER:9092", b.endereco)

	spec, err := schema.Parse([]byte(yml), schema.BuiltinDefaults)
	if err != nil {
		b.t.Fatalf("spec: %v", err)
	}
	b.spec = spec
	cfg, err := kafka.ResolveKafkaConfig(spec.Name, spec.Source.Kafka, spec.Errors)
	if err != nil {
		b.t.Fatalf("resolve: %v", err)
	}
	b.cfg = cfg
}

// open (re)open o dataset, como um reinício de processo faria.
func (b *bench) open() {
	b.t.Helper()
	st, err := storage.Open(storage.Options{
		Dir:                 filepath.Join(b.dir, "radius_sessions"),
		Dataset:             "radius_sessions",
		SegmentSize:         1 << 20,
		IndexedFieldsByName: []string{"status"},
		Metrics:             b.reg,
	})
	if err != nil {
		b.t.Fatalf("storage.Open: %v", err)
	}
	compilado, err := b.spec.Compile(st.FieldDict())
	if err != nil {
		if cerr := st.Close(); cerr != nil {
			b.t.Errorf("fechar o store: %v", cerr)
		}
		b.t.Fatalf("compile: %v", err)
	}
	b.st = st
	mat, err := materializer.New(compilado, st)
	if err != nil {
		b.t.Fatalf("materializer.New: %v", err)
	}
	b.mat = mat
}

func (b *bench) close() {
	b.t.Helper()
	if b.st != nil {
		if err := b.st.Close(); err != nil {
			b.t.Fatalf("Close: %v", err)
		}
		b.st = nil
	}
}

// reinicia fecha e reabre o dataset — o equivalente a um restart de processo,
// com recovery de verdade.
func (b *bench) reinicia() {
	b.t.Helper()
	b.close()
	b.open()
}

// produz publica eventos no tópico.
func (b *bench) produz(recs ...*kgo.Record) {
	b.t.Helper()
	// ManualPartitioner: o particionador default IGNORA r.Partition e escolhe
	// pelo hash da key, o que tornaria o teste de múltiplas partições uma
	// loteria.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.endereco),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		b.t.Fatalf("produtor: %v", err)
	}
	defer cl.Close()
	for _, r := range recs {
		r.Topic = b.cfg.Topic
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		b.t.Fatalf("produzir: %v", err)
	}
}

// event monta um record de uma entidade.
func event(id string, seq int64, status string) *kgo.Record {
	return &kgo.Record{
		Key:   []byte(id),
		Value: []byte(fmt.Sprintf(`{"id":%q,"_SEQ":%d,"status":%q}`, id, seq, status)),
	}
}

func eventNaPartition(id string, seq int64, status string, p int32) *kgo.Record {
	r := event(id, seq, status)
	r.Partition = p
	return r
}

// consomeUntil roda o consumidor até `parar` devolver true ou o prazo estourar.
//
// Devolve o desfecho e o erro do Run. O padrão de parada por CONDIÇÃO (e não
// por sleep) é o que mantém o teste determinístico: dormir esconde tanto a
// lentidão quanto a ausência de progresso.
func (b *bench) consomeUntil(prazo time.Duration, stop func(kafka.Desfecho) bool) (kafka.Desfecho, error) {
	b.t.Helper()
	return b.consomeWithCadencia(prazo, kafka.Cadencia{Eventos: 1, Intervalo: time.Second}, stop)
}

// consomeWithCadencia é o mesmo, com a cadência de checkpoint escolhida pelo
// teste — o que permite provar o checkpoint de ENCERRAMENTO isolado do
// periódico.
func (b *bench) consomeWithCadencia(prazo time.Duration, cad kafka.Cadencia,
	stop func(kafka.Desfecho) bool,
) (kafka.Desfecho, error) {
	b.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), prazo)
	defer cancel()

	cons, err := kafka.Abre(ctx, b.cfg, kafka.Options{
		Store:    b.st,
		Mat:      b.mat,
		Metrics:  b.reg,
		Cadencia: cad,
	})
	if err != nil {
		return kafka.Desfecho{}, err
	}
	defer cons.Close()

	end := make(chan error, 1)
	go func() { end <- cons.Run(ctx) }()

	tique := time.NewTicker(5 * time.Millisecond)
	defer tique.Stop()
	for {
		select {
		case err := <-end:
			return cons.Desfecho(), err
		case <-tique.C:
			if stop(cons.Desfecho()) {
				cancel()
				<-end
				return cons.Desfecho(), nil
			}
		case <-ctx.Done():
			// Prazo esgotado é fim NORMAL: os testes que esperam "nada
			// acontece" dependem disso, e os que esperam N eventos falham na
			// asserção sobre o desfecho, que é onde a mensagem é útil.
			return cons.Desfecho(), <-end
		}
	}
}

// openConsumidor devolve o consumidor sem rodá-lo, para os testes que só
// exercitam a validação de subida.
func (b *bench) openConsumidor() (*kafka.Consumidor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return kafka.Abre(ctx, b.cfg, kafka.Options{
		Store:    b.st,
		Mat:      b.mat,
		Metrics:  b.reg,
		Cadencia: kafka.Cadencia{Eventos: 1},
	})
}

// readOffsets lê o kafka.offsets direto do disco: os testes de durabilidade
// verificam o ARQUIVO, e não o estado em memória.
func (b *bench) readOffsets() (map[int32]int64, bool) {
	b.t.Helper()
	path := filepath.Join(b.dir, "radius_sessions", "checkpoint", "kafka.offsets")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, false
	}
	o, _, err := b.st.SourceOffsets()
	if err != nil {
		b.t.Fatalf("SourceOffsets: %v", err)
	}
	return o.Offsets, true
}

// deleteOffsets remove o kafka.offsets, como um restore parcial faria.
func (b *bench) deleteOffsets() {
	b.t.Helper()
	b.close()
	if err := os.Remove(filepath.Join(b.dir, "radius_sessions", "checkpoint", "kafka.offsets")); err != nil {
		b.t.Fatalf("apagar offsets: %v", err)
	}
	b.open()
}

func replaceUm(s, velho, nw string) string {
	i := indexOf(s, velho)
	if i < 0 {
		return s
	}
	return s[:i] + nw + s[i+len(velho):]
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
