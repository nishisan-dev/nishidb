package replicationkafka_test

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// switchProducer falha, sem entregar ao broker, os registros escolhidos por
// fail enquanto ele estiver ligado, e SEGURA (nunca confirma) os escolhidos
// por hold; o resto segue para o kgo real.
type switchProducer struct {
	replicationkafka.Producer
	mu   sync.Mutex
	fail func(*kgo.Record) bool
	hold func(*kgo.Record) bool
}

func (p *switchProducer) set(fail func(*kgo.Record) bool) {
	p.mu.Lock()
	p.fail = fail
	p.mu.Unlock()
}

func (p *switchProducer) holdWhere(hold func(*kgo.Record) bool) {
	p.mu.Lock()
	p.hold = hold
	p.mu.Unlock()
}

func (p *switchProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	p.mu.Lock()
	hit := p.fail != nil && p.fail(r)
	held := p.hold != nil && p.hold(r)
	p.mu.Unlock()
	switch {
	case held:
		return
	case hit:
		promise(r, errors.New("injected produce failure"))
		return
	}
	p.Producer.Produce(ctx, r, promise)
}

// opOf é o header nishidb-op do registro.
func opOf(r *kgo.Record) string {
	for _, h := range r.Headers {
		if h.Key == replicationkafka.HeaderOp {
			return string(h.Value)
		}
	}
	return ""
}

// controlCount conta, por partição, os registros com o op.
func controlCount(all []msg, op string) map[int32]int {
	out := make(map[int32]int)
	for _, m := range all {
		if m.op() == op {
			out[m.partition]++
		}
	}
	return out
}

// eachPartitionHas diz se cada uma das n partições tem ao menos want
// registros com o op.
func eachPartitionHas(n int32, op string, want int) func([]msg) bool {
	return func(all []msg) bool {
		per := controlCount(all, op)
		for p := range n {
			if per[p] < want {
				return false
			}
		}
		return true
	}
}

// liveHarness sobe o cenário com o publicador iniciado, espera o snapshot da
// subida e entrega duas mudanças pelo tail. Devolve o harness e a sequence da
// última mudança entregue.
func liveHarness(t *testing.T, o harnessOpts) (*harness, uint64) {
	t.Helper()
	h := newHarness(t, o)
	h.pub.Start()
	h.readUntil("snapshot da subida", snapshotEnds(3, 1))
	h.upsert("A", 1)
	last := h.upsert("B", 2)
	h.feed.Flush(time.Now(), "test")
	h.readUntil("tail da primeira vida", hasSequence(last))
	waitFor(t, "cursor no fim do tail", func() bool { return h.pub.Stats().Cursor == last })
	return h, last
}

// rewriteCheckpoint lê o .ckpt do cenário, aplica edit e o grava de volta.
func rewriteCheckpoint(t *testing.T, h *harness, edit func(*replicationkafka.CheckpointFile)) {
	t.Helper()
	c, ok, err := replicationkafka.LoadCheckpoint(h.checkpointPath())
	if err != nil || !ok {
		t.Fatalf("o .ckpt da primeira vida não existe: ok=%v err=%v", ok, err)
	}
	edit(&c)
	if err := replicationkafka.StoreCheckpoint(h.checkpointPath(), c); err != nil {
		t.Fatal(err)
	}
}

// secondGeneration espera o segundo snapshot completo em todas as partições
// e devolve os resets dele, um por partição, já conferidos: mesmo motivo no
// corpo e no header nishidb-reason, em todas.
func secondGeneration(t *testing.T, h *harness) []msg {
	t.Helper()
	all := h.readUntil("segundo snapshot", snapshotEnds(3, 2))
	var resets []msg
	seen := make(map[int32]int)
	for _, m := range all {
		if m.op() != "reset" {
			continue
		}
		seen[m.partition]++
		if seen[m.partition] == 2 {
			resets = append(resets, m)
		}
	}
	if len(resets) != 3 {
		t.Fatalf("segundo reset em %d partições, quer 3:\n%s", len(resets), dump(all))
	}
	for _, m := range resets[1:] {
		if m.str("reason") != resets[0].str("reason") || m.str("epoch") != resets[0].str("epoch") {
			t.Fatalf("os resets da mesma geração divergem:\n%s", dump(resets))
		}
	}
	for _, m := range resets {
		if m.reasonHeader != m.str("reason") {
			t.Fatalf("header nishidb-reason %q difere do corpo %q", m.reasonHeader, m.str("reason"))
		}
	}
	return resets
}

// expectResume espera o resume em todas as partições e assevera que a vida
// nova não publicou reset nenhum. Devolve o tópico inteiro.
func expectResume(t *testing.T, h *harness) []msg {
	t.Helper()
	h.readUntil("resume em todas as partições", eachPartitionHas(3, "resume", 1))
	waitFor(t, "estado following", func() bool { return h.pub.Stats().State == replicationkafka.StateFollowing })
	all := h.readAll()
	for p, n := range controlCount(all, "reset") {
		if n != 1 {
			t.Fatalf("partição %d com %d resets; a retomada não publica reset:\n%s", p, n, dump(all))
		}
	}
	return all
}

// A tabela de decisão da subida, uma linha por regra: cada caso monta o
// .ckpt, o feed e o tópico e assevera o motivo LITERAL do reset da vida nova,
// ou a ausência de reset e a presença do resume.
func TestStartupDecisionTable(t *testing.T) {
	type tc struct {
		name  string
		clean bool
		// live roda na primeira vida, antes do encerramento; before, com tudo
		// parado; after, com o feed reaberto e o publicador ainda parado.
		live   func(t *testing.T, h *harness)
		before func(t *testing.T, h *harness, last uint64)
		after  func(t *testing.T, h *harness)
		want   string // "" = retoma
		// elsewhere: o reset sai noutro tópico ou cluster, que o leitor do
		// harness não lê; o motivo é conferido pelo retrato.
		elsewhere bool
	}
	cases := []tc{
		{name: "1 sem ckpt", clean: false, want: "startup", before: func(t *testing.T, h *harness, _ uint64) {
			if err := os.Remove(h.checkpointPath()); err != nil {
				t.Fatal(err)
			}
		}},
		// O destino trocado é destino NOVO só quando o tópico novo está vazio:
		// com registros, nada prova que ele não tem publicação nossa
		// posterior, e o resultado é rewind (a conferência).
		{name: "1 tópico diferente", clean: true, want: "startup", elsewhere: true, before: func(t *testing.T, h *harness, _ uint64) {
			cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			if _, err := kadm.NewClient(cl).CreateTopic(context.Background(), 3, 1, nil, "other.topic"); err != nil {
				t.Fatal(err)
			}
			h.o.target = func(k *config.KafkaTargetSpec) { k.Topic = "other.topic" }
		}},
		{name: "1 nenhum broker em comum", clean: true, want: "startup", elsewhere: true, before: func(t *testing.T, h *harness, _ uint64) {
			other, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, pubTopic))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(other.Close)
			h.o.target = func(k *config.KafkaTargetSpec) { k.BootstrapServers = other.ListenAddrs() }
		}},
		{name: "2 impressão diferente", clean: true, want: "config_changed", before: func(_ *testing.T, h *harness, _ uint64) {
			h.o.publisher = func(o *replicationkafka.Options) { o.RenderHash = [32]byte{1} }
		}},
		{name: "3 snapshot interrompido", clean: true, want: "snapshot_failed", before: func(t *testing.T, h *harness, _ uint64) {
			rewriteCheckpoint(t, h, func(c *replicationkafka.CheckpointFile) {
				c.Snapshotting, c.TargetEpoch, c.TargetCursor = true, c.Epoch, c.Cursor
			})
		}},
		{name: "4 pedido pendente", clean: true, want: "requested", before: func(t *testing.T, h *harness, _ uint64) {
			if err := h.pub.RequestSnapshot(); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "5 queda da geração do ckpt", clean: false, want: "crash"},
		{name: "5 feed rotacionado", clean: true, want: "rewind", after: func(_ *testing.T, h *harness) {
			h.feed.Rotate("test")
		}},
		{name: "5 queda de outra geração", clean: false, want: "startup", before: func(t *testing.T, h *harness, _ uint64) {
			rewriteCheckpoint(t, h, func(c *replicationkafka.CheckpointFile) { c.Epoch = "00000000000000000000000000000000" })
		}},
		{name: "6 cursor abaixo do piso", clean: true, want: "retention",
			// O Invalidate leva o piso à sequence produzida, que é o cursor
			// do publicador (sem lacuna para ele); o .ckpt é rebaixado para
			// um abaixo do piso.
			live: func(t *testing.T, h *harness) {
				h.feed.Invalidate()
				waitFor(t, "piso no fim do feed", func() bool { return h.feed.Status().AvailableAfter > 0 })
			},
			before: func(t *testing.T, h *harness, last uint64) {
				rewriteCheckpoint(t, h, func(c *replicationkafka.CheckpointFile) { c.Cursor = last - 1 })
			}},
		{name: "7 partições diferentes", clean: true, want: "partitions_changed", before: func(t *testing.T, h *harness, _ uint64) {
			rewriteCheckpoint(t, h, func(c *replicationkafka.CheckpointFile) { c.Partitions = 2 })
		}},
		{name: "8 periódico vencido", clean: true, want: "periodic", before: func(_ *testing.T, h *harness, _ uint64) {
			h.o.publisher = func(o *replicationkafka.Options) {
				o.PeriodicSnapshot = time.Hour
				o.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
			}
		}},
		{name: "9 retoma", clean: true, want: ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, last := liveHarness(t, harnessOpts{partitions: 3})
			epoch := h.pub.Stats().Epoch
			if c.live != nil {
				c.live(t, h)
			}
			h.stop(c.clean)
			if c.before != nil {
				c.before(t, h, last)
			}
			h.open()
			if c.after != nil {
				c.after(t, h)
			}
			h.pub.Start()
			if c.elsewhere {
				waitFor(t, "reset no destino trocado", func() bool { return h.pub.Stats().LastResetReason != "" })
				if got := h.pub.Stats().LastResetReason; got != c.want {
					t.Fatalf("motivo do reset = %q, quer %q", got, c.want)
				}
				if o := h.feed.Origin(); o.Kind != replication.OriginClean {
					t.Fatalf("o destino trocado rotacionou o feed (%v)", o.Kind)
				}
				return
			}
			if c.want == "" {
				all := expectResume(t, h)
				for _, m := range all {
					if m.op() == "resume" && (m.str("epoch") != epoch || m.num("cursor") != last) {
						t.Fatalf("resume com epoch %q cursor %d, quer %q e %d", m.str("epoch"), m.num("cursor"), epoch, last)
					}
				}
				return
			}
			resets := secondGeneration(t, h)
			if got := resets[0].str("reason"); got != c.want {
				t.Fatalf("motivo do reset = %q, quer %q", got, c.want)
			}
			if n := controlCount(h.readAll(), "resume"); len(n) != 0 {
				t.Fatalf("a vida nova publicou resume junto com o reset: %v", n)
			}
		})
	}
}

// A retomada continua a numeração da vida anterior na MESMA epoch, sem reset:
// a mudança da vida nova sai com a sequence seguinte à última da anterior.
func TestResumeContinuesSequencesWithoutReset(t *testing.T) {
	h, last := liveHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	epoch := h.pub.Stats().Epoch
	h.restart(true)
	expectResume(t, h)
	next := h.upsert("C", 3)
	h.feed.Flush(time.Now(), "test")
	if next != last+1 {
		t.Fatalf("a vida nova numerou a mudança %d, quer %d (continuação de %d)", next, last+1, last)
	}
	all := h.readUntil("mudança da vida nova", hasSequence(next))
	for _, m := range all {
		if m.op() == "insert" && m.key == "C" && m.str("epoch") != epoch {
			t.Fatalf("mudança da vida nova na epoch %q, quer %q", m.str("epoch"), epoch)
		}
	}
	if s := h.pub.Stats(); s.Epoch != epoch || s.Snapshots["startup"] != 0 {
		t.Fatalf("stats da retomada: %+v", s)
	}
}

// O resume sai na hora, sem esperar o primeiro lote do tail: com o tail
// ocioso, ele está no tópico mesmo sem mudança nenhuma depois do reinício.
func TestResumeMarkerIsSentWhileTailIdle(t *testing.T) {
	h, last := liveHarness(t, harnessOpts{partitions: 3})
	h.restart(true)
	all := expectResume(t, h)
	for _, m := range all {
		if m.op() == "resume" && (m.key != string(replicationkafka.ControlKey) || m.header != "resume" || m.num("cursor") != last) {
			t.Fatalf("resume malformado: key %q header %q cursor %d", m.key, m.header, m.num("cursor"))
		}
	}
	if _, produced := h.feed.Cursor(); produced != last {
		t.Fatalf("o feed produziu além de %d (%d): o tail não estava ocioso", last, produced)
	}
}

// O reinício no meio de um snapshot (reset publicado, snapshot_end ainda
// não) nunca retoma o tail: o .ckpt marca o reset em curso antes de qualquer
// registro dele, e a vida nova publica reset com snapshot_failed.
func TestRestartDuringSnapshotNeverResumesTail(t *testing.T) {
	var prod *switchProducer
	h, _ := liveHarness(t, harnessOpts{partitions: 3, publisher: func(o *replicationkafka.Options) {
		replicationkafka.WithProducerWrapper(o, func(p replicationkafka.Producer) replicationkafka.Producer {
			prod = &switchProducer{Producer: p}
			return prod
		})
	}})
	// O segundo snapshot (pedido) publica o reset, e o snapshot_end fica
	// preso sem confirmação: o snapshot não termina nem recomeça até o
	// encerramento.
	prod.holdWhere(func(r *kgo.Record) bool { return opOf(r) == "snapshot_end" })
	if err := h.pub.RequestSnapshot(); err != nil {
		t.Fatal(err)
	}
	pending := h.readUntil("reset do snapshot pedido", eachPartitionHas(3, "reset", 2))
	// A marca está no DISCO enquanto o snapshot anda, e não só na memória: o
	// checkpoint da saída mascararia a gravação que faltasse antes do reset.
	var target msg
	for _, m := range pending {
		if m.op() == "reset" && m.str("reason") == "requested" {
			target = m
		}
	}
	c, ok, err := replicationkafka.LoadCheckpoint(h.checkpointPath())
	if err != nil || !ok || !c.Snapshotting || c.TargetEpoch != target.str("epoch") || c.TargetCursor != target.num("cursor") {
		t.Fatalf("o .ckpt no disco durante o snapshot não marca o reset em curso (epoch %q cursor %d): %+v ok=%v err=%v",
			target.str("epoch"), target.num("cursor"), c, ok, err)
	}
	h.stop(true)
	before := h.readAll()
	if n := controlCount(before, "reset"); n[0] != 2 || n[1] != 2 || n[2] != 2 {
		t.Fatalf("a primeira vida publicou %v resets, quer 2 por partição", n)
	}
	h.open()
	h.pub.Start()
	// Pára também no primeiro resume, para a falha dizer que retomou em vez
	// de vencer o prazo.
	all := h.readUntil("reset da vida nova", func(all []msg) bool {
		return eachPartitionHas(3, "reset", 3)(all) || len(controlCount(all, "resume")) > 0
	})
	if n := controlCount(all, "resume"); len(n) != 0 {
		t.Fatalf("o reinício no meio do snapshot retomou o tail: resume em %v", n)
	}
	seen := map[int32]int{}
	for _, m := range all {
		if m.op() != "reset" {
			continue
		}
		if seen[m.partition]++; seen[m.partition] == 3 && m.str("reason") != "snapshot_failed" {
			t.Fatalf("reset depois do reinício no meio do snapshot com motivo %q, quer snapshot_failed", m.str("reason"))
		}
	}
	waitFor(t, "following", func() bool { return h.pub.Stats().State == replicationkafka.StateFollowing })
	if n := controlCount(h.readAll(), "resume"); len(n) != 0 {
		t.Fatalf("o reinício no meio do snapshot retomou o tail: resume em %v", n)
	}
}

// copyTree copia os arquivos regulares de src para dst (criado).
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, info.Mode())
	})
	if err != nil {
		t.Fatal(err)
	}
}

// backup copia o feed e o .ckpt (a pasta do dataset) para um diretório
// próprio; restore os traz de volta, apagando o que havia.
type backup struct{ feed, ckpt string }

func takeBackup(t *testing.T, h *harness) backup {
	t.Helper()
	b := backup{feed: filepath.Join(t.TempDir(), "feed"), ckpt: filepath.Join(t.TempDir(), "ckpt")}
	copyTree(t, h.feedDir, b.feed)
	copyTree(t, h.ckptDir, b.ckpt)
	return b
}

func (b backup) restore(t *testing.T, h *harness) {
	t.Helper()
	for _, d := range []string{h.feedDir, h.ckptDir} {
		if err := os.RemoveAll(d); err != nil {
			t.Fatal(err)
		}
	}
	copyTree(t, b.feed, h.feedDir)
	copyTree(t, b.ckpt, h.ckptDir)
}

// rewoundHarness monta o backup restaurado: a primeira vida entrega A e B e
// fecha limpa; o backup é tirado; a segunda vida retoma e publica mais
// mudanças, que o tópico recebe; o backup é restaurado. Devolve a epoch da
// primeira vida e a última sequence que o tópico recebeu.
func rewoundHarness(t *testing.T, o harnessOpts) (*harness, string, uint64) {
	t.Helper()
	h, _ := liveHarness(t, o)
	epoch := h.pub.Stats().Epoch
	h.stop(true)
	b := takeBackup(t, h)
	h.open()
	h.pub.Start()
	expectResume(t, h)
	var top uint64
	for i := range 6 {
		top = h.upsert("later-"+strconv.Itoa(i), int64(i))
	}
	h.feed.Flush(time.Now(), "test")
	h.readUntil("mudanças depois do backup", hasSequence(top))
	h.stop(true)
	b.restore(t, h)
	return h, epoch, top
}

// assertRewind confere a vida nova depois do rewind: reset com rewind numa
// epoch NOVA, previousEpoch com a epoch da geração restaurada, e o snapshot
// inteiro na epoch nova.
func assertRewind(t *testing.T, h *harness, old string) {
	t.Helper()
	resets := secondGeneration(t, h)
	r := resets[0]
	if r.str("reason") != "rewind" {
		t.Fatalf("motivo do reset = %q, quer rewind", r.str("reason"))
	}
	if r.str("epoch") == old || r.str("epoch") == "" {
		t.Fatalf("o rewind publicou o reset na epoch antiga %q", r.str("epoch"))
	}
	if r.str("previousEpoch") != old {
		t.Fatalf("previousEpoch = %q, quer %q", r.str("previousEpoch"), old)
	}
	if epoch, _ := h.feed.Cursor(); epoch != r.str("epoch") {
		t.Fatalf("o feed está na epoch %q e o reset na %q", epoch, r.str("epoch"))
	}
	for _, m := range h.readAll() {
		if m.op() == "snapshot_end" && m.offset > r.offset && m.partition == r.partition && m.str("epoch") != r.str("epoch") {
			t.Fatalf("snapshot_end do rewind na epoch %q, quer %q", m.str("epoch"), r.str("epoch"))
		}
		if m.op() == "resume" && m.str("epoch") == old && m.offset > r.offset {
			t.Fatalf("resume na epoch antiga depois do rewind")
		}
	}
}

// Backup restaurado: o feed e o .ckpt voltam coerentes, mas no passado. O
// topo do tópico tem a epoch do feed com sequence acima da retomada, e a
// vida nova não retoma: rewind, epoch nova e previousEpoch preenchido.
func TestResumeDetectsRewoundFeedFromTopicTail(t *testing.T) {
	h, old, _ := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	h.open()
	h.pub.Start()
	assertRewind(t, h, old)
	if s := h.pub.Stats(); s.Snapshots["rewind"] != 1 || s.LastResetReason != "rewind" {
		t.Fatalf("stats do rewind: %+v", s)
	}
}

// A conferência compara com a sequence da RETOMADA, e não com a produzida
// corrente: com a leitura do tópico falhando na subida, o feed restaurado
// recebe mudanças além do topo antigo do tópico; quando a leitura volta, o
// resultado continua sendo rewind.
func TestRewindDetectedEvenWhenFeedAdvancesBeforeTopicCheck(t *testing.T) {
	h, old, top := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	var failing atomic.Bool
	failing.Store(true)
	h.o.publisher = func(o *replicationkafka.Options) {
		replicationkafka.WithTopicReadHook(o, func() error {
			if failing.Load() {
				return errors.New("injected topic read failure")
			}
			return nil
		})
	}
	h.open()
	h.pub.Start()
	waitFor(t, "leitura do tópico falhando", func() bool { return h.pub.Stats().Errors["topic_read"] >= 1 })
	_, resumed := h.feed.Cursor()
	var produced uint64
	for i := 0; produced <= top; i++ {
		produced = h.upsert("ingested-"+strconv.Itoa(i), int64(i))
	}
	h.feed.Flush(time.Now(), "test")
	if produced <= top || resumed >= top {
		t.Fatalf("o feed não passou do topo antigo (%d ≤ %d) ou já começou acima dele (%d); o teste não prova nada", produced, top, resumed)
	}
	failing.Store(false)
	assertRewind(t, h, old)
}

// O topo do tópico com uma epoch que o .ckpt não conhece é publicação nossa
// posterior ao backup (o tópico é exclusivo do dataset): rewind, mesmo com a
// posição dele abaixo da retomada.
func TestRewindWhenTopicTailHasForeignEpoch(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	old := h.pub.Stats().Epoch
	h.stop(true)
	b := takeBackup(t, h)
	// Depois do backup, a vida seguinte retoma e cai; a outra publica numa
	// geração nova, e o topo do tópico fica nela.
	h.open()
	h.pub.Start()
	expectResume(t, h)
	h.stop(false)
	h.open()
	h.pub.Start()
	if r := secondGeneration(t, h)[0]; r.str("reason") != "crash" || r.str("epoch") == old {
		t.Fatalf("a vida depois da queda publicou %q na epoch %q", r.str("reason"), r.str("epoch"))
	}
	h.stop(true)
	b.restore(t, h)
	h.open()
	h.pub.Start()
	all := h.readUntil("terceiro snapshot", snapshotEnds(3, 3))
	var third []msg
	seen := map[int32]int{}
	for _, m := range all {
		if m.op() == "reset" {
			seen[m.partition]++
			if seen[m.partition] == 3 {
				third = append(third, m)
			}
		}
	}
	if len(third) != 3 || third[0].str("reason") != "rewind" || third[0].str("previousEpoch") != old || third[0].str("epoch") == old {
		t.Fatalf("terceira vida sem o rewind da epoch estranha:\n%s", dump(third))
	}
}

// O backup restaurado com um alias mudado cai em DUAS linhas da decisão: a
// conferência de rewind e a impressão divergente. O rewind vem antes, e o
// snapshot sai na epoch NOVA: um reset config_changed na epoch antiga do
// feed restaurado colidiria com o que o tópico já tem.
func TestRewindPrecedesConfigChangedReset(t *testing.T) {
	h, old, _ := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	changed := replicationkafka.RenderHash(replicationkafka.RenderSpec{
		Tables:  []string{"events"},
		Aliases: []replicationkafka.FieldAlias{{Table: "events", Field: "severity", Alias: "sev2"}},
	})
	h.o.publisher = func(o *replicationkafka.Options) { o.RenderHash = changed }
	h.open()
	h.pub.Start()
	assertRewind(t, h, old)
	if s := h.pub.Stats(); s.Snapshots["config_changed"] != 0 {
		t.Fatalf("o rewind também contou config_changed: %+v", s.Snapshots)
	}
	c, _, err := replicationkafka.LoadCheckpoint(h.checkpointPath())
	if err != nil || c.RenderHash != changed {
		t.Fatalf("o .ckpt depois do snapshot não guardou a impressão nova: %v", err)
	}
}

// topicReadFails monta o gancho que falha a leitura do topo do tópico
// enquanto a chave estiver ligada.
func topicReadFails(failing *atomic.Bool) func(*replicationkafka.Options) {
	return func(o *replicationkafka.Options) {
		replicationkafka.WithTopicReadHook(o, func() error {
			if failing.Load() {
				return errors.New("injected topic read failure")
			}
			return nil
		})
	}
}

// Com a leitura do topo do tópico falhando, a subida limpa não retoma nem
// publica reset: fica em retrying, contando topic_read. Quando a leitura
// volta, retoma.
func TestTopicReadFailureNeverResumesBlindly(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	var failing atomic.Bool
	failing.Store(true)
	h.o.publisher = topicReadFails(&failing)
	h.open()
	h.pub.Start()
	waitFor(t, "três leituras falhas", func() bool { return h.pub.Stats().Errors["topic_read"] >= 3 })
	s := h.pub.Stats()
	if s.State != replicationkafka.StateRetrying || !strings.Contains(s.LastError, "topic_read") {
		t.Fatalf("stats com a leitura falhando: %+v", s)
	}
	all := h.readAll()
	if n := controlCount(all, "resume"); len(n) != 0 {
		t.Fatalf("retomou às cegas com a leitura do tópico falhando: resume em %v", n)
	}
	for p, n := range controlCount(all, "reset") {
		if n != 1 {
			t.Fatalf("partição %d com %d resets: publicou reset às cegas", p, n)
		}
	}
	failing.Store(false)
	expectResume(t, h)
}

// assertRewindAfterTopicLoss confere a vida nova quando o topo do tópico
// perdeu registros (retenção ou tópico recriado): reset com rewind numa epoch
// NOVA, em todas as partições, com previousEpoch da geração anterior e o
// snapshot_end na mesma epoch. A contagem é por epoch, e não por posição no
// tópico, porque a partição esvaziada não tem mais os resets antigos.
func assertRewindAfterTopicLoss(t *testing.T, h *harness, old string) {
	t.Helper()
	var epoch string
	all := h.readUntil("rewind em todas as partições", func(all []msg) bool {
		resets, ends := map[int32]bool{}, map[int32]bool{}
		for _, m := range all {
			switch {
			case m.op() == "resume":
				// Pára no resume, para a falha dizer que retomou em vez de
				// vencer o prazo.
				return true
			case m.op() == "reset" && m.str("epoch") != old:
				resets[m.partition] = true
				epoch = m.str("epoch")
			case m.op() == "snapshot_end" && m.str("epoch") != old && m.str("epoch") == epoch:
				ends[m.partition] = true
			}
		}
		return len(resets) == 3 && len(ends) == 3
	})
	for _, m := range all {
		if m.op() == "reset" && m.str("epoch") == epoch {
			if m.str("reason") != "rewind" || m.reasonHeader != "rewind" || m.str("previousEpoch") != old {
				t.Fatalf("reset da vida nova: motivo %q header %q previousEpoch %q, quer rewind/rewind/%q",
					m.str("reason"), m.reasonHeader, m.str("previousEpoch"), old)
			}
		}
		if m.op() == "resume" {
			t.Fatalf("a vida nova retomou o tail sem poder conferir o topo do tópico: %v", m.env)
		}
	}
	if cur, _ := h.feed.Cursor(); cur != epoch || epoch == old {
		t.Fatalf("o feed está na epoch %q e o reset na %q (antiga %q)", cur, epoch, old)
	}
}

// A partição esvaziada pela retenção (início = fim > 0) não tem registro
// legível: a conferência não prova que o feed não voltou no tempo, e por isso
// não retoma nem fica retentando. A subida rotaciona o feed e publica reset
// com rewind.
func TestEmptiedPartitionForcesRewind(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	old := h.pub.Stats().Epoch
	h.stop(true)
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ends, err := adm.ListEndOffsets(ctx, pubTopic)
	if err != nil {
		t.Fatal(err)
	}
	end, _ := ends.Lookup(pubTopic, 0)
	if end.Offset <= 0 {
		t.Fatalf("a partição 0 está vazia (%d); o teste não prova nada", end.Offset)
	}
	if _, err := adm.DeleteRecords(ctx, kadm.Offsets{pubTopic: {0: {Topic: pubTopic, Partition: 0, At: end.Offset}}}); err != nil {
		t.Fatal(err)
	}
	starts, err := adm.ListStartOffsets(ctx, pubTopic)
	if start, _ := starts.Lookup(pubTopic, 0); err != nil || start.Offset != end.Offset {
		t.Fatalf("a partição 0 não ficou esvaziada: início %d, fim %d, %v", start.Offset, end.Offset, err)
	}
	h.open()
	h.pub.Start()
	assertRewindAfterTopicLoss(t, h, old)
	if n := h.pub.Stats().Errors["topic_read"]; n != 0 {
		t.Fatalf("a partição esvaziada contou %d falhas de leitura: é ausência comprovada, e não infraestrutura", n)
	}
}

// O tópico recriado vazio (fim = 0 em toda partição), diante de um .ckpt
// que diz que o destino já recebeu registros, também não pode ser conferido:
// rewind, e não a retomada num tópico que não tem o estado.
func TestRecreatedEmptyTopicWithCheckpointForcesRewind(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	old := h.pub.Stats().Epoch
	h.stop(true)
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := adm.DeleteTopic(ctx, pubTopic); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "tópico apagado", func() bool {
		d, err := adm.ListTopics(ctx, pubTopic)
		return err == nil && !d.Has(pubTopic)
	})
	if _, err := adm.CreateTopic(ctx, 3, 1, nil, pubTopic); err != nil {
		t.Fatal(err)
	}
	ends, err := adm.ListEndOffsets(ctx, pubTopic)
	if err != nil {
		t.Fatal(err)
	}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset != 0 {
			t.Fatalf("o tópico recriado não está vazio: partição %d com fim %d", o.Partition, o.Offset)
		}
	})
	h.open()
	h.pub.Start()
	assertRewindAfterTopicLoss(t, h, old)
}

// Broker fora na subida é falha de infraestrutura, e não ausência de
// registro: o publicador fica em retrying contando topic_read, e não retoma
// nem publica reset (nem o rewind) enquanto o broker não volta.
func TestUnreachableBrokerKeepsStartupRetryingWithoutResuming(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	before, err := adm.ListEndOffsets(ctx, pubTopic)
	if err != nil {
		t.Fatal(err)
	}
	// O destino aponta para um broker inalcançável; o .ckpt, o tópico real e
	// o feed continuam os da primeira vida.
	h.o.target = func(k *config.KafkaTargetSpec) {
		k.BootstrapServers = []string{"127.0.0.1:1"}
		k.Timeout = "1s"
	}
	h.open()
	h.pub.Start()
	waitFor(t, "duas leituras falhas", func() bool { return h.pub.Stats().Errors["topic_read"] >= 2 })
	if s := h.pub.Stats(); s.State != replicationkafka.StateRetrying || !strings.Contains(s.LastError, "topic_read") {
		t.Fatalf("stats com o broker fora: %+v", s)
	}
	if o := h.feed.Origin(); o.Kind != replication.OriginClean {
		t.Fatalf("o feed saiu da geração retomada (%v) com o broker fora: o rewind não pode vir de falha de infraestrutura", o.Kind)
	}
	after, err := adm.ListEndOffsets(ctx, pubTopic)
	if err != nil {
		t.Fatal(err)
	}
	before.Each(func(o kadm.ListedOffset) {
		if a, _ := after.Lookup(pubTopic, o.Partition); a.Offset != o.Offset {
			t.Errorf("partição %d recebeu registros com o broker fora (fim %d → %d)", o.Partition, o.Offset, a.Offset)
		}
	})
}

// O pedido de snapshot registrado antes de um reinício sobrevive a ele: a
// vida nova publica reset com requested. Atendido, ele é consumido, e a vida
// seguinte retoma.
func TestRequestedSnapshotSurvivesRestart(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	// O publicador parado só registra o pedido: o reinício acontece antes do
	// snapshot.
	if err := h.pub.RequestSnapshot(); err != nil {
		t.Fatal(err)
	}
	h.open()
	h.pub.Start()
	if r := secondGeneration(t, h)[0]; r.str("reason") != "requested" {
		t.Fatalf("motivo do reset = %q, quer requested", r.str("reason"))
	}
	waitFor(t, "following depois do pedido", func() bool { return h.pub.Stats().State == replicationkafka.StateFollowing })
	h.restart(true)
	h.readUntil("resume depois do pedido atendido", eachPartitionHas(3, "resume", 1))
	if all := h.readAll(); len(controlCount(all, "reset")) != 3 || controlCount(all, "reset")[0] != 2 {
		t.Fatalf("o pedido atendido gerou outro reset:\n%s", dump(all))
	}
}

// clock é um relógio ajustável, seguro entre goroutines.
type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time  { return time.Unix(0, c.ns.Load()) }
func (c *clock) set(t time.Time) { c.ns.Store(t.UnixNano()) }

// O snapshot periódico conta a partir do último snapshot_end GRAVADO no
// .ckpt, e por isso atravessa o reinício: a vida que sobe antes do prazo
// retoma, a que sobe depois dele publica reset com periodic. Em operação, a
// conferência acontece na cadência da conferência de partições.
func TestPeriodicSnapshotUsesPersistedLastSnapshotEnd(t *testing.T) {
	var clk clock
	t0 := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	clk.set(t0)
	periodic := func(o *replicationkafka.Options) {
		o.PeriodicSnapshot = time.Hour
		o.Now = clk.now
		o.PartitionCheckInterval = 50 * time.Millisecond
	}
	h, _ := liveHarness(t, harnessOpts{partitions: 3, publisher: periodic})
	if s := h.pub.Stats(); s.LastSnapshotAt != float64(t0.UnixMilli())/1000 {
		t.Fatalf("LastSnapshotAt = %v, quer %v", s.LastSnapshotAt, float64(t0.UnixMilli())/1000)
	}
	clk.set(t0.Add(30 * time.Minute))
	h.restart(true)
	h.readUntil("resume antes do prazo", eachPartitionHas(3, "resume", 1))
	// A vida que retomou herda o prazo do .ckpt: a 61 min do snapshot, e não
	// da própria subida, ela publica o periódico.
	clk.set(t0.Add(61 * time.Minute))
	if r := secondGeneration(t, h)[0]; r.str("reason") != "periodic" {
		t.Fatalf("motivo do reset em operação = %q, quer periodic", r.str("reason"))
	}
	waitFor(t, "following depois do periódico", func() bool { return h.pub.Stats().State == replicationkafka.StateFollowing })
	// O prazo recomeça do snapshot_end do periódico (61 min): o reinício a
	// 90 min retoma, o de 122 min publica outro.
	clk.set(t0.Add(90 * time.Minute))
	h.o.publisher = func(o *replicationkafka.Options) {
		periodic(o)
		o.PartitionCheckInterval = time.Hour
	}
	h.restart(true)
	h.readUntil("resume dentro do novo prazo", eachPartitionHas(3, "resume", 2))
	clk.set(t0.Add(122 * time.Minute))
	h.restart(true)
	all := h.readUntil("terceiro snapshot", snapshotEnds(3, 3))
	var reasons []string
	for _, m := range all {
		if m.op() == "reset" && m.partition == 0 {
			reasons = append(reasons, m.str("reason"))
		}
	}
	if strings.Join(reasons, ",") != "startup,periodic,periodic" {
		t.Fatalf("motivos dos resets = %v, quer startup,periodic,periodic", reasons)
	}
}

// A falha de gravação do .ckpt no tail é contada e o publicador segue
// entregando; o checkpoint alcança o cursor quando a gravação volta. Com o
// disco falhando, o tail tenta no máximo uma gravação por segundo, e não uma
// por lote. Antes do reset, a mesma falha impede o snapshot de começar:
// retrying, sem reset no tópico, até a gravação voltar. Cada tentativa falha
// conta UMA vez em checkpoint_write.
func TestCheckpointWriteFailureIsCountedAndPublisherContinues(t *testing.T) {
	var broken atomic.Bool
	var failed atomic.Int64
	h, last := liveHarness(t, harnessOpts{partitions: 3, publisher: func(o *replicationkafka.Options) {
		replicationkafka.WithCheckpointFault(o, func() error {
			if broken.Load() {
				failed.Add(1)
				return errors.New("injected checkpoint failure")
			}
			return nil
		})
	}})
	waitFor(t, "checkpoint no cursor", func() bool {
		// A gravação do tail é no máximo uma por segundo: o próximo lote
		// leva o cursor ao disco.
		h.upsert("tick", 0)
		h.feed.Flush(time.Now(), "test")
		time.Sleep(50 * time.Millisecond)
		return h.pub.Stats().CheckpointCursor > last
	})
	errs := h.pub.Stats().Errors["checkpoint_write"]
	broken.Store(true)
	time.Sleep(1100 * time.Millisecond)
	// Cinco lotes confirmados em bem menos de um segundo: uma tentativa só.
	started := time.Now()
	var seq uint64
	for i := range 5 {
		seq = h.upsert("burst-"+strconv.Itoa(i), int64(i))
		h.feed.Flush(time.Now(), "test")
		waitFor(t, "lote confirmado", func() bool { return h.pub.Stats().Cursor == seq })
	}
	if elapsed := time.Since(started); elapsed >= 900*time.Millisecond {
		t.Fatalf("os cinco lotes levaram %v; o teste não prova a cadência", elapsed)
	}
	if n := failed.Load(); n != 1 {
		t.Fatalf("com o disco falhando, o tail tentou %d gravações em cinco lotes, quer 1 (uma por segundo)", n)
	}
	s := h.pub.Stats()
	if s.State != replicationkafka.StateFollowing || s.Cursor != seq || s.CheckpointCursor >= seq ||
		s.Errors["checkpoint_write"] != errs+1 {
		t.Fatalf("com o .ckpt quebrado (1 tentativa falha): %+v", s)
	}
	broken.Store(false)
	time.Sleep(1100 * time.Millisecond)
	seq = h.upsert("D", 4)
	h.feed.Flush(time.Now(), "test")
	waitFor(t, "checkpoint de volta", func() bool { return h.pub.Stats().CheckpointCursor == seq })
	if c, _, err := replicationkafka.LoadCheckpoint(h.checkpointPath()); err != nil || c.Cursor != seq {
		t.Fatalf("o .ckpt no disco tem cursor %d, quer %d (%v)", c.Cursor, seq, err)
	}

	// Antes do reset: o snapshot pedido não começa com o .ckpt quebrado.
	failed.Store(0)
	errs = h.pub.Stats().Errors["checkpoint_write"]
	broken.Store(true)
	if err := h.pub.RequestSnapshot(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "snapshot travado na gravação", func() bool {
		return failed.Load() >= 2 && h.pub.Stats().State == replicationkafka.StateRetrying
	})
	if n := controlCount(h.readAll(), "reset"); n[0] != 1 {
		t.Fatalf("o reset saiu sem o .ckpt marcar o snapshot em curso: %v", n)
	}
	broken.Store(false)
	if r := secondGeneration(t, h)[0]; r.str("reason") != "requested" {
		t.Fatalf("motivo = %q, quer requested", r.str("reason"))
	}
	if got, want := h.pub.Stats().Errors["checkpoint_write"]-errs, uint64(failed.Load()); got != want {
		t.Fatalf("checkpoint_write subiu %d com %d tentativas falhas; quer uma contagem por tentativa", got, want)
	}
}

// previousEpoch só sai quando a epoch muda: o primeiro reset de um destino
// não tem geração anterior, o de lacuna é da MESMA geração, e o da queda
// traz a epoch que ele substitui.
func TestResetCarriesPreviousEpochOnlyWhenEpochChanges(t *testing.T) {
	var h *harness
	var once sync.Once
	h = newHarness(t, harnessOpts{partitions: 3, publisher: func(o *replicationkafka.Options) {
		replicationkafka.WithAfterSnapshot(o, func() {
			once.Do(func() {
				h.upsert("late", 1)
				h.feed.Invalidate()
			})
		})
	}})
	h.pub.Start()
	all := h.readUntil("snapshot de lacuna", snapshotEnds(3, 2))
	epoch := ""
	for _, m := range all {
		if m.op() != "reset" {
			continue
		}
		if _, has := m.env["previousEpoch"]; has {
			t.Fatalf("reset %q com previousEpoch na mesma geração (ou sem anterior): %v", m.str("reason"), m.env)
		}
		epoch = m.str("epoch")
	}
	waitFor(t, "following", func() bool { return h.pub.Stats().State == replicationkafka.StateFollowing })
	h.o.publisher = nil
	h.restart(false)
	all = h.readUntil("snapshot da queda", snapshotEnds(3, 3))
	var crash []msg
	for _, m := range all {
		if m.op() == "reset" && m.str("reason") == "crash" {
			crash = append(crash, m)
		}
	}
	if len(crash) != 3 {
		t.Fatalf("resets crash em %d partições, quer 3:\n%s", len(crash), dump(all))
	}
	for _, m := range crash {
		if m.str("previousEpoch") != epoch || m.str("epoch") == epoch {
			t.Fatalf("reset crash com previousEpoch %q na epoch %q, quer previousEpoch %q", m.str("previousEpoch"), m.str("epoch"), epoch)
		}
	}
}

// Acrescentar um broker à lista não troca o cluster: com um broker em comum
// com o .ckpt, a subida retoma em vez de publicar startup.
func TestBootstrapServerAddedDoesNotForceStartup(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	h.o.target = func(k *config.KafkaTargetSpec) {
		k.BootstrapServers = append(append([]string(nil), k.BootstrapServers...), "127.0.0.1:1")
	}
	h.open()
	h.pub.Start()
	expectResume(t, h)
}

// O atraso em bytes é a posição do feed menos a do cursor confirmado. Ele
// cresce com o lote fechado que o broker ainda não confirmou, vai a zero
// quando o feed muda de geração (a posição recomeça do zero, e comparar as
// duas não mede nada) e nunca dá a volta quando a posição do cursor está à
// frente da do feed.
func TestLagBytesTracksFeedPositionAndClampsAfterRotate(t *testing.T) {
	var prod *switchProducer
	h, _ := liveHarness(t, harnessOpts{partitions: 3, feed: manualFeed, publisher: func(o *replicationkafka.Options) {
		replicationkafka.WithProducerWrapper(o, func(p replicationkafka.Producer) replicationkafka.Producer {
			prod = &switchProducer{Producer: p}
			return prod
		})
	}})
	confirmed := h.feed.Status().EndPos
	if confirmed == 0 {
		t.Fatal("o tail não confirmou lote nenhum; o teste não prova nada")
	}
	if s := h.pub.Stats(); s.LagBytes != 0 {
		t.Fatalf("LagBytes = %d com tudo confirmado", s.LagBytes)
	}
	prod.set(func(r *kgo.Record) bool { op := opOf(r); return op == "insert" || op == "update" })
	h.upsert("held", 1)
	h.feed.Flush(time.Now(), "test")
	// O lote está na reprodução, preso: daqui em diante o publicador não
	// volta ao Acquire, e não vê a rotação.
	waitFor(t, "lote preso na reprodução", func() bool { return h.pub.Stats().Errors["publish_error"] >= 1 })
	if got, want := h.pub.Stats().LagBytes, h.feed.Status().EndPos-confirmed; got != want || want == 0 {
		t.Fatalf("LagBytes = %d, quer %d (posição do feed menos a do cursor)", got, want)
	}
	// O cursor continua na geração antiga.
	h.feed.Rotate("test")
	if got := h.pub.Stats().LagBytes; got != 0 {
		t.Fatalf("LagBytes = %d depois da rotação, quer 0", got)
	}
	for i := 0; h.feed.Status().EndPos <= confirmed; i++ {
		h.upsert("next-"+strconv.Itoa(i), 1)
		h.feed.Flush(time.Now(), "test")
	}
	if got := h.pub.Stats().LagBytes; got != 0 {
		t.Fatalf("LagBytes = %d com a posição da geração nova acima da do cursor da antiga, quer 0", got)
	}
}

// O .ckpt com a posição do cursor À FRENTE da do feed (um .ckpt mais novo que
// o `state`) não dá a volta no atraso em bytes: ele sai zero.
func TestLagBytesNeverWrapsWhenCheckpointPositionIsAhead(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	rewriteCheckpoint(t, h, func(c *replicationkafka.CheckpointFile) { c.CursorPos += 1 << 20 })
	h.open()
	h.pub.Start()
	expectResume(t, h)
	if got := h.pub.Stats().LagBytes; got != 0 {
		t.Fatalf("LagBytes = %d com a posição do cursor à frente da do feed, quer 0", got)
	}
}

// O RenderHash é formato de disco (vai no .ckpt): o literal foi calculado
// FORA do código, com printf da codificação documentada no godoc e
// sha256sum. A ordem das tabelas e dos aliases na entrada não muda o hash.
func TestRenderHashGoldenBytes(t *testing.T) {
	const golden = "d15a27e5b0ac9ace75af620e71e0da8f42344ee9ba8bd6ad9494d9f45aa2c180"
	spec := replicationkafka.RenderSpec{
		Tables:         []string{"events_history", "events"},
		Aliases:        []replicationkafka.FieldAlias{{Table: "events", Field: "severity", Alias: "sev"}},
		PublishHistory: true,
	}
	got := replicationkafka.RenderHash(spec)
	if hex.EncodeToString(got[:]) != golden {
		t.Fatalf("RenderHash = %x, quer %s", got, golden)
	}
	spec.Tables = []string{"events", "events_history"}
	if replicationkafka.RenderHash(spec) != got {
		t.Fatal("a ordem das tabelas mudou o hash")
	}
}

// Cada componente declarado entra no hash: trocar um alias, tirar a
// histórica ou desligar publishHistory muda a impressão.
func TestRenderHashChangesWithEveryDeclaredComponent(t *testing.T) {
	base := replicationkafka.RenderSpec{
		Tables: []string{"events", "events_history"},
		Aliases: []replicationkafka.FieldAlias{
			{Table: "events", Field: "severity", Alias: "sev"},
			{Table: "events_history", Field: "severity", Alias: "sev"},
		},
		PublishHistory: true,
	}
	ref := replicationkafka.RenderHash(base)
	swapped := base
	swapped.Aliases = []replicationkafka.FieldAlias{base.Aliases[1], base.Aliases[0]}
	if replicationkafka.RenderHash(swapped) != ref {
		t.Fatal("a ordem dos aliases mudou o hash")
	}
	for name, spec := range map[string]replicationkafka.RenderSpec{
		"alias trocado":         {Tables: base.Tables, Aliases: []replicationkafka.FieldAlias{{Table: "events", Field: "severity", Alias: "level"}, base.Aliases[1]}, PublishHistory: true},
		"alias de outro campo":  {Tables: base.Tables, Aliases: []replicationkafka.FieldAlias{{Table: "events", Field: "status", Alias: "sev"}, base.Aliases[1]}, PublishHistory: true},
		"alias de outra tabela": {Tables: base.Tables, Aliases: []replicationkafka.FieldAlias{base.Aliases[0], {Table: "events", Field: "severity", Alias: "sev"}}, PublishHistory: true},
		"sem a histórica":       {Tables: []string{"events"}, Aliases: base.Aliases[:1]},
		"publishHistory":        {Tables: base.Tables, Aliases: base.Aliases},
	} {
		if replicationkafka.RenderHash(spec) == ref {
			t.Errorf("%s não mudou o hash", name)
		}
	}
}

// A rotação do feed em operação (o rewind de outro destino do mesmo dataset)
// muda a epoch debaixo do tail: o ErrResyncRequired que segue não é lacuna
// na mesma geração, é a linha 5 da decisão, e o reset sai com rewind.
func TestRotationInOperationIsRewindNotGap(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.feed.Rotate("test")
	if r := secondGeneration(t, h)[0]; r.str("reason") != "rewind" {
		t.Fatalf("motivo do reset depois da rotação em operação = %q, quer rewind", r.str("reason"))
	}
	if s := h.pub.Stats(); s.Snapshots["gap"] != 0 {
		t.Fatalf("a rotação contou como lacuna: %+v", s.Snapshots)
	}
}

// O .resync ilegível não é pedido de snapshot: tratá-lo como pedido
// publicaria um snapshot a cada conferência, porque o contador ilegível nunca
// é consumido. A subida retoma, com o WARN que nomeia o arquivo.
func TestUnreadableResyncCounterIsNotARequest(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(true)
	if err := os.WriteFile(filepath.Join(h.ckptDir, "alarms_cdc.resync"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.open()
	h.pub.Start()
	expectResume(t, h)
	if !strings.Contains(h.logs.String(), `"replication.kafka.resync.unreadable"`) {
		t.Fatalf("sem o WARN do .resync ilegível:\n%s", h.logs.String())
	}
}

// O .ckpt ilegível vale como destino novo, com WARN: a subida segue pela
// linha "sem .ckpt" (startup), e nunca retoma de um ponto que não consegue
// ler.
func TestUnreadableCheckpointIsTreatedAsNewDestination(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.stop(false)
	if err := os.WriteFile(h.checkpointPath(), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.open()
	h.pub.Start()
	r := secondGeneration(t, h)[0]
	if r.str("reason") != "startup" {
		t.Fatalf("motivo com o .ckpt ilegível = %q, quer startup", r.str("reason"))
	}
	if _, has := r.env["previousEpoch"]; has {
		t.Fatalf("o .ckpt ilegível não diz a epoch anterior, e o reset trouxe previousEpoch: %v", r.env)
	}
	if !strings.Contains(h.logs.String(), `"replication.kafka.checkpoint.unreadable"`) {
		t.Fatalf("sem o WARN do .ckpt ilegível:\n%s", h.logs.String())
	}
}

// O .ckpt à frente do feed reaberto (só o feed voltou de um backup) é rewind
// SEM consultar o tópico: o ponto confirmado acima da sequence da retomada,
// ou o reset em curso com o cursor acima dela. A leitura do tópico fica
// falhando o tempo todo, e mesmo assim a decisão sai.
func TestCheckpointAheadOfFeedIsRewindWithoutReadingTheTopic(t *testing.T) {
	for name, edit := range map[string]func(c *replicationkafka.CheckpointFile, last uint64){
		"ponto confirmado": func(c *replicationkafka.CheckpointFile, last uint64) { c.Cursor = last + 5 },
		"reset em curso": func(c *replicationkafka.CheckpointFile, last uint64) {
			c.Snapshotting, c.TargetEpoch, c.TargetCursor = true, c.Epoch, last+5
		},
	} {
		t.Run(name, func(t *testing.T) {
			h, last := liveHarness(t, harnessOpts{partitions: 3})
			old := h.pub.Stats().Epoch
			h.stop(true)
			rewriteCheckpoint(t, h, func(c *replicationkafka.CheckpointFile) { edit(c, last) })
			var failing atomic.Bool
			failing.Store(true)
			h.o.publisher = topicReadFails(&failing)
			h.open()
			h.pub.Start()
			assertRewind(t, h, old)
			if n := h.pub.Stats().Errors["topic_read"]; n != 0 {
				t.Fatalf("a decisão leu o tópico (%d falhas): as regras do .ckpt vêm antes", n)
			}
		})
	}
}

// O rewind decidido com o feed já fechado (o encerramento chegou enquanto a
// leitura do tópico falhava) não pode rotacionar o feed, e por isso não sai:
// um reset na epoch antiga deixaria a vida seguinte reusar (epoch, sequence)
// já entregues. A subida sai sem publicar nada, e a vida seguinte refaz a
// conferência e publica o rewind numa epoch nova.
func TestRewindWithFeedClosedDuringTheCheckNeverReusesTheOldEpoch(t *testing.T) {
	h, old, _ := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	var failing atomic.Bool
	failing.Store(true)
	h.o.publisher = topicReadFails(&failing)
	h.open()
	h.pub.Start()
	waitFor(t, "leitura falhando", func() bool { return h.pub.Stats().Errors["topic_read"] >= 1 })
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	before, err := adm.ListEndOffsets(ctx, pubTopic)
	if err != nil {
		t.Fatal(err)
	}
	// O encerramento: o feed fecha antes do publicador, como no servidor.
	if err := h.feed.Close(&replication.Fingerprint{}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	failing.Store(false)
	waitFor(t, "publicador parado sem decidir", func() bool { return h.pub.Stats().State == replicationkafka.StateStopped })
	if epoch, _ := h.feed.Cursor(); epoch != old {
		t.Fatalf("o feed fechado mudou de epoch: %s → %s", old, epoch)
	}
	after, err := adm.ListEndOffsets(ctx, pubTopic)
	if err != nil {
		t.Fatal(err)
	}
	before.Each(func(o kadm.ListedOffset) {
		if a, _ := after.Lookup(pubTopic, o.Partition); a.Offset != o.Offset {
			t.Errorf("partição %d recebeu registros com o feed fechado (fim %d → %d): reset na epoch antiga",
				o.Partition, o.Offset, a.Offset)
		}
	})
	if !strings.Contains(h.logs.String(), `"replication.kafka.closed_before_decision"`) {
		t.Fatalf("sem o INFO da subida abandonada:\n%s", h.logs.String())
	}
	h.o.publisher = nil
	h.stop(true)
	h.open()
	h.pub.Start()
	assertRewind(t, h, old)
}

// O destino trocado no YAML para um tópico VAZIO é destino novo: o .ckpt é
// de outro tópico e conta como ausente na conferência, a partição nunca
// escrita é pulada, e a linha 1 da tabela dá startup, sem rotacionar o feed.
func TestTopicChangedToAnEmptyTopicIsStartupWithoutRotation(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	old := h.pub.Stats().Epoch
	h.stop(true)
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(context.Background(), 3, 1, nil, "other.topic"); err != nil {
		t.Fatal(err)
	}
	h.o.target = func(k *config.KafkaTargetSpec) { k.Topic = "other.topic" }
	h.open()
	h.pub.Start()
	waitFor(t, "reset no tópico novo", func() bool { return h.pub.Stats().LastResetReason != "" })
	if r := h.pub.Stats().LastResetReason; r != "startup" {
		t.Fatalf("tópico trocado para um vazio deu %q, quer startup", r)
	}
	if o := h.feed.Origin(); o.Kind != replication.OriginClean {
		t.Fatalf("o feed foi rotacionado (%v): destino novo não é rewind", o.Kind)
	}
	if epoch, _ := h.feed.Cursor(); epoch != old {
		t.Fatalf("a epoch mudou: %s → %s", old, epoch)
	}
}

// A partição acrescentada ao tópico com o master parado nunca recebeu
// registro nosso: ela não é "partição sem registro legível" (o rewind), e a
// subida publica partitions_changed sem rotacionar o feed.
func TestPartitionAddedWhileStoppedIsPartitionsChangedWithoutRotation(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	old := h.pub.Stats().Epoch
	h.stop(true)
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreatePartitions(context.Background(), 1, pubTopic); err != nil {
		t.Fatal(err)
	}
	h.open()
	h.pub.Start()
	waitFor(t, "reset depois da partição nova", func() bool { return h.pub.Stats().LastResetReason != "" })
	if r := h.pub.Stats().LastResetReason; r != "partitions_changed" {
		t.Fatalf("partição acrescentada com o master parado deu %q, quer partitions_changed", r)
	}
	if o := h.feed.Origin(); o.Kind != replication.OriginClean {
		t.Fatalf("o feed foi rotacionado (%v)", o.Kind)
	}
	if epoch, _ := h.feed.Cursor(); epoch != old {
		t.Fatalf("a epoch mudou: %s → %s", old, epoch)
	}
}

// O previousEpoch é a última epoch cujo reset já saiu NESTE destino. Um
// snapshot que falhou no meio numa epoch intermediária já publicou o reset
// dela: a vida seguinte aponta para ela, e não para a do último snapshot
// concluído.
func TestPreviousEpochIsTheLastEpochWhoseResetReachedTheDestination(t *testing.T) {
	var holdEnds atomic.Bool
	h, _ := liveHarness(t, harnessOpts{partitions: 3, publisher: func(o *replicationkafka.Options) {
		replicationkafka.WithProducerWrapper(o, func(p replicationkafka.Producer) replicationkafka.Producer {
			sp := &switchProducer{Producer: p}
			sp.holdWhere(func(r *kgo.Record) bool { return holdEnds.Load() && opOf(r) == "snapshot_end" })
			return sp
		})
	}})
	completed := h.pub.Stats().Epoch
	// Vida 2: a queda abre a epoch intermediária, e o snapshot dela publica
	// o reset e nunca confirma o snapshot_end.
	holdEnds.Store(true)
	h.restart(false)
	all := h.readUntil("reset da epoch intermediária", eachPartitionHas(3, "reset", 2))
	var intermediate string
	for _, m := range all {
		if m.op() == "reset" && m.str("epoch") != completed {
			intermediate = m.str("epoch")
		}
	}
	if intermediate == "" {
		t.Fatal("sem o reset da epoch intermediária")
	}
	// Vida 3: outra queda; o .ckpt tem Epoch da concluída e TargetEpoch da
	// intermediária.
	// A trava só sai com a vida 2 já parada: soltá-la antes deixaria o
	// snapshot_end ainda não produzido passar, e o snapshot concluiria.
	h.stop(false)
	holdEnds.Store(false)
	h.open()
	h.pub.Start()
	all = h.readUntil("reset da terceira vida", eachPartitionHas(3, "reset", 3))
	seen := map[int32]int{}
	for _, m := range all {
		if m.op() != "reset" {
			continue
		}
		if seen[m.partition]++; seen[m.partition] == 3 {
			if m.str("reason") != "snapshot_failed" || m.str("previousEpoch") != intermediate {
				t.Fatalf("reset da terceira vida: motivo %q previousEpoch %q, quer snapshot_failed e %q (e não a concluída %q)",
					m.str("reason"), m.str("previousEpoch"), intermediate, completed)
			}
		}
	}
}

// Sem .ckpt (destino renomeado, .ckpt apagado à mão) e com o tópico já com
// registros, o feed reaberto limpo dá rewind, e não startup: sem checkpoint
// não dá para saber se a epoch do feed já apareceu no tópico e foi superada,
// e um reset nela seria descartado pelo consumidor como geração antiga.
func TestMissingCheckpointBeforeATopicWithRecordsIsRewind(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	old := h.pub.Stats().Epoch
	h.stop(true)
	if err := os.Remove(h.checkpointPath()); err != nil {
		t.Fatal(err)
	}
	h.open()
	h.pub.Start()
	r := secondGeneration(t, h)[0]
	if r.str("reason") != "rewind" || r.str("epoch") == old {
		t.Fatalf("sem .ckpt diante do tópico com registros: motivo %q na epoch %q (antiga %q), quer rewind numa epoch nova",
			r.str("reason"), r.str("epoch"), old)
	}
	// Sem .ckpt não há epoch publicada conhecida NESTE destino.
	if _, has := r.env["previousEpoch"]; has {
		t.Fatalf("rewind sem .ckpt com previousEpoch: %v", r.env)
	}
}

// neverDecidingLife sobe uma vida com a leitura do tópico falhando o tempo
// todo: ela não decide nada, ingere até passar de above e fecha limpo.
// Devolve a sequence em que o feed reabriu nela e a última produzida.
func neverDecidingLife(t *testing.T, h *harness, above uint64, prefix string) (resumed, produced uint64) {
	t.Helper()
	var failing atomic.Bool
	failing.Store(true)
	h.o.publisher = topicReadFails(&failing)
	h.open()
	resumed = h.feed.Origin().ResumedSequence
	h.pub.Start()
	waitFor(t, "leitura falhando", func() bool { return h.pub.Stats().Errors["topic_read"] >= 1 })
	for i := 0; produced <= above; i++ {
		produced = h.upsert(prefix+strconv.Itoa(i), int64(i))
	}
	h.feed.Flush(time.Now(), "test")
	h.stop(true)
	h.o.publisher = nil
	return resumed, produced
}

// A vida que reabre o feed restaurado, não consegue decidir (o tópico
// ilegível a vida inteira), ingere além do topo antigo e fecha limpo não
// apaga o rewind: a vida seguinte reabre com uma sequence MAIOR que o topo,
// mas a régua gravada no .ckpt é a da primeira reabertura, e o resultado é
// rewind, e não a retomada que reusaria (epoch, sequence) já entregues.
func TestRewindSurvivesALifeThatNeverDecided(t *testing.T) {
	h, old, top := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	_, produced := neverDecidingLife(t, h, top, "ingested-")
	h.open()
	if got := h.feed.Origin().ResumedSequence; got <= top || got != produced {
		t.Fatalf("a vida seguinte reabriu em %d (topo antigo %d, produzido %d); o teste não prova nada", got, top, produced)
	}
	h.pub.Start()
	assertRewind(t, h, old)
}

// Duas vidas seguidas sem decidir: a régua gravada é a da PRIMEIRA reabertura
// e não sobe com a segunda, que reabre mais adiante. A terceira, com o
// tópico legível, dá rewind.
func TestRewindRulerNeverRisesAcrossLivesThatNeverDecide(t *testing.T) {
	h, old, top := rewoundHarness(t, harnessOpts{partitions: 3, feed: manualFeed})
	first, produced := neverDecidingLife(t, h, top, "first-")
	c, _, err := replicationkafka.LoadCheckpoint(h.checkpointPath())
	if err != nil || c.RulerEpoch != old || c.RulerSequence != first {
		t.Fatalf("régua depois da primeira vida: %q/%d (%v), quer %q/%d", c.RulerEpoch, c.RulerSequence, err, old, first)
	}
	second, _ := neverDecidingLife(t, h, produced, "second-")
	if second <= first {
		t.Fatalf("a segunda vida reabriu em %d, não acima de %d; o teste não prova nada", second, first)
	}
	c, _, err = replicationkafka.LoadCheckpoint(h.checkpointPath())
	if err != nil || c.RulerEpoch != old || c.RulerSequence != first {
		t.Fatalf("a régua subiu na segunda vida: %q/%d (%v), quer %q/%d", c.RulerEpoch, c.RulerSequence, err, old, first)
	}
	h.open()
	h.pub.Start()
	assertRewind(t, h, old)
}

// A régua sai do .ckpt quando a decisão termina: depois da retomada (o
// resume enviado), o arquivo não guarda mais régua nenhuma, e a próxima
// reabertura confere com a sequence dela.
func TestRewindRulerIsClearedAfterASuccessfulResume(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	h.restart(true)
	expectResume(t, h)
	c, _, err := replicationkafka.LoadCheckpoint(h.checkpointPath())
	if err != nil || c.RulerEpoch != "" || c.RulerSequence != 0 {
		t.Fatalf("a régua ficou no .ckpt depois da retomada: %q/%d (%v)", c.RulerEpoch, c.RulerSequence, err)
	}
}

// O destino trocado (o .ckpt é de outro tópico) não tem epoch publicada
// neste destino: o reset dele sai sem previousEpoch, mesmo com o feed numa
// geração nova depois de queda.
func TestDestinationChangeResetCarriesNoPreviousEpoch(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	old := h.pub.Stats().Epoch
	h.stop(false)
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(context.Background(), 3, 1, nil, "other.topic"); err != nil {
		t.Fatal(err)
	}
	h.o.target = func(k *config.KafkaTargetSpec) { k.Topic = "other.topic" }
	h.open()
	h.pub.Start()
	waitFor(t, "reset no tópico novo", func() bool { return h.pub.Stats().LastResetReason != "" })
	if r := h.pub.Stats().LastResetReason; r != "startup" {
		t.Fatalf("destino trocado deu %q, quer startup", r)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(h.addrs...), kgo.ConsumeTopics("other.topic"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	var reset *msg
	for reset == nil {
		fs := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("sem o reset no tópico novo: %v", err)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if m := decode(t, r); m.op() == "reset" && reset == nil {
				reset = &m
			}
		})
	}
	if reset.str("epoch") == old {
		t.Fatalf("a queda não abriu geração nova; o teste não prova nada")
	}
	if _, has := reset.env["previousEpoch"]; has {
		t.Fatalf("reset do destino trocado com previousEpoch %q: a epoch anterior é de outro tópico", reset.str("previousEpoch"))
	}
}

// A marca de reset em curso também leva a régua embora: depois de um reset
// (aqui config_changed) e de um tail que passa da sequence da reabertura, o
// reinício limpo retoma. Uma régua esquecida no .ckpt, mais baixa que o que
// o tail publicou, daria rewind falso.
func TestRewindRulerIsClearedByTheReset(t *testing.T) {
	h, _ := liveHarness(t, harnessOpts{partitions: 3})
	changed := [32]byte{9}
	h.o.publisher = func(o *replicationkafka.Options) { o.RenderHash = changed }
	h.restart(true)
	if r := secondGeneration(t, h)[0]; r.str("reason") != "config_changed" {
		t.Fatalf("motivo = %q, quer config_changed", r.str("reason"))
	}
	c, _, err := replicationkafka.LoadCheckpoint(h.checkpointPath())
	if err != nil || c.RulerEpoch != "" {
		t.Fatalf("a régua ficou no .ckpt depois do reset: %q/%d (%v)", c.RulerEpoch, c.RulerSequence, err)
	}
	var seq uint64
	for i := range 3 {
		seq = h.upsert("after-reset-"+strconv.Itoa(i), int64(i))
	}
	h.feed.Flush(time.Now(), "test")
	waitFor(t, "tail depois do reset", func() bool { return h.pub.Stats().Cursor == seq })
	h.restart(true)
	h.readUntil("resume depois do reset", func(all []msg) bool {
		// Pára também no terceiro reset, para a falha dizer o motivo.
		return eachPartitionHas(3, "resume", 1)(all) || controlCount(all, "reset")[0] >= 3
	})
	for _, m := range h.readAll() {
		if m.op() == "reset" && m.str("reason") != "startup" && m.str("reason") != "config_changed" {
			t.Fatalf("o reinício depois do reset publicou %q, quer a retomada", m.str("reason"))
		}
	}
}
