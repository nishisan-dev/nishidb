package replicationkafka

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Este arquivo é a SUBIDA do publicador: a conferência de rewind no topo do
// tópico e a tabela de decisão entre retomar o tail (com o marcador `resume`)
// e publicar um reset, além das gravações do .ckpt e dos gatilhos de snapshot
// (pedido e periódico) que o laço confere em operação.

// stageError é a falha de infraestrutura fora da entrega: a leitura do topo
// do tópico (ErrorTopicRead) e a gravação do .ckpt antes do reset
// (ErrorCheckpointWrite). É retentada para sempre, como a entrega.
type stageError struct {
	reason string
	err    error
}

func (e *stageError) Error() string {
	return "replicationkafka: " + e.reason + ": " + e.err.Error()
}
func (e *stageError) Unwrap() error { return e.err }

// resetRequired é a transição que pede um snapshot novo em operação, com o
// motivo: a conferência periódica (partitions_changed) e os gatilhos
// (requested, periodic).
type resetRequired struct{ reason ResetReason }

func (e *resetRequired) Error() string {
	return "replicationkafka: snapshot required (" + e.reason.String() + ")"
}

// FieldAlias é um alias declarado na spec: o campo field da tabela table sai
// no `record` com o rótulo alias.
type FieldAlias struct {
	Table, Field, Alias string
}

// RenderSpec é o que muda o `record` renderizado e que a configuração
// DECLARA: as tabelas publicadas (o dataset base e, quando publicada, a
// histórica), os aliases declarados e publishHistory. NÃO cobre as colunas
// descobertas em runtime: o FieldID→nome é imutável, e descobrir uma coluna
// não muda o rótulo de uma coluna que já existe — hashear o Renderer, ou o
// dicionário de campos, daria config_changed em toda subida depois de uma
// descoberta.
type RenderSpec struct {
	Tables         []string
	Aliases        []FieldAlias
	PublishHistory bool
}

// renderHashMagic abre a codificação do RenderHash. É formato de disco (o
// hash vai no .ckpt): mudar a codificação muda o hash, e a primeira subida
// depois disso publica reset com config_changed em todo destino.
var renderHashMagic = [5]byte{'N', 'D', 'B', 'H', 1}

// RenderHash é a impressão da RenderSpec: sha256 sobre uma codificação
// canônica, que não depende da ordem das tabelas nem dos aliases (os dois são
// ordenados antes). A codificação, little-endian:
//
//	"NDBH" 0x01
//	u32 número de tabelas,  e por tabela  u32 len | bytes
//	u32 número de aliases,  e por alias   (u32 len | bytes) × table, field, alias
//	u8  publishHistory (0 ou 1)
//
// Tem golden test (TestRenderHashGoldenBytes), com o hash calculado fora do
// código.
func RenderHash(spec RenderSpec) [32]byte {
	tables := slices.Clone(spec.Tables)
	slices.Sort(tables)
	aliases := slices.Clone(spec.Aliases)
	slices.SortFunc(aliases, func(a, b FieldAlias) int {
		if c := strings.Compare(a.Table, b.Table); c != 0 {
			return c
		}
		if c := strings.Compare(a.Field, b.Field); c != 0 {
			return c
		}
		return strings.Compare(a.Alias, b.Alias)
	})
	b := append([]byte(nil), renderHashMagic[:]...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(tables)))
	for _, t := range tables {
		b = appendHashString(b, t)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(aliases)))
	for _, a := range aliases {
		b = appendHashString(b, a.Table)
		b = appendHashString(b, a.Field)
		b = appendHashString(b, a.Alias)
	}
	if spec.PublishHistory {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return sha256.Sum256(b)
}

func appendHashString(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// startup decide, na subida, entre retomar o tail e publicar um reset.
// Devolve o motivo do reset, ou 0 para a retomada (o `resume` já saiu e o
// tail continua do cursor do .ckpt). Falha de infraestrutura (leitura do topo
// do tópico, consulta de partições) retenta para sempre, em retrying, com o
// backoff da entrega: o publicador nunca retoma nem publica reset na epoch
// antiga às cegas. ok=false é o cancelamento, e também o feed fechado antes
// de o rewind conseguir rotacioná-lo: aí a subida sai sem publicar nada, e a
// vida seguinte refaz a conferência sobre o mesmo feed.
func (p *Publisher) startup(ctx context.Context) (reason ResetReason, ok bool, err error) {
	p.loadCheckpoint()
	backoff := p.minBackoff
	for {
		reason, err := p.decide(ctx)
		if err == nil {
			if reason == 0 {
				if err := p.resume(ctx); err != nil {
					return 0, true, err
				}
			}
			return reason, true, nil
		}
		if ctx.Err() != nil {
			return 0, false, nil
		}
		if errors.Is(err, replication.ErrClosed) {
			p.log.Info("replication.kafka.closed_before_decision",
				"the replication feed closed before the startup decision; nothing was published and the next start checks again")
			return 0, false, nil
		}
		p.noteFailure(err)
		if !p.sleep(ctx, backoff) {
			return 0, false, nil
		}
		backoff = min(2*backoff, p.maxBackoff)
	}
}

// loadCheckpoint lê o .ckpt do destino. Ausente é destino novo. Ilegível
// (CRC, versão, I/O) também vale como ausente, com WARN: o checkpoint
// corrompido não diz mais até onde o tópico foi, e a subida segue pela linha
// de "sem .ckpt" da tabela (startup), que republica o dataset inteiro.
func (p *Publisher) loadCheckpoint() {
	c, found, err := loadCheckpoint(p.ckptPath)
	if err != nil {
		p.log.Warn("replication.kafka.checkpoint.unreadable",
			"the destination checkpoint cannot be read; the publisher treats the destination as new and publishes a full snapshot",
			"path", p.ckptPath, "error", err.Error())
		c, found = checkpointFile{}, false
	}
	p.ckpt, p.ckptFound = c, found
	p.mu.Lock()
	p.stats.CheckpointCursor = c.Cursor
	if !c.LastSnapshotEnd.IsZero() {
		p.stats.LastSnapshotAt = float64(c.LastSnapshotEnd.UnixMilli()) / 1000
	}
	p.mu.Unlock()
}

// decide aplica a conferência de rewind e, depois dela, a tabela de decisão
// da subida. A primeira linha que casa vence:
//
//	0  rewind (conferência no topo do tópico, só com OriginClean)     rewind
//	1  sem .ckpt, Topic diferente ou nenhum broker em comum            startup
//	2  RenderHash diferente                                            config_changed
//	3  Snapshotting (o snapshot anterior morreu no meio)               snapshot_failed
//	4  pedido de snapshot pendente (.resync)                           requested
//	5  Epoch ≠ epoch do feed: queda da geração do .ckpt                crash
//	                          feed rotacionado                         rewind
//	                          outro caso                               startup
//	6  Cursor abaixo do piso do feed                                   retention
//	7  número de partições do tópico ≠ Partitions                      partitions_changed
//	8  snapshot periódico vencido desde LastSnapshotEnd                periodic
//	9  nenhuma: retoma o tail                                          (resume)
//
// A conferência de rewind vem ANTES de tudo: um reset na epoch antiga de um
// feed restaurado também colidiria com o que o tópico já tem.
func (p *Publisher) decide(ctx context.Context) (ResetReason, error) {
	origin := p.feed.Origin()
	epoch, _, _ := p.feed.Position()
	if origin.Kind == replication.OriginClean {
		ruler, err := p.pinRuler(epoch, origin.ResumedSequence)
		if err != nil {
			return 0, err
		}
		rewound, err := p.checkRewind(ctx, ruler, epoch)
		if err != nil {
			return 0, err
		}
		if rewound {
			if err := p.rotate(epoch); err != nil {
				return 0, err
			}
			return ResetRewind, nil
		}
	}
	c := p.ckpt
	switch {
	case !p.ckptFound || c.Topic != p.topic || !sameCluster(c.Bootstrap, p.bootstrap):
		return ResetStartup, nil
	case c.RenderHash != p.renderHash:
		return ResetConfigChanged, nil
	case c.Snapshotting:
		return ResetSnapshotFailed, nil
	case p.resyncPending():
		return ResetRequested, nil
	case c.Epoch != epoch:
		return generationReason(origin, c.Epoch), nil
	case c.Cursor < p.feed.Status().AvailableAfter:
		return ResetRetention, nil
	}
	n, err := p.partitions(ctx)
	if err != nil {
		return 0, err
	}
	switch {
	case int32(n) != c.Partitions:
		return ResetPartitionsChanged, nil
	case p.periodicDue():
		return ResetPeriodic, nil
	}
	return 0, nil
}

// generationReason é a linha 5 da tabela: o .ckpt é de outra geração do
// feed. Queda com a geração anterior igual à do .ckpt é crash; feed
// rotacionado (o rewind de outro destino do mesmo dataset, por exemplo) é
// rewind; o resto (geração mais antiga que a anterior, feed sem `state`) é
// startup. Vale também em operação, quando o Acquire devolve
// ErrResyncRequired porque a epoch do feed mudou.
func generationReason(origin replication.Origin, ckptEpoch string) ResetReason {
	switch {
	case origin.Kind == replication.OriginCrash && origin.PreviousEpoch == ckptEpoch:
		return ResetCrash
	case origin.Kind == replication.OriginRotated:
		return ResetRewind
	default:
		return ResetStartup
	}
}

// sameCluster diz se as duas listas de bootstrap têm algum host:porta em
// comum, comparados sem espaço e em minúsculas. É a mesma regra do
// sameCluster do servidor (a conferência de tópico repetido): acrescentar um
// broker à lista não troca o cluster, e trocar a lista inteira troca.
func sameCluster(a, b []string) bool {
	seen := make(map[string]bool, len(a))
	for _, s := range a {
		seen[strings.ToLower(strings.TrimSpace(s))] = true
	}
	for _, s := range b {
		if seen[strings.ToLower(strings.TrimSpace(s))] {
			return true
		}
	}
	return false
}

// resyncPending diz se há pedido de snapshot ainda não atendido. O .resync
// ilegível não é pedido: tratá-lo como pedido publicaria um snapshot a cada
// conferência, porque o contador ilegível nunca seria consumido. Sai um WARN
// (limitado, como o da retentativa) e o pedido fica para quando o arquivo
// voltar a ser legível.
func (p *Publisher) resyncPending() bool {
	_, pending, err := pendingResync(p.resyncPath, p.ckpt.ResyncConsumed)
	if err != nil {
		now := time.Now()
		if p.lastCkptWarn.IsZero() || now.Sub(p.lastCkptWarn) >= retryWarnInterval {
			p.lastCkptWarn = now
			p.log.Warn("replication.kafka.resync.unreadable",
				"the snapshot request counter cannot be read; pending requests are ignored until it is readable",
				"path", p.resyncPath, "error", err.Error())
		}
		return false
	}
	return pending
}

// periodicDue diz se o snapshot periódico venceu: o intervalo contado do
// último snapshot_end confirmado, que está no .ckpt e por isso sobrevive ao
// reinício.
func (p *Publisher) periodicDue() bool {
	return p.periodic > 0 && p.now().Sub(p.ckpt.LastSnapshotEnd) >= p.periodic
}

// rotate abre a geração nova do rewind a partir da geração conferida, de
// forma atômica (Stream.RotateFrom). Se o feed já saiu dela (outro destino do
// mesmo dataset rotacionou primeiro), a rotação conta como feita: uma segunda
// só derrubaria o reset que o outro já publicou. Se o feed CONTINUA nela, a
// rotação não aconteceu porque o feed fechou (o encerramento chegou no meio
// da conferência), e o rewind não pode sair: o reset iria na epoch antiga, e
// a vida seguinte reusaria (epoch, sequence) já entregues. Devolve
// replication.ErrClosed nesse caso.
func (p *Publisher) rotate(conferred string) error {
	if p.feed.RotateFrom(conferred, "rewind") {
		return nil
	}
	if epoch, _, _ := p.feed.Position(); epoch != conferred {
		return nil
	}
	return replication.ErrClosed
}

// pinRuler fixa no .ckpt, ANTES da primeira conferência do topo, a régua
// dela: a geração do feed e a sequence em que ele reabriu. A régua gravada
// da MESMA geração nunca sobe: vale o menor entre ela e ResumedSequence. É o
// que impede o rewind de se perder numa vida que reabre o feed restaurado,
// não consegue decidir (o tópico ilegível a vida inteira), ingere além do
// topo antigo e fecha limpo: a vida seguinte reabre com uma sequence maior,
// mas confere com a régua da primeira. A régua de outra geração (o feed
// nasceu de queda desde então) é descartada: a geração nova não colide com
// nada. Devolve a régua efetiva. Falha da gravação é ErrorCheckpointWrite, e
// a conferência não roda.
func (p *Publisher) pinRuler(epoch string, resumed uint64) (uint64, error) {
	c := p.ckpt
	ruler := resumed
	if c.RulerEpoch == epoch && c.RulerSequence < ruler {
		ruler = c.RulerSequence
	}
	if p.ckptFound && c.RulerEpoch == epoch && c.RulerSequence == ruler {
		return ruler, nil
	}
	c.RulerEpoch, c.RulerSequence = epoch, ruler
	if err := p.writeCheckpoint(c); err != nil {
		return 0, err
	}
	return ruler, nil
}

// checkRewind é a conferência de rewind: o feed reaberto de um fechamento
// limpo pode ser um backup restaurado, coerente mas no passado, e retomar dali
// reusaria (epoch, sequence) já entregues com outro conteúdo.
//
// A posição comparada é a régua (pinRuler): a menor sequence em que o feed
// reabriu limpo desde a última decisão deste destino, e NUNCA a produzida
// corrente. A ingestão já roda, e um feed restaurado que recebeu alterações
// enquanto a leitura do tópico falhava, nesta vida ou numa anterior que não
// chegou a decidir, alcançaria o topo antigo e esconderia o rewind.
//
// Sem Kafka, pelo .ckpt (que mora junto do feed e volta junto no backup, mas
// pode ser mais novo que o `state` quando só o feed voltou):
//   - mesma epoch e Cursor acima da régua;
//   - Snapshotting, TargetEpoch igual à do feed e TargetCursor acima da
//     régua.
//
// Com Kafka, pelo último registro de cada partição: a epoch dele fora de
// {Epoch, TargetEpoch} do .ckpt (o tópico é exclusivo do dataset, então epoch
// estranha é publicação nossa posterior ao .ckpt; sem .ckpt, toda epoch é
// estranha), ou a epoch do feed com posição acima da régua. O
// registro que nem é envelope nosso também conta como estranho.
//
// Sem .ckpt, a epoch estranha é rewind, e não startup, porque não dá para
// saber se a epoch do feed já apareceu no tópico e foi superada por uma
// posterior: um reset nela seria descartado pelo consumidor como geração
// antiga. É o caso do destino renomeado (o .ckpt é por nome) e do .ckpt
// apagado à mão. O .ckpt de OUTRO tópico ou cluster (o destino trocado no
// YAML) conta como ausente em toda a conferência: ele não diz nada sobre o
// tópico conferido, e as regras sem Kafka só valem com o .ckpt do mesmo
// tópico e cluster.
//
// A partição sem registro legível também é rewind: a conferência não
// consegue provar que o feed não voltou no tempo, e uma geração nova nunca
// reusa (epoch, sequence). Sem registro legível é a ausência COMPROVADA pelos
// offsets: a partição esvaziada pela retenção (início = fim > 0) e, com o
// .ckpt deste tópico, a partição que nunca foi escrita (fim = 0) com índice
// abaixo das Partitions dele, que é o tópico recriado vazio. A partição
// nunca escrita sem .ckpt (destino novo) ou com índice ≥ Partitions (a
// partição acrescentada com o master parado, que nunca recebeu registro
// nosso) não tem o que conferir: a primeira segue para startup, a segunda
// para partitions_changed.
// A falha de infraestrutura na leitura (broker fora, timeout, fetch sem
// registro no prazo) não é ausência comprovada: falha com ErrorTopicRead e é
// retentada, e nunca vira resume nem reset às cegas.
func (p *Publisher) checkRewind(ctx context.Context, resumed uint64, epoch string) (bool, error) {
	c := p.ckpt
	own := p.ckptFound && c.Topic == p.topic && sameCluster(c.Bootstrap, p.bootstrap)
	if own {
		switch {
		case c.Epoch == epoch && c.Cursor > resumed:
			p.logRewind("checkpoint", -1, c.Epoch, c.Cursor, resumed, epoch)
			return true, nil
		case c.Snapshotting && c.TargetEpoch == epoch && c.TargetCursor > resumed:
			p.logRewind("checkpoint_target", -1, c.TargetEpoch, c.TargetCursor, resumed, epoch)
			return true, nil
		}
	}
	marks, err := p.readTopicTail(ctx)
	if err != nil {
		return false, err
	}
	parts := make([]int32, 0, len(marks))
	for part := range marks {
		parts = append(parts, part)
	}
	slices.Sort(parts)
	for _, part := range parts {
		m := marks[part]
		if m.unreadable {
			if !m.written && (!own || part >= c.Partitions) {
				continue
			}
			p.logRewind("topic_unreadable", part, "", 0, resumed, epoch)
			return true, nil
		}
		known := m.valid && own && m.epoch != "" && (m.epoch == c.Epoch || m.epoch == c.TargetEpoch)
		switch {
		case !known:
			p.logRewind("topic_foreign_epoch", part, m.epoch, m.position, resumed, epoch)
			return true, nil
		case m.epoch == epoch && m.position > resumed:
			p.logRewind("topic_position", part, m.epoch, m.position, resumed, epoch)
			return true, nil
		}
	}
	return false, nil
}

func (p *Publisher) logRewind(source string, partition int32, seenEpoch string, position, resumed uint64, epoch string) {
	p.log.Warn("replication.kafka.rewind",
		"the replication feed is behind what this destination already received (a restored backup); the feed starts a new generation and the destination is rebuilt",
		"source", source, "partition", partition, "seen_epoch", seenEpoch, "position", position,
		"resumed_sequence", resumed, "epoch", epoch)
}

// topicMark é o que a conferência extrai do último registro de uma partição:
// a epoch e a posição (`sequence` no dado e no retain, `cursor` no controle).
// valid=false é o registro que não é envelope nosso. unreadable marca a
// partição sem registro legível pelos offsets; written diz se ela já recebeu
// algum registro (fim > 0, esvaziada pela retenção) ou nunca (fim = 0).
type topicMark struct {
	epoch      string
	position   uint64
	valid      bool
	unreadable bool
	written    bool
}

// readTopicTail lê o último registro de cada partição do tópico. Lista o
// início e o fim de cada partição; a partição com fim > início tem o registro
// em fim-1 lido por um cliente consumidor próprio, que vive só durante a
// leitura, e a partição com fim = início sai marcada unreadable. Toda a
// leitura cabe no timeout do destino; o fetch que não devolve o registro no
// prazo é falha (ErrorTopicRead), e não ausência.
func (p *Publisher) readTopicTail(ctx context.Context) (map[int32]topicMark, error) {
	fail := func(err error) error { return &stageError{reason: ErrorTopicRead, err: err} }
	if p.beforeTopicRead != nil {
		if err := p.beforeTopicRead(); err != nil {
			return nil, fail(err)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	starts, err := p.admin.ListStartOffsets(cctx, p.topic)
	if err != nil {
		return nil, fail(fmt.Errorf("list start offsets of topic %q: %w", p.topic, err))
	}
	ends, err := p.admin.ListEndOffsets(cctx, p.topic)
	if err != nil {
		return nil, fail(fmt.Errorf("list end offsets of topic %q: %w", p.topic, err))
	}
	if len(ends[p.topic]) == 0 {
		return nil, fail(fmt.Errorf("topic %q not found", p.topic))
	}
	want := make(map[int32]int64)
	for part, end := range ends[p.topic] {
		start, ok := starts.Lookup(p.topic, part)
		switch {
		case end.Err != nil:
			return nil, fail(fmt.Errorf("topic %q partition %d: %w", p.topic, part, end.Err))
		case !ok || start.Err != nil:
			return nil, fail(fmt.Errorf("topic %q partition %d: no start offset", p.topic, part))
		case end.Offset > start.Offset:
			want[part] = end.Offset - 1
		}
	}
	marks := make(map[int32]topicMark, len(ends[p.topic]))
	for part, end := range ends[p.topic] {
		if _, ok := want[part]; !ok {
			marks[part] = topicMark{unreadable: true, written: end.Offset > 0}
		}
	}
	if len(want) == 0 {
		return marks, nil
	}
	offsets := make(map[int32]kgo.Offset, len(want))
	for part, off := range want {
		offsets[part] = kgo.NewOffset().At(off)
	}
	opts := append(slices.Clone(p.readerOpts),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{p.topic: offsets}),
		kgo.FetchMaxWait(topicReadFetchWait))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fail(err)
	}
	defer cl.Close()
	for read := 0; read < len(want); {
		fs := cl.PollFetches(cctx)
		if cctx.Err() != nil {
			for part := range want {
				if _, ok := marks[part]; !ok {
					return nil, fail(fmt.Errorf("topic %q partition %d returned no record at offset %d before the deadline",
						p.topic, part, want[part]))
				}
			}
		}
		var fetchErr error
		fs.EachError(func(_ string, part int32, err error) {
			if fetchErr == nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				fetchErr = fmt.Errorf("fetch topic %q partition %d: %w", p.topic, part, err)
			}
		})
		if fetchErr != nil {
			return nil, fail(fetchErr)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if _, done := marks[r.Partition]; done || r.Offset < want[r.Partition] {
				return
			}
			marks[r.Partition] = parseTopicMark(r.Value)
			read++
		})
	}
	return marks, nil
}

// parseTopicMark extrai a epoch e a posição de um envelope. Não é caminho
// quente (um registro por partição, só na subida), por isso o encoding/json.
func parseTopicMark(value []byte) topicMark {
	var env struct {
		Epoch    string  `json:"epoch"`
		Sequence *uint64 `json:"sequence"`
		Cursor   *uint64 `json:"cursor"`
	}
	if err := json.Unmarshal(value, &env); err != nil || env.Epoch == "" {
		return topicMark{}
	}
	switch {
	case env.Sequence != nil:
		return topicMark{epoch: env.Epoch, position: *env.Sequence, valid: true}
	case env.Cursor != nil:
		return topicMark{epoch: env.Epoch, position: *env.Cursor, valid: true}
	}
	return topicMark{epoch: env.Epoch}
}

// resume é a linha 9 da tabela: retoma o tail do cursor do .ckpt, na mesma
// epoch, e publica o `resume` em todas as partições IMEDIATAMENTE, sem
// barreira de ack (é só observabilidade: o consumidor não faz nada com ele).
// O lote do resume é apurado pelo tail (reapControl) e na saída do laço.
func (p *Publisher) resume(ctx context.Context) error {
	c := p.ckpt
	n := int(c.Partitions)
	p.epoch, p.cursor, p.workEpoch = c.Epoch, c.Cursor, c.Epoch
	p.pinned.Store(c.Partitions)
	p.awaitProducerMetadata(ctx, n)
	p.nextCheck = time.Now().Add(p.checkEvery)
	b := &p.ctrl
	b.reset()
	b.buf = p.renderer.AppendResume(b.buf, c.Epoch, c.Cursor, p.now())
	for part := range n {
		b.addControl(OpResume, int32(part), 0, len(b.buf))
	}
	if err := p.checkSizes(b); err != nil {
		return err
	}
	p.send(ctx, b, nil)
	// A decisão terminou: a régua sai do .ckpt. A falha só conta, e a
	// pendência leva a gravação ao próximo checkpoint do tail; enquanto
	// isso a régua fica, e uma régua velha só pode adiantar um rewind, nunca
	// escondê-lo.
	c.RulerEpoch, c.RulerSequence = "", 0
	p.ckpt, p.ckptDirty = c, true
	p.noteCheckpointFailure(c, p.writeCheckpoint(c))
	p.mu.Lock()
	p.stats.Epoch, p.stats.Cursor = c.Epoch, c.Cursor
	p.stats.State = StateFollowing
	p.lagBase = c.Cursor
	p.lagEpoch, p.lagPos = c.Epoch, c.CursorPos
	p.mu.Unlock()
	p.log.Info("replication.kafka.resumed", "publisher resumed the tail from its checkpoint without a snapshot",
		"epoch", c.Epoch, "cursor", c.Cursor, "partitions", n)
	return nil
}

// reapControl apura o lote do resume quando as promises dele já voltaram; com
// wait, espera por elas (limitado por ctx). Falha conta como falha de
// entrega (retrying até o próximo lote confirmado), mas não é reenviada: o
// resume é só observabilidade.
func (p *Publisher) reapControl(ctx context.Context, wait bool) {
	b := &p.ctrl
	if !b.inflight {
		return
	}
	if !wait {
		select {
		case <-b.done:
		default:
			return
		}
	}
	if err := p.collect(ctx, b); err != nil && ctx.Err() == nil {
		p.noteFailure(err)
	}
}

// writeCheckpoint grava c e, em sucesso, o adota como o checkpoint corrente
// (ckpt, sem pendência). Falha não toca ckpt nem conta nada: quem quer a
// gravação retentada depois (o fim do snapshot, o tail e a saída) adota c e
// marca a pendência ANTES de chamar, e conta pela noteCheckpointFailure; o
// reset que não conseguiu se marcar no disco não adota, porque não começou,
// e é contado UMA vez pelo noteFailure do laço. A tentativa, com sucesso ou
// não, reinicia a cadência do tail (lastCkptWrite): com o disco falhando, o
// tail tenta no máximo uma vez por checkpointInterval, e não a cada lote.
func (p *Publisher) writeCheckpoint(c checkpointFile) error {
	err := error(nil)
	if p.checkpointFault != nil {
		err = p.checkpointFault()
	}
	if err == nil {
		err = storeCheckpoint(p.ckptPath, c)
	}
	p.lastCkptWrite = time.Now()
	if err != nil {
		return &stageError{reason: ErrorCheckpointWrite, err: err}
	}
	p.ckpt, p.ckptFound, p.ckptDirty = c, true, false
	p.mu.Lock()
	p.stats.CheckpointCursor = c.Cursor
	if !c.LastSnapshotEnd.IsZero() {
		p.stats.LastSnapshotAt = float64(c.LastSnapshotEnd.UnixMilli()) / 1000
	}
	p.mu.Unlock()
	return nil
}

// noteCheckpointFailure conta a gravação do .ckpt que falhou fora do reset
// (fim do snapshot, tail, saída): ErrorCheckpointWrite, LastError e o WARN
// limitado. O publicador segue.
func (p *Publisher) noteCheckpointFailure(c checkpointFile, err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	p.stats.Errors[ErrorCheckpointWrite]++
	p.stats.LastError = err.Error()
	p.mu.Unlock()
	now := time.Now()
	if p.lastCkptWarn.IsZero() || now.Sub(p.lastCkptWarn) >= retryWarnInterval {
		p.lastCkptWarn = now
		p.log.Warn("replication.kafka.checkpoint.failed", "the destination checkpoint could not be written",
			"path", p.ckptPath, "cursor", c.Cursor, "snapshotting", c.Snapshotting, "error", err.Error())
	}
}

// flushCheckpoint grava o checkpoint pendente: o último ponto confirmado que
// a cadência de uma gravação por segundo ainda não levou ao disco. Roda na
// saída do laço (ErrClosed, cancelamento, blocked e failed).
func (p *Publisher) flushCheckpoint() {
	if p.ckptDirty {
		p.noteCheckpointFailure(p.ckpt, p.writeCheckpoint(p.ckpt))
	}
}

// identity preenche em c o que fixa o destino: tópico, brokers, partições e
// a impressão da renderização.
func (p *Publisher) identity(c checkpointFile, partitions int) checkpointFile {
	c.Topic = p.topic
	c.Bootstrap = p.bootstrap
	c.Partitions = int32(partitions)
	c.RenderHash = p.renderHash
	return c
}

// beginCheckpoint é a gravação antes do reset: Snapshotting, a epoch e o
// cursor do reset em TargetEpoch/TargetCursor e o pedido de snapshot
// consumido; Epoch, Cursor e CursorPos continuam os do último ponto
// confirmado. previous é o `previousEpoch` do reset: a última epoch cujo
// reset já saiu NESTE destino, lida do .ckpt ANTES desta gravação — a
// TargetEpoch quando há um reset em curso (o snapshot que falhou numa epoch
// intermediária já publicou o reset dela), senão a Epoch —, e vazia quando é
// a própria epoch do reset novo. O .ckpt de OUTRO tópico ou cluster (o
// destino trocado) não tem epoch publicada neste destino, e previous sai
// vazio. A gravação leva a régua da conferência embora: a decisão terminou.
// Falha impede o snapshot de começar.
func (p *Publisher) beginCheckpoint(h replication.SnapshotHeader, partitions int) (previous string, err error) {
	c := p.ckpt
	last := c.Epoch
	if c.Snapshotting && c.TargetEpoch != "" {
		last = c.TargetEpoch
	}
	if own := c.Topic == p.topic && sameCluster(c.Bootstrap, p.bootstrap); own && last != h.Epoch {
		previous = last
	}
	if counter, pending, err := pendingResync(p.resyncPath, c.ResyncConsumed); err == nil && pending {
		c.ResyncConsumed = counter
	}
	c = p.identity(c, partitions)
	c.Snapshotting = true
	c.TargetEpoch, c.TargetCursor = h.Epoch, h.Cursor
	c.RulerEpoch, c.RulerSequence = "", 0
	return previous, p.writeCheckpoint(c)
}

// endCheckpoint é a gravação depois do snapshot_end confirmado: o ponto
// confirmado salta para o do reset, e LastSnapshotEnd = now. Falha só conta:
// o .ckpt fica com Snapshotting no disco (a próxima subida faz
// snapshot_failed, que é seguro) até a próxima gravação do tail.
func (p *Publisher) endCheckpoint(h replication.SnapshotHeader, partitions int, now time.Time) {
	c := p.identity(p.ckpt, partitions)
	c.Snapshotting = false
	c.Epoch, c.Cursor, c.CursorPos = h.Epoch, h.Cursor, h.EndPos
	c.TargetEpoch, c.TargetCursor = "", 0
	c.LastSnapshotEnd = now
	p.ckpt, p.ckptDirty = c, true
	p.noteCheckpointFailure(c, p.writeCheckpoint(c))
}

// tailCheckpoint registra o lote do tail confirmado e grava o .ckpt no
// máximo uma vez por checkpointInterval.
func (p *Publisher) tailCheckpoint(b *replication.Batch) {
	p.ckpt.Cursor, p.ckpt.CursorPos = b.To, b.EndPos
	p.ckptDirty = true
	if time.Since(p.lastCkptWrite) >= checkpointInterval {
		p.noteCheckpointFailure(p.ckpt, p.writeCheckpoint(p.ckpt))
	}
}

// takeWake consome o pedido de acordar o laço (RequestSnapshot).
func (p *Publisher) takeWake() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	w := p.wakeRequested
	p.wakeRequested = false
	return w
}

// checkTriggers confere os gatilhos de snapshot em operação, na cadência da
// conferência de partições (e na hora, depois de um RequestSnapshot): pedido
// pendente no .resync e snapshot periódico vencido.
func (p *Publisher) checkTriggers() error {
	switch {
	case p.resyncPending():
		p.log.Info("replication.kafka.snapshot.requested", "a snapshot was requested for this destination")
		return &resetRequired{reason: ResetRequested}
	case p.periodicDue():
		return &resetRequired{reason: ResetPeriodic}
	}
	return nil
}
