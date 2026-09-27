package replicationkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
)

// errPartitionsChanged é a divergência entre o número de partições do tópico
// e o fixado no último reset.
var errPartitionsChanged = errors.New("replicationkafka: topic partition count changed")

// haltError é a parada terminal (blocked ou failed): o laço sai sem
// retentar.
type haltError struct{ err error }

func (e *haltError) Error() string { return e.err.Error() }
func (e *haltError) Unwrap() error { return e.err }

// deliveryError é a falha de infraestrutura: entrega ou consulta de metadado.
// É retentada para sempre.
type deliveryError struct {
	reason string
	err    error
}

func (e *deliveryError) Error() string {
	return "replicationkafka: delivery failed (" + e.reason + "): " + e.err.Error()
}
func (e *deliveryError) Unwrap() error { return e.err }

// run é o laço do publicador: snapshot quando falta continuidade, tail no
// resto. Os três baldes de erro (§101) se separam aqui: infraestrutura
// retenta para sempre (retrying); bug de renderização para o publicador
// (failed); registro grande demais para sem pular nada (blocked). Nenhum dos
// três trava a ingestão: o feed nunca espera o publicador.
func (p *Publisher) run(ctx context.Context) {
	defer close(p.done)
	reason := ResetStartup
	needSnapshot := true
	backoff := p.minBackoff
	for {
		var err error
		produced := false
		if needSnapshot {
			if produced, err = p.snapshot(ctx, reason); err == nil {
				needSnapshot = false
				backoff = p.minBackoff
				continue
			}
		} else if err = p.tailOnce(ctx); err == nil {
			backoff = p.minBackoff
			continue
		}
		var halt *haltError
		switch {
		case ctx.Err() != nil:
			p.setState(StateStopped)
			return
		case errors.As(err, &halt):
			return
		case errors.Is(err, replication.ErrClosed):
			p.setState(StateStopped)
			p.log.Info("replication.kafka.drained", "replication feed closed and its tail was delivered",
				"cursor", p.cursor)
			return
		case errors.Is(err, replication.ErrResyncRequired):
			if needSnapshot {
				// O feed perdeu o cursor do reset NO MEIO do snapshot. Nenhum
				// contador de snapshots sobe (só os concluídos contam), então
				// sem o contador, o LastError e o ERROR daqui este laço seria
				// invisível: resets em série e nada mais.
				p.noteSnapshotGap(err)
				if !p.sleep(ctx, p.minBackoff) {
					p.setState(StateStopped)
					return
				}
			} else if p.justSnapshotted {
				// O snapshot terminou, mas o feed já tinha perdido o cursor dele
				// antes do primeiro lote do tail: o próximo snapshot tende a
				// acabar igual. O contador de snapshots `gap` sobe a cada volta,
				// e o ERROR diz o que dimensionar.
				p.logLivelock("tail_start", p.lastSnapshot)
			}
			needSnapshot, reason = true, ResetGap
		case errors.Is(err, errPartitionsChanged):
			needSnapshot, reason = true, ResetPartitionsChanged
		default:
			p.noteFailure(err)
			// A falha antes de qualquer registro produzido (tópico ausente,
			// broker fora na consulta de metadado) não publicou reset nenhum:
			// o motivo continua o original.
			if needSnapshot && produced {
				reason = ResetSnapshotFailed
			}
			if !p.sleep(ctx, backoff) {
				p.setState(StateStopped)
				return
			}
			backoff = min(2*backoff, p.maxBackoff)
		}
	}
}

// snapshot publica reset, o estado vivo e snapshot_end em todas as
// partições. produced diz se algum registro chegou ao produtor: a falha
// depois disso recomeça com o motivo snapshot_failed.
//
// Os blocos vão ao produtor de forma assíncrona, com no máximo dois em voo:
// enquanto um espera os acks, o outro é montado. Nenhum buffer é
// sobrescrito antes de as promises dele voltarem, e qualquer falha aborta o
// snapshot inteiro (o próximo publica outro reset).
func (p *Publisher) snapshot(ctx context.Context, reason ResetReason) (produced bool, err error) {
	n, err := p.partitions(ctx)
	if err != nil {
		return false, err
	}
	p.pinned.Store(int32(n))
	p.awaitProducerMetadata(ctx, n)
	p.nextCheck = time.Now().Add(p.checkEvery)
	p.counts = append(p.counts[:0], make([]uint64, n)...)
	p.chunk = 0
	p.chunks[0].reset()
	p.chunks[1].reset()
	p.setState(StateSnapshotting)
	started := time.Now()
	p.snapshotStarted = started
	var hdr replication.SnapshotHeader
	var rows uint64

	// flush entrega o bloco corrente e troca para o outro, esperando-o se
	// ainda estiver em voo.
	flush := func() error {
		b := &p.chunks[p.chunk]
		if len(b.recs) > 0 {
			if err := p.checkSizes(b); err != nil {
				return err
			}
			p.send(ctx, b, nil)
			produced = true
		}
		p.chunk ^= 1
		other := &p.chunks[p.chunk]
		if other.inflight {
			if err := p.collect(ctx, other); err != nil {
				return err
			}
		}
		other.reset()
		// A lacuna no meio do snapshot aborta cedo: o tail não teria de onde
		// continuar, e o resto do snapshot seria trabalho jogado fora.
		return p.feed.Check(hdr.Epoch, hdr.Cursor)
	}

	begin := func(h replication.SnapshotHeader) error {
		hdr = h
		p.mu.Lock()
		p.lagBase = h.Cursor
		p.mu.Unlock()
		p.log.Info("replication.kafka.snapshot.started", "publishing snapshot",
			"reason", reason.String(), "epoch", h.Epoch, "cursor", h.Cursor, "partitions", n)
		b := &p.chunks[p.chunk]
		start := len(b.buf)
		buf, rerr := p.renderer.AppendReset(b.buf, h.Epoch, reason, h.Cursor, h.At)
		if rerr != nil {
			return p.halt(StateFailed, ErrorRender, rerr, 0, "")
		}
		b.buf = buf
		for part := range n {
			b.addControl(OpReset, int32(part), start, len(b.buf))
		}
		if err := p.checkSizes(b); err != nil {
			return err
		}
		// Barreira: nenhuma linha sai antes de o reset estar confirmado em
		// todas as partições.
		p.send(ctx, b, nil)
		produced = true
		if err := p.collect(ctx, b); err != nil {
			return err
		}
		b.reset()
		return nil
	}

	emit := func(e replication.Entry) error {
		// e.Record é EMPRESTADO: render o copia para o buffer do bloco, e o
		// Produce só vê o buffer.
		b := &p.chunks[p.chunk]
		part, data, rerr := p.render(b, e, true, hdr, n)
		if rerr != nil {
			return p.halt(StateFailed, ErrorRender, rerr, e.Sequence, e.Table)
		}
		if data {
			p.counts[part]++
			rows++
		}
		if len(b.recs) >= p.chunkRows || len(b.buf) >= snapshotChunkBytes {
			return flush()
		}
		return nil
	}

	err = replication.ExportSnapshot(ctx, p.feed, p.tables, p.chunkRows, begin, emit)
	if err == nil {
		err = flush()
	}
	if err == nil {
		err = p.collectAll(ctx)
	}
	if err != nil {
		p.abandon(ctx)
		return produced, err
	}

	b := &p.chunks[p.chunk]
	b.reset()
	for part := range n {
		start := len(b.buf)
		b.buf = p.renderer.AppendSnapshotEnd(b.buf, hdr.Epoch, hdr.Cursor, p.counts[part], hdr.At)
		b.addControl(OpSnapshotEnd, int32(part), start, len(b.buf))
	}
	if err := p.checkSizes(b); err != nil {
		return produced, err
	}
	p.send(ctx, b, nil)
	if err := p.collect(ctx, b); err != nil {
		p.abandon(ctx)
		return true, err
	}
	b.reset()

	p.epoch, p.cursor = hdr.Epoch, hdr.Cursor
	p.justSnapshotted = true
	p.lastSnapshot = time.Since(started)
	now := p.now()
	p.mu.Lock()
	p.stats.Epoch, p.stats.Cursor = hdr.Epoch, hdr.Cursor
	p.stats.Snapshots[reason.String()]++
	p.stats.State = StateFollowing
	p.stats.LastSuccessTimestamp = float64(now.UnixMilli()) / 1000
	p.mu.Unlock()
	p.log.Info("replication.kafka.snapshot.completed", "snapshot published",
		"reason", reason.String(), "cursor", hdr.Cursor, "rows", rows, "partitions", n,
		"duration", p.lastSnapshot.String())
	if p.afterSnap != nil {
		p.afterSnap()
	}
	return true, nil
}

// tailOnce entrega um lote do feed. Solta o lease logo depois de renderizar
// o lote, e só avança o cursor depois de TODOS os registros do lote
// confirmados.
func (p *Publisher) tailOnce(ctx context.Context) error {
	if err := p.maybeCheckPartitions(ctx); err != nil {
		return err
	}
	// O Acquire espera no máximo até a próxima conferência de partições: o
	// tail ocioso não pode deixar de enxergar o tópico que cresceu.
	actx, cancel := context.WithDeadline(ctx, p.nextCheck)
	b, release, err := p.feed.Acquire(actx, p.epoch, p.cursor)
	cancel()
	if err != nil {
		if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			// Tail ocioso até a conferência: o snapshot já não é "recém
			// terminado", e uma lacuna daqui em diante não é o livelock do
			// snapshot.
			p.justSnapshotted = false
			return nil
		}
		return err
	}
	// O lease é solto em TODO caminho — inclusive failed e blocked, que
	// param o publicador. O release é idempotente.
	defer release()
	p.justSnapshotted = false
	out := &p.tail
	out.reset()
	n := int(p.pinned.Load())
	for _, e := range b.Entries {
		// O que o snapshot já cobriu (sequence ≤ cursor) não se repete: o
		// lote pode começar antes do cursor do reset (resync.go faz o mesmo).
		if e.Sequence <= p.cursor {
			continue
		}
		if _, _, err := p.render(out, e, false, replication.SnapshotHeader{}, n); err != nil {
			return p.halt(StateFailed, ErrorRender, err, e.Sequence, e.Table)
		}
	}
	// O render já copiou tudo para o buffer do publicador: o lease é solto
	// AGORA, e não depois dos acks. Um broker travado não segura memória do
	// feed, que os followers HTTP compartilham. Despejar SÓ este lote enquanto
	// a entrega retenta não cria lacuna: o piso do feed vira b.To, que é o
	// cursor novo. A lacuna — ErrResyncRequired no próximo Acquire, e o
	// snapshot gap que segue — só aparece se o feed despejar lotes que o
	// publicador ainda não leu, ou invalidar o intervalo retido.
	release()
	if err := p.checkSizes(out); err != nil {
		return err
	}
	if err := p.deliver(ctx, out); err != nil {
		return err
	}
	p.cursor = b.To
	now := p.now()
	p.mu.Lock()
	p.stats.Cursor = b.To
	p.lagBase = b.To
	p.stats.State = StateFollowing
	p.stats.LastSuccessTimestamp = float64(now.UnixMilli()) / 1000
	p.mu.Unlock()
	return nil
}

// deliver entrega o lote do tail e retenta até todos os registros serem
// confirmados. Numa partição com falha, reproduz a partir do PRIMEIRO
// registro falho até o fim do lote naquela partição, inclusive os que já
// tiveram ack: reproduzir só os falhos poria um update antigo DEPOIS do
// delete da mesma key. At-least-once, last-write-wins.
func (p *Publisher) deliver(ctx context.Context, b *outBatch) error {
	p.idx = p.idx[:0]
	for i := range b.recs {
		p.idx = append(p.idx, i)
	}
	backoff := p.minBackoff
	for {
		p.send(ctx, b, p.idx)
		err := p.collect(ctx, b)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		p.noteFailure(err)
		if !p.sleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = min(2*backoff, p.maxBackoff)
		if err := p.maybeCheckPartitions(ctx); err != nil {
			return err
		}
		p.idx = p.retrySuffix(b, p.idx)
	}
}

// retrySuffix filtra idx (o último envio, na ordem original) para o sufixo
// de cada partição com falha, a partir do primeiro registro falho dela.
func (p *Publisher) retrySuffix(b *outBatch, idx []int) []int {
	p.firstFail = p.firstFail[:0]
	for k, i := range idx {
		part := int(b.recs[i].part)
		for len(p.firstFail) <= part {
			p.firstFail = append(p.firstFail, -1)
		}
		if b.results[i] != nil && p.firstFail[part] < 0 {
			p.firstFail[part] = k
		}
	}
	out := idx[:0]
	for k, i := range idx {
		if f := p.firstFail[b.recs[i].part]; f >= 0 && k >= f {
			out = append(out, i)
		}
	}
	return out
}

// render escreve a Entry em b: o retain vira um registro de controle por
// partição; o resto, um registro de dado na partição da key. data diz se foi
// registro de dado, e part é a partição dele. Em erro, b volta ao estado de
// entrada.
func (p *Publisher) render(b *outBatch, e replication.Entry, snapshot bool, hdr replication.SnapshotHeader, n int) (part int32, data bool, err error) {
	start := len(b.buf)
	if e.Operation == replication.Retain {
		if snapshot {
			b.buf, err = p.renderer.AppendSnapshot(b.buf, hdr.Epoch, hdr.Cursor, hdr.At, e)
		} else {
			b.buf, err = p.renderer.AppendEntry(b.buf, p.epoch, e)
		}
		if err != nil {
			return 0, false, err
		}
		for part := range n {
			b.addControl(OpRetain, int32(part), start, len(b.buf))
		}
		return 0, false, nil
	}
	var op Op
	if snapshot {
		op, err = SnapshotEntryOp(e.Operation)
	} else {
		op, err = EntryOp(e.Operation)
	}
	if err != nil {
		return 0, false, err
	}
	b.buf, err = p.renderer.KafkaKey(b.buf, e)
	if err != nil {
		b.buf = b.buf[:start]
		return 0, false, err
	}
	keyEnd := len(b.buf)
	if snapshot {
		b.buf, err = p.renderer.AppendSnapshot(b.buf, hdr.Epoch, hdr.Cursor, hdr.At, e)
	} else {
		b.buf, err = p.renderer.AppendEntry(b.buf, p.epoch, e)
	}
	if err != nil {
		b.buf = b.buf[:start]
		return 0, false, err
	}
	// A key é um recorte NÃO nulo do buffer, mesmo vazia: o particionador de
	// key do kgo só faz hash de key não nula (a nula iria para uma partição
	// qualquer).
	p.hashRec.Key = b.buf[start:keyEnd:keyEnd]
	part = int32(p.hasher.Partition(&p.hashRec, n))
	p.hashRec.Key = nil
	b.recs = append(b.recs, outRecord{
		op: op, part: part, keyStart: start, keyEnd: keyEnd, valEnd: len(b.buf),
		sequence: e.Sequence, table: e.Table,
	})
	return part, true, nil
}

// checkSizes confere todo registro de b contra maxMessageBytes ANTES de
// produzir qualquer um deles. Estourou: blocked, sem produzir aquele registro
// nem nada depois dele. O kgo recusaria só o registro grande e deixaria os
// seguintes passarem, e o tópico ficaria com um buraco que o consumidor não
// enxerga.
func (p *Publisher) checkSizes(b *outBatch) error {
	for i := range b.recs {
		r := &b.recs[i]
		key := b.key(r)
		size := int64(len(key)) + int64(r.valEnd-r.valueStart()) +
			int64(len(HeaderOp)+len(OpHeader(r.op))) + recordOverheadBytes
		if size <= p.messageBytes {
			continue
		}
		err := fmt.Errorf("replicationkafka: record of %d bytes exceeds maxMessageBytes %d", size, p.messageBytes)
		p.mu.Lock()
		p.stats.State = StateBlocked
		p.stats.LastError = err.Error()
		p.stats.Errors[ErrorMessageTooLarge]++
		p.mu.Unlock()
		p.log.Error("replication.kafka.blocked",
			"record exceeds the destination message limit; the publisher stopped without skipping it. Raise kafkaTargets[].maxMessageBytes and the topic max.message.bytes, then restart",
			"key", string(key[:min(len(key), blockedKeyPreview)]), "size", size, "limit", p.messageBytes,
			"sequence", r.sequence, "table", r.table, "op", r.op.String())
		return &haltError{err: err}
	}
	return nil
}

// halt para o publicador em estado terminal, com o ERROR que nomeia a
// Entry.
func (p *Publisher) halt(state, reason string, err error, sequence uint64, table string) error {
	p.mu.Lock()
	p.stats.State = state
	p.stats.LastError = err.Error()
	p.stats.Errors[reason]++
	p.mu.Unlock()
	p.log.Error("replication.kafka.failed",
		"feed entry cannot be rendered; the publisher stopped and ingestion continues",
		"sequence", sequence, "table", table, "error", err.Error())
	return &haltError{err: err}
}

// partitions consulta o número de partições do tópico. Tópico ausente é
// falha de infraestrutura: o publicador nunca cria tópico.
func (p *Publisher) partitions(ctx context.Context) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	topics, err := p.admin.ListTopics(cctx, p.topic)
	if err != nil {
		return 0, &deliveryError{reason: errorReason(err), err: fmt.Errorf("list topic %q: %w", p.topic, err)}
	}
	d, ok := topics[p.topic]
	switch {
	case !ok:
		return 0, &deliveryError{reason: ErrorUnavailable, err: fmt.Errorf("topic %q not found", p.topic)}
	case d.Err != nil:
		return 0, &deliveryError{reason: errorReason(d.Err), err: fmt.Errorf("topic %q: %w", p.topic, d.Err)}
	case len(d.Partitions) == 0:
		return 0, &deliveryError{reason: ErrorUnavailable, err: fmt.Errorf("topic %q has no partitions", p.topic)}
	}
	return len(d.Partitions), nil
}

// maybeCheckPartitions faz a conferência periódica do número de partições.
// A falha da consulta não é transição: a próxima conferência tenta de novo,
// e quem diz se o broker está fora é a entrega.
func (p *Publisher) maybeCheckPartitions(ctx context.Context) error {
	now := time.Now()
	if now.Before(p.nextCheck) {
		return nil
	}
	p.nextCheck = now.Add(p.checkEvery)
	n, err := p.partitions(ctx)
	if err != nil {
		p.log.Debug("replication.kafka.partitions.unknown", "topic partition check failed", "error", err.Error())
		return nil
	}
	p.log.Debug("replication.kafka.partitions.checked", "topic partition count checked", "partitions", n)
	if pinned := int(p.pinned.Load()); n != pinned {
		p.log.Warn("replication.kafka.partitions.changed", "topic partition count changed; publishing a new snapshot",
			"partitions", n, "pinned", pinned)
		return errPartitionsChanged
	}
	return nil
}

// awaitProducerMetadata espera o metadado do produtor conhecer as n
// partições fixadas antes do reset. Sem isso, logo depois de o tópico
// crescer, o reset para a partição nova falharia pelo metadado velho do kgo
// (até MetadataMaxAge) e cada tentativa publicaria um reset parcial. É
// limitado: vencido o prazo, segue, e a falha cai na retentativa normal.
func (p *Publisher) awaitProducerMetadata(ctx context.Context, n int) {
	if p.kcl == nil {
		return
	}
	// Tópico ainda não carregado pelo produtor: o primeiro Produce carrega o
	// metadado fresco.
	if leader, _, _ := p.kcl.PartitionLeader(p.topic, 0); leader < 0 {
		return
	}
	deadline := time.Now().Add(metadataWait)
	for {
		if leader, _, _ := p.kcl.PartitionLeader(p.topic, int32(n-1)); leader >= 0 {
			return
		}
		p.kcl.ForceMetadataRefresh()
		if time.Now().After(deadline) || !p.sleep(ctx, 20*time.Millisecond) {
			return
		}
	}
}

// noteFailure registra a falha de infraestrutura: estado retrying, o motivo
// contado e o WARN limitado a um a cada retryWarnInterval.
func (p *Publisher) noteFailure(err error) {
	reason := ErrorPublish
	var de *deliveryError
	if errors.As(err, &de) {
		reason = de.reason
	}
	p.mu.Lock()
	p.stats.State = StateRetrying
	p.stats.LastError = err.Error()
	p.stats.Errors[reason]++
	count := p.stats.Errors[reason]
	p.mu.Unlock()
	now := time.Now()
	if p.lastWarn.IsZero() || now.Sub(p.lastWarn) >= retryWarnInterval {
		p.lastWarn = now
		p.log.Warn("replication.kafka.retrying", "delivery to Kafka failed; retrying",
			"reason", reason, "errors", count, "cursor", p.cursor, "error", err.Error())
	}
}

// noteSnapshotGap registra o snapshot abortado por lacuna: o motivo
// snapshot_gap contado, o LastError e o ERROR de livelock.
func (p *Publisher) noteSnapshotGap(err error) {
	p.mu.Lock()
	p.stats.LastError = err.Error()
	p.stats.Errors[ErrorSnapshotGap]++
	p.mu.Unlock()
	p.logLivelock("snapshot", time.Since(p.snapshotStarted))
}

// logLivelock emite o ERROR do feed pequeno demais para um snapshot, no
// máximo um a cada retryWarnInterval. stage diz onde o cursor se perdeu: no
// meio do snapshot, ou entre o snapshot_end e o primeiro lote do tail.
func (p *Publisher) logLivelock(stage string, d time.Duration) {
	now := time.Now()
	if !p.lastLivelock.IsZero() && now.Sub(p.lastLivelock) < retryWarnInterval {
		return
	}
	p.lastLivelock = now
	p.log.Error("replication.kafka.livelock",
		"the feed evicted the snapshot cursor; the publisher will snapshot again and may never catch up. Raise replication.maxBufferedBytes (and maxBatchBytes) so the feed retains the changes of a whole snapshot",
		"stage", stage, "cursor", p.cursor, "snapshot_duration", d.String(), "reason", ResetGap.String())
}

func (p *Publisher) setState(s string) {
	p.mu.Lock()
	p.stats.State = s
	p.mu.Unlock()
}

// sleep espera d ou o cancelamento; false é cancelamento.
func (p *Publisher) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
