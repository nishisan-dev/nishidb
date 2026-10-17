package replicationkafka

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"syscall"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// partitionerFor é o particionador do kgo. A partição de TODO registro é
// escolhida pelo publicador antes do Produce — a de controle, explícita; a de
// dado, pelo murmur2 com o número de partições FIXADO no último reset — e o
// kgo só a devolve: ele não honra r.Partition sozinho.
//
// Escolher antes, e não aqui, faz a contagem do snapshot_end e o agrupamento
// da reprodução por partição usarem a MESMA decisão que o kgo aplica. O kgo
// pode particionar mais tarde (tópico ainda sem metadado), quando o número
// fixado já seria outro.
//
// n é o número de partições que o METADADO do kgo conhece. Menor que o
// fixado é metadado velho (a partição nova ainda não chegou ao cache do kgo):
// o registro fora da faixa falha, staleMetadata pede um refresh, e a falha
// segue o caminho normal de retentativa. A mudança real do número de
// partições é a conferência pelo kadm quem detecta, não este caminho.
func (p *Publisher) partitionerFor(string) func(*kgo.Record, int) int {
	return func(r *kgo.Record, n int) int {
		if int(r.Partition) >= n {
			p.staleMetadata.Store(true)
		}
		return int(r.Partition)
	}
}

// opHeaders é o header de cada op, compartilhado por todos os registros: o
// kgo só o lê.
var opHeaders = func() [len(opNames)][]kgo.RecordHeader {
	var h [len(opNames)][]kgo.RecordHeader
	for op := OpInsert; int(op) < len(opNames); op++ {
		h[op] = []kgo.RecordHeader{{Key: HeaderOp, Value: OpHeader(op)}}
	}
	return h
}()

// resetHeaders é o header de cada motivo de reset, indexado pelo ResetReason:
// o reset sai com DOIS headers, nishidb-op e nishidb-reason. Pré-montada como
// opHeaders, para o envio não alocar um slice de headers por registro. O
// índice 0 (motivo inválido) fica nil: o AppendReset recusa o motivo antes de
// o registro existir.
var resetHeaders = func() [len(resetReasonJSON)][]kgo.RecordHeader {
	var h [len(resetReasonJSON)][]kgo.RecordHeader
	for reason := ResetStartup; int(reason) < len(resetReasonJSON); reason++ {
		h[reason] = []kgo.RecordHeader{
			{Key: HeaderOp, Value: OpHeader(OpReset)},
			{Key: HeaderReason, Value: ReasonHeader(reason)},
		}
	}
	return h
}()

// outRecord é um registro de um lote. Key e valor moram no buffer do lote,
// por deslocamento, porque o buffer cresce enquanto o lote é montado.
type outRecord struct {
	rec kgo.Record
	op  Op
	// reason só vale quando op == OpReset: escolhe a linha de resetHeaders.
	reason ResetReason
	part   int32
	// keyStart < 0 marca o registro de controle, cuja chave é ControlKey e
	// cujo valor começa em -keyStart-1.
	keyStart, keyEnd, valEnd int
	// sequence e table identificam a Entry nos logs.
	sequence uint64
	table    string
}

// headers devolve os headers Kafka do registro: dois (op e motivo) no reset,
// um (op) nos demais. O slice é compartilhado e o kgo só o lê.
func (r *outRecord) headers() []kgo.RecordHeader {
	if r.op == OpReset {
		return resetHeaders[r.reason]
	}
	return opHeaders[r.op]
}

// headerBytes é o tamanho dos headers do registro no fio (chave e valor de
// cada um), para o checkSizes.
func (r *outRecord) headerBytes() int {
	n := 0
	for _, h := range r.headers() {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

func (r *outRecord) valueStart() int {
	if r.keyStart < 0 {
		return -r.keyStart - 1
	}
	return r.keyEnd
}

// outBatch é um lote em montagem ou em voo. Nada de buf, recs ou results é
// reaproveitado antes de todas as promises do envio voltarem.
type outBatch struct {
	buf     []byte
	recs    []outRecord
	results []error
	// sent são os índices do envio corrente, na ordem do envio.
	sent     []int
	promises []func(*kgo.Record, error)
	// remaining e done são o contador do envio corrente: a última promise
	// fecha done.
	remaining atomic.Int64
	done      chan struct{}
	inflight  bool
}

func (b *outBatch) reset() {
	b.buf = b.buf[:0]
	b.recs = b.recs[:0]
}

func (b *outBatch) addControl(op Op, part int32, start, end int) {
	b.recs = append(b.recs, outRecord{op: op, part: part, keyStart: -start - 1, valEnd: end})
}

// addReset registra o marcador de reset de uma partição, com o motivo que
// escolhe o header nishidb-reason.
func (b *outBatch) addReset(reason ResetReason, part int32, start, end int) {
	b.recs = append(b.recs, outRecord{op: OpReset, reason: reason, part: part, keyStart: -start - 1, valEnd: end})
}

func (b *outBatch) key(r *outRecord) []byte {
	if r.keyStart < 0 {
		return ControlKey
	}
	return b.buf[r.keyStart:r.keyEnd:r.keyEnd]
}

// promise devolve a promise do índice i. Cada índice tem UMA closure,
// criada na primeira vez e reaproveitada: o caminho quente não aloca uma
// promise por registro.
func (b *outBatch) promise(i int) func(*kgo.Record, error) {
	for len(b.promises) <= i {
		j := len(b.promises)
		b.promises = append(b.promises, func(_ *kgo.Record, err error) {
			b.results[j] = err
			if b.remaining.Add(-1) == 0 {
				close(b.done)
			}
		})
	}
	return b.promises[i]
}

// send produz os registros de b nos índices idx (nil = todos), na ordem, de
// forma assíncrona. O Produce bloqueia com o buffer do produtor cheio, e é
// isso que dá ritmo ao publicador.
func (p *Publisher) send(ctx context.Context, b *outBatch, idx []int) {
	b.sent = b.sent[:0]
	if idx == nil {
		for i := range b.recs {
			b.sent = append(b.sent, i)
		}
	} else {
		b.sent = append(b.sent, idx...)
	}
	for len(b.results) < len(b.recs) {
		b.results = append(b.results, nil)
	}
	for _, i := range b.sent {
		b.results[i] = nil
	}
	b.done = make(chan struct{})
	b.inflight = true
	b.remaining.Store(int64(len(b.sent)))
	if len(b.sent) == 0 {
		close(b.done)
		return
	}
	for _, i := range b.sent {
		// Os promises do envio anterior já voltaram (é a regra do lote), então
		// o índice i pode ser reescrito: nenhum outro envio o referencia.
		r := &b.recs[i]
		b.promise(i)
		// O kgo escreve no Record (tópico, offset, producer id): todo envio
		// parte de um Record limpo.
		r.rec = kgo.Record{
			Key:       b.key(r),
			Value:     b.buf[r.valueStart():r.valEnd:r.valEnd],
			Headers:   r.headers(),
			Partition: r.part,
		}
		p.client.Produce(ctx, &r.rec, b.promises[i])
	}
}

// collect espera as promises do envio corrente de b e apura o resultado.
// Devolve nil com todos confirmados; senão um *deliveryError com o motivo da
// primeira falha, na ordem do envio. Com ctx cancelado, volta sem esperar:
// o laço vai sair e nada de b é reaproveitado.
func (p *Publisher) collect(ctx context.Context, b *outBatch) error {
	select {
	case <-b.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.inflight = false
	var acked uint64
	var first error
	for _, i := range b.sent {
		if b.results[i] == nil {
			acked++
		} else if first == nil {
			first = b.results[i]
		}
	}
	p.mu.Lock()
	p.stats.Delivered += acked
	p.mu.Unlock()
	if first == nil {
		return nil
	}
	if p.staleMetadata.Swap(false) && p.kcl != nil {
		p.kcl.ForceMetadataRefresh()
	}
	return &deliveryError{reason: errorReason(first), err: first}
}

// collectAll espera os blocos do snapshot ainda em voo.
func (p *Publisher) collectAll(ctx context.Context) error {
	for i := range p.chunks {
		if p.chunks[i].inflight {
			if err := p.collect(ctx, &p.chunks[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// abandon espera, sem apurar, os blocos em voo de um snapshot que falhou,
// para que o próximo não reaproveite buffer com promise pendente.
func (p *Publisher) abandon(ctx context.Context) {
	for i := range p.chunks {
		if !p.chunks[i].inflight {
			continue
		}
		select {
		case <-p.chunks[i].done:
			p.chunks[i].inflight = false
		case <-ctx.Done():
			return
		}
	}
}

// errorReason classifica a falha de entrega num motivo de Stats.Errors.
func errorReason(err error) string {
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrRecordTimeout),
		errors.Is(err, kerr.RequestTimedOut), errors.As(err, &netErr) && netErr.Timeout():
		return ErrorTimeout
	case errors.Is(err, kerr.MessageTooLarge), errors.Is(err, kerr.RecordListTooLarge):
		return ErrorMessageTooLarge
	case errors.As(err, &netErr), errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, io.EOF),
		errors.Is(err, kgo.ErrRecordRetries),
		errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.LeaderNotAvailable),
		errors.Is(err, kerr.NotLeaderForPartition), errors.Is(err, kerr.BrokerNotAvailable),
		errors.Is(err, kerr.NotEnoughReplicas), errors.Is(err, kerr.NotEnoughReplicasAfterAppend):
		return ErrorUnavailable
	default:
		return ErrorPublish
	}
}
