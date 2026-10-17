package replicationkafka

import (
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// resetHeaderCases é a tabela de headers do reset como LITERAL: os dez motivos
// do contrato e o valor do nishidb-reason de cada um. A tabela do código
// (resetHeaders) é o que está sob prova, nunca a régua.
var resetHeaderCases = []struct {
	reason ResetReason
	name   string
}{
	{ResetStartup, "startup"},
	{ResetGap, "gap"},
	{ResetSnapshotFailed, "snapshot_failed"},
	{ResetPartitionsChanged, "partitions_changed"},
	{ResetCrash, "crash"},
	{ResetRewind, "rewind"},
	{ResetRetention, "retention"},
	{ResetConfigChanged, "config_changed"},
	{ResetRequested, "requested"},
	{ResetPeriodic, "periodic"},
}

// Cada motivo sai com os dois headers certos: nishidb-op=reset e
// nishidb-reason=<motivo>. O registro de dado e os outros marcadores
// continuam com um header só.
func TestResetHeaderTableCarriesEveryReason(t *testing.T) {
	var b outBatch
	for _, c := range resetHeaderCases {
		b.addReset(c.reason, 0, 0, 0)
	}
	for i, c := range resetHeaderCases {
		got := b.recs[i].headers()
		want := []kgo.RecordHeader{
			{Key: "nishidb-op", Value: []byte("reset")},
			{Key: "nishidb-reason", Value: []byte(c.name)},
		}
		if !slices.EqualFunc(got, want, func(a, b kgo.RecordHeader) bool {
			return a.Key == b.Key && string(a.Value) == string(b.Value)
		}) {
			t.Errorf("motivo %q: headers %v, quer %v", c.name, got, want)
		}
	}
	for _, op := range []Op{OpInsert, OpUpdate, OpDelete, OpSnapshot, OpRetain, OpSnapshotEnd, OpResume} {
		r := outRecord{op: op, reason: ResetPeriodic}
		got := r.headers()
		if len(got) != 1 || got[0].Key != "nishidb-op" || string(got[0].Value) != op.String() {
			t.Errorf("op %q: headers %v, quer só nishidb-op=%q", op, got, op)
		}
	}
}

// O checkSizes soma os bytes do header de motivo: o registro que cabe por um
// byte no limite com os dois headers estoura se o limite cair um byte, e o
// reset com motivo comprido pesa mais que o de motivo curto. Sem a soma, o
// publicador produziria um reset que o broker recusa, e o tópico ficaria com
// um buraco no meio do snapshot.
func TestCheckSizesCountsReasonHeader(t *testing.T) {
	newPublisher := func(limit int64) *Publisher {
		return &Publisher{
			log:          logging.Discard(),
			messageBytes: limit,
			stats:        Stats{Errors: map[string]uint64{ErrorMessageTooLarge: 0}},
		}
	}
	body := strings.Repeat("x", 100)
	batch := func(reason ResetReason) *outBatch {
		b := &outBatch{buf: []byte(body)}
		b.addReset(reason, 0, 0, len(b.buf))
		return b
	}
	// chave de controle + corpo + "nishidb-op"+"reset" + "nishidb-reason"+motivo
	// + folga do enquadramento, tudo contado à mão.
	size := func(reasonName string) int64 {
		return int64(len(ControlKey)) + int64(len(body)) +
			int64(len("nishidb-op")+len("reset")) + int64(len("nishidb-reason")+len(reasonName)) +
			recordOverheadBytes
	}

	for _, c := range []struct {
		reason ResetReason
		name   string
	}{{ResetGap, "gap"}, {ResetPartitionsChanged, "partitions_changed"}} {
		exact := size(c.name)
		if err := newPublisher(exact).checkSizes(batch(c.reason)); err != nil {
			t.Errorf("reset %q com limite %d (o tamanho exato) foi recusado: %v", c.name, exact, err)
		}
		pub := newPublisher(exact - 1)
		if err := pub.checkSizes(batch(c.reason)); err == nil {
			t.Errorf("reset %q com limite %d (um byte abaixo) devia estourar: o header de motivo não foi contado", c.name, exact-1)
		} else if pub.stats.State != StateBlocked || pub.stats.Errors[ErrorMessageTooLarge] != 1 {
			t.Errorf("reset %q: state=%q errors=%v, quer blocked/message_too_large=1", c.name, pub.stats.State, pub.stats.Errors)
		}
	}
	if size("gap") == size("partitions_changed") {
		t.Fatal("o caso não discrimina: os dois motivos pesam igual")
	}
}
