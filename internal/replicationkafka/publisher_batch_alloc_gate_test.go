//go:build !race

package replicationkafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestRecordHeadersAllocateNothing é o gate de zero alocação do header por
// registro: headers() e headerBytes() rodam em CADA registro do envio e do
// checkSizes, e devolvem fatias das tabelas pré-montadas (opHeaders e
// resetHeaders), sem montar um slice de headers por chamada.
//
// A passada cobre os dois caminhos — o reset, com dois headers, e o registro
// de dado, com um — porque o gate só protege o que exercita. Antes de medir,
// confere que os headers certos saem: o zero não prova nada se o caminho
// devolver vazio.
func TestRecordHeadersAllocateNothing(t *testing.T) {
	reset := outRecord{op: OpReset, reason: ResetPeriodic}
	insert := outRecord{op: OpInsert}

	if h := reset.headers(); len(h) != 2 || h[0].Key != HeaderOp || string(h[0].Value) != "reset" ||
		h[1].Key != HeaderReason || string(h[1].Value) != "periodic" {
		t.Fatalf("headers do reset = %v; quer nishidb-op=reset e nishidb-reason=periodic", h)
	}
	if h := insert.headers(); len(h) != 1 || h[0].Key != HeaderOp || string(h[0].Value) != "insert" {
		t.Fatalf("headers do insert = %v; quer só nishidb-op=insert", h)
	}
	if n := reset.headerBytes(); n != len("nishidb-op")+len("reset")+len("nishidb-reason")+len("periodic") {
		t.Fatalf("headerBytes do reset = %d", n)
	}

	var sink []kgo.RecordHeader
	var total int
	allocs := testing.AllocsPerRun(200, func() {
		sink = reset.headers()
		total += reset.headerBytes()
		sink = insert.headers()
		total += insert.headerBytes()
	})
	_, _ = sink, total
	if allocs != 0 {
		t.Errorf("headers()/headerBytes() alocaram %.1f vezes por passada; quer 0", allocs)
	}
}
