package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// goldenFrameHex é o quadro de sampleBatch gerado por um oráculo INDEPENDENTE
// do código (script Python com CRC32C bit a bit, validado contra o vetor
// "123456789" → 0xE3069283). O literal não sai de appendFrame: se saísse, a
// troca de dois campos de ordem mudaria o golden junto com o código e o teste
// concordaria consigo mesmo.
//
// Leitura, em little-endian:
//
//	4e444652              magic "NDFR"
//	9e000000              bodyLen = 158
//	bfea3808              crc32c do corpo
//	-- corpo --
//	0100000000000000      From = 1
//	0200000000000000      To = 2
//	05002a36fe9c9717      At = 1700000000000000005
//	2c01000000000000      EndPos = 300
//	08 696e74657276616c   reasonLen + "interval"
//	02000000              count = 2
//	-- entrada 1 (Update) --
//	0100000000000000      Sequence = 1
//	02                    Operation = Update
//	0600 6576656e7473     tableLen + "events"
//	00cac471fe9c9717      At
//	0700000000000000      RowID = 7
//	2a00000000000000      RecordSequence = 42
//	01                    Flags
//	02000000 6b31         keyLen + "k1"
//	03000000 aabbcc       payloadLen + payload
//	-- entrada 2 (Retain) --
//	0200000000000000      Sequence = 2
//	06                    Operation = Retain
//	0e00 6576656e74735f686973746f7279   tableLen + "events_history"
//	945fadfe9c971700      At
//	0900000000000000      RowID = 9
//	ffffffffffffffff      RecordSequence = -1
//	00                    Flags
//	00000000              keyLen = 0
//	00000000              payloadLen = 0
const goldenFrameHex = "4e4446529e000000bfea38080100000000000000020000000000000005002a36fe9c97172c0100000000000008696e74657276616c0200000001000000000000000206006576656e747300cac471fe9c971707000000000000002a0000000000000001020000006b3103000000aabbcc0200000000000000060e006576656e74735f686973746f727900945fadfe9c97170900000000000000ffffffffffffffff000000000000000000"

// goldenSegmentHeaderHex: magic "NDBF", versão 1, 2 reservados, epoch
// 10..1f, FirstSequence = 21, StartPos = 4096 e zeros até 64 bytes.
const goldenSegmentHeaderHex = "4e44424601000000101112131415161718191a1b1c1d1e1f15000000000000000010000000000000000000000000000000000000000000000000000000000000"

const goldenFramePayloadOffset = 12 + 8 + 8 + 8 + 8 + 1 + 8 + 4 + 8 + 1 + 2 + 6 + 8 + 8 + 8 + 1 + 4 + 2 + 4

func sampleBatch() *Batch {
	return &Batch{
		From:   1,
		To:     2,
		At:     time.Unix(1700000000, 5),
		Reason: "interval",
		Entries: []Entry{
			{
				Sequence:  1,
				Table:     "events",
				Operation: Update,
				At:        time.Unix(1700000001, 0),
				Record:    types.Record{RowID: 7, Sequence: 42, Flags: 1, Key: []byte("k1"), Payload: []byte{0xAA, 0xBB, 0xCC}},
			},
			{
				Sequence:  2,
				Table:     "events_history",
				Operation: Retain,
				At:        time.Unix(1700000002, 0),
				Record:    types.Record{RowID: 9, Sequence: -1},
			},
		},
	}
}

// mustAppendFrame é appendFrame para lotes que cabem: o erro é bug do teste.
func mustAppendFrame(dst []byte, b *Batch, endPos uint64) []byte {
	out, err := appendFrame(dst, b, endPos)
	if err != nil {
		panic(err)
	}
	return out
}

func goldenFrame(t *testing.T) []byte {
	t.Helper()
	b, err := hex.DecodeString(goldenFrameHex)
	if err != nil {
		t.Fatalf("golden inválido: %v", err)
	}
	return b
}

// assertEntriesEqual compara entradas campo a campo (o tempo por Equal, pois
// a decodificação devolve UTC e o original pode ter Location local).
func assertEntriesEqual(t *testing.T, got, want []Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("entradas = %d, esperado %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Sequence != w.Sequence || g.Table != w.Table || g.Operation != w.Operation || !g.At.Equal(w.At) ||
			g.Record.RowID != w.Record.RowID || g.Record.Sequence != w.Record.Sequence || g.Record.Flags != w.Record.Flags ||
			!bytes.Equal(g.Record.Key, w.Record.Key) || !bytes.Equal(g.Record.Payload, w.Record.Payload) {
			t.Fatalf("entrada %d = %+v, esperado %+v", i, g, w)
		}
	}
}

func TestFrameGoldenBytes(t *testing.T) {
	want := goldenFrame(t)
	b := sampleBatch()
	got := mustAppendFrame(nil, b, 300)
	if !bytes.Equal(got, want) {
		t.Fatalf("quadro divergiu do golden:\n got %x\nwant %x", got, want)
	}
	if frameSize(b) != len(want) {
		t.Fatalf("frameSize = %d, esperado %d", frameSize(b), len(want))
	}

	f, n, err := decodeFrame(want)
	if err != nil {
		t.Fatalf("decodeFrame do golden: %v", err)
	}
	if n != len(want) {
		t.Fatalf("n = %d, esperado %d", n, len(want))
	}
	if f.From != 1 || f.To != 2 || f.EndPos != 300 || f.Reason != "interval" || !f.At.Equal(time.Unix(1700000000, 5)) {
		t.Fatalf("cabeçalho do quadro decodificado = %+v", f)
	}
	assertEntriesEqual(t, f.Entries, b.Entries)
}

func TestAppendFramePreservesDestinationPrefix(t *testing.T) {
	prefix := []byte("prefixo")
	got := mustAppendFrame(append([]byte(nil), prefix...), sampleBatch(), 300)
	if !bytes.HasPrefix(got, prefix) {
		t.Fatalf("o prefixo de dst foi alterado: %x", got[:len(prefix)])
	}
	if !bytes.Equal(got[len(prefix):], goldenFrame(t)) {
		t.Fatalf("o quadro depois do prefixo diverge do golden")
	}
}

func TestSegmentHeaderGoldenBytes(t *testing.T) {
	want, err := hex.DecodeString(goldenSegmentHeaderHex)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) != segmentHeaderSize {
		t.Fatalf("golden tem %d bytes, esperado %d", len(want), segmentHeaderSize)
	}
	h := segmentHeader{
		Epoch:         [16]byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f},
		FirstSequence: 21,
		StartPos:      4096,
	}
	if got := appendSegmentHeader(nil, h); !bytes.Equal(got, want) {
		t.Fatalf("cabeçalho divergiu do golden:\n got %x\nwant %x", got, want)
	}
	back, err := decodeSegmentHeader(want)
	if err != nil {
		t.Fatalf("decodeSegmentHeader do golden: %v", err)
	}
	if back != h {
		t.Fatalf("round trip = %+v, esperado %+v", back, h)
	}
	// Bytes depois dos 64 são do primeiro quadro: o decoder lê só o cabeçalho.
	if back, err = decodeSegmentHeader(append(append([]byte(nil), want...), 0xFF, 0xEE)); err != nil || back != h {
		t.Fatalf("decode com bytes extras = %+v, %v", back, err)
	}
}

func TestDecodeSegmentHeaderRejectsMalformedHeaders(t *testing.T) {
	good, err := hex.DecodeString(goldenSegmentHeaderHex)
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(f func(b []byte)) []byte {
		b := append([]byte(nil), good...)
		f(b)
		return b
	}
	cases := []struct {
		name string
		src  []byte
	}{
		{"empty", nil},
		{"short", good[:segmentHeaderSize-1]},
		{"wrong magic", mutate(func(b []byte) { b[0] = 'X' })},
		{"future version", mutate(func(b []byte) { b[4] = 2 })},
		{"version zero", mutate(func(b []byte) { b[4] = 0 })},
		{"reserved bytes used", mutate(func(b []byte) { b[7] = 1 })},
		{"padding used", mutate(func(b []byte) { b[63] = 1 })},
		{"first sequence zero", mutate(func(b []byte) { clear(b[24:32]) })},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeSegmentHeader(c.src); !errors.Is(err, errCorruptSegment) {
				t.Fatalf("err = %v, esperado errCorruptSegment", err)
			}
		})
	}
}

func TestEpochBytesRoundTripsAndRejectsMalformedEpochs(t *testing.T) {
	raw, err := epochBytes("101112131415161718191a1b1c1d1e1f")
	if err != nil {
		t.Fatal(err)
	}
	if raw != [16]byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f} {
		t.Fatalf("epochBytes = %x", raw)
	}
	if got := epochString(raw); got != "101112131415161718191a1b1c1d1e1f" {
		t.Fatalf("epochString = %q", got)
	}
	for _, bad := range []string{"", "zz", "1011", strings.Repeat("a", 34), strings.Repeat("g", 32)} {
		if _, err := epochBytes(bad); err == nil {
			t.Fatalf("epochBytes(%q) aceitou epoch malformada", bad)
		}
	}
}

// rebuildFrame monta um quadro com o CRC CERTO sobre um corpo qualquer: é
// como se fabrica uma incoerência interna que o CRC não enxerga.
func rebuildFrame(body []byte) []byte {
	out := []byte("NDFR")
	out = binary.LittleEndian.AppendUint32(out, uint32(len(body)))
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	return append(out, body...)
}

func TestDecodeFrameRejectsCorruption(t *testing.T) {
	good := goldenFrame(t)
	if good[goldenFramePayloadOffset] != 0xAA {
		t.Fatalf("goldenFramePayloadOffset não aponta para o payload do golden")
	}
	body := good[frameHeaderSize:]
	mutate := func(f func(b []byte)) []byte {
		b := append([]byte(nil), good...)
		f(b)
		return b
	}
	mutateBody := func(f func(b []byte) []byte) []byte {
		return rebuildFrame(f(append([]byte(nil), body...)))
	}

	type tc struct {
		name string
		src  []byte
	}
	var cases []tc
	// Um byte trocado em pontos distintos do corpo. O do payload (aabbcc)
	// decodificaria SEM erro se o CRC fosse ignorado: é o caso que mata o
	// mutante "ignora o CRC".
	for _, off := range []int{frameHeaderSize, frameHeaderSize + 40, goldenFramePayloadOffset, len(good) - 1} {
		cases = append(cases, tc{"flipped body byte at " + strconv.Itoa(off), mutate(func(b []byte) { b[off] ^= 0x01 })})
	}
	cases = append(cases,
		tc{"flipped crc", mutate(func(b []byte) { b[8] ^= 0x01 })},
		tc{"wrong magic", mutate(func(b []byte) { b[0] = 'X' })},
		tc{"bodyLen one past the rest", mutate(func(b []byte) { binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-frameHeaderSize+1)) })},
		tc{"bodyLen huge", mutate(func(b []byte) { binary.LittleEndian.PutUint32(b[4:], 0xFFFFFFFF) })},
		tc{"bodyLen shorter than the body", mutate(func(b []byte) { binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-frameHeaderSize-1)) })},
		// Com CRC certo: a incoerência é estrutural, só o parser pode vê-la.
		tc{"count above the entries present", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[8+8+8+8+1+8:], 3); return b })},
		tc{"count absurdly large", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[8+8+8+8+1+8:], 0xFFFFFFFF); return b })},
		tc{"count below the entries present", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[8+8+8+8+1+8:], 1); return b })},
		tc{"reasonLen past the body", mutateBody(func(b []byte) []byte { b[8+8+8+8] = 0xFF; return b })},
		tc{"trailing bytes inside the body", mutateBody(func(b []byte) []byte { return append(b, 0) })},
		tc{"body too short for the fixed header", mutateBody(func(b []byte) []byte { return b[:10] })},
		tc{"from above to", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint64(b[0:], 9); return b })},
	)
	// Truncamento em TODO comprimento possível, o que cobre cada fronteira de
	// campo do cabeçalho e do corpo.
	for n := 0; n < len(good); n++ {
		cases = append(cases, tc{"truncated to " + strconv.Itoa(n), good[:n]})
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := decodeFrame(c.src); !errors.Is(err, errCorruptFrame) {
				t.Fatalf("err = %v, esperado errCorruptFrame", err)
			}
		})
	}
}

func TestDecodeFrameStopsAtTheFrameBoundary(t *testing.T) {
	good := goldenFrame(t)
	two := append(append([]byte(nil), good...), good...)
	_, n, err := decodeFrame(two)
	if err != nil || n != len(good) {
		t.Fatalf("n = %d, err = %v; esperado %d e nil", n, err, len(good))
	}
	_, n2, err := decodeFrame(two[n:])
	if err != nil || n2 != len(good) {
		t.Fatalf("segundo quadro: n = %d, err = %v", n2, err)
	}
}

func TestFrameRoundTripPreservesEntries(t *testing.T) {
	big := bytes.Repeat([]byte{0x00, 0x01, 0x02, 0xFE, 0xFF}, (9<<20)/5+1) // > 8 MiB
	if len(big) <= 8<<20 {
		t.Fatalf("payload de teste deveria passar de 8 MiB, tem %d", len(big))
	}
	cases := []struct {
		name  string
		batch *Batch
	}{
		{"empty key and payload", &Batch{From: 5, To: 5, At: time.Unix(10, 1), Reason: "bytes", Entries: []Entry{
			{Sequence: 5, Table: "t", Operation: Delete, At: time.Unix(10, 1), Record: types.Record{RowID: 1}},
		}}},
		{"history table", &Batch{From: 1, To: 3, At: time.Unix(20, 0), Reason: "interval", Entries: []Entry{
			{Sequence: 1, Table: "events", Operation: Insert, At: time.Unix(19, 0), Record: types.Record{RowID: 1, Sequence: 1, Key: []byte("a"), Payload: []byte{1}}},
			{Sequence: 2, Table: "events_history", Operation: Insert, At: time.Unix(19, 1), Record: types.Record{RowID: 100, Sequence: 1, Flags: 4, Payload: []byte{9, 9}}},
			{Sequence: 3, Table: "events_history", Operation: Retain, At: time.Unix(19, 2), Record: types.Record{RowID: 100}},
		}}},
		{"single entry above 8 MiB", &Batch{From: 9, To: 9, At: time.Unix(30, 0), Reason: "bytes", Entries: []Entry{
			{Sequence: 9, Table: "events", Operation: Update, At: time.Unix(30, 0), Record: types.Record{RowID: 3, Sequence: 7, Key: []byte("big"), Payload: big}},
		}}},
		{"zero timestamps and extreme numbers", &Batch{From: 1 << 62, To: 1<<63 + 5, Reason: "", Entries: []Entry{
			{Sequence: 1<<63 + 5, Table: "", Operation: Reset, Record: types.Record{RowID: types.RowID(1<<64 - 1), Sequence: -1 << 63, Flags: 0xFF}},
		}}},
		{"longest reason", &Batch{From: 1, To: 1, At: time.Unix(1, 0), Reason: strings.Repeat("r", 255), Entries: []Entry{
			{Sequence: 1, Table: "t", Operation: Schema, At: time.Unix(1, 0), Record: types.Record{Key: []byte("x")}},
		}}},
		{"no entries", &Batch{From: 4, To: 4, At: time.Unix(1, 0), Reason: "rotate"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := mustAppendFrame(nil, c.batch, 123456789)
			if frameSize(c.batch) != len(src) {
				t.Fatalf("frameSize = %d, len(quadro) = %d", frameSize(c.batch), len(src))
			}
			f, n, err := decodeFrame(src)
			if err != nil {
				t.Fatalf("decodeFrame: %v", err)
			}
			if n != len(src) {
				t.Fatalf("n = %d, esperado %d", n, len(src))
			}
			if f.From != c.batch.From || f.To != c.batch.To || f.EndPos != 123456789 || f.Reason != c.batch.Reason || !f.At.Equal(c.batch.At) {
				t.Fatalf("cabeçalho = %+v", f)
			}
			assertEntriesEqual(t, f.Entries, c.batch.Entries)
		})
	}
}

// TestFrameRoundTripPreservesZeroTime: o instante zero de time.Time não cabe
// em UnixNano; sem o sentinela ele viraria uma data qualquer do século XIX.
func TestFrameRoundTripPreservesZeroTime(t *testing.T) {
	b := &Batch{From: 1, To: 1, Entries: []Entry{{Sequence: 1, Table: "t", Operation: Insert}}}
	f, _, err := decodeFrame(mustAppendFrame(nil, b, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !f.At.IsZero() || !f.Entries[0].At.IsZero() {
		t.Fatalf("instante zero não sobreviveu: frame %v, entrada %v", f.At, f.Entries[0].At)
	}
}

// TestDecodeFrameNeverAliasesTheSource trava a garantia do godoc: o leitor
// reaproveita o buffer de leitura, e uma key que apontasse para ele mudaria
// debaixo do consumidor.
func TestDecodeFrameNeverAliasesTheSource(t *testing.T) {
	src := goldenFrame(t)
	f, _, err := decodeFrame(src)
	if err != nil {
		t.Fatal(err)
	}
	for i := range src {
		src[i] = 0xEE
	}
	assertEntriesEqual(t, f.Entries, sampleBatch().Entries)
	if f.Reason != "interval" || f.Entries[0].Table != "events" || f.Entries[1].Table != "events_history" {
		t.Fatalf("strings alteradas pela reutilização do buffer: %q %q %q", f.Reason, f.Entries[0].Table, f.Entries[1].Table)
	}
}

// TestDecodeFrameArenaSlicesAreCapped: appendar à key de uma entrada não pode
// escrever na vizinha, que mora na mesma arena.
func TestDecodeFrameArenaSlicesAreCapped(t *testing.T) {
	f, _, err := decodeFrame(goldenFrame(t))
	if err != nil {
		t.Fatal(err)
	}
	k := f.Entries[0].Record.Key
	_ = append(k, 'Z', 'Z', 'Z', 'Z')
	if !bytes.Equal(f.Entries[0].Record.Payload, []byte{0xAA, 0xBB, 0xCC}) {
		t.Fatalf("append na key sobrescreveu o payload vizinho: %x", f.Entries[0].Record.Payload)
	}
}

func TestAppendFrameRejectsFieldsThatDoNotFitTheirLengthPrefix(t *testing.T) {
	cases := map[string]*Batch{
		"reason above 255 bytes":       {Reason: strings.Repeat("r", 256)},
		"table above 65535 bytes":      {Entries: []Entry{{Table: strings.Repeat("t", 65536)}}},
		"second entry's table too big": {Entries: []Entry{{Table: "ok"}, {Table: strings.Repeat("t", 65536)}}},
	}
	for name, b := range cases {
		prefix := []byte("keep")
		got, err := appendFrame(prefix, b, 1)
		if !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("%s: err = %v, esperado errFrameTooLarge", name, err)
		}
		if string(got) != "keep" {
			t.Fatalf("%s: dst foi alterado apesar do erro: %q", name, got)
		}
	}
	// No limite exato ainda cabe.
	if _, err := appendFrame(nil, &Batch{Reason: strings.Repeat("r", 255), Entries: []Entry{{Table: strings.Repeat("t", 65535)}}}, 1); err != nil {
		t.Fatalf("limite exato recusado: %v", err)
	}
}

// TestFrameBatchRebuildsWhatTheFrameDoesNotStore: Epoch, Messages e bytes não
// vão para o disco; batch() os reconstrói. Messages = To - From + 1.
func TestFrameBatchRebuildsWhatTheFrameDoesNotStore(t *testing.T) {
	f, _, err := decodeFrame(goldenFrame(t))
	if err != nil {
		t.Fatal(err)
	}
	b := f.batch("00112233445566778899aabbccddeeff")
	if b.Epoch != "00112233445566778899aabbccddeeff" || b.From != 1 || b.To != 2 || b.Messages != 2 || b.Reason != "interval" || !b.At.Equal(time.Unix(1700000000, 5)) {
		t.Fatalf("batch = %+v", b)
	}
	var want int64
	for _, e := range sampleBatch().Entries {
		want += entryBytes(e)
	}
	if b.bytes != want || want == 0 {
		t.Fatalf("bytes = %d, esperado %d", b.bytes, want)
	}
	assertEntriesEqual(t, b.Entries, sampleBatch().Entries)

	wide := &Batch{From: 10, To: 14, At: time.Unix(1, 0), Entries: []Entry{{Sequence: 14, Table: "t", Operation: Update}}}
	fw, _, err := decodeFrame(mustAppendFrame(nil, wide, 1))
	if err != nil {
		t.Fatal(err)
	}
	if got := fw.batch("e").Messages; got != 5 {
		t.Fatalf("Messages = %d, esperado 5 (To-From+1), mesmo com 1 entrada consolidada", got)
	}
}

// TestAppendFrameRejectsFieldsAboveTheUint32Prefix: key, payload e corpo acima
// de MaxUint32 não cabem nos prefixos de 4 bytes. As fatias de 4 GiB são
// memória virtual que ninguém toca (a validação só lê len), então o teste não
// consome RAM de verdade.
func TestAppendFrameRejectsFieldsAboveTheUint32Prefix(t *testing.T) {
	huge := make([]byte, math.MaxUint32+1)
	cases := map[string]*Batch{
		"key":     {Entries: []Entry{{Table: "t", Record: types.Record{Key: huge}}}},
		"payload": {Entries: []Entry{{Table: "t", Record: types.Record{Payload: huge}}}},
		// Cada campo cabe, mas a soma do corpo não.
		"body": {Entries: []Entry{
			{Table: "t", Record: types.Record{Payload: huge[:math.MaxUint32-100]}},
			{Table: "t", Record: types.Record{Payload: huge[:1000]}},
		}},
	}
	for name, b := range cases {
		if _, err := appendFrame(nil, b, 1); !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("%s: err = %v, esperado errFrameTooLarge", name, err)
		}
	}
}
