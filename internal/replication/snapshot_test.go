package replication

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// fakeRow é uma linha do fakeSource.
type fakeRow struct {
	key, payload []byte
	sequence     int64
}

// fakeSource imita o contrato do Store que o exportador usa: rowIds
// crescentes, teto e piso fixados na abertura do snapshot, e contagem de
// snapshots abertos para as garantias de ciclo de vida.
type fakeSource struct {
	t       *testing.T
	rows    map[types.RowID]fakeRow
	base    types.RowID
	next    types.RowID
	instant time.Time

	opened, closed int
	// open é quantos snapshots estão abertos agora.
	open int
	// readsPerSnapshot é quantas linhas o GetByRowID leu em cada snapshot,
	// na ordem da abertura.
	readsPerSnapshot []int

	// onOpen roda dentro do OpenSnapshot, DEPOIS de o snapshot ser fixado; n é
	// a ordem da abertura, a partir de 1.
	onOpen func(n int)
	// scanErr faz o iterador parar com erro depois de scanErrAfter linhas.
	scanErr      error
	scanErrAfter int
	// getErr faz o GetByRowID falhar na linha getErrAt.
	getErr   error
	getErrAt types.RowID
	// dead são linhas que o iterador entrega mas o GetByRowID declara mortas
	// (versão do snapshot com lápide ou vencida).
	dead map[types.RowID]bool
}

func newFakeSource(t *testing.T, rows int) *fakeSource {
	f := &fakeSource{t: t, rows: make(map[types.RowID]fakeRow), instant: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)}
	for range rows {
		f.add(fmt.Sprintf("k%03d", f.next), 7)
	}
	return f
}

func (f *fakeSource) add(key string, sequence int64) types.RowID {
	id := f.next
	f.rows[id] = fakeRow{key: []byte(key), payload: []byte("payload-" + key), sequence: sequence}
	f.next++
	return id
}

func (f *fakeSource) OpenSnapshot() types.Snapshot {
	f.opened++
	f.open++
	f.readsPerSnapshot = append(f.readsPerSnapshot, 0)
	snap := types.Snapshot{
		Epoch:      uint64(f.opened),
		Instant:    f.instant.Add(time.Duration(f.opened) * time.Second),
		LimitRowID: f.next,
		BaseRowID:  f.base,
	}
	if f.onOpen != nil {
		f.onOpen(f.opened)
	}
	return snap
}

func (f *fakeSource) CloseSnapshot(types.Snapshot) {
	f.closed++
	f.open--
	if f.open < 0 {
		f.t.Fatal("CloseSnapshot sem snapshot aberto")
	}
}

func (f *fakeSource) ScanLiveFrom(snap types.Snapshot, from, limit types.RowID) types.RowIDIterator {
	start, end := max(from, snap.BaseRowID), min(limit, snap.LimitRowID)
	it := &fakeIterator{err: f.scanErr, errAfter: f.scanErrAfter}
	for id := start; id < end; id++ {
		if _, ok := f.rows[id]; ok {
			it.ids = append(it.ids, id)
		}
	}
	return it
}

func (f *fakeSource) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if f.open == 0 {
		f.t.Error("GetByRowID sem snapshot aberto")
	}
	f.readsPerSnapshot[int(snap.Epoch)-1]++
	if f.getErr != nil && id == f.getErrAt {
		return types.Record{}, buf, false, f.getErr
	}
	r, ok := f.rows[id]
	if !ok || f.dead[id] {
		return types.Record{}, buf, false, nil
	}
	buf = append(buf[:0], r.key...)
	buf = append(buf, r.payload...)
	return types.Record{RowID: id, Sequence: r.sequence, Flags: 3, Key: buf[:len(r.key)], Payload: buf[len(r.key):]}, buf, true, nil
}

type fakeIterator struct {
	ids      []types.RowID
	pos      int
	cur      types.RowID
	err      error
	errAfter int
	failed   bool
}

func (it *fakeIterator) Next() bool {
	if it.err != nil && it.pos >= it.errAfter {
		it.failed = true
		return false
	}
	if it.pos >= len(it.ids) {
		return false
	}
	it.cur = it.ids[it.pos]
	it.pos++
	return true
}

func (it *fakeIterator) RowID() types.RowID { return it.cur }

func (it *fakeIterator) Err() error {
	if it.failed {
		return it.err
	}
	return nil
}

func (it *fakeIterator) Close() error { return nil }

// copyEntry copia uma entrada emitida: Key e Payload são
// emprestados pelo exportador.
func copyEntry(e Entry) Entry {
	e.Record.Key = append([]byte(nil), e.Record.Key...)
	e.Record.Payload = append([]byte(nil), e.Record.Payload...)
	return e
}

func newTestFeed(t *testing.T) *Stream {
	t.Helper()
	return openTestFeed(t, Options{Interval: 0, MaxMessages: 100, MaxBatchBytes: 1 << 20, MaxBufferedBytes: 4 << 20})
}

type exportResult struct {
	header  SnapshotHeader
	begins  int
	entries []Entry
	err     error
}

func runExport(ctx context.Context, feed *Stream, tables []SnapshotTable, chunkRows int, onEmit func(Entry) error) exportResult {
	var out exportResult
	out.err = ExportSnapshot(ctx, feed, tables, chunkRows, func(h SnapshotHeader) error {
		out.header = h
		out.begins++
		return nil
	}, func(e Entry) error {
		out.entries = append(out.entries, copyEntry(e))
		if onEmit != nil {
			return onEmit(e)
		}
		return nil
	})
	return out
}

func requireBalanced(t *testing.T, f *fakeSource) {
	t.Helper()
	if f.open != 0 || f.opened != f.closed {
		t.Fatalf("snapshots abertos=%d, abertos no total=%d, fechados=%d: algum ficou aberto", f.open, f.opened, f.closed)
	}
}

func TestExportCapturesCursorBeforeScan(t *testing.T) {
	feed := newTestFeed(t)
	feed.Publish("events", Insert, rec(0, "antes"), time.Now())
	src := newFakeSource(t, 3)
	var hidden uint64
	src.onOpen = func(n int) {
		if n != 1 {
			return
		}
		// A mudança nasce depois do teto do primeiro snapshot: a linha não
		// entra na exportação, e só o tail pode entregá-la.
		id := src.add("nova", 9)
		hidden = feed.Publish("events", Insert, types.Record{RowID: id, Key: []byte("nova")}, time.Now())
	}
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 10, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if res.header.Cursor >= hidden {
		t.Fatalf("cursor=%d, quer < %d: a mudança publicada durante a exportação ficaria fora do tail", res.header.Cursor, hidden)
	}
	for _, e := range res.entries {
		if string(e.Record.Key) == "nova" {
			t.Fatal("a linha nascida depois do primeiro snapshot foi exportada")
		}
	}
}

// A posição em bytes do header é a do MESMO instante do cursor: um lote
// fechado durante a exportação avança a posição do feed, mas não a do
// header. Lida depois, ela contaria como entregue um lote que o tail ainda
// vai entregar, e o atraso em bytes do publicador sairia menor que o real.
func TestExportReadsEndPosTogetherWithTheCursor(t *testing.T) {
	feed := newTestFeed(t)
	feed.Publish("events", Insert, rec(0, "antes"), time.Now())
	feed.Flush(time.Now(), "test")
	_, cursor, endPos := feed.Position()
	if endPos == 0 {
		t.Fatal("o lote fechado não avançou a posição em bytes; o teste não prova nada")
	}
	src := newFakeSource(t, 3)
	src.onOpen = func(n int) {
		if n != 1 {
			return
		}
		feed.Publish("events", Insert, rec(1, "durante"), time.Now())
		feed.Flush(time.Now(), "test")
	}
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 10, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if _, _, now := feed.Position(); now <= endPos {
		t.Fatalf("a posição não avançou durante a exportação (%d ≤ %d); o teste não prova nada", now, endPos)
	}
	if res.header.Cursor != cursor || res.header.EndPos != endPos {
		t.Fatalf("header cursor=%d endPos=%d, quer %d e %d (lidos juntos, antes da varredura)",
			res.header.Cursor, res.header.EndPos, cursor, endPos)
	}
}

func TestExportFailsOnIteratorError(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 6)
	boom := errors.New("scan interrompido")
	src.scanErr, src.scanErrAfter = boom, 2
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 10, nil)
	if !errors.Is(res.err, boom) {
		t.Fatalf("err=%v, quer %v: uma varredura interrompida passaria por snapshot completo", res.err, boom)
	}
	requireBalanced(t, src)
}

func TestExportClosesSnapshotOnError(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 6)
	expired := errors.New("snapshot expirado")
	src.getErr, src.getErrAt = expired, 3
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 2, nil)
	if !errors.Is(res.err, expired) {
		t.Fatalf("err=%v, quer %v", res.err, expired)
	}
	if src.opened != 2 {
		t.Fatalf("snapshots abertos=%d, quer 2: o erro tinha de cair no segundo bloco", src.opened)
	}
	requireBalanced(t, src)
	if len(res.entries) != 2 {
		t.Fatalf("entradas emitidas=%d, quer só as 2 do primeiro bloco", len(res.entries))
	}
}

func TestExportStopsAndClosesSnapshotOnContextCancel(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	emitted := 0
	res := runExport(ctx, feed, []SnapshotTable{{Name: "events", Source: src}}, 3, func(Entry) error {
		emitted++
		if emitted == 2 {
			cancel()
		}
		return nil
	})
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("err=%v, quer context.Canceled", res.err)
	}
	if emitted != 2 {
		t.Fatalf("emitidas=%d depois do cancelamento, quer 2", emitted)
	}
	requireBalanced(t, src)
}

func TestExportAbortsOnBeginError(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 3)
	refused := errors.New("begin recusado")
	emitted := 0
	err := ExportSnapshot(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 10,
		func(SnapshotHeader) error { return refused },
		func(Entry) error { emitted++; return nil })
	if !errors.Is(err, refused) || emitted != 0 {
		t.Fatalf("err=%v emitidas=%d, quer %v e nenhuma emissão", err, emitted, refused)
	}
	requireBalanced(t, src)
}

func TestExportCallsBeginOnceBeforeAnyEmit(t *testing.T) {
	feed := newTestFeed(t)
	base, hist := newFakeSource(t, 3), newFakeSource(t, 2)
	begins := 0
	err := ExportSnapshot(context.Background(), feed,
		[]SnapshotTable{{Name: "events", Source: base}, {Name: "events_history", History: true, Source: hist}}, 2,
		func(SnapshotHeader) error { begins++; return nil },
		func(Entry) error {
			if begins != 1 {
				t.Fatalf("emit com begin chamado %d vezes, quer 1", begins)
			}
			return nil
		})
	if err != nil || begins != 1 {
		t.Fatalf("err=%v begins=%d", err, begins)
	}
}

func TestExportEmitsWithSnapshotClosed(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 7)
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 3, func(Entry) error {
		if src.open != 0 {
			t.Fatalf("emit rodou com %d snapshot(s) aberto(s)", src.open)
		}
		return nil
	})
	if res.err != nil || len(res.entries) != 7 {
		t.Fatalf("err=%v entradas=%d", res.err, len(res.entries))
	}
}

func TestChunkedExportBoundsRowsPerSnapshot(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 10)
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 3, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if src.opened < 4 {
		t.Fatalf("snapshots=%d, quer ≥ 4 para 10 linhas em blocos de 3", src.opened)
	}
	for i, n := range src.readsPerSnapshot {
		if n > 3 {
			t.Fatalf("o snapshot %d leu %d linhas, teto é 3", i+1, n)
		}
	}
	if len(res.entries) != 10 {
		t.Fatalf("entradas=%d, quer 10", len(res.entries))
	}
	for i, e := range res.entries {
		if e.Record.RowID != types.RowID(i) {
			t.Fatalf("entrada %d com rowID %d: bloco repetido ou pulado", i, e.Record.RowID)
		}
	}
	requireBalanced(t, src)
}

func TestExportStopsAtFirstSnapshotLimit(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 6)
	src.onOpen = func(n int) {
		if n > 1 {
			src.add(fmt.Sprintf("tarde%d", n), 1)
		}
	}
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 2, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if len(res.entries) != 6 {
		t.Fatalf("entradas=%d, quer as 6 abaixo do teto do primeiro snapshot", len(res.entries))
	}
	for _, e := range res.entries {
		if e.Record.RowID >= 6 {
			t.Fatalf("rowID %d ≥ teto do primeiro snapshot foi exportado", e.Record.RowID)
		}
	}
}

func TestExportSnapshotEntriesCarryCursorSequence(t *testing.T) {
	feed := newTestFeed(t)
	for i := range 3 {
		feed.Publish("events", Insert, rec(uint64(100+i), "x"), time.Now())
	}
	base, hist := newFakeSource(t, 5), newFakeSource(t, 2)
	res := runExport(context.Background(), feed,
		[]SnapshotTable{{Name: "events", Source: base}, {Name: "events_history", History: true, Source: hist}}, 2, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if res.header.Cursor != 3 {
		t.Fatalf("cursor=%d, quer 3", res.header.Cursor)
	}
	firstInstant := base.instant.Add(time.Second)
	if !res.header.At.Equal(firstInstant) {
		t.Fatalf("At=%v, quer o Instant do primeiro snapshot %v", res.header.At, firstInstant)
	}
	if len(res.entries) != 5+1+2 {
		t.Fatalf("entradas=%d, quer 8", len(res.entries))
	}
	for i, e := range res.entries {
		if e.Sequence != 3 {
			t.Fatalf("entrada %d com Sequence=%d, quer o cursor 3", i, e.Sequence)
		}
		if !e.At.Equal(firstInstant) {
			t.Fatalf("entrada %d com At=%v, quer %v para o snapshot inteiro", i, e.At, firstInstant)
		}
		if e.Operation == Retain {
			continue
		}
		if e.Operation != Insert || e.Record.Sequence != 7 || e.Record.Flags != 3 {
			t.Fatalf("entrada %d: op=%v seq=%d flags=%d, quer Insert 7 3", i, e.Operation, e.Record.Sequence, e.Record.Flags)
		}
		want := fmt.Sprintf("k%03d", e.Record.RowID)
		if string(e.Record.Key) != want || string(e.Record.Payload) != "payload-"+want {
			t.Fatalf("entrada %d: key=%q payload=%q", i, e.Record.Key, e.Record.Payload)
		}
	}
	if res.entries[0].Table != "events" || res.entries[5].Table != "events_history" || res.entries[5].Operation != Retain {
		t.Fatalf("ordem das tabelas errada: %+v", res.entries)
	}
}

func TestExportHistoryRetainUsesFirstSnapshotBase(t *testing.T) {
	feed := newTestFeed(t)
	hist := newFakeSource(t, 8)
	hist.base = 2
	// A retenção avança a cada snapshot aberto: o piso publicado tem de ser o
	// do snapshot que fixou o teto, e não o de uma leitura posterior.
	hist.onOpen = func(int) { hist.base++ }
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events_history", History: true, Source: hist}}, 2, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	first := res.entries[0]
	if first.Operation != Retain || first.Record.RowID != 2 || first.Table != "events_history" {
		t.Fatalf("primeira entrada=%+v, quer Retain com o piso 2 do primeiro snapshot", first)
	}
	for _, e := range res.entries[1:] {
		if e.Operation != Insert {
			t.Fatalf("Retain repetido ou fora do lugar: %+v", e)
		}
	}
}

func TestExportReusesChunkBuffers(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 6)
	var firstKeys []*byte
	var seen int
	err := ExportSnapshot(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 3,
		func(SnapshotHeader) error { return nil },
		func(e Entry) error {
			if seen%3 == 0 {
				firstKeys = append(firstKeys, &e.Record.Key[0])
			}
			seen++
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(firstKeys) != 2 || firstKeys[0] != firstKeys[1] {
		t.Fatal("o segundo bloco não reaproveitou o buffer do primeiro: a memória cresceria com o dataset")
	}
}

func TestExportWithoutTablesStillCallsBegin(t *testing.T) {
	feed := newTestFeed(t)
	res := runExport(context.Background(), feed, nil, 10, nil)
	if res.err != nil || res.begins != 1 || res.header.At.IsZero() || len(res.entries) != 0 {
		t.Fatalf("err=%v begins=%d at=%v entradas=%d", res.err, res.begins, res.header.At, len(res.entries))
	}
}

func TestExportRejectsInvalidArguments(t *testing.T) {
	feed := newTestFeed(t)
	noop := func(SnapshotHeader) error { return nil }
	emit := func(Entry) error { return nil }
	for name, err := range map[string]error{
		"sem feed":   ExportSnapshot(context.Background(), nil, nil, 1, noop, emit),
		"bloco zero": ExportSnapshot(context.Background(), feed, nil, 0, noop, emit),
		"sem begin":  ExportSnapshot(context.Background(), feed, nil, 1, nil, emit),
		"sem emit":   ExportSnapshot(context.Background(), feed, nil, 1, noop, nil),
	} {
		if !errors.Is(err, ErrInvalidSnapshotExport) {
			t.Errorf("%s: err=%v, quer ErrInvalidSnapshotExport", name, err)
		}
	}
}

func TestExportSkipsDeadRowsWithoutEndingOrStalling(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 12)
	// Blocos de 3: o primeiro tem uma morta, o segundo é INTEIRO de mortas.
	src.dead = map[types.RowID]bool{1: true, 3: true, 4: true, 5: true}
	// O prazo é a proteção contra o laço infinito de um bloco que não avança.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := runExport(ctx, feed, []SnapshotTable{{Name: "events", Source: src}}, 3, nil)
	if res.err != nil {
		t.Fatalf("err=%v: a exportação não terminou sobre um bloco de linhas mortas", res.err)
	}
	var got []types.RowID
	for _, e := range res.entries {
		got = append(got, e.Record.RowID)
	}
	want := []types.RowID{0, 2, 6, 7, 8, 9, 10, 11}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rowIds exportados=%v, quer as vivas %v", got, want)
	}
}

func TestExportUsesTheFirstCursorForEveryTable(t *testing.T) {
	feed := newTestFeed(t)
	base, hist := newFakeSource(t, 2), newFakeSource(t, 2)
	// Uma mudança publicada durante a base não pode mudar o cursor das
	// entradas da tabela seguinte: o header tem um cursor só.
	base.onOpen = func(n int) {
		if n == 1 {
			feed.Publish("events", Insert, rec(0, "entre"), time.Now())
		}
	}
	res := runExport(context.Background(), feed,
		[]SnapshotTable{{Name: "events", Source: base}, {Name: "events_history", History: true, Source: hist}}, 10, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if len(res.entries) != 2+1+2 {
		t.Fatalf("entradas=%d, quer 5", len(res.entries))
	}
	for i, e := range res.entries {
		if e.Sequence != res.header.Cursor {
			t.Fatalf("entrada %d (%s) com Sequence=%d, quer o cursor %d do header", i, e.Table, e.Sequence, res.header.Cursor)
		}
	}
}

func TestExportBorrowedKeyIsCappedAtItsLength(t *testing.T) {
	feed := newTestFeed(t)
	src := newFakeSource(t, 3)
	err := ExportSnapshot(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 10,
		func(SnapshotHeader) error { return nil },
		func(e Entry) error {
			want := string(e.Record.Payload)
			_ = append(e.Record.Key, "XXXXXXXX"...)
			if string(e.Record.Payload) != want {
				t.Fatalf("append na Key emprestada sobrescreveu o Payload: %q", e.Record.Payload)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
}

// addPayload grava uma linha com o payload dado, compartilhado entre linhas
// (só leitura).
func (f *fakeSource) addPayload(key string, payload []byte) {
	f.rows[f.next] = fakeRow{key: []byte(key), payload: payload, sequence: 1}
	f.next++
}

func TestChunkedExportBoundsBytesPerSnapshot(t *testing.T) {
	feed := newTestFeed(t)
	src := &fakeSource{t: t, rows: make(map[types.RowID]fakeRow), instant: time.Now()}
	const rowBytes = 1 << 20
	big := make([]byte, rowBytes)
	for i := range 20 {
		src.addPayload(fmt.Sprintf("big%02d", i), big)
	}
	// O teto em linhas nem chega perto: só o teto em bytes fecha o bloco.
	res := runExport(context.Background(), feed, []SnapshotTable{{Name: "events", Source: src}}, 4096, nil)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if len(res.entries) != 20 {
		t.Fatalf("entradas=%d, quer 20", len(res.entries))
	}
	if src.opened < 3 {
		t.Fatalf("snapshots=%d, quer ≥ 3 para 20 MiB com teto de %d bytes por bloco", src.opened, snapshotChunkMaxBytes)
	}
	// O bloco fecha na linha que atinge o teto: no máximo o teto mais uma linha.
	maxRows := snapshotChunkMaxBytes/rowBytes + 1
	for i, n := range src.readsPerSnapshot {
		if n > maxRows {
			t.Fatalf("o snapshot %d copiou %d linhas de 1 MiB, teto é %d", i+1, n, maxRows)
		}
	}
	requireBalanced(t, src)
}

func TestExportDropsOversizedBuffersBetweenChunks(t *testing.T) {
	src := &fakeSource{t: t, rows: make(map[types.RowID]fakeRow), instant: time.Now()}
	src.addPayload("gigante", make([]byte, 3*snapshotChunkMaxBytes))
	for i := range 3 {
		src.addPayload(fmt.Sprintf("small%d", i), []byte("x"))
	}
	var peak int
	x := &snapshotExporter{
		chunkRows: 1,
		begin:     func(SnapshotHeader) error { return nil },
	}
	x.emit = func(Entry) error {
		peak = max(peak, cap(x.arena))
		return nil
	}
	// O prazo transforma um laço sem fim (bloco que não avança) em falha
	// limpa, em vez de travar a suíte.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := x.exportTable(ctx, SnapshotTable{Name: "events", Source: src}); err != nil {
		t.Fatal(err)
	}
	if peak < 3*snapshotChunkMaxBytes {
		t.Fatalf("pico do arena=%d: o cenário não exercita a linha gigante", peak)
	}
	if cap(x.arena) > snapshotArenaKeepBytes || cap(x.buf) > snapshotArenaKeepBytes {
		t.Fatalf("arena=%d buf=%d depois dos blocos pequenos, teto %d: a linha gigante prendeu a memória",
			cap(x.arena), cap(x.buf), snapshotArenaKeepBytes)
	}
}
