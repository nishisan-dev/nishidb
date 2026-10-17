package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
)

// putRows grava n rows novas de chaves distintas, com o prefixo dado.
func putRows(t *testing.T, s *Store, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%s-%d", prefix, i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
}

func mustWritePosition(t *testing.T, s *Store) replication.StorePosition {
	t.Helper()
	pos, err := s.WritePosition()
	if err != nil {
		t.Fatalf("WritePosition: %v", err)
	}
	return pos
}

func TestWritePositionOfEmptyStoreIsFirstSegmentAfterHeader(t *testing.T) {
	s := openStore(t, Options{Dataset: "events"})
	// Literal: nome do Store, segmento 1, offset logo depois do cabeçalho de
	// 64 bytes do segmento, nenhum RowID alocado.
	want := replication.StorePosition{Name: "events", Segment: 1, Offset: 64, NextRowID: 0}
	if got := mustWritePosition(t, s); got != want {
		t.Fatalf("posição do Store vazio = %+v, quer %+v", got, want)
	}
}

func TestWritePositionAdvancesOnPut(t *testing.T) {
	s := openStore(t, Options{Dataset: "events"})
	before := mustWritePosition(t, s)

	putRows(t, s, "adv", 3)
	after := mustWritePosition(t, s)

	if after.Name != "events" || after.Segment != before.Segment {
		t.Fatalf("nome/segmento mudaram sem rotação: antes=%+v depois=%+v", before, after)
	}
	if after.NextRowID != before.NextRowID+3 {
		t.Fatalf("NextRowID = %d, quer %d (3 rows novas)", after.NextRowID, before.NextRowID+3)
	}
	if after.Offset <= before.Offset {
		t.Fatalf("Offset = %d, quer maior que %d após 3 Put", after.Offset, before.Offset)
	}

	// Um Put sobre key existente não aloca RowID, mas grava record: só o
	// offset anda.
	mustPut(t, s, "adv-0", 99, statusPayload(t, s, "adv-0", "Stop", 99), nil)
	updated := mustWritePosition(t, s)
	if updated.NextRowID != after.NextRowID {
		t.Fatalf("NextRowID = %d após update, quer %d", updated.NextRowID, after.NextRowID)
	}
	if updated.Offset <= after.Offset {
		t.Fatalf("Offset = %d após update, quer maior que %d", updated.Offset, after.Offset)
	}
}

func TestWritePositionAdvancesSegmentOnRotation(t *testing.T) {
	// Segmento pequeno: poucos Put forçam a rotação, e o par (segmento, offset)
	// tem de acompanhar o segmento novo.
	s := openStore(t, Options{Dataset: "events", SegmentSize: 1024})
	first := mustWritePosition(t, s)
	putRows(t, s, "rot", 40)
	last := mustWritePosition(t, s)
	if last.Segment <= first.Segment {
		t.Fatalf("Segment = %d, quer maior que %d após rotação", last.Segment, first.Segment)
	}
	if last.Offset > 1024 {
		t.Fatalf("Offset = %d excede o tamanho do segmento (1024)", last.Offset)
	}
}

func TestWritePositionIsStableAcrossCleanReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Dataset: "events", SegmentSize: 1 << 20}
	s := openStore(t, opts)
	putRows(t, s, "stable", 5)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	want := mustWritePosition(t, s)

	s2 := reopen(t, s, opts)
	if got := mustWritePosition(t, s2); got != want {
		t.Fatalf("posição após reabrir = %+v, quer %+v", got, want)
	}
}

func TestWritePositionUnchangedByOpenCheckpoint(t *testing.T) {
	// Sem Checkpoint explícito: o tail inteiro é reaplicado pela recovery e
	// só então o Open faz o seu Checkpoint. Nem a recovery nem esse
	// checkpoint podem mover a posição de escrita.
	dir := t.TempDir()
	opts := Options{Dir: dir, Dataset: "events", SegmentSize: 1 << 20}
	s := openStore(t, opts)
	putRows(t, s, "tail", 7)
	want := mustWritePosition(t, s)

	s2 := reopen(t, s, opts)
	if got := mustWritePosition(t, s2); got != want {
		t.Fatalf("posição após recovery + checkpoint do Open = %+v, quer %+v", got, want)
	}

	// Uma segunda reabertura (agora com o checkpoint do Open já durável)
	// também não move.
	s3 := reopen(t, s2, opts)
	if got := mustWritePosition(t, s3); got != want {
		t.Fatalf("posição após segunda reabertura = %+v, quer %+v", got, want)
	}
}

func TestWritePositionChangesAfterOfflineMutation(t *testing.T) {
	// Simula o comando offline: com o feed parado, outro processo abre o
	// dataset, grava e fecha. A impressão guardada pelo feed (s1) deixa de
	// bater com a posição recuperada.
	dir := t.TempDir()
	opts := Options{Dir: dir, Dataset: "events", SegmentSize: 1 << 20}
	s := openStore(t, opts)
	putRows(t, s, "off", 4)
	fingerprint := mustWritePosition(t, s)

	s2 := reopen(t, s, opts)
	if got := mustWritePosition(t, s2); got != fingerprint {
		t.Fatalf("pré-condição: reabrir sem mutação mudou a posição: %+v, quer %+v", got, fingerprint)
	}
	putRows(t, s2, "offline", 1)

	s3 := reopen(t, s2, opts)
	got := mustWritePosition(t, s3)
	if got == fingerprint {
		t.Fatalf("posição após mutação offline = %+v, igual à impressão guardada", got)
	}
	if got.NextRowID != fingerprint.NextRowID+1 {
		t.Fatalf("NextRowID = %d, quer %d", got.NextRowID, fingerprint.NextRowID+1)
	}

	// Segundo ciclo: a mutação offline é um Put sobre key EXISTENTE. Ela não
	// aloca RowID, então só o offset a denuncia.
	fingerprint = got
	s4 := reopen(t, s3, opts)
	mustPut(t, s4, "off-0", 99, statusPayload(t, s4, "off-0", "Stop", 99), nil)
	s5 := reopen(t, s4, opts)
	got = mustWritePosition(t, s5)
	if got.NextRowID != fingerprint.NextRowID {
		t.Fatalf("NextRowID = %d após update de key existente, quer %d", got.NextRowID, fingerprint.NextRowID)
	}
	if got == fingerprint {
		t.Fatalf("posição após update offline de key existente = %+v, igual à impressão guardada", got)
	}
}

func TestWritePositionChangesWhenHistoryRetentionAdvancesFloor(t *testing.T) {
	// A retenção avança o piso (rowIDBase) e aposenta segmentos sem gravar
	// record: segmento, offset e NextRowID não se mexem. Só o piso na posição
	// impede que um feed religado depois de uma subida com a replicação
	// desligada retome limpo sem o Retain.
	base := openStore(t, Options{})
	dir := t.TempDir()
	opts := Options{
		Dir:                   dir,
		Dataset:               "events_history",
		SegmentSize:           1 << 20,
		Mode:                  HistoryMode,
		SharedDictionary:      base.FieldDict(),
		WriteInstantFieldName: "_writtenAt",
	}
	rel := &relogioFake{t: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	hist := openStore(t, opts)
	hist.UsaRelogio(rel.agora)
	for range 4 {
		apenda(t, hist, "a", rel.t)
		rel.advance(10 * time.Minute)
	}
	if err := hist.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	fingerprint := mustWritePosition(t, hist)
	if fingerprint.RowIDBase != 0 {
		t.Fatalf("pré-condição: piso = %d, quer 0", fingerprint.RowIDBase)
	}

	// Feed parado; a subida seguinte roda a retenção e fecha de novo.
	h2 := reopen(t, hist, opts)
	h2.UsaRelogio(rel.agora)
	if got := mustWritePosition(t, h2); got != fingerprint {
		t.Fatalf("pré-condição: reabrir sem retenção mudou a posição: %+v, quer %+v", got, fingerprint)
	}
	// Escritas em t, t+10m, t+20m, t+30m; agora é t+40m; corte em t+15m.
	res, err := h2.RetentionOnePass(context.Background(), LimitesOfRetention{ExpireAfterWrite: 25 * time.Minute})
	if err != nil {
		t.Fatalf("RetentionOnePass: %v", err)
	}
	if res.MarkAvancou != 2 {
		t.Fatalf("a marca avançou %d, quer 2", res.MarkAvancou)
	}
	if err := h2.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	h3 := reopen(t, h2, opts)
	got := mustWritePosition(t, h3)
	if got.Segment != fingerprint.Segment || got.Offset != fingerprint.Offset || got.NextRowID != fingerprint.NextRowID {
		t.Fatalf("a retenção moveu a posição de escrita: %+v, quer só o piso diferente de %+v", got, fingerprint)
	}
	if got.RowIDBase != 2 {
		t.Fatalf("RowIDBase = %d, quer 2", got.RowIDBase)
	}
	if got == fingerprint {
		t.Fatalf("posição após a retenção = %+v, igual à impressão guardada", got)
	}
}

func TestWritePositionReturnsErrWriterFailedWhenWriterIsSticky(t *testing.T) {
	s := openStore(t, Options{Dataset: "events"})
	putRows(t, s, "sticky", 1)
	s.wmu.Lock()
	s.failWriter(errors.New("EIO simulado"))
	s.wmu.Unlock()
	if _, err := s.WritePosition(); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("WritePosition com o writer falho = %v, quer ErrWriterFailed", err)
	}
}

func TestWritePositionDefaultsNameToDirectoryBase(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir})
	got := mustWritePosition(t, s)
	if got.Name != s.opts.Dataset || got.Name == "" {
		t.Fatalf("Name = %q, quer o nome efetivo do dataset %q (não vazio)", got.Name, s.opts.Dataset)
	}
}

func TestWritePositionReturnsErrDatasetClosedAfterClose(t *testing.T) {
	s := openStore(t, Options{Dataset: "events"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WritePosition(); !errors.Is(err, ErrDatasetClosed) {
		t.Fatalf("WritePosition após Close = %v, quer ErrDatasetClosed", err)
	}
}

func TestWritePositionIsSerializedWithConcurrentPut(t *testing.T) {
	// A garantia do godoc (leitura sob o wmu) só aparece sob -race: sem o
	// mutex, secW.Current() lê segmento e offset em corrida com o Append. Sob
	// o `make test` sem -race o teste ainda confere a monotonicidade.
	s := openStore(t, Options{Dataset: "events"})
	// Os payloads saem antes da goroutine: statusPayload usa Fatal, que só vale
	// na goroutine do teste.
	payloads := make([][]byte, 300)
	for i := range payloads {
		payloads[i] = statusPayload(t, s, fmt.Sprintf("conc-%d", i), "Start", int64(i))
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i, p := range payloads {
			// Errorf + return: FailNow só encerra a goroutine que o chama.
			if _, _, err := s.Put([]byte(fmt.Sprintf("conc-%d", i)), int64(i), 0, p, nil); err != nil {
				t.Errorf("Put(conc-%d): %v", i, err)
				return
			}
		}
	}()
	var last replication.StorePosition
	for reading := true; reading; {
		select {
		case <-done:
			reading = false
		default:
		}
		pos := mustWritePosition(t, s)
		if pos.Segment < last.Segment || (pos.Segment == last.Segment && pos.Offset < last.Offset) || pos.NextRowID < last.NextRowID {
			t.Fatalf("posição regrediu: %+v depois de %+v", pos, last)
		}
		last = pos
	}
	if last.NextRowID != 300 {
		t.Fatalf("NextRowID final = %d, quer 300", last.NextRowID)
	}
}
