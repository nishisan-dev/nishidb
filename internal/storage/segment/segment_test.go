package segment

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func makeRecord(t *testing.T, rowID types.RowID, seq int64, key string, payloadSize int) []byte {
	t.Helper()
	payload := encoding.AppendField(nil, 1, types.Value{Type: types.TypeString, Bytes: []byte(key)})
	if payloadSize > 0 {
		filler := bytes.Repeat([]byte{'z'}, payloadSize)
		payload = encoding.AppendField(payload, 2, types.Value{Type: types.TypeBytes, Bytes: filler})
	}
	rec, err := encoding.BuildRecord(nil, rowID, seq, 0, []byte(key), payload)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	defer w.Close()

	recs := [][]byte{
		makeRecord(t, 1, 10, "key-a", 0),
		makeRecord(t, 2, 20, "key-b", 100),
		makeRecord(t, 3, 30, "key-c", 500),
	}
	type loc struct{ seg, off uint32 }
	var locs []loc
	for _, rec := range recs {
		seg, off, err := w.Append(rec)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		locs = append(locs, loc{seg, off})
	}
	if locs[0].seg != 1 || locs[0].off != HeaderSize {
		t.Fatalf("primeiro record: esperado seg=1 off=%d, obtido seg=%d off=%d", HeaderSize, locs[0].seg, locs[0].off)
	}

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()
	var buf []byte
	for i, l := range locs {
		var rec types.Record
		rec, buf, err = r.ReadRecord(l.seg, l.off, buf)
		if err != nil {
			t.Fatalf("ReadRecord %d: %v", i, err)
		}
		want, err := encoding.ParseRecord(recs[i])
		if err != nil {
			t.Fatal(err)
		}
		if rec.RowID != want.RowID || rec.Sequence != want.Sequence ||
			!bytes.Equal(rec.Key, want.Key) || !bytes.Equal(rec.Payload, want.Payload) {
			t.Fatalf("record %d difere do original", i)
		}
	}
}

func TestHeaderGolden(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	raw, err := os.ReadFile(filepath.Join(dir, "segment-00000001.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != HeaderSize {
		t.Fatalf("segmento recém-criado deveria ter só o header (%d bytes), tem %d", HeaderSize, len(raw))
	}
	if !bytes.Equal(raw[0:4], []byte("NDBS")) {
		t.Fatalf("magic: %q", raw[0:4])
	}
	if raw[4] != 1 {
		t.Fatalf("version: %d", raw[4])
	}
}

func TestRotacaoCriaNovoSegmento(t *testing.T) {
	dir := t.TempDir()
	rec := makeRecord(t, 1, 1, "rotation-key", 100)
	// Espaço para exatamente 2 records por segmento.
	segSize := int64(HeaderSize + 2*len(rec))
	w, err := OpenWriter(dir, segSize, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var segs []uint32
	for i := 0; i < 5; i++ {
		seg, _, err := w.Append(rec)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		segs = append(segs, seg)
	}
	want := []uint32{1, 1, 2, 2, 3}
	for i := range want {
		if segs[i] != want[i] {
			t.Fatalf("segmentos: esperado %v, obtido %v", want, segs)
		}
	}
	for _, name := range []string{"segment-00000001.dat", "segment-00000002.dat", "segment-00000003.dat"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("esperado %s: %v", name, err)
		}
	}

	// Todos os records continuam legíveis após rotação.
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.ReadRecord(3, HeaderSize, nil); err != nil {
		t.Fatalf("leitura pós-rotação: %v", err)
	}
}

func TestRecordMaiorQueSegmentoFalha(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, HeaderSize+64, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, _, err := w.Append(makeRecord(t, 1, 1, "too-big", 1000)); err == nil {
		t.Fatal("record maior que o segmento deveria falhar")
	}
}

func TestReopenParaAppendTruncaLixo(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rec1 := makeRecord(t, 1, 1, "first", 0)
	seg, off1, err := w.Append(rec1)
	if err != nil {
		t.Fatal(err)
	}
	validEnd := int64(off1) + int64(len(rec1))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Simula lixo de crash após o fim válido.
	path := filepath.Join(dir, "segment-00000001.dat")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("GARBAGE-GARBAGE")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reabre posicionado no fim válido: o lixo deve ser truncado.
	w2, err := OpenWriter(dir, 1<<20, seg, validEnd)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != validEnd {
		t.Fatalf("lixo não truncado: arquivo com %d bytes, válido era %d", st.Size(), validEnd)
	}

	rec2 := makeRecord(t, 2, 2, "second", 0)
	seg2, off2, err := w2.Append(rec2)
	if err != nil {
		t.Fatal(err)
	}
	if seg2 != seg || int64(off2) != validEnd {
		t.Fatalf("append pós-reopen: esperado seg=%d off=%d, obtido seg=%d off=%d", seg, validEnd, seg2, off2)
	}

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.ReadRecord(seg2, off2, nil); err != nil {
		t.Fatalf("leitura do record pós-reopen: %v", err)
	}
}

func TestReadRecordDetectaCorrupcao(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rec := makeRecord(t, 9, 9, "corrupt-me", 50)
	seg, off, err := w.Append(rec)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	path := filepath.Join(dir, "segment-00000001.dat")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[int(off)+len(rec)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.ReadRecord(seg, off, nil); !errors.Is(err, encoding.ErrCorrupt) {
		t.Fatalf("esperado ErrCorrupt, obtido %v", err)
	}
}

func TestReadRecordComLengthCorrompidoNaoAlocaGigante(t *testing.T) {
	// C1: payloadLength corrompido em disco é rejeitado pelos tetos do
	// ParseHeader ANTES do ReadRecord alocar o "total" declarado — sem os
	// tetos, 1 bit invertido induziria make([]byte, ~4GiB) antes do CRC.
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rec := makeRecord(t, 9, 9, "len-corrupto", 50)
	seg, off, err := w.Append(rec)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	path := filepath.Join(dir, "segment-00000001.dat")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// payloadLength fica nos offsets 28..31 do record.
	for i := 0; i < 4; i++ {
		raw[int(off)+28+i] = 0xFF
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, buf, err := r.ReadRecord(seg, off, nil)
	if !errors.Is(err, encoding.ErrCorrupt) {
		t.Fatalf("esperado ErrCorrupt, obtido %v", err)
	}
	if cap(buf) > 4096 {
		t.Fatalf("buffer cresceu para %d bytes com length corrompido", cap(buf))
	}
}

func TestReadRecordGrande(t *testing.T) {
	// Record maior que o pread especulativo de 4 KiB (caminho de 2 preads).
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	rec := makeRecord(t, 5, 5, "big-record", 8192)
	seg, off, err := w.Append(rec)
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, _, err := r.ReadRecord(seg, off, nil)
	if err != nil {
		t.Fatalf("ReadRecord grande: %v", err)
	}
	want, _ := encoding.ParseRecord(rec)
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatal("payload do record grande difere")
	}
}

func TestReadSegmentoInexistente(t *testing.T) {
	r, err := OpenReader(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.ReadRecord(42, HeaderSize, nil); err == nil {
		t.Fatal("segmento inexistente deveria falhar")
	}
}

func TestSyncNaoFalha(t *testing.T) {
	w, err := OpenWriter(t.TempDir(), 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, _, err := w.Append(makeRecord(t, 1, 1, "sync", 0)); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}
