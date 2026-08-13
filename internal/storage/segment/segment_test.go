package segment

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// mustAppendField codifica um campo que o teste sabe ser válido.
func mustAppendField(tb testing.TB, dst []byte, id types.FieldID, v types.Value) []byte {
	tb.Helper()
	out, err := encoding.AppendField(dst, id, v)
	if err != nil {
		tb.Fatalf("AppendField(%d, %#x): %v", id, v.Type, err)
	}
	return out
}

func makeRecord(t *testing.T, rowID types.RowID, seq int64, key string, payloadSize int) []byte {
	t.Helper()
	payload := mustAppendField(t, nil, 1, types.Value{Type: types.TypeString, Bytes: []byte(key)})
	if payloadSize > 0 {
		filler := bytes.Repeat([]byte{'z'}, payloadSize)
		payload = mustAppendField(t, payload, 2, types.Value{Type: types.TypeBytes, Bytes: filler})
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

func TestOpenWriterRejeitaSegmentSizeAcimaDe1GiB(t *testing.T) {
	// C2: o offset de Location tem 30 bits (design §3/§14.7). segmentSize
	// entre 1-4 GiB faria rowindex.Pack panicar APÓS o record estar no
	// segmento (crash loop no replay); acima de 4 GiB, uint32(off) truncaria
	// silenciosamente. A rejeição tem que acontecer na config, aqui.
	if _, err := OpenWriter(t.TempDir(), 1<<30+1, 0, 0); err == nil {
		t.Fatal("segmentSize > 1 GiB deveria ser rejeitado")
	}
	if _, err := OpenWriter(t.TempDir(), 5<<30, 0, 0); err == nil {
		t.Fatal("segmentSize de 5 GiB deveria ser rejeitado")
	}
	// 1 GiB exato é o teto permitido.
	w, err := OpenWriter(t.TempDir(), 1<<30, 0, 0)
	if err != nil {
		t.Fatalf("segmentSize de exatamente 1 GiB deveria ser aceito: %v", err)
	}
	w.Close()
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

// recordingReaderAt registra o tamanho de cada leitura para verificar a
// janela do pread especulativo.
type recordingReaderAt struct {
	data  []byte
	reads []int
}

func (r *recordingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.reads = append(r.reads, len(p))
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestPrimeiroPreadLimitadoComBufferGrandeReutilizado(t *testing.T) {
	// I5: o 1º pread deve ser min(len(buf), 4096) — NUNCA cap(buf). Com o
	// reuso de buffer do §15A.6, um único record de 4 MiB no buffer faria
	// toda leitura seguinte emitir preads de 4 MiB para records de 700B
	// (~5000× de amplificação num full scan).
	rec := makeRecord(t, 1, 1, "small", 100)
	fake := &recordingReaderAt{data: rec}

	bigBuf := make([]byte, 0, 1<<20) // buffer "envenenado" por um record grande anterior
	got, _, err := readRecordAt(fake, 1, 0, bigBuf)
	if err != nil {
		t.Fatalf("readRecordAt: %v", err)
	}
	if got.RowID != 1 {
		t.Fatalf("record errado: %+v", got)
	}
	if len(fake.reads) == 0 || fake.reads[0] > 4096 {
		t.Fatalf("1º pread deveria ser ≤ 4096 bytes, foi %d (reads=%v)", fake.reads[0], fake.reads)
	}
}

func TestSegundoPreadCobreRecordGrande(t *testing.T) {
	// Record maior que a janela: 1º pread de 4096, 2º pread do restante.
	rec := makeRecord(t, 2, 2, "big", 8192)
	fake := &recordingReaderAt{data: rec}

	got, _, err := readRecordAt(fake, 1, 0, nil)
	if err != nil {
		t.Fatalf("readRecordAt: %v", err)
	}
	if got.RowID != 2 {
		t.Fatalf("record errado: %+v", got)
	}
	if fake.reads[0] > 4096 {
		t.Fatalf("1º pread: %d", fake.reads[0])
	}
	if len(fake.reads) < 2 || fake.reads[len(fake.reads)-1] != len(rec)-4096 {
		t.Fatalf("2º pread deveria ler o restante (%d), reads=%v", len(rec)-4096, fake.reads)
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

func TestListSegments(t *testing.T) {
	dir := t.TempDir()
	rec := makeRecord(t, 1, 1, "list-key", 100)
	segSize := int64(HeaderSize + 2*len(rec))
	w, err := OpenWriter(dir, segSize, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ { // 3 segmentos: {1,1,2,2,3}
		if _, _, err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	refs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("esperado 3 segmentos, obtido %v", refs)
	}
	for i, ref := range refs {
		if ref.ID != uint32(i+1) {
			t.Fatalf("ordem/IDs errados: %v", refs)
		}
		if ref.SizeBytes < HeaderSize {
			t.Fatalf("tamanho inválido: %v", ref)
		}
	}
	if refs[0].SizeBytes != int64(HeaderSize+2*len(rec)) {
		t.Fatalf("segmento 1 selado deveria ter %d bytes, tem %d", HeaderSize+2*len(rec), refs[0].SizeBytes)
	}

	if empty, err := ListSegments(t.TempDir()); err != nil || len(empty) != 0 {
		t.Fatalf("diretório vazio: (%v,%v)", empty, err)
	}
}

func TestScanFromPercorreEDevolveFimValido(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var offs []uint32
	for i := 1; i <= 3; i++ {
		_, off, err := w.Append(makeRecord(t, types.RowID(i), int64(i*10), fmt.Sprintf("scan-%d", i), 50*i))
		if err != nil {
			t.Fatal(err)
		}
		offs = append(offs, off)
	}
	_, tail := w.Current()

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var gotOffs []uint32
	var gotRows []types.RowID
	end, err := r.ScanFrom(1, HeaderSize, func(off uint32, rec types.Record) error {
		gotOffs = append(gotOffs, off)
		gotRows = append(gotRows, rec.RowID)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFrom: %v", err)
	}
	if !slices.Equal(gotOffs, offs) || !slices.Equal(gotRows, []types.RowID{1, 2, 3}) {
		t.Fatalf("scan: offs=%v rows=%v", gotOffs, gotRows)
	}
	if end != tail {
		t.Fatalf("fim válido: esperado %d, obtido %d", tail, end)
	}

	// Scan a partir do 2º record.
	var n int
	end2, err := r.ScanFrom(1, int64(offs[1]), func(uint32, types.Record) error { n++; return nil })
	if err != nil || n != 2 || end2 != tail {
		t.Fatalf("scan parcial: (n=%d end=%d err=%v)", n, end2, err)
	}

	// Erro do callback aborta e propaga.
	sentinel := errors.New("abort")
	if _, err := r.ScanFrom(1, HeaderSize, func(uint32, types.Record) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("erro do callback deveria propagar, obtido %v", err)
	}
}

func TestScanFromParaEmCorrupcaoETruncamento(t *testing.T) {
	build := func(t *testing.T) (string, []uint32, []int) {
		dir := t.TempDir()
		w, err := OpenWriter(dir, 1<<20, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		var offs []uint32
		var lens []int
		for i := 1; i <= 3; i++ {
			rec := makeRecord(t, types.RowID(i), int64(i), fmt.Sprintf("c-%d", i), 40)
			_, off, err := w.Append(rec)
			if err != nil {
				t.Fatal(err)
			}
			offs = append(offs, off)
			lens = append(lens, len(rec))
		}
		w.Close()
		return dir, offs, lens
	}

	t.Run("crc_corrompido_no_meio", func(t *testing.T) {
		dir, offs, _ := build(t)
		path := filepath.Join(dir, "segment-00000001.dat")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		raw[offs[1]+40] ^= 0xFF // corrompe o 2º record
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := OpenReader(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var n int
		end, err := r.ScanFrom(1, HeaderSize, func(uint32, types.Record) error { n++; return nil })
		if err != nil || n != 1 || end != int64(offs[1]) {
			t.Fatalf("parada na corrupção: (n=%d end=%d err=%v), esperado end=%d", n, end, err, offs[1])
		}
	})

	t.Run("tail_truncado_no_meio_do_ultimo", func(t *testing.T) {
		dir, offs, lens := build(t)
		path := filepath.Join(dir, "segment-00000001.dat")
		cut := int64(offs[2]) + int64(lens[2])/2
		if err := os.Truncate(path, cut); err != nil {
			t.Fatal(err)
		}
		r, err := OpenReader(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var n int
		end, err := r.ScanFrom(1, HeaderSize, func(uint32, types.Record) error { n++; return nil })
		if err != nil || n != 2 || end != int64(offs[2]) {
			t.Fatalf("tail truncado: (n=%d end=%d err=%v), esperado end=%d", n, end, err, offs[2])
		}
	})
}

// failingReaderAt devolve um erro de I/O genuíno (não EOF) a partir de
// failFrom, para separar "fim do prefixo válido" de "leitura falhou".
type failingReaderAt struct {
	data     []byte
	failFrom int64
}

var errFakeIO = errors.New("erro de I/O simulado")

func (r *failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.failFrom {
		return 0, errFakeIO
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestScanFromPropagaErroDeIOEmVezDeTruncar(t *testing.T) {
	// Um EIO no meio do log NÃO pode virar "fim válido": o caller trunca no
	// offset devolvido, então confundir falha de leitura com fim do prefixo
	// descartaria records íntegros em silêncio.
	rec1 := makeRecord(t, 1, 1, "io-1", 40)
	rec2 := makeRecord(t, 2, 2, "io-2", 40)
	data := append(append([]byte{}, rec1...), rec2...)
	fake := &failingReaderAt{data: data, failFrom: int64(len(rec1))}

	var n int
	end, err := scanFrom(fake, 1, 0, func(uint32, types.Record) error { n++; return nil })
	if !errors.Is(err, errFakeIO) {
		t.Fatalf("erro de I/O deveria propagar, obtido %v", err)
	}
	if n != 1 || end != int64(len(rec1)) {
		t.Fatalf("scan deveria parar no record legível: (n=%d end=%d)", n, end)
	}
}

func TestScanFromParaNoEOFSemErro(t *testing.T) {
	// EOF exato após o último record é fim de prefixo válido, não falha.
	rec := makeRecord(t, 1, 1, "eof", 40)
	fake := &failingReaderAt{data: rec, failFrom: math.MaxInt64}

	var n int
	end, err := scanFrom(fake, 1, 0, func(uint32, types.Record) error { n++; return nil })
	if err != nil || n != 1 || end != int64(len(rec)) {
		t.Fatalf("EOF deveria encerrar sem erro: (n=%d end=%d err=%v)", n, end, err)
	}
}

func TestListSegmentsIgnoraNomesNaoCanonicos(t *testing.T) {
	// S11-I4: sufixos e padding divergente casariam no Sscanf e produziriam
	// IDs duplicados no manifest/recovery. Só o nome canônico exato entra.
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<20, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Append(makeRecord(t, 1, 1, "canon", 0)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	for _, garbage := range []string{
		"segment-00000001.dat.tmp",
		"segment-00000001.dat.new",
		"segment-1.dat",
		"segment-00000002.txt",
	} {
		if err := os.WriteFile(filepath.Join(dir, garbage), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "segment-00000009.dat"), 0o755); err != nil {
		t.Fatal(err)
	}

	refs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].ID != 1 {
		t.Fatalf("só o canônico deveria entrar: %v", refs)
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
