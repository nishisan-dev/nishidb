package fakestorage

import (
	"bytes"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func payload(t *testing.T, fields ...struct {
	ID  types.FieldID
	Val types.Value
}) []byte {
	t.Helper()
	var p []byte
	for _, f := range fields {
		p = encoding.AppendField(p, f.ID, f.Val)
	}
	return p
}

func fv(id types.FieldID, v types.Value) struct {
	ID  types.FieldID
	Val types.Value
} {
	return struct {
		ID  types.FieldID
		Val types.Value
	}{id, v}
}

func strVal(s string) types.Value {
	return types.Value{Type: types.TypeString, Bytes: []byte(s)}
}

func intVal(n int64) types.Value {
	return types.Value{Type: types.TypeInt64, I64: n}
}

func TestPutCreateAndGetByKey(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}, Hash: []types.FieldID{3}})
	p := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Start")), fv(3, intVal(7)))
	rowID, created, err := s.Put([]byte("ABC"), 100, 0, p, nil)
	if err != nil || !created || rowID == 0 {
		t.Fatalf("Put: rowID=%d created=%v err=%v", rowID, created, err)
	}
	var buf []byte
	rec, buf2, ok, err := s.GetByKey([]byte("ABC"), buf)
	if err != nil || !ok {
		t.Fatalf("GetByKey: ok=%v err=%v", ok, err)
	}
	if rec.RowID != types.RowID(rowID) || rec.Sequence != 100 {
		t.Errorf("rec = %+v", rec)
	}
	if !bytes.Equal(rec.Key, []byte("ABC")) || !bytes.Equal(rec.Payload, p) {
		t.Errorf("Key/Payload incorretos")
	}
	_ = buf2
	if _, _, ok, _ := s.GetByKey([]byte("NOPE"), nil); ok {
		t.Error("key inexistente não deveria resolver")
	}
}

// O fake espelha o contrato de buffers do Store real: Record.Key/Payload são
// zero-copy sobre o buf devolvido, e NUNCA aliasam o estado interno do fake.
func TestGetByKeyCopiesIntoCallerBuf(t *testing.T) {
	s := New(Indexes{})
	p := payload(t, fv(1, strVal("ABC")))
	if _, _, err := s.Put([]byte("ABC"), 1, 0, p, nil); err != nil {
		t.Fatal(err)
	}
	rec, buf, ok, err := s.GetByKey([]byte("ABC"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	// Mutar o buf devolvido não pode afetar o estado interno.
	for i := range buf {
		buf[i] = 0xAA
	}
	rec2, _, ok, _ := s.GetByKey([]byte("ABC"), nil)
	if !ok || !bytes.Equal(rec2.Payload, p) {
		t.Errorf("estado interno foi corrompido via buf do caller: %x", rec2.Payload)
	}
	_ = rec
}

func TestPutUpdateDiffsIndexPostings(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	p1 := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Start")))
	rowID, _, err := s.Put([]byte("ABC"), 1, 0, p1, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertPosting(t, s, 2, strVal("Start"), []uint64{rowID})

	// Update trocando o valor indexado: posting antigo sai, novo entra.
	var buf []byte
	prev, _, _, _ := s.GetByKey([]byte("ABC"), buf)
	p2 := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Stop")))
	rowID2, created, err := s.Put([]byte("ABC"), 2, 0, p2, &prev)
	if err != nil || created || rowID2 != rowID {
		t.Fatalf("update: rowID=%d created=%v err=%v", rowID2, created, err)
	}
	assertPosting(t, s, 2, strVal("Start"), nil)
	assertPosting(t, s, 2, strVal("Stop"), []uint64{rowID})
}

func TestPutRemovedFieldClearsPosting(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	p1 := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Start")))
	rowID, _, _ := s.Put([]byte("ABC"), 1, 0, p1, nil)
	// Novo payload sem o campo 2 (NULL = ausência): posting deve sumir.
	p2 := payload(t, fv(1, strVal("ABC")))
	var buf []byte
	prev, _, _, _ := s.GetByKey([]byte("ABC"), buf)
	if _, _, err := s.Put([]byte("ABC"), 2, 0, p2, &prev); err != nil {
		t.Fatal(err)
	}
	assertPosting(t, s, 2, strVal("Start"), nil)
	_ = rowID
}

func TestPutWithNilPrevResolvesInternally(t *testing.T) {
	// prev==nil com key existente: o Put resolve sozinho (fallback do
	// contrato) e o diff continua correto.
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	p1 := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Start")))
	rowID, _, _ := s.Put([]byte("ABC"), 1, 0, p1, nil)
	p2 := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Stop")))
	rowID2, created, err := s.Put([]byte("ABC"), 2, 0, p2, nil)
	if err != nil || created || rowID2 != rowID {
		t.Fatalf("rowID=%d created=%v err=%v", rowID2, created, err)
	}
	assertPosting(t, s, 2, strVal("Start"), nil)
	assertPosting(t, s, 2, strVal("Stop"), []uint64{rowID})
}

func TestPutRejectsInvalidPayload(t *testing.T) {
	s := New(Indexes{})
	// Campos fora de ordem (id 3 antes do id 1) violam a invariante.
	var p []byte
	p = encoding.AppendField(p, 3, intVal(1))
	p = encoding.AppendField(p, 1, strVal("x"))
	if _, _, err := s.Put([]byte("K"), 1, 0, p, nil); err == nil {
		t.Error("payload fora de ordem deveria ser rejeitado")
	}
}

func TestGetByRowIDAndLiveness(t *testing.T) {
	s := New(Indexes{})
	p := payload(t, fv(1, strVal("ABC")))
	rowID, _, _ := s.Put([]byte("ABC"), 1, 0, p, nil)
	rec, _, ok, err := s.GetByRowID(rowID, nil)
	if err != nil || !ok || !bytes.Equal(rec.Payload, p) {
		t.Fatalf("GetByRowID: ok=%v err=%v", ok, err)
	}
	if _, _, ok, _ := s.GetByRowID(999, nil); ok {
		t.Error("row inexistente")
	}
	// Tombstone: some das leituras e do scan.
	s.SetTombstone(rowID)
	if _, _, ok, _ := s.GetByRowID(rowID, nil); ok {
		t.Error("tombstoned deveria dar ok=false")
	}
	if got := drain(t, s.ScanLive()); len(got) != 0 {
		t.Errorf("ScanLive com tombstone = %v", got)
	}
}

func TestScanLiveAscending(t *testing.T) {
	s := New(Indexes{})
	for _, k := range []string{"C", "A", "B", "D"} {
		p := payload(t, fv(1, strVal(k)))
		if _, _, err := s.Put([]byte(k), 1, 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	got := drain(t, s.ScanLive())
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("rowIds fora de ordem: %v", got)
		}
	}
	if len(got) != 4 {
		t.Errorf("len = %d", len(got))
	}
}

func TestLookupBitmapAndHashAscending(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}, Hash: []types.FieldID{3}})
	for i, k := range []string{"K1", "K2", "K3"} {
		p := payload(t, fv(1, strVal(k)), fv(2, strVal("Start")), fv(3, intVal(int64(i%2))))
		if _, _, err := s.Put([]byte(k), 1, 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	it, err := s.LookupBitmap(2, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, it)
	if len(got) != 3 {
		t.Fatalf("bitmap Start = %v", got)
	}
	it, err = s.LookupHash(3, intVal(0))
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, it); len(got) != 2 {
		t.Errorf("hash 0 = %v", got)
	}
	// Valor sem postings → iterador vazio, não erro.
	it, err = s.LookupBitmap(2, strVal("Inexistente"))
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, it); len(got) != 0 {
		t.Errorf("posting inexistente = %v", got)
	}
}

func TestFieldDict(t *testing.T) {
	d := NewDict()
	id1, err := d.LookupOrCreate([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := d.LookupOrCreate([]byte("b"))
	again, _ := d.LookupOrCreate([]byte("a"))
	if id1 == id2 || again != id1 {
		t.Errorf("ids: %d %d %d", id1, id2, again)
	}
	if name, ok := d.Name(id2); !ok || name != "b" {
		t.Errorf("Name(%d) = %q, %v", id2, name, ok)
	}
	if _, ok := d.Name(999); ok {
		t.Error("id inexistente")
	}
}

func drain(t *testing.T, it *RowIDIter) []uint64 {
	t.Helper()
	var out []uint64
	for it.Next() {
		out = append(out, it.Value())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertPosting(t *testing.T, s *Store, field types.FieldID, v types.Value, want []uint64) {
	t.Helper()
	it, err := s.LookupBitmap(field, v)
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, it)
	if len(got) != len(want) {
		t.Fatalf("postings(%d) = %v, want %v", field, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("postings(%d) = %v, want %v", field, got, want)
		}
	}
}
