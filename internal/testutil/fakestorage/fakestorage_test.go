package fakestorage

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// consumerStore é a interface que a Q10 (Apply) e a Q15 (Executor) vão
// declarar, com as assinaturas copiadas verbatim do *storage.Store
// (§15A item 4). Go não tem retorno covariante: uma divergência de
// assinatura, ainda que só uint64 no lugar de types.RowID, torna fake e real
// mutuamente inutilizáveis.
//
// As DUAS asserções abaixo são o contrato entre os ramos deixando de ser
// papel: enquanto ambas compilarem, o dublê e o Store real são
// intercambiáveis em qualquer consumidor. A segunda só passou a ser possível
// com o RowIDIterator morando em internal/types — com a interface
// redeclarada em cada lado, nenhuma das duas satisfaria a mesma assinatura.
type consumerStore interface {
	GetByKey(key, buf []byte) (types.Record, []byte, bool, error)
	GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error)
	Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error)
	ScanLive() types.RowIDIterator
	LookupBitmap(field types.FieldID, v types.Value) (types.RowIDIterator, error)
	HasBitmap(field types.FieldID) bool
	Sequence(id types.RowID) (int64, bool)
}

var (
	_ consumerStore = (*Store)(nil)
	_ consumerStore = (*storage.Store)(nil)
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

func payload(t *testing.T, fields ...struct {
	ID  types.FieldID
	Val types.Value
}) []byte {
	t.Helper()
	var p []byte
	for _, f := range fields {
		p = mustAppendField(t, p, f.ID, f.Val)
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
	if err != nil || !created {
		t.Fatalf("Put: rowID=%d created=%v err=%v", rowID, created, err)
	}
	// rowIds são 0-based, como no real.
	if rowID != 0 {
		t.Errorf("primeiro rowId = %d, want 0", rowID)
	}
	var buf []byte
	rec, buf2, ok, err := s.GetByKey([]byte("ABC"), buf)
	if err != nil || !ok {
		t.Fatalf("GetByKey: ok=%v err=%v", ok, err)
	}
	if rec.RowID != rowID || rec.Sequence != 100 {
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

// O fake espelha o contrato de buffers do Store real nos DOIS sentidos: o que
// ele devolve é cópia sobre o buf do caller (mutar não corrompe o estado) e o
// que ele recebe também é copiado (mutar key/payload após o Put não altera o
// estado guardado).
func TestBufferOwnershipBothDirections(t *testing.T) {
	s := New(Indexes{})
	key := []byte("ABC")
	p := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Start")))
	if _, _, err := s.Put(key, 1, 0, p, nil); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), p...)

	// (a) Record.Key/Payload são janelas sobre o buf do caller, não sobre o
	// estado interno — a checagem de identidade é o que distingue cópia real
	// de "mutei outra região e não percebi".
	rec, buf, ok, err := s.GetByKey(key, nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if &rec.Key[0] != &buf[0] {
		t.Error("Record.Key deveria ser janela sobre o buf devolvido")
	}
	if &rec.Payload[0] != &buf[len(rec.Key)] {
		t.Error("Record.Payload deveria ser janela sobre o buf devolvido")
	}
	for i := range buf {
		buf[i] = 0xAA
	}
	rec2, _, ok, _ := s.GetByKey(key, nil)
	if !ok || !bytes.Equal(rec2.Payload, want) {
		t.Errorf("estado interno corrompido via buf do caller: %x", rec2.Payload)
	}

	// (b) mutar os buffers passados ao Put depois da chamada não altera o
	// estado guardado.
	for i := range key {
		key[i] = 'Z'
	}
	for i := range p {
		p[i] = 0xFF
	}
	rec3, _, ok, _ := s.GetByKey([]byte("ABC"), nil)
	if !ok {
		t.Fatal("key mutada pelo caller apagou a row")
	}
	if !bytes.Equal(rec3.Key, []byte("ABC")) || !bytes.Equal(rec3.Payload, want) {
		t.Errorf("estado interno aliasa os buffers do caller: key=%q payload=%x", rec3.Key, rec3.Payload)
	}
}

func TestPutUpdateDiffsIndexPostings(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	p1 := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Start")))
	rowID, _, err := s.Put([]byte("ABC"), 1, 0, p1, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertPosting(t, s, 2, strVal("Start"), []types.RowID{rowID})

	// Update trocando o valor indexado: posting antigo sai, novo entra.
	var buf []byte
	prev, _, _, _ := s.GetByKey([]byte("ABC"), buf)
	p2 := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Stop")))
	rowID2, created, err := s.Put([]byte("ABC"), 2, 0, p2, &prev)
	if err != nil || created || rowID2 != rowID {
		t.Fatalf("update: rowID=%d created=%v err=%v", rowID2, created, err)
	}
	assertPosting(t, s, 2, strVal("Start"), nil)
	assertPosting(t, s, 2, strVal("Stop"), []types.RowID{rowID})
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
	assertPosting(t, s, 2, strVal("Stop"), []types.RowID{rowID})
}

func TestPutRejectsInvalidPayload(t *testing.T) {
	s := New(Indexes{})
	// Campos fora de ordem (id 3 antes do id 1) violam a invariante.
	var p []byte
	p = mustAppendField(t, p, 3, intVal(1))
	p = mustAppendField(t, p, 1, strVal("x"))
	if _, _, err := s.Put([]byte("K"), 1, 0, p, nil); err == nil {
		t.Error("payload fora de ordem deveria ser rejeitado")
	}
}

// assertInvalidArgument confere o sentinela E a substring da mensagem: a
// mensagem é contrato de teste (§14.23), porque é ela que diz ao operador
// qual dos argumentos estava errado.
func assertInvalidArgument(t *testing.T, err error, wantSubstr string) {
	t.Helper()
	if !errors.Is(err, types.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
		return
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("err = %q, deveria conter %q", err, wantSubstr)
	}
}

// Argumentos inválidos do Put são os mesmos do real: key vazia e prev de
// outra key (que gravaria o estado na row errada e apagaria a key legítima).
func TestPutRejectsInvalidArguments(t *testing.T) {
	s := New(Indexes{})
	p := payload(t, fv(1, strVal("A")))
	_, _, err := s.Put(nil, 1, 0, p, nil)
	assertInvalidArgument(t, err, "key vazia")
	_, _, err = s.Put([]byte(""), 1, 0, p, nil)
	assertInvalidArgument(t, err, "key vazia")

	if _, _, err := s.Put([]byte("A"), 1, 0, p, nil); err != nil {
		t.Fatal(err)
	}
	prevA, _, _, _ := s.GetByKey([]byte("A"), nil)
	pb := payload(t, fv(1, strVal("B")))
	_, _, err = s.Put([]byte("B"), 1, 0, pb, &prevA)
	assertInvalidArgument(t, err, "prev pertence à key")
	if _, ok := s.byKey["B"]; ok {
		t.Error("Put rejeitado não deveria ter registrado a key")
	}
}

// rowId é monotônico, nunca reutilizado E nunca queimado por evento
// rejeitado: a validação inteira roda antes da reserva. Id queimado abre
// buraco nos índices densos por rowId, que ninguém preenche depois.
func TestRejectedPutDoesNotBurnRowID(t *testing.T) {
	s := New(Indexes{})
	first, _, err := s.Put([]byte("A"), 1, 0, payload(t, fv(1, strVal("A"))), nil)
	if err != nil {
		t.Fatal(err)
	}

	var bad []byte
	bad = mustAppendField(t, bad, 3, intVal(1))
	bad = mustAppendField(t, bad, 1, strVal("x"))
	if _, _, err := s.Put([]byte("B"), 1, 0, bad, nil); err == nil {
		t.Fatal("payload fora de ordem deveria falhar")
	}
	if _, _, err := s.Put(nil, 1, 0, payload(t, fv(1, strVal("B"))), nil); err == nil {
		t.Fatal("key vazia deveria falhar")
	}

	// O próximo Put válido recebe exatamente o id que os rejeitados teriam
	// recebido: nenhum buraco na sequência.
	next, created, err := s.Put([]byte("C"), 1, 0, payload(t, fv(1, strVal("C"))), nil)
	if err != nil || !created {
		t.Fatalf("Put: created=%v err=%v", created, err)
	}
	if next != first+1 {
		t.Errorf("rowId após Puts rejeitados = %d, want %d (id queimado)", next, first+1)
	}

	// E o id de uma row viva nunca é reatribuído a outra key.
	other, created, err := s.Put([]byte("D"), 1, 0, payload(t, fv(1, strVal("D"))), nil)
	if err != nil || !created || other != next+1 {
		t.Errorf("rowId = %d, want %d (created=%v err=%v)", other, next+1, created, err)
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
	// Tombstone: some das leituras (inclusive por key) e do scan.
	s.SetTombstone(rowID)
	if _, _, ok, _ := s.GetByRowID(rowID, nil); ok {
		t.Error("tombstoned deveria dar ok=false")
	}
	if _, _, ok, _ := s.GetByKey([]byte("ABC"), nil); ok {
		t.Error("GetByKey de row tombstoned deveria dar ok=false")
	}
	if got := drain(t, s.ScanLive()); len(got) != 0 {
		t.Errorf("ScanLive com tombstone = %v", got)
	}
}

// A liveness sai das flags do Put — não existe estado paralelo. É o mesmo bit
// que o real empacota na location.
func TestPutFlagsDriveLiveness(t *testing.T) {
	s := New(Indexes{})
	p := payload(t, fv(1, strVal("ABC")))
	rowID, _, err := s.Put([]byte("ABC"), 1, types.FlagTombstone, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := s.GetByRowID(rowID, nil); ok {
		t.Error("Put com FlagTombstone deveria nascer morta")
	}
	if got := drain(t, s.ScanLive()); len(got) != 0 {
		t.Errorf("ScanLive = %v", got)
	}
	// Ressuscitar é um Put sem a flag.
	if _, _, err := s.Put([]byte("ABC"), 2, 0, p, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := s.GetByRowID(rowID, nil); !ok {
		t.Error("Put sem flag deveria reviver a row")
	}
}

func TestSequence(t *testing.T) {
	s := New(Indexes{})
	rowID, _, _ := s.Put([]byte("ABC"), 42, 0, payload(t, fv(1, strVal("ABC"))), nil)
	if seq, ok := s.Sequence(rowID); !ok || seq != 42 {
		t.Errorf("Sequence = %d, %v", seq, ok)
	}
	if _, ok := s.Sequence(999); ok {
		t.Error("row inexistente não tem sequence")
	}
	// Row tombstoned mantém a sequence visível (no real a location existe,
	// apenas com o bit ligado).
	s.SetTombstone(rowID)
	if seq, ok := s.Sequence(rowID); !ok || seq != 42 {
		t.Errorf("Sequence pós-tombstone = %d, %v", seq, ok)
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
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("rowIds fora de ordem: %v", got)
		}
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

// Campo sem o índice consultado é erro de argumento, nunca iterador vazio: um
// planner que escolha caminho indexado para campo não indexado precisa
// quebrar no teste, não passar por acaso com resultado vazio.
func TestLookupWithoutIndexIsError(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}, Hash: []types.FieldID{3}})
	if _, _, err := s.Put([]byte("K"), 1, 0, payload(t, fv(1, strVal("K")), fv(2, strVal("Start"))), nil); err != nil {
		t.Fatal(err)
	}
	_, err := s.LookupBitmap(1, strVal("K"))
	assertInvalidArgument(t, err, "não possui índice bitmap")
	// Índice hash não atende consulta bitmap e vice-versa.
	_, err = s.LookupBitmap(3, intVal(0))
	assertInvalidArgument(t, err, "não possui índice bitmap")
	_, err = s.LookupHash(2, strVal("Start"))
	assertInvalidArgument(t, err, "não possui índice hash")

	// O capability check concorda com o que o lookup aceita.
	if s.HasBitmap(1) || s.HasBitmap(3) || !s.HasBitmap(2) {
		t.Error("HasBitmap diverge dos índices declarados")
	}
	if got := s.BitmapFields(); len(got) != 1 || got[0] != 2 {
		t.Errorf("BitmapFields = %v, want [2]", got)
	}
}

// O lookup indexado devolve SUPERSET (§44): a row tombstoned continua nas
// postings até um clear, e é o consumidor que valida liveness via
// GetByRowID. Um fake que pré-filtrasse esconderia o bug de rows mortas
// vazando no resultado.
func TestLookupReturnsSupersetWithTombstones(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	p := payload(t, fv(1, strVal("K1")), fv(2, strVal("Start")))
	rowID, _, err := s.Put([]byte("K1"), 1, 0, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.SetTombstone(rowID)

	it, err := s.LookupBitmap(2, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, it)
	if len(got) != 1 || got[0] != rowID {
		t.Fatalf("lookup deveria devolver o candidato morto: %v", got)
	}
	if _, _, ok, _ := s.GetByRowID(got[0], nil); ok {
		t.Error("a revalidação por GetByRowID é que derruba o candidato")
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

func drain(t *testing.T, it types.RowIDIterator) []types.RowID {
	t.Helper()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertPosting(t *testing.T, s *Store, field types.FieldID, v types.Value, want []types.RowID) {
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
