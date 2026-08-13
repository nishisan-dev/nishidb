package fakestorage

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// HasHash é o capability check POR CAMPO do índice hash, análogo do HasBitmap.
// Sem ele, um schema que declara `index: hash` num campo que o store não
// indexa faria o planner escolher HashIndex e o executor falhar em runtime —
// exatamente o que a degradação para full scan existe para evitar.
func TestHasHash(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}, Hash: []types.FieldID{3}})

	casos := []struct {
		field     types.FieldID
		hash, bmp bool
		porque    string
	}{
		{3, true, false, "campo declarado com índice hash"},
		{2, false, true, "campo bitmap não é atendido pelo hash"},
		{99, false, false, "campo sem índice nenhum"},
		{0, false, false, "fieldID reservado"},
	}
	for _, tc := range casos {
		if got := s.HasHash(tc.field); got != tc.hash {
			t.Errorf("HasHash(%d) = %v, want %v (%s)", tc.field, got, tc.hash, tc.porque)
		}
		if got := s.HasBitmap(tc.field); got != tc.bmp {
			t.Errorf("HasBitmap(%d) = %v, want %v (%s)", tc.field, got, tc.bmp, tc.porque)
		}
	}
}

// InjectPosting reproduz o falso positivo que o Store real tolera por
// construção (§14.16: erro de Clear é contabilizado e deixa posting órfão). É
// o único jeito de provar que o executor REVALIDA o predicate (§44) em vez de
// confiar no índice — sem ele, um executor que devolvesse os candidatos crus
// passaria em todos os testes.
func TestInjectPostingCriaFalsoPositivo(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	p := payload(t, fv(1, strVal("ABC")), fv(2, strVal("Start")))
	rowID, _, err := s.Put([]byte("ABC"), 1, 0, p, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.InjectPosting(2, strVal("Stop"), rowID); err != nil {
		t.Fatal(err)
	}

	// O índice passa a apontar a row para um valor que ela NÃO tem.
	if got := ids(t, s, 2, strVal("Stop")); len(got) != 1 || got[0] != rowID {
		t.Errorf("LookupBitmap(Stop) = %v, want [%d]", got, rowID)
	}
	// E o posting verdadeiro continua lá: injetar não substitui.
	if got := ids(t, s, 2, strVal("Start")); len(got) != 1 || got[0] != rowID {
		t.Errorf("LookupBitmap(Start) = %v, want [%d]", got, rowID)
	}
	// O ESTADO da row não muda — é o que separa um posting órfão de um update.
	rec, _, ok, err := s.GetByRowID(rowID, nil)
	if err != nil || !ok {
		t.Fatalf("GetByRowID: ok=%v err=%v", ok, err)
	}
	if string(rec.Payload) != string(p) {
		t.Errorf("payload mudou: %x, want %x", rec.Payload, p)
	}
}

// Posting para rowId que não existe também é estado alcançável no real (a row
// some do índice denso, o posting fica). O consumidor tem de sobreviver a ele.
func TestInjectPostingRowInexistente(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	if err := s.InjectPosting(2, strVal("Stop"), types.RowID(42)); err != nil {
		t.Fatal(err)
	}
	if got := ids(t, s, 2, strVal("Stop")); len(got) != 1 || got[0] != 42 {
		t.Errorf("candidatos = %v, want [42]", got)
	}
	if _, _, ok, err := s.GetByRowID(42, nil); ok || err != nil {
		t.Errorf("GetByRowID(42) = ok:%v err:%v, want false/nil", ok, err)
	}
}

// Índice hash recebe injeção pelo mesmo caminho: o falso positivo não é
// exclusividade do bitmap.
func TestInjectPostingHash(t *testing.T) {
	s := New(Indexes{Hash: []types.FieldID{3}})
	if err := s.InjectPosting(3, intVal(7), types.RowID(1)); err != nil {
		t.Fatal(err)
	}
	it, err := s.LookupHash(3, intVal(7))
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() || it.RowID() != 1 {
		t.Error("LookupHash não devolveu o posting injetado")
	}
}

// Campo sem índice não tem posting a injetar: aceitar em silêncio criaria um
// estado que o real não alcança, e o teste que dependesse dele passaria aqui e
// falharia na integração.
func TestInjectPostingCampoSemIndice(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	err := s.InjectPosting(7, strVal("Stop"), types.RowID(0))
	if !errors.Is(err, types.ErrInvalidArgument) {
		t.Fatalf("erro = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "não possui índice") {
		t.Errorf("mensagem = %q, want dizer que o campo não tem índice", err)
	}
}

// O teto do values.dict vale para a injeção como vale para o Put: um posting
// de valor acima do teto é um estado que o real recusa a criar.
func TestInjectPostingRespeitaTetoDoValor(t *testing.T) {
	s := New(Indexes{Bitmap: []types.FieldID{2}})
	grande := types.Value{Type: types.TypeString, Bytes: make([]byte, types.MaxIndexedValueBytes+1)}
	err := s.InjectPosting(2, grande, types.RowID(0))
	if !errors.Is(err, types.ErrInvalidArgument) {
		t.Fatalf("erro = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "teto") {
		t.Errorf("mensagem = %q, want mencionar o teto", err)
	}
}

func ids(t *testing.T, s *Store, field types.FieldID, v types.Value) []types.RowID {
	t.Helper()
	it, err := s.LookupBitmap(field, v)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
