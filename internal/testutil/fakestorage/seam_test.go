package fakestorage

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Indexado é o capability check POR CAMPO do planner. Sem ele, um schema que
// declara `index: true` num campo que o store não indexa faria o planner
// escolher o caminho indexado e o executor falhar em runtime — exatamente o que
// a degradação para full scan existe para evitar.
//
// Este teste chamava-se TestHasHash e cruzava as DUAS capabilities, HasBitmap e
// HasHash: além dos casos abaixo, ele afirmava que "campo bitmap não é atendido
// pelo hash" e vice-versa. Essa asserção não perdeu força — perdeu SUJEITO: com um motor só,
// declarar um campo em um motor e não no outro deixou de ser um estado
// construível, aqui e no Store real.
func TestIndexado(t *testing.T) {
	s := New(Indexes{Fields: []types.FieldID{2, 3}})

	cases := []struct {
		field   types.FieldID
		indexed bool
		reason  string
	}{
		{3, true, "campo declarado indexado"},
		{2, true, "campo declarado indexado"},
		{99, false, "campo sem índice"},
		{0, false, "fieldID reservado"},
	}
	for _, tc := range cases {
		if got := s.Indexed(tc.field); got != tc.indexed {
			t.Errorf("Indexado(%d) = %v, want %v (%s)", tc.field, got, tc.indexed, tc.reason)
		}
	}
}

// InjectPosting reproduz o falso positivo que o Store real tolera por
// construção (§14.16: erro de Clear é contabilizado e deixa posting órfão). É
// o único jeito de provar que o executor REVALIDA o predicate (§44) em vez de
// confiar no índice — sem ele, um executor que devolvesse os candidatos crus
// passaria em todos os testes.
func TestInjectPostingCriaFalsoPositivo(t *testing.T) {
	s := New(Indexes{Fields: []types.FieldID{2}})
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
		t.Errorf("Lookup(Stop) = %v, want [%d]", got, rowID)
	}
	// E o posting verdadeiro continua lá: injetar não substitui.
	if got := ids(t, s, 2, strVal("Start")); len(got) != 1 || got[0] != rowID {
		t.Errorf("Lookup(Start) = %v, want [%d]", got, rowID)
	}
	// O ESTADO da row não muda — é o que separa um posting órfão de um update.
	rec, _, ok, err := s.GetByRowID(s.OpenSnapshot(), rowID, nil)
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
	s := New(Indexes{Fields: []types.FieldID{2}})
	if err := s.InjectPosting(2, strVal("Stop"), types.RowID(42)); err != nil {
		t.Fatal(err)
	}
	if got := ids(t, s, 2, strVal("Stop")); len(got) != 1 || got[0] != 42 {
		t.Errorf("candidatos = %v, want [42]", got)
	}
	if _, _, ok, err := s.GetByRowID(s.OpenSnapshot(), 42, nil); ok || err != nil {
		t.Errorf("GetByRowID(42) = ok:%v err:%v, want false/nil", ok, err)
	}
}

// A injeção vale para valor NÃO-textual pelo mesmo caminho: a chave canônica
// do posting inclui o tipo, e um addPosting que só soubesse serializar bytes
// perderia o int silenciosamente.
//
// Chamava-se TestInjectPostingHash e provava que "o falso positivo não é
// exclusividade do bitmap" — asserção sem sujeito desde que há um motor só, e
// sem API desde que LookupHash colapsou em Lookup. O corpo não mudou: o campo 3
// continua sendo o campo de valor inteiro.
func TestInjectPostingValorInteiro(t *testing.T) {
	s := New(Indexes{Fields: []types.FieldID{3}})
	if err := s.InjectPosting(3, intVal(7), types.RowID(1)); err != nil {
		t.Fatal(err)
	}
	it, err := s.Lookup(3, intVal(7))
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() || it.RowID() != 1 {
		t.Error("Lookup não devolveu o posting injetado")
	}
}

// Campo sem índice não tem posting a injetar: aceitar em silêncio criaria um
// estado que o real não alcança, e o teste que dependesse dele passaria aqui e
// falharia na integração.
func TestInjectPostingCampoSemIndice(t *testing.T) {
	s := New(Indexes{Fields: []types.FieldID{2}})
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
	s := New(Indexes{Fields: []types.FieldID{2}})
	large := types.Value{Type: types.TypeString, Bytes: make([]byte, types.MaxIndexedValueBytes+1)}
	err := s.InjectPosting(2, large, types.RowID(0))
	if !errors.Is(err, types.ErrInvalidArgument) {
		t.Fatalf("erro = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "teto") {
		t.Errorf("mensagem = %q, want mencionar o teto", err)
	}
}

func ids(t *testing.T, s *Store, field types.FieldID, v types.Value) []types.RowID {
	t.Helper()
	it, err := s.Lookup(field, v)
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
