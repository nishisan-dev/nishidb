package storage

// Itens de contrato da SC: a interface de iteração compartilhada (C2), o
// sentinela do modo falho (C4) e o capability check de bitmap (G1).

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// C2: os dois provedores (Store real e o fake do ramo query) precisam
// satisfazer a MESMA interface, declarada em internal/types. Estas asserções
// de compilação são o lado do storage.
var (
	_ types.RowIDIterator = (*liveIterator)(nil)
	_ types.RowIDIterator = emptyIterator{}
)

func TestScanLiveELookupBitmapDevolvemAInterfaceCompartilhada(t *testing.T) {
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	mustPut(t, s, "it", 1, statusPayload(t, s, "it", "Start", 1), nil)

	// A atribuição a types.RowIDIterator é o que o teste verifica: se as
	// assinaturas voltarem a ser uma interface local do pacote, não compila.
	var scan types.RowIDIterator = s.ScanLive()
	defer scan.Close()
	if !scan.Next() {
		t.Fatal("ScanLive deveria devolver a row")
	}

	lookup, err := s.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	var _ types.RowIDIterator = lookup
	defer lookup.Close()
	if !lookup.Next() {
		t.Fatal("LookupBitmap deveria devolver a posting")
	}
}

func TestErrWriterFailedDistingueWriterMortoDeEventoRuim(t *testing.T) {
	// C4: sem o sentinela, a política de log-and-skip por evento (§101) não
	// consegue separar "pule este evento" de "pare a ingestão" e vira um
	// skip infinito com o writer morto.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	mustPut(t, s, "w", 1, statusPayload(t, s, "w", "Start", 1), nil)

	// Evento ruim: erro de argumento, writer segue vivo.
	_, _, err := s.Put(nil, 1, 0, statusPayload(t, s, "w", "Start", 1), nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("evento ruim: esperado ErrInvalidArgument, obtido %v", err)
	}
	if errors.Is(err, ErrWriterFailed) {
		t.Fatal("evento ruim não pode ser confundido com writer morto")
	}

	// Writer morto: erro pós-Append deixa o modo falho sticky.
	s.SetSetErrHookForTest(func(types.FieldID) error { return errors.New("set falho simulado") })
	if _, _, err := s.Put([]byte("w2"), 1, 0, statusPayload(t, s, "w2", "Start", 1), nil); err == nil {
		t.Fatal("o Put com Set falho deveria falhar")
	}
	s.SetSetErrHookForTest(nil)

	_, _, err = s.Put([]byte("w3"), 1, 0, statusPayload(t, s, "w3", "Start", 1), nil)
	if !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("writer morto: esperado ErrWriterFailed, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "reabra o dataset") {
		t.Fatalf("a mensagem precisa dizer o remédio: %v", err)
	}
	// Sync e Checkpoint carregam o mesmo sentinela.
	if err := s.Sync(); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.Checkpoint(); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("Checkpoint: %v", err)
	}
	// E a causa original sobrevive na cadeia.
	if !strings.Contains(err.Error(), "set falho simulado") {
		t.Fatalf("a causa original deveria ser preservada: %v", err)
	}
}

func TestHasBitmapEhCapabilityCheck(t *testing.T) {
	// G1: sem isto o planner escolhe caminho indexado e a query falha em
	// runtime em vez de degradar para full scan.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	if !s.HasBitmap(fldStatus) {
		t.Fatal("campo declarado deveria ter bitmap")
	}
	if s.HasBitmap(fldCounter) {
		t.Fatal("campo não declarado não pode reportar bitmap")
	}
	if got := s.BitmapFields(); len(got) != 1 || got[0] != fldStatus {
		t.Fatalf("BitmapFields: %v", got)
	}
	// A resposta bate com o comportamento real do LookupBitmap.
	if _, err := s.LookupBitmap(fldCounter, strVal("x")); err == nil {
		t.Fatal("LookupBitmap de campo sem índice deveria falhar")
	}
	if _, err := s.LookupBitmap(fldStatus, strVal("Start")); err != nil {
		t.Fatalf("LookupBitmap de campo indexado: %v", err)
	}

	semBitmap := openStore(t, Options{})
	if semBitmap.HasBitmap(fldStatus) {
		t.Fatal("store sem bitmaps não pode reportar capability")
	}
	if got := semBitmap.BitmapFields(); len(got) != 0 {
		t.Fatalf("BitmapFields sem índices: %v", got)
	}
}
