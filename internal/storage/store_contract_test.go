package storage

// Itens de contrato da SC: a interface de iteração compartilhada (C2), o
// sentinela do modo falho (C4) e o capability check de índice (G1).

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// C2: os dois provedores (Store real e o fake do ramo query) precisam
// satisfazer a MESMA interface, declarada em internal/types. Estas asserções
// de compilação são o lado do storage.
var _ types.RowIDIterator = (*liveIterator)(nil)

func TestScanLiveELookupDevolvemAInterfaceCompartilhada(t *testing.T) {
	s := openStore(t, Options{IndexedFields: []types.FieldID{fldStatus}})
	mustPut(t, s, "it", 1, statusPayload(t, s, "it", "Start", 1), nil)

	// A atribuição a types.RowIDIterator é o que o teste verifica: se as
	// assinaturas voltarem a ser uma interface local do pacote, não compila.
	var scan types.RowIDIterator = s.ScanLive(s.presente())
	defer scan.Close()
	if !scan.Next() {
		t.Fatal("ScanLive deveria devolver a row")
	}

	lookup, err := s.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	var _ types.RowIDIterator = lookup
	defer lookup.Close()
	if !lookup.Next() {
		t.Fatal("Lookup deveria devolver a posting")
	}
}

func TestErrWriterFailedDistingueWriterMortoDeEventoRuim(t *testing.T) {
	// C4: sem o sentinela, a política de log-and-skip por evento (§101) não
	// consegue separar "pule este evento" de "pare a ingestão" e vira um
	// skip infinito com o writer morto.
	s := openStore(t, Options{IndexedFields: []types.FieldID{fldStatus}})
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

func TestSentinelasSaoOsMesmosValoresDeTypes(t *testing.T) {
	// Os sentinelas moram em internal/types para que o materializer e o
	// dublê de teste dele possam classificar e DEVOLVER esses erros sem
	// importar a implementação que dublam. Os símbolos daqui são
	// reexportações: se alguém os redeclarar com errors.New, a identidade
	// quebra e o consumidor para de reconhecer o erro — sem falhar o build.
	if ErrInvalidArgument != types.ErrInvalidArgument {
		t.Fatal("ErrInvalidArgument precisa ser o MESMO valor de types.ErrInvalidArgument")
	}
	if ErrWriterFailed != types.ErrWriterFailed {
		t.Fatal("ErrWriterFailed precisa ser o MESMO valor de types.ErrWriterFailed")
	}

	// E a classificação funciona pelos dois caminhos, sobre erro real.
	s := openStore(t, Options{})
	_, _, err := s.Put(nil, 1, 0, statusPayload(t, s, "sent", "Start", 1), nil)
	if !errors.Is(err, ErrInvalidArgument) || !errors.Is(err, types.ErrInvalidArgument) {
		t.Fatalf("errors.Is deveria casar pelos dois pacotes: %v", err)
	}
}

func TestIndexadoEhCapabilityCheck(t *testing.T) {
	// G1: sem isto o planner escolhe caminho indexado e a query falha em
	// runtime em vez de degradar para full scan.
	s := openStore(t, Options{IndexedFields: []types.FieldID{fldStatus}})
	if !s.Indexed(fldStatus) {
		t.Fatal("campo declarado deveria ser indexado")
	}
	if s.Indexed(fldCounter) {
		t.Fatal("campo não declarado não pode reportar índice")
	}
	if got := s.FieldsIndexados(); len(got) != 1 || got[0] != fldStatus {
		t.Fatalf("FieldsIndexados: %v", got)
	}
	// A resposta bate com o comportamento real do Lookup.
	if _, err := s.Lookup(fldCounter, strVal("x")); err == nil {
		t.Fatal("Lookup de campo sem índice deveria falhar")
	}
	if _, err := s.Lookup(fldStatus, strVal("Start")); err != nil {
		t.Fatalf("Lookup de campo indexado: %v", err)
	}

	withoutIndex := openStore(t, Options{})
	if withoutIndex.Indexed(fldStatus) {
		t.Fatal("store sem campos indexados não pode reportar capability")
	}
	if got := withoutIndex.FieldsIndexados(); len(got) != 0 {
		t.Fatalf("FieldsIndexados sem índices: %v", got)
	}
}
