package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// O par base/histórico compartilha o dicionário porque o payload gravado na
// tabela irmã é o MESMO que o patch do base montou, endereçado por FieldID.
func TestDictCompartilhadoDaOsMesmosFieldIDs(t *testing.T) {
	base := openStore(t, Options{})
	irma := openStore(t, Options{SharedDictionary: base.FieldDict()})

	id, err := base.FieldDict().LookupOrCreate([]byte("severity"))
	if err != nil {
		t.Fatalf("LookupOrCreate no base: %v", err)
	}
	mesmo, err := irma.FieldDict().LookupOrCreate([]byte("severity"))
	if err != nil {
		t.Fatalf("LookupOrCreate na irmã: %v", err)
	}
	if id != mesmo {
		t.Fatalf("FieldID %d no base e %d na irmã", id, mesmo)
	}
	if irma.FieldDict() != base.FieldDict() {
		t.Fatal("a irmã abriu um dicionário próprio")
	}
}

// Fechar a irmã no meio da vida do base não pode derrubar a ingestão dele: o
// dicionário é do base, e um LookupOrCreate sobre arquivo fechado é falha de
// infra, que para o writer.
func TestFecharAIrmaNaoFechaODicionarioDoBase(t *testing.T) {
	base := openStore(t, Options{})
	dirIrma := t.TempDir()
	irma, err := Open(Options{Dir: dirIrma, SegmentSize: 1 << 20, SharedDictionary: base.FieldDict()})
	if err != nil {
		t.Fatalf("Open da irmã: %v", err)
	}
	if err := irma.Close(); err != nil {
		t.Fatalf("Close da irmã: %v", err)
	}
	if _, err := base.FieldDict().LookupOrCreate([]byte("depoisDoClose")); err != nil {
		t.Fatalf("o dicionário do base morreu junto com a irmã: %v", err)
	}
}

// Nenhum fields.dict é criado no diretório de quem compartilha. Um arquivo
// vazio ali seria um segundo dicionário aparente, e quem o encontrasse teria
// razão para achar que o dataset abre sozinho — ele não abre.
func TestQuemCompartilhaNaoCriaFieldsDict(t *testing.T) {
	base := openStore(t, Options{})
	dirIrma := t.TempDir()
	irma := openStore(t, Options{Dir: dirIrma, SharedDictionary: base.FieldDict()})
	if err := irma.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dirIrma, "metadata", "fields.dict")); !os.IsNotExist(err) {
		t.Fatalf("o diretório da irmã tem fields.dict (err = %v)", err)
	}
}
