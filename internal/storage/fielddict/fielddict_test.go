package fielddict

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func openDict(t *testing.T) (*Dict, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, path
}

func mustCreate(t *testing.T, d *Dict, name string) types.FieldID {
	t.Helper()
	id, err := d.LookupOrCreate([]byte(name))
	if err != nil {
		t.Fatalf("LookupOrCreate(%q): %v", name, err)
	}
	return id
}

func TestIDsSequenciaisAPartirDe1(t *testing.T) {
	d, _ := openDict(t)
	names := []string{"id", "_id", "_SEQ", "Acct-Status-Type", "Framed-IP-Address"}
	for i, n := range names {
		if id := mustCreate(t, d, n); id != types.FieldID(i+1) {
			t.Fatalf("campo %q: esperado id %d, obtido %d", n, i+1, id)
		}
	}
	if d.Len() != len(names) {
		t.Fatalf("Len: esperado %d, obtido %d", len(names), d.Len())
	}
}

func TestLookupOrCreateIdempotente(t *testing.T) {
	d, _ := openDict(t)
	a := mustCreate(t, d, "Acct-Status-Type")
	b := mustCreate(t, d, "Acct-Status-Type")
	if a != b {
		t.Fatalf("mesmo nome com ids distintos: %d vs %d", a, b)
	}
	if d.Len() != 1 {
		t.Fatalf("Len: esperado 1, obtido %d", d.Len())
	}
}

func TestNameRoundTrip(t *testing.T) {
	d, _ := openDict(t)
	id := mustCreate(t, d, "Calling-Station-Id")
	name, ok := d.Name(id)
	if !ok || name != "Calling-Station-Id" {
		t.Fatalf("Name(%d): (%q,%v)", id, name, ok)
	}
	if _, ok := d.Name(0); ok {
		t.Fatal("id 0 é reservado")
	}
	if _, ok := d.Name(999); ok {
		t.Fatal("id inexistente deveria devolver false")
	}
}

func TestLookupSemCriacao(t *testing.T) {
	d, _ := openDict(t)
	mustCreate(t, d, "existe")
	if id, ok := d.Lookup([]byte("existe")); !ok || id != 1 {
		t.Fatalf("Lookup: (%d,%v)", id, ok)
	}
	if _, ok := d.Lookup([]byte("nao-existe")); ok {
		t.Fatal("Lookup não pode criar")
	}
	if d.Len() != 1 {
		t.Fatalf("Len alterado por Lookup: %d", d.Len())
	}
}

func TestReopenPreservaMapeamento(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"id", "_SEQ", "NAS-Identifier", "campo-com-acentuação-çãé"}
	ids := make([]types.FieldID, len(names))
	for i, n := range names {
		ids[i] = mustCreate(t, d, n)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if d2.Len() != len(names) {
		t.Fatalf("Len pós-reopen: %d", d2.Len())
	}
	for i, n := range names {
		if id, ok := d2.Lookup([]byte(n)); !ok || id != ids[i] {
			t.Fatalf("Lookup(%q) pós-reopen: (%d,%v), esperado %d", n, id, ok, ids[i])
		}
		if name, ok := d2.Name(ids[i]); !ok || name != n {
			t.Fatalf("Name(%d) pós-reopen: (%q,%v)", ids[i], name, ok)
		}
	}
	// Criação continua da sequência correta.
	if id := mustCreate(t, d2, "novo-campo"); id != types.FieldID(len(names)+1) {
		t.Fatalf("id pós-reopen: esperado %d, obtido %d", len(names)+1, id)
	}
}

func TestTailRasgadoTruncaEPermiteContinuar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, d, "sobrevive-1")
	mustCreate(t, d, "sobrevive-2")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	validSize := fileSize(t, path)

	// Simula crash no meio do append de uma entry.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x0B, 0x00, 0x00, 'p', 'a', 'r'}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	d2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen com tail rasgado: %v", err)
	}
	defer d2.Close()
	if d2.Len() != 2 {
		t.Fatalf("prefixo válido: esperado 2 entries, obtido %d", d2.Len())
	}
	if got := fileSize(t, path); got != validSize {
		t.Fatalf("tail não truncado: %d bytes (válido era %d)", got, validSize)
	}
	if id := mustCreate(t, d2, "pos-crash"); id != 3 {
		t.Fatalf("id pós-truncate: esperado 3, obtido %d", id)
	}
}

func TestEntryCorrompidaTruncaAli(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fields.dict")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, d, "integra")
	sizeAfterFirst := fileSize(t, path)
	mustCreate(t, d, "sera-corrompida")
	d.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF // corrompe o CRC da última entry
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen com entry corrompida: %v", err)
	}
	defer d2.Close()
	if d2.Len() != 1 {
		t.Fatalf("esperado 1 entry válida, obtido %d", d2.Len())
	}
	if got := fileSize(t, path); got != sizeAfterFirst {
		t.Fatalf("truncate na entry corrompida: %d bytes (esperado %d)", got, sizeAfterFirst)
	}
}

func TestNomesInvalidos(t *testing.T) {
	d, _ := openDict(t)
	if _, err := d.LookupOrCreate(nil); err == nil {
		t.Fatal("nome vazio deveria falhar")
	}
	if _, err := d.LookupOrCreate(bytes.Repeat([]byte{'x'}, 70_000)); err == nil {
		t.Fatal("nome maior que u16 deveria falhar")
	}
}

func TestLimiteDeIDs(t *testing.T) {
	d, _ := openDict(t)
	d.setNextIDForTest(65535)
	if id := mustCreate(t, d, "ultimo-valido"); id != 65535 {
		t.Fatalf("esperado id 65535, obtido %d", id)
	}
	if _, err := d.LookupOrCreate([]byte("estoura")); err == nil {
		t.Fatal("id além de 65535 deveria falhar")
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}
