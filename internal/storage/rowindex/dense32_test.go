package rowindex

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

var testMagic32 = [4]byte{'T', 'S', 'T', '1'}

func openDense32(t *testing.T, path string, minRows uint64) *Dense32 {
	t.Helper()
	d, err := OpenDense32(path, testMagic32, minRows)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestDenso32GravaLeECresce(t *testing.T) {
	d := openDense32(t, filepath.Join(t.TempDir(), "g.idx"), 512)
	if !d.Born() {
		t.Fatal("arquivo novo tem de nascer")
	}
	// Dentro do arquivo, entrada nunca gravada é zero e covered=true (é o
	// que o fallocate deixa); covered=false é SÓ para rowId além do arquivo —
	// a mesma semântica de load nos vetores de 64.
	if v, ok := d.Get(10); !ok || v != 0 {
		t.Fatalf("Get(10) = %d, %v; quer 0, true", v, ok)
	}
	if _, ok := d.Get(1 << 30); ok {
		t.Fatal("rowId além do arquivo tem de sair covered=false")
	}
	// Além do piso: cresce por doubling, como os vetores de 64.
	const high = types.RowID(100_000)
	const v = uint32(1<<31 | 7)
	if err := d.Set(high, v); err != nil {
		t.Fatal(err)
	}
	if got, ok := d.Get(high); !ok || got != v {
		t.Fatalf("Get(%d) = %#x, %v; quer %#x, true", high, got, ok, v)
	}
	if !d.Covers(high+1) || d.Covers(1<<40) {
		t.Fatal("Covers errado")
	}
	// Vizinhas intactas: a entrada de 4 bytes não pisa na do lado.
	if err := d.Set(high+1, 0x0000_0001); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.Get(high); got != v {
		t.Fatalf("a escrita na vizinha alterou a entrada: %#x", got)
	}
	if err := d.Clear(); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.Get(high); got != 0 {
		t.Fatalf("Clear não zerou: %#x", got)
	}
}

func TestDenso32ReabreEPreserva(t *testing.T) {
	path := filepath.Join(t.TempDir(), "g.idx")
	d := openDense32(t, path, 512)
	if err := d.Set(3, 42); err != nil {
		t.Fatal(err)
	}
	if err := d.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2 := openDense32(t, path, 512)
	if d2.Born() {
		t.Fatal("reabertura não é nascimento")
	}
	if got, ok := d2.Get(3); !ok || got != 42 {
		t.Fatalf("Get(3) = %d, %v; quer 42, true", got, ok)
	}
}

// TestDenso32RecusaArquivoDeOitoBytes: o byte 5 do header grava o tamanho da
// entrada, e um vetor de 64 aberto como 32 (ou o inverso) endereçaria a
// entrada errada em toda linha — a recusa é o que impede isso de virar número
// silenciosamente errado.
func TestDenso32RecusaArquivoDeOitoBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.idx")
	e, err := OpenLastWrites(path, 512)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDense32(path, lastWritesMagic, 512); err == nil {
		t.Fatal("arquivo com entrySize 8 aberto como 4 tem de ser recusado")
	}
	path32 := filepath.Join(t.TempDir(), "y.idx")
	d := openDense32(t, path32, 512)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLastWrites(path32, 512); err == nil {
		t.Fatal("arquivo com entrySize 4 aberto como 8 tem de ser recusado")
	}
}

func TestDenso32SizeBytesAcompanhaOArquivo(t *testing.T) {
	d := openDense32(t, filepath.Join(t.TempDir(), "g.idx"), 512)
	before := d.SizeBytes()
	if before < headerSize+512*4 {
		t.Fatalf("tamanho inicial %d abaixo do piso", before)
	}
	if err := d.Set(1<<20, 1); err != nil {
		t.Fatal(err)
	}
	if d.SizeBytes() <= before {
		t.Fatal("o arquivo tinha de crescer")
	}
}
