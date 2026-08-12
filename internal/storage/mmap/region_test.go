package mmap

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestClassificacaoDeFallocateNaoSuportado(t *testing.T) {
	// I2: filesystem sem fallocate NÃO pode degradar silenciosamente para
	// Truncate (sparse + disco cheio = SIGBUS). Open deve falhar com
	// ErrFallocateUnsupported nesses errnos.
	unsupported := []error{unix.EOPNOTSUPP, unix.ENOSYS, unix.EINVAL}
	for _, e := range unsupported {
		if !fallocateUnsupported(e) {
			t.Errorf("errno %v deveria classificar como fallocate não suportado", e)
		}
	}
	supported := []error{unix.ENOSPC, unix.EIO, unix.EBADF, nil}
	for _, e := range supported {
		if fallocateUnsupported(e) {
			t.Errorf("errno %v NÃO é 'não suportado' (deve propagar cru)", e)
		}
	}
}

func openTemp(t *testing.T, size int64) (*Region, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "region.idx")
	r, err := Open(path, size)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, path
}

func TestOpenCriaArquivoComTamanho(t *testing.T) {
	r, path := openTemp(t, 4096)
	if r.Size() != 4096 {
		t.Fatalf("Size: esperado 4096, obtido %d", r.Size())
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 4096 {
		t.Fatalf("arquivo: esperado 4096 bytes, obtido %d", st.Size())
	}
}

func TestAtomicStoreLoadRoundTrip(t *testing.T) {
	r, _ := openTemp(t, 4096)
	r.AtomicStore64(64, 0xDEADBEEFCAFEBABE)
	if got := r.AtomicLoad64(64); got != 0xDEADBEEFCAFEBABE {
		t.Fatalf("load: obtido %#x", got)
	}
	if got := r.AtomicLoad64(72); got != 0 {
		t.Fatalf("área nova deveria ser zero, obtido %#x", got)
	}
}

func TestAtomicOrAnd(t *testing.T) {
	r, _ := openTemp(t, 4096)
	r.AtomicOr64(0, 0b1010)
	r.AtomicOr64(0, 0b0101)
	if got := r.AtomicLoad64(0); got != 0b1111 {
		t.Fatalf("or: obtido %#b", got)
	}
	r.AtomicAnd64(0, ^uint64(0b0011))
	if got := r.AtomicLoad64(0); got != 0b1100 {
		t.Fatalf("and: obtido %#b", got)
	}
}

func TestBytesView(t *testing.T) {
	r, _ := openTemp(t, 4096)
	b := r.Bytes(100, 4)
	copy(b, "abcd")
	if string(r.Bytes(100, 4)) != "abcd" {
		t.Fatal("Bytes deveria ser uma view mutável da região")
	}
}

func TestMsyncEReopenPersistem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.idx")
	r, err := Open(path, 8192)
	if err != nil {
		t.Fatal(err)
	}
	r.AtomicStore64(8, 42)
	copy(r.Bytes(0, 4), "NDBX")
	if err := r.Msync(); err != nil {
		t.Fatalf("Msync: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r2, err := Open(path, 8192)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()
	if got := r2.AtomicLoad64(8); got != 42 {
		t.Fatalf("persistência: esperado 42, obtido %d", got)
	}
	if string(r2.Bytes(0, 4)) != "NDBX" {
		t.Fatalf("persistência de bytes: %q", r2.Bytes(0, 4))
	}
}

func TestOpenComArquivoMaiorMapeiaTudo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bigger.idx")
	r, err := Open(path, 16384)
	if err != nil {
		t.Fatal(err)
	}
	r.AtomicStore64(16000, 7)
	r.Close()

	// Reabrir pedindo tamanho menor não encolhe: mapeia o arquivo inteiro.
	r2, err := Open(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Size() != 16384 {
		t.Fatalf("Size: esperado 16384, obtido %d", r2.Size())
	}
	if got := r2.AtomicLoad64(16000); got != 7 {
		t.Fatalf("esperado 7, obtido %d", got)
	}
}

func TestGrowPreservaConteudo(t *testing.T) {
	r, path := openTemp(t, 4096)
	r.AtomicStore64(1024, 99)
	if err := r.Grow(65536); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	if r.Size() != 65536 {
		t.Fatalf("Size pós-grow: %d", r.Size())
	}
	if got := r.AtomicLoad64(1024); got != 99 {
		t.Fatalf("conteúdo antigo: esperado 99, obtido %d", got)
	}
	if got := r.AtomicLoad64(60000); got != 0 {
		t.Fatalf("área nova deveria ser zero, obtido %d", got)
	}
	r.AtomicStore64(65536-8, 123)
	if got := r.AtomicLoad64(65536 - 8); got != 123 {
		t.Fatalf("escrita além do tamanho antigo: obtido %d", got)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 65536 {
		t.Fatalf("arquivo pós-grow: %d", st.Size())
	}
}

func TestGrowNaoEncolhe(t *testing.T) {
	r, _ := openTemp(t, 8192)
	if err := r.Grow(4096); err == nil {
		t.Fatal("Grow para tamanho menor deveria falhar")
	}
}

func TestOffsetDesalinhadoPanica(t *testing.T) {
	r, _ := openTemp(t, 4096)
	defer func() {
		if recover() == nil {
			t.Fatal("esperado panic com offset desalinhado")
		}
	}()
	r.AtomicLoad64(3)
}

func TestOffsetForaDosLimitesPanica(t *testing.T) {
	r, _ := openTemp(t, 4096)
	defer func() {
		if recover() == nil {
			t.Fatal("esperado panic com offset além do fim")
		}
	}()
	r.AtomicLoad64(4096)
}

func TestLeituraConcorrenteComGrow(t *testing.T) {
	r, _ := openTemp(t, 4096)
	r.AtomicStore64(0, 7)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			if got := r.AtomicLoad64(0); got != 7 {
				t.Errorf("leitura durante grow: obtido %d", got)
				return
			}
		}
	}()
	for sz := int64(8192); sz <= 8192*8; sz *= 2 {
		if err := r.Grow(sz); err != nil {
			t.Fatalf("Grow(%d): %v", sz, err)
		}
	}
	<-done
}
