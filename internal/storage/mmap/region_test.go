package mmap

import (
	"errors"
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

func TestWithBytesView(t *testing.T) {
	// I1: a view NUNCA escapa do RLock — o acesso é escopado no callback,
	// então é impossível reter a slice através de um Grow (SIGSEGV).
	r, _ := openTemp(t, 4096)
	r.WithBytes(100, 4, func(b []byte) {
		copy(b, "abcd")
	})
	var got string
	r.WithBytes(100, 4, func(b []byte) {
		got = string(b)
	})
	if got != "abcd" {
		t.Fatalf("WithBytes deveria expor view mutável da região: %q", got)
	}
}

func TestWithBytesForaDosLimitesPanica(t *testing.T) {
	r, _ := openTemp(t, 4096)
	defer func() {
		if recover() == nil {
			t.Fatal("esperado panic com range além do fim")
		}
	}()
	r.WithBytes(4090, 8, func([]byte) {})
}

func TestMsyncEReopenPersistem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.idx")
	r, err := Open(path, 8192)
	if err != nil {
		t.Fatal(err)
	}
	r.AtomicStore64(8, 42)
	r.WithBytes(0, 4, func(b []byte) { copy(b, "NDBX") })
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
	var hdr string
	r2.WithBytes(0, 4, func(b []byte) { hdr = string(b) })
	if hdr != "NDBX" {
		t.Fatalf("persistência de bytes: %q", hdr)
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

// TestAbreSomenteLeituraNaoRetemDescritor é a razão de esta função existir:
// um dataset de 250 GiB tem 500 segmentos, e um descritor por segmento
// estouraria o ulimit -n típico de 1024. No Linux o mapeamento sobrevive ao
// close do fd, e um arquivo selado nunca precisa do fd de volta — não cresce,
// não faz msync e não faz punch-hole.
func TestAbreSomenteLeituraNaoRetemDescritor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.bin")
	if err := os.WriteFile(path, []byte("abcdefgh"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := countOpenDescriptors(t)
	r, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if after := countOpenDescriptors(t); after != before {
		t.Errorf("descritores abertos: %d antes, %d depois — quero iguais", before, after)
	}
	if got := string(r.Bytes()); got != "abcdefgh" {
		t.Errorf("Bytes() = %q, quero %q — o mapeamento tem de sobreviver ao close", got, "abcdefgh")
	}
}

// countOpenDescriptors conta as entradas de /proc/self/fd.
func countOpenDescriptors(t *testing.T) int {
	t.Helper()
	e, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("sem /proc/self/fd: %v", err)
	}
	return len(e)
}

// TestRegiaoDeEscritaRecusaBytes trava a fronteira: entregar o slice cru de uma
// região que o Grow pode remapear devolveria memória liberada ao chamador
// depois do primeiro crescimento. Este é o tipo de erro que só aparece em
// produção, e o pânico o transforma em falha de teste.
func TestRegiaoDeEscritaRecusaBytes(t *testing.T) {
	r, err := Open(filepath.Join(t.TempDir(), "w.bin"), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() {
		if recover() == nil {
			t.Error("Bytes() numa região de escrita deveria entrar em pânico")
		}
	}()
	_ = r.Bytes()
}

// TestRegiaoSomenteLeituraRecusaMutacao cobre a outra metade da fronteira: a
// região somente-leitura não tem descritor e o mapeamento é PROT_READ, então
// TODO caminho de escrita precisa parar ANTES de chegar ao unix — em r.f, que
// é nil, viraria nil pointer dereference, e no mapeamento viraria SIGSEGV
// incapturável. Os que devolvem erro recusam com erro; os que não têm por onde
// devolver, entram em pânico.
func TestRegiaoSomenteLeituraRecusaMutacao(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ro.bin")
	if err := os.WriteFile(path, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	refusesWithError := map[string]func() error{
		"Grow":         func() error { return r.Grow(8192) },
		"Msync":        func() error { return r.Msync() },
		"LiberaBlocos": func() error { return r.ReleaseBlocks(0, 4096) },
	}
	for name, fn := range refusesWithError {
		// A asserção é pela SENTINELA, não por "err != nil": os três já tinham
		// caminhos de erro antes (Grow que não excede, LiberaBlocos fora dos
		// limites), então um erro qualquer não prova que a recusa veio da
		// guarda de somente-leitura. É o errors.Is que o chamador vai usar.
		err := fn()
		if err == nil {
			t.Errorf("%s numa região somente-leitura devolveu nil — quero erro", name)
			continue
		}
		if !errors.Is(err, ErrReadOnly) {
			t.Errorf("%s devolveu %v, quero ErrSomenteLeitura", name, err)
		}
	}
	// Comprimento zero também recusa: deixar o curto-circuito de length <= 0
	// preceder a guarda faria a mesma chamada errada devolver nil ou erro
	// conforme o argumento.
	if err := r.ReleaseBlocks(0, 0); !errors.Is(err, ErrReadOnly) {
		t.Errorf("LiberaBlocos(0, 0) devolveu %v, quero ErrSomenteLeitura", err)
	}

	refusesWithPanic := map[string]func(){
		"AtomicStore64": func() { r.AtomicStore64(0, 1) },
		"AtomicOr64":    func() { r.AtomicOr64(0, 1) },
		"AtomicAnd64":   func() { r.AtomicAnd64(0, 1) },
		"WithBytes":     func() { r.WithBytes(0, 8, func([]byte) {}) },
	}
	for name, fn := range refusesWithPanic {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("%s numa região somente-leitura deveria entrar em pânico", name)
				}
			}()
			fn()
		})
	}

	// Ler continua valendo: é para isso que a região existe.
	if got := r.AtomicLoad64(0); got != 0 {
		t.Errorf("AtomicLoad64(0) = %d, quero 0", got)
	}
}

// TestAbreSomenteLeituraRecusaArquivoVazio: unix.Mmap com length 0 falha com
// EINVAL, e devolver esse errno cru não diria nada ao operador. Um .sidx de
// zero bytes só existe como escrita truncada — o escritor recusa emitir índice
// vazio —, então a recusa nomeada é a resposta certa, e o chamador a trata
// como "sem índice" e reconstrói.
func TestAbreSomenteLeituraRecusaArquivoVazio(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vazio.bin")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReadOnly(path)
	if err == nil {
		_ = r.Close()
		t.Fatal("AbreSomenteLeitura de arquivo vazio deveria falhar")
	}
	if !errors.Is(err, ErrEmptyFile) {
		t.Errorf("erro = %v, quero ErrArquivoVazio", err)
	}
}

func TestLeituraConcorrenteComGrow(t *testing.T) {
	// M7/I7a: o leitor roda até TODOS os Grows terminarem (canal), então a
	// janela crítica (RLock do load × Lock+Munmap do remap) é exercitada de
	// fato — um contador fixo terminaria antes do primeiro Grow.
	r, _ := openTemp(t, 4096)
	r.AtomicStore64(0, 7)
	growsDone := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-growsDone:
				return
			default:
			}
			if got := r.AtomicLoad64(0); got != 7 {
				t.Errorf("leitura durante grow: obtido %d", got)
				return
			}
		}
	}()
	for sz := int64(8192); sz <= 8192*64; sz *= 2 {
		if err := r.Grow(sz); err != nil {
			t.Fatalf("Grow(%d): %v", sz, err)
		}
	}
	close(growsDone)
	<-done
}
