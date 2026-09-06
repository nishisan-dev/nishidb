package mmap

import "testing"

// TestAtomicos32EnderecamMeiaWord prova que o acesso de 32 bits endereça a
// meia-word certa e NÃO pisa na vizinha: as duas metades escritas separadamente
// têm de compor, em little-endian, a word de 64 lida por AtomicLoad64.
//
// Existe porque o vetor de grupos do contador guarda 4 bytes por linha, e um
// endereçamento errado aqui não falharia em lugar nenhum — apenas contaria a
// linha no grupo da vizinha.
func TestAtomicos32EnderecamMeiaWord(t *testing.T) {
	r, _ := openTemp(t, 4096)
	r.AtomicStore32(64, 0xAABBCCDD)
	r.AtomicStore32(68, 0x11223344)
	if got := r.AtomicLoad32(64); got != 0xAABBCCDD {
		t.Fatalf("off 64: obtido %#x", got)
	}
	if got := r.AtomicLoad32(68); got != 0x11223344 {
		t.Fatalf("off 68: obtido %#x", got)
	}
	if got := r.AtomicLoad64(64); got != 0x11223344AABBCCDD {
		t.Fatalf("word composta: obtido %#x", got)
	}
	if got := r.AtomicLoad32(72); got != 0 {
		t.Fatalf("área nova deveria ser zero, obtido %#x", got)
	}
}

// TestAtomico32DesalinhadoPanica trava o alinhamento de 4 bytes: um offset
// que não seja múltiplo de 4 cairia entre duas entradas do vetor.
func TestAtomico32DesalinhadoPanica(t *testing.T) {
	r, _ := openTemp(t, 4096)
	defer func() {
		if recover() == nil {
			t.Fatal("offset 66 deveria entrar em pânico (exige múltiplo de 4)")
		}
	}()
	r.AtomicLoad32(66)
}

// TestAtomico32SomenteLeituraPanica: a região selada é PROT_READ, e um store
// nela seria SIGSEGV incapturável — o pânico nomeado é a rede, como no de 64.
func TestAtomico32SomenteLeituraPanica(t *testing.T) {
	_, path := openTemp(t, 4096)
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if got := ro.AtomicLoad32(0); got != 0 {
		t.Fatalf("leitura em região selada: obtido %#x", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("AtomicStore32 em região somente-leitura deveria entrar em pânico")
		}
	}()
	ro.AtomicStore32(0, 1)
}
