package contador

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func abreDicionario(t *testing.T, path string, teto uint32) *Dicionario {
	t.Helper()
	d, err := AbreDicionario(path, teto)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestDicionarioInternaEReabre(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := abreDicionario(t, path, 100)
	chaves := [][]byte{[]byte("PRCTA_G1M10"), []byte("BR_SPOMO_OLT10"), []byte("\x00bin\xff")}
	for i, ch := range chaves {
		id, err := d.Interna(ch)
		if err != nil {
			t.Fatal(err)
		}
		if id != uint32(i+1) {
			t.Fatalf("id de %q = %d, quer %d (1-based, em ordem de internação)", ch, id, i+1)
		}
	}
	// Internar de novo devolve o MESMO id, sem crescer o arquivo.
	antes := d.SizeBytes()
	if id, err := d.Interna(chaves[1]); err != nil || id != 2 {
		t.Fatalf("reinternar: %d, %v", id, err)
	}
	if d.SizeBytes() != antes {
		t.Fatal("reinternar chave existente não pode escrever no arquivo")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2 := abreDicionario(t, path, 100)
	if d2.Limite() != 4 || d2.Vivos() != 3 {
		t.Fatalf("reabertura: limite %d vivos %d, quer 4 e 3", d2.Limite(), d2.Vivos())
	}
	for i, ch := range chaves {
		got, vivo := d2.Chave(uint32(i + 1))
		if !vivo || string(got) != string(ch) {
			t.Fatalf("Chave(%d) = %q, %v; quer %q, true", i+1, got, vivo, ch)
		}
		if id, ok := d2.PorChave(ch); !ok || id != uint32(i+1) {
			t.Fatalf("PorChave(%q) = %d, %v", ch, id, ok)
		}
	}
	if _, vivo := d2.Chave(0); vivo {
		t.Fatal("id 0 é SemGrupo, nunca vivo")
	}
	if _, vivo := d2.Chave(4); vivo {
		t.Fatal("id além do limite não é vivo")
	}
}

func TestDicionarioTruncaCaudaRasgada(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := abreDicionario(t, path, 100)
	for _, ch := range []string{"a", "bb"} {
		if _, err := d.Interna([]byte(ch)); err != nil {
			t.Fatal(err)
		}
	}
	tamanho := d.SizeBytes()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Meio registro no fim: o prefixo de tamanho diz 5 bytes, e só há 2.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{5, 0, 'x', 'y'}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	d2 := abreDicionario(t, path, 100)
	if d2.Limite() != 3 {
		t.Fatalf("limite depois da cauda rasgada = %d, quer 3", d2.Limite())
	}
	if d2.SizeBytes() != tamanho {
		t.Fatalf("o arquivo tinha de voltar a %d bytes, tem %d", tamanho, d2.SizeBytes())
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != tamanho {
		t.Fatalf("o arquivo em disco tem %d bytes, quer %d (truncado)", st.Size(), tamanho)
	}
}

func TestDicionarioAposentaENuncaReemite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := abreDicionario(t, path, 100)
	for _, ch := range []string{"a", "b", "c"} {
		if _, err := d.Interna([]byte(ch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Aposenta(2); err != nil {
		t.Fatal(err)
	}
	if _, vivo := d.Chave(2); vivo {
		t.Fatal("grupo aposentado não é vivo")
	}
	if _, ok := d.PorChave([]byte("b")); ok {
		t.Fatal("chave aposentada não resolve mais")
	}
	if d.Vivos() != 2 || d.Aposentados() != 1 {
		t.Fatalf("vivos %d aposentados %d", d.Vivos(), d.Aposentados())
	}
	// A mesma chave, de volta, é um grupo NOVO: o id 2 nunca é reemitido.
	id, err := d.Interna([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if id != 4 {
		t.Fatalf("chave que reaparece ganhou id %d, quer 4", id)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// A aposentadoria é durável: sobrevive à reabertura.
	d2 := abreDicionario(t, path, 100)
	if _, vivo := d2.Chave(2); vivo {
		t.Fatal("aposentadoria perdida na reabertura")
	}
	if id, ok := d2.PorChave([]byte("b")); !ok || id != 4 {
		t.Fatalf("PorChave(b) depois da reabertura = %d, %v; quer 4", id, ok)
	}
	if d2.Limite() != 5 || d2.Vivos() != 3 || d2.Aposentados() != 1 {
		t.Fatalf("reabertura: limite %d vivos %d aposentados %d", d2.Limite(), d2.Vivos(), d2.Aposentados())
	}
	// Aposentar duas vezes, ou um id que não existe, é erro: aposentadoria é
	// decisão do coletor sobre um grupo vivo, e a repetição denunciaria um
	// coletor que perdeu o próprio estado.
	if err := d2.Aposenta(2); err == nil {
		t.Fatal("aposentar de novo tem de falhar")
	}
	if err := d2.Aposenta(99); err == nil {
		t.Fatal("aposentar id inexistente tem de falhar")
	}
}

func TestDicionarioRecusaOTeto(t *testing.T) {
	d := abreDicionario(t, filepath.Join(t.TempDir(), "grupos.dict"), 2)
	if _, err := d.Interna([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Interna([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Interna([]byte("c")); !errors.Is(err, ErrTetoDeGrupos) {
		t.Fatalf("terceira chave com teto 2 tem de dar ErrTetoDeGrupos, veio %v", err)
	}
	// Chave já internada continua resolvendo mesmo no teto.
	if id, err := d.Interna([]byte("a")); err != nil || id != 1 {
		t.Fatalf("chave existente no teto: %d, %v", id, err)
	}
	// O teto conta grupos VIVOS: aposentar um libera uma vaga.
	if err := d.Aposenta(1); err != nil {
		t.Fatal(err)
	}
	if id, err := d.Interna([]byte("c")); err != nil || id != 3 {
		t.Fatalf("depois de aposentar: %d, %v; quer 3", id, err)
	}
}

// TestDicionarioFsyncaAntesDeDevolverOId trava a invariante do fielddict
// aplicada aos grupos: uma entrada do vetor só pode apontar para um id que a
// reabertura consiga resolver, então o registro é fsyncado ANTES de o id sair
// daqui. A ordem é conferida pelo gravador de chamadas — o único jeito de
// distinguir "escreveu e sincronizou" de "escreveu e sincronizou depois" sem
// derrubar a máquina.
func TestDicionarioFsyncaAntesDeDevolverOId(t *testing.T) {
	d := abreDicionario(t, filepath.Join(t.TempDir(), "grupos.dict"), 10)
	rec := &gravadorDeOrdem{}
	d.envolveArquivoParaTeste(func(f arquivoDoDicionario) arquivoDoDicionario {
		rec.inner = f
		return rec
	})
	if _, err := d.Interna([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.chamadas, want) {
		t.Fatalf("ordem de durabilidade da internação = %v, quer %v", rec.chamadas, want)
	}
	rec.chamadas = nil
	if err := d.Aposenta(1); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.chamadas, want) {
		t.Fatalf("ordem de durabilidade da aposentadoria = %v, quer %v", rec.chamadas, want)
	}
}

func TestDicionarioRecusaChaveVaziaOuGrandeDemais(t *testing.T) {
	d := abreDicionario(t, filepath.Join(t.TempDir(), "grupos.dict"), 10)
	if _, err := d.Interna(nil); err == nil {
		t.Fatal("chave vazia tem de ser recusada")
	}
	if _, err := d.Interna(make([]byte, 1<<16)); err == nil {
		t.Fatal("chave acima de u16 tem de ser recusada")
	}
}

// gravadorDeOrdem registra a sequência de chamadas de durabilidade.
type gravadorDeOrdem struct {
	inner    arquivoDoDicionario
	chamadas []string
}

func (r *gravadorDeOrdem) WriteAt(p []byte, off int64) (int, error) {
	r.chamadas = append(r.chamadas, "WriteAt")
	return r.inner.WriteAt(p, off)
}

func (r *gravadorDeOrdem) Sync() error {
	r.chamadas = append(r.chamadas, "Sync")
	return r.inner.Sync()
}

func (r *gravadorDeOrdem) Truncate(n int64) error { return r.inner.Truncate(n) }
func (r *gravadorDeOrdem) Close() error           { return r.inner.Close() }
