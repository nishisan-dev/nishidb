package counter

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func openDict(t *testing.T, path string, ceiling uint32) *Dict {
	t.Helper()
	d, err := OpenDict(path, ceiling)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestDicionarioInternaEReabre(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := openDict(t, path, 100)
	keys := [][]byte{[]byte("PRCTA_G1M10"), []byte("BR_SPOMO_OLT10"), []byte("\x00bin\xff")}
	for i, key := range keys {
		id, err := d.Intern(key)
		if err != nil {
			t.Fatal(err)
		}
		if id != uint32(i+1) {
			t.Fatalf("id de %q = %d, quer %d (1-based, em ordem de internação)", key, id, i+1)
		}
	}
	// Internar de novo devolve o MESMO id, sem crescer o arquivo.
	before := d.SizeBytes()
	if id, err := d.Intern(keys[1]); err != nil || id != 2 {
		t.Fatalf("reinternar: %d, %v", id, err)
	}
	if d.SizeBytes() != before {
		t.Fatal("reinternar chave existente não pode escrever no arquivo")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2 := openDict(t, path, 100)
	if d2.Limit() != 4 || d2.LiveGroups() != 3 {
		t.Fatalf("reabertura: limite %d vivos %d, quer 4 e 3", d2.Limit(), d2.LiveGroups())
	}
	for i, ch := range keys {
		got, live := d2.Key(uint32(i + 1))
		if !live || string(got) != string(ch) {
			t.Fatalf("Key(%d) = %q, %v; quer %q, true", i+1, got, live, ch)
		}
		if id, ok := d2.ByKey(ch); !ok || id != uint32(i+1) {
			t.Fatalf("ByKey(%q) = %d, %v", ch, id, ok)
		}
	}
	if _, vivo := d2.Key(0); vivo {
		t.Fatal("id 0 é WithoutGroup, nunca vivo")
	}
	if _, vivo := d2.Key(4); vivo {
		t.Fatal("id além do limite não é vivo")
	}
}

func TestDicionarioTruncaCaudaRasgada(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := openDict(t, path, 100)
	for _, key := range []string{"a", "bb"} {
		if _, err := d.Intern([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	size := d.SizeBytes()
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
	d2 := openDict(t, path, 100)
	if d2.Limit() != 3 {
		t.Fatalf("limite depois da cauda rasgada = %d, quer 3", d2.Limit())
	}
	if d2.SizeBytes() != size {
		t.Fatalf("o arquivo tinha de voltar a %d bytes, tem %d", size, d2.SizeBytes())
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != size {
		t.Fatalf("o arquivo em disco tem %d bytes, quer %d (truncado)", st.Size(), size)
	}
}

func TestDicionarioAposentaENuncaReemite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := openDict(t, path, 100)
	for _, key := range []string{"a", "b", "c"} {
		if _, err := d.Intern([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Retire(2); err != nil {
		t.Fatal(err)
	}
	if _, live := d.Key(2); live {
		t.Fatal("grupo aposentado não é vivo")
	}
	if _, ok := d.ByKey([]byte("b")); ok {
		t.Fatal("chave aposentada não resolve mais")
	}
	if d.LiveGroups() != 2 || d.RetiredGroups() != 1 {
		t.Fatalf("vivos %d aposentados %d", d.LiveGroups(), d.RetiredGroups())
	}
	// A mesma chave, de volta, é um grupo NOVO: o id 2 nunca é reemitido.
	id, err := d.Intern([]byte("b"))
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
	d2 := openDict(t, path, 100)
	if _, vivo := d2.Key(2); vivo {
		t.Fatal("aposentadoria perdida na reabertura")
	}
	if id, ok := d2.ByKey([]byte("b")); !ok || id != 4 {
		t.Fatalf("ByKey(b) depois da reabertura = %d, %v; quer 4", id, ok)
	}
	if d2.Limit() != 5 || d2.LiveGroups() != 3 || d2.RetiredGroups() != 1 {
		t.Fatalf("reabertura: limite %d vivos %d aposentados %d", d2.Limit(), d2.LiveGroups(), d2.RetiredGroups())
	}
	// Aposentar duas vezes, ou um id que não existe, é erro: aposentadoria é
	// decisão do coletor sobre um grupo vivo, e a repetição denunciaria um
	// coletor que perdeu o próprio estado.
	if err := d2.Retire(2); err == nil {
		t.Fatal("aposentar de novo tem de falhar")
	}
	if err := d2.Retire(99); err == nil {
		t.Fatal("aposentar id inexistente tem de falhar")
	}
}

func TestDicionarioRecusaOTeto(t *testing.T) {
	d := openDict(t, filepath.Join(t.TempDir(), "grupos.dict"), 2)
	if _, err := d.Intern([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Intern([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Intern([]byte("c")); !errors.Is(err, ErrGroupCeilingReached) {
		t.Fatalf("terceira chave com teto 2 tem de dar ErrGroupCeilingReached, veio %v", err)
	}
	// Chave já internada continua resolvendo mesmo no teto.
	if id, err := d.Intern([]byte("a")); err != nil || id != 1 {
		t.Fatalf("chave existente no teto: %d, %v", id, err)
	}
	// O teto conta grupos VIVOS: aposentar um libera uma vaga.
	if err := d.Retire(1); err != nil {
		t.Fatal(err)
	}
	if id, err := d.Intern([]byte("c")); err != nil || id != 3 {
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
	d := openDict(t, filepath.Join(t.TempDir(), "grupos.dict"), 10)
	rec := &orderRecorder{}
	d.wrapFileForTest(func(f dictFile) dictFile {
		rec.inner = f
		return rec
	})
	if _, err := d.Intern([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("ordem de durabilidade da internação = %v, quer %v", rec.calls, want)
	}
	rec.calls = nil
	if err := d.Retire(1); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("ordem de durabilidade da aposentadoria = %v, quer %v", rec.calls, want)
	}
}

func TestDicionarioRecusaChaveVaziaOuGrandeDemais(t *testing.T) {
	d := openDict(t, filepath.Join(t.TempDir(), "grupos.dict"), 10)
	if _, err := d.Intern(nil); err == nil {
		t.Fatal("chave vazia tem de ser recusada")
	}
	if _, err := d.Intern(make([]byte, 1<<16)); err == nil {
		t.Fatal("chave acima de u16 tem de ser recusada")
	}
}

// orderRecorder registra a sequência de chamadas de durabilidade.
type orderRecorder struct {
	inner dictFile
	calls []string
}

func (r *orderRecorder) WriteAt(p []byte, off int64) (int, error) {
	r.calls = append(r.calls, "WriteAt")
	return r.inner.WriteAt(p, off)
}

func (r *orderRecorder) Sync() error {
	r.calls = append(r.calls, "Sync")
	return r.inner.Sync()
}

func (r *orderRecorder) Truncate(n int64) error { return r.inner.Truncate(n) }
func (r *orderRecorder) Close() error           { return r.inner.Close() }
