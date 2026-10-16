package counter

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func openDict(t *testing.T, path string, ceiling uint32) *Dict {
	t.Helper()
	d, err := OpenDict(path, ceiling, 0)
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
	for i, key := range keys {
		got, live := d2.Key(uint32(i + 1))
		if !live || string(got) != string(key) {
			t.Fatalf("Key(%d) = %q, %v; quer %q, true", i+1, got, live, key)
		}
		if id, ok := d2.ByKey(key); !ok || id != uint32(i+1) {
			t.Fatalf("ByKey(%q) = %d, %v", key, id, ok)
		}
	}
	if _, live := d2.Key(0); live {
		t.Fatal("id 0 é WithoutGroup, nunca vivo")
	}
	if _, live := d2.Key(4); live {
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
	if err := d.Retire(2, nil); err != nil {
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
	if _, live := d2.Key(2); live {
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
	if err := d2.Retire(2, nil); err == nil {
		t.Fatal("aposentar de novo tem de falhar")
	}
	if err := d2.Retire(99, nil); err == nil {
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
	if err := d.Retire(1, nil); err != nil {
		t.Fatal(err)
	}
	if id, err := d.Intern([]byte("c")); err != nil || id != 3 {
		t.Fatalf("depois de aposentar: %d, %v; quer 3", id, err)
	}
}

// TestDictInternDefersTheSyncToTheCheckpoint trava a nova ordem de
// durabilidade do dicionário de grupos: o grupo novo é só um append, e o fsync
// de TODOS os appends pendentes acontece uma vez, no Sync do checkpoint. Um
// Sync com o dicionário limpo não toca o arquivo — é o que faz um checkpoint
// sem grupo novo não pagar fsync nenhum. A aposentadoria continua com fsync
// próprio, que leva junto o que estiver pendente.
//
// A ordem é conferida pelo gravador de chamadas — o único jeito de
// distinguir "escreveu e sincronizou" de "escreveu e sincronizou depois" sem
// derrubar a máquina.
func TestDictInternDefersTheSyncToTheCheckpoint(t *testing.T) {
	d := openDict(t, filepath.Join(t.TempDir(), "grupos.dict"), 10)
	rec := &orderRecorder{}
	d.wrapFileForTest(func(f dictFile) dictFile {
		rec.inner = f
		return rec
	})
	for _, key := range []string{"a", "b", "c"} {
		if _, err := d.Intern([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"WriteAt", "WriteAt", "WriteAt"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("chamadas das internações = %v, quer %v: o grupo novo não pode pagar fsync", rec.calls, want)
	}
	rec.calls = nil
	if err := d.Sync(nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("chamadas do Sync com três appends pendentes = %v, quer %v", rec.calls, want)
	}
	rec.calls = nil
	if err := d.Sync(nil); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("o Sync de um dicionário limpo não pode tocar o arquivo; chamadas = %v", rec.calls)
	}
	if _, err := d.Intern([]byte("d")); err != nil {
		t.Fatal(err)
	}
	rec.calls = nil
	if err := d.Retire(1, nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("ordem de durabilidade da aposentadoria = %v, quer %v", rec.calls, want)
	}
	rec.calls = nil
	if err := d.Sync(nil); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("o fsync da aposentadoria leva junto o append pendente; o Sync seguinte tinha de achar o dicionário limpo, e chamou %v", rec.calls)
	}
}

// TestDictSyncFailureIsReportedAsDictSync: a falha do fsync — no checkpoint e
// na aposentadoria — sai como ErrDictSync, que é o que o Store transforma em
// writer falho. Com outro erro qualquer, o checkpoint a trataria como erro
// retentável e a próxima tentativa "passaria" (fsyncgate). A aposentadoria
// que falhou não tira o grupo da memória.
func TestDictSyncFailureIsReportedAsDictSync(t *testing.T) {
	d := openDict(t, filepath.Join(t.TempDir(), "grupos.dict"), 10)
	fail := &failingSync{err: errors.New("EIO simulado")}
	d.wrapFileForTest(func(f dictFile) dictFile {
		fail.dictFile = f
		return fail
	})
	if _, err := d.Intern([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := d.Sync(nil); !errors.Is(err, ErrDictSync) {
		t.Fatalf("Sync com fsync falho = %v, quer ErrDictSync", err)
	}
	if err := d.Retire(1, nil); !errors.Is(err, ErrDictSync) {
		t.Fatalf("Retire com fsync falho = %v, quer ErrDictSync", err)
	}
	if _, live := d.Key(1); !live {
		t.Fatal("a aposentadoria que não ficou durável não pode tirar o grupo da memória")
	}
}

// TestDictCloseSyncsAPendingAppend: o Close sincroniza o append pendente. É o
// que protege o rollback com parada limpa — o binário anterior confia no
// prefixo do arquivo como durável e nunca sincroniza a cauda que herda.
func TestDictCloseSyncsAPendingAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d, err := OpenDict(path, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	rec := &orderRecorder{}
	d.wrapFileForTest(func(f dictFile) dictFile {
		rec.inner = f
		return rec
	})
	if _, err := d.Intern([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("chamadas até o Close = %v, quer %v", rec.calls, want)
	}
}

// TestOpenDictTreatsADuplicateLiveKeyAsATornTail: uma entrada cuja chave já é
// de um grupo vivo só existe num arquivo danificado — o Intern nunca a escreve.
// Ela é tratada como cauda rasgada: a abertura trunca ali, e a chave continua
// resolvendo para o id ORIGINAL. Aceitá-la sobrescreveria o mapa em silêncio.
func TestOpenDictTreatsADuplicateLiveKeyAsATornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := openDict(t, path, 10)
	if _, err := d.Intern([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	appendRawEntryForTest(t, path, []byte("a"))
	appendRawEntryForTest(t, path, []byte("b"))

	d2 := openDict(t, path, 10)
	if d2.Limit() != 2 {
		t.Fatalf("limite %d depois da chave duplicada; quer 2 (só o id 1 sobrevive)", d2.Limit())
	}
	if id, ok := d2.ByKey([]byte("a")); !ok || id != 1 {
		t.Fatalf("ByKey(a) = %d, %v; a chave tem de continuar no id original 1", id, ok)
	}
	if d2.SizeBytes() != st.Size() {
		t.Fatalf("tamanho válido %d; a abertura tinha de truncar na entrada duplicada, em %d", d2.SizeBytes(), st.Size())
	}
}

// failingSync é o dublê de arquivo cujo fsync falha.
type failingSync struct {
	dictFile
	err error
}

func (f *failingSync) Sync() error { return f.err }

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

// appendRawEntryForTest escreve uma entrada de grupo direto no arquivo, sem
// fsync — o que um processo anterior deixou no page cache antes de morrer.
func appendRawEntryForTest(t *testing.T, path string, key []byte) {
	t.Helper()
	entry := binary.LittleEndian.AppendUint16(nil, uint16(len(key)))
	entry = append(entry, key...)
	entry = binary.LittleEndian.AppendUint32(entry, crc32.Checksum(entry, castagnoli))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(entry); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestOpenDictSyncsTheTailItInherits prova a abertura sincronizada: a cauda
// que o processo anterior escreveu e não sincronizou continua no page cache
// depois de um kill -9, e a abertura a lê como se estivesse em disco. O fsync
// UMA vez por abertura é o que torna verdade o que ela leu.
//
// A segunda metade é a guarda contra o fsync gratuito: o arquivo só com o
// header não tem o que tornar durável, e a abertura dele não sincroniza.
func TestOpenDictSyncsTheTailItInherits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := openDict(t, path, 10)
	if _, err := d.Intern([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	appendRawEntryForTest(t, path, []byte("b"))

	var rec *orderRecorder
	restore := wrapDictOpenForTest(func(f dictFile) dictFile {
		rec = &orderRecorder{inner: f}
		return rec
	})
	d2 := openDict(t, path, 10)
	restore()
	if d2.Limit() != 3 {
		t.Fatalf("a cauda herdada tinha de ser lida: limite %d, quer 3", d2.Limit())
	}
	if want := []string{"Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("chamadas de durabilidade da abertura = %v, quer %v", rec.calls, want)
	}

	empty := filepath.Join(t.TempDir(), "grupos.dict")
	if err := openDict(t, empty, 10).Close(); err != nil {
		t.Fatal(err)
	}
	restore = wrapDictOpenForTest(func(f dictFile) dictFile {
		rec = &orderRecorder{inner: f}
		return rec
	})
	defer restore()
	openDict(t, empty, 10)
	if len(rec.calls) != 0 {
		t.Fatalf("o arquivo só com o header não tem o que sincronizar na abertura; chamadas = %v", rec.calls)
	}
}

// TestOpenDictBelowTheFloorRefusesWithoutTruncating: prefixo válido menor que
// o piso é ErrDictBelowFloor, e o arquivo fica exatamente como estava — quem
// reconstrói o põe de lado como evidência. O arquivo vazio com piso (o
// dicionário perdido inteiro) é o mesmo caso. Prefixo igual ao piso abre.
func TestOpenDictBelowTheFloorRefusesWithoutTruncating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grupos.dict")
	d := openDict(t, path, 10)
	for _, key := range []string{"a", "b"} {
		if _, err := d.Intern([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	floor := st.Size()
	if err := os.Truncate(path, floor-1); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDict(path, 10, floor); !errors.Is(err, ErrDictBelowFloor) {
		t.Fatalf("OpenDict abaixo do piso = %v, quer ErrDictBelowFloor", err)
	}
	if st, err := os.Stat(path); err != nil || st.Size() != floor-1 {
		t.Fatalf("a abertura abaixo do piso mexeu no arquivo: tamanho %d, quer %d intocado (%v)", st.Size(), floor-1, err)
	}
	if d2, err := OpenDict(path, 10, floor-entryOverhead-1); err != nil {
		t.Fatalf("prefixo igual ao piso tem de abrir: %v", err)
	} else if err := d2.Close(); err != nil {
		t.Fatal(err)
	}

	empty := filepath.Join(t.TempDir(), "grupos.dict")
	if _, err := OpenDict(empty, 10, headerSize); !errors.Is(err, ErrDictBelowFloor) {
		t.Fatalf("OpenDict de um dicionário ausente com piso = %v, quer ErrDictBelowFloor", err)
	}
}

// TestOpenDictShorterThanItsHeaderWithAFloorIsBelowTheFloor: com piso
// registrado, um grupos.dict truncado DENTRO do header (1 a 7 bytes) é dado
// durável perdido como qualquer prefixo abaixo do piso — ErrDictBelowFloor, que
// reconstrói o contador —, e não "header inválido", que recusaria a abertura do
// dataset inteiro. Sem piso, o header incompleto continua recusado como
// inválido. O arquivo fica intocado nos dois casos.
func TestOpenDictShorterThanItsHeaderWithAFloorIsBelowTheFloor(t *testing.T) {
	header := append(dictMagic[:], dictVersion, 0, 0, 0)
	for size := 1; size < headerSize; size++ {
		path := filepath.Join(t.TempDir(), "grupos.dict")
		if err := os.WriteFile(path, header[:size], 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenDict(path, 10, headerSize+20); !errors.Is(err, ErrDictBelowFloor) {
			t.Fatalf("dicionário de %d bytes com piso = %v, quer ErrDictBelowFloor", size, err)
		}
		if st, err := os.Stat(path); err != nil || st.Size() != int64(size) {
			t.Fatalf("a abertura mexeu no dicionário de %d bytes (%v)", size, err)
		}
		if _, err := OpenDict(path, 10, 0); err == nil || errors.Is(err, ErrDictBelowFloor) {
			t.Fatalf("dicionário de %d bytes SEM piso = %v, quer o erro de header inválido", size, err)
		}
	}
}
