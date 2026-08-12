package primary

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// keyStore simula a validação de colisão contra a key real do record:
// no Store real, validate lê o record via locations e compara bytes.
type keyStore struct {
	mu   sync.RWMutex
	keys map[types.RowID][]byte
}

func newKeyStore() *keyStore { return &keyStore{keys: make(map[types.RowID][]byte)} }

func (s *keyStore) put(id types.RowID, key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[id] = bytes.Clone(key)
}

func (s *keyStore) validator(key []byte) ValidateFunc {
	return func(id types.RowID) (bool, error) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return bytes.Equal(s.keys[id], key), nil
	}
}

func openIndex(t *testing.T, minSlots uint64) (*Index, string, *keyStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "primary.idx")
	ix, err := Open(path, minSlots)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	return ix, path, newKeyStore()
}

func insertKey(t *testing.T, ix *Index, ks *keyStore, key []byte, id types.RowID) {
	t.Helper()
	ks.put(id, key)
	got, inserted, err := ix.Insert(key, id, ks.validator(key))
	if err != nil {
		t.Fatalf("Insert(%q): %v", key, err)
	}
	if !inserted || got != id {
		t.Fatalf("Insert(%q): esperado (%d,true), obtido (%d,%v)", key, id, got, inserted)
	}
}

func TestOpenCriaArquivoComSlotCountPow2(t *testing.T) {
	ix, path, _ := openIndex(t, 1000)
	if ix.SlotCount() != 1024 {
		t.Fatalf("slotCount: esperado 1024 (próxima pot. de 2), obtido %d", ix.SlotCount())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[0:4], []byte("NDBP")) {
		t.Fatalf("magic: %q", raw[0:4])
	}
	if raw[4] != 1 {
		t.Fatalf("version: %d", raw[4])
	}
	if raw[5] != 1 {
		t.Fatalf("hashKind: esperado 1 (fnv1a64+fmix64), obtido %d", raw[5])
	}
	if int64(len(raw)) != 64+1024*16 {
		t.Fatalf("tamanho do arquivo: esperado %d, obtido %d", 64+1024*16, len(raw))
	}
}

func TestInsertFindRoundTrip(t *testing.T) {
	ix, _, ks := openIndex(t, 1<<14)
	const n = 10_000
	for i := 0; i < n; i++ {
		insertKey(t, ix, ks, fmt.Appendf(nil, "CMP-V%04dPPCLP-%03d", i, i%1000), types.RowID(i))
	}
	if ix.Used() != n {
		t.Fatalf("Used: esperado %d, obtido %d", n, ix.Used())
	}
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "CMP-V%04dPPCLP-%03d", i, i%1000)
		id, ok, err := ix.Find(key, ks.validator(key))
		if err != nil || !ok || id != types.RowID(i) {
			t.Fatalf("Find(%q): esperado (%d,true), obtido (%d,%v,%v)", key, i, id, ok, err)
		}
	}
}

func TestFindAusente(t *testing.T) {
	ix, _, ks := openIndex(t, 1024)
	insertKey(t, ix, ks, []byte("existe"), 1)
	key := []byte("nao-existe")
	if _, ok, err := ix.Find(key, ks.validator(key)); ok || err != nil {
		t.Fatalf("Find de key ausente: esperado (false,nil), obtido (%v,%v)", ok, err)
	}
}

func TestInsertDuplicadoDevolveExistente(t *testing.T) {
	ix, _, ks := openIndex(t, 1024)
	key := []byte("dup-key")
	insertKey(t, ix, ks, key, 42)
	got, inserted, err := ix.Insert(key, 999, ks.validator(key))
	if err != nil {
		t.Fatal(err)
	}
	if inserted || got != 42 {
		t.Fatalf("insert duplicado: esperado (42,false), obtido (%d,%v)", got, inserted)
	}
	if ix.Used() != 1 {
		t.Fatalf("Used após duplicata: esperado 1, obtido %d", ix.Used())
	}
}

func TestColisoesForcadasComProbing(t *testing.T) {
	ix, _, ks := openIndex(t, 1024)
	// Todas as keys caem no mesmo slot inicial: só a validação da key real
	// distingue (spec §36) e o linear probing acomoda.
	ix.setHashForTest(func([]byte) uint64 { return 7 })

	const n = 100
	for i := 0; i < n; i++ {
		insertKey(t, ix, ks, fmt.Appendf(nil, "colide-%d", i), types.RowID(i))
	}
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "colide-%d", i)
		id, ok, err := ix.Find(key, ks.validator(key))
		if err != nil || !ok || id != types.RowID(i) {
			t.Fatalf("Find(%q) com colisão: (%d,%v,%v)", key, id, ok, err)
		}
	}
	// Duplicata continua detectada mesmo com colisão total.
	key0 := []byte("colide-0")
	if got, inserted, _ := ix.Insert(key0, 500, ks.validator(key0)); inserted || got != 0 {
		t.Fatalf("duplicata sob colisão: esperado (0,false), obtido (%d,%v)", got, inserted)
	}
}

func TestProbingComWrapAround(t *testing.T) {
	ix, _, ks := openIndex(t, 1024)
	// Slot inicial no fim da tabela: o probe precisa dar a volta.
	ix.setHashForTest(func([]byte) uint64 { return uint64(ix.SlotCount() - 1) })
	for i := 0; i < 10; i++ {
		insertKey(t, ix, ks, fmt.Appendf(nil, "wrap-%d", i), types.RowID(i))
	}
	key := []byte("wrap-9")
	if id, ok, err := ix.Find(key, ks.validator(key)); err != nil || !ok || id != 9 {
		t.Fatalf("wrap-around: (%d,%v,%v)", id, ok, err)
	}
}

func TestAutoGrowPreservaTudo(t *testing.T) {
	ix, path, ks := openIndex(t, 1024)
	before := ix.SlotCount()
	// 0.70 * 1024 = 716; inserir 2000 força pelo menos um rehash.
	const n = 2000
	for i := 0; i < n; i++ {
		insertKey(t, ix, ks, fmt.Appendf(nil, "grow-key-%d", i), types.RowID(i))
	}
	if ix.SlotCount() <= before {
		t.Fatalf("esperado crescimento de slotCount (antes=%d, depois=%d)", before, ix.SlotCount())
	}
	if ix.Used() != n {
		t.Fatalf("Used pós-grow: esperado %d, obtido %d", n, ix.Used())
	}
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "grow-key-%d", i)
		id, ok, err := ix.Find(key, ks.validator(key))
		if err != nil || !ok || id != types.RowID(i) {
			t.Fatalf("Find(%q) pós-grow: (%d,%v,%v)", key, id, ok, err)
		}
	}
	if _, err := os.Stat(path + ".new"); !os.IsNotExist(err) {
		t.Fatalf("arquivo .new não deveria sobrar após rehash: %v", err)
	}
}

func TestReopenPreservaEstado(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "primary.idx")
	ks := newKeyStore()

	ix, err := Open(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	const n = 500
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "reopen-%d", i)
		ks.put(types.RowID(i), key)
		if _, _, err := ix.Insert(key, types.RowID(i), ks.validator(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ix.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}

	ix2, err := Open(path, 4096)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ix2.Close()
	if ix2.Used() != n {
		t.Fatalf("Used pós-reopen: esperado %d, obtido %d", n, ix2.Used())
	}
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "reopen-%d", i)
		id, ok, err := ix2.Find(key, ks.validator(key))
		if err != nil || !ok || id != types.RowID(i) {
			t.Fatalf("Find(%q) pós-reopen: (%d,%v,%v)", key, id, ok, err)
		}
	}
}

func TestFindConcorrenteComInsert(t *testing.T) {
	ix, _, ks := openIndex(t, 1<<13)
	const n = 3000
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < n; i += 97 {
				key := fmt.Appendf(nil, "conc-%d", i)
				if id, ok, err := ix.Find(key, ks.validator(key)); err != nil {
					t.Errorf("Find concorrente: %v", err)
					return
				} else if ok && id != types.RowID(i) {
					t.Errorf("Find concorrente: key %q → rowId %d", key, id)
					return
				}
			}
		}
	}()
	for i := 0; i < n; i++ {
		insertKey(t, ix, ks, fmt.Appendf(nil, "conc-%d", i), types.RowID(i))
	}
	close(stop)
	wg.Wait()
}
