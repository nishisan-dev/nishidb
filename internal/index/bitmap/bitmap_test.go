package bitmap

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// orderRecorder embrulha o values.dict real registrando a ordem das
// chamadas de escrita/durabilidade.
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
func (r *orderRecorder) Truncate(size int64) error {
	r.calls = append(r.calls, "Truncate")
	return r.inner.Truncate(size)
}
func (r *orderRecorder) Close() error { return r.inner.Close() }

func TestValorNovoEhFsyncadoAntesDoPrimeiroUso(t *testing.T) {
	// I8: mesma invariante de replay do fields.dict (§8/§14.6) — o valor
	// novo é gravado E fsyncado no values.dict ANTES de qualquer bit ser
	// setado no bitmap do valueID correspondente.
	ix, _ := openIndex(t, 7)
	rec := &orderRecorder{}
	ix.WrapDictFileForTest(func(f dictFile) dictFile {
		rec.inner = f
		return rec
	})

	if err := ix.Set(strValue("valor-novo"), 1); err != nil {
		t.Fatal(err)
	}
	if want := []string{"WriteAt", "Sync"}; !slices.Equal(rec.calls, want) {
		t.Fatalf("ordem de durabilidade: esperado %v, obtido %v", want, rec.calls)
	}

	// Valor já conhecido: nenhum toque no dict.
	rec.calls = nil
	if err := ix.Set(strValue("valor-novo"), 2); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("Set de valor conhecido não pode tocar o values.dict: %v", rec.calls)
	}
}

func TestIteracaoConcorrenteComGrowLazy(t *testing.T) {
	// I7b: o Iterator segura um *mmap.Region através de múltiplas chamadas
	// enquanto o writer pode remapear a região (grow lazy do Set). Cada
	// Next re-resolve o ponteiro sob RLock — este teste exercita a janela
	// Lookup/Next × Grow sob -race.
	ix, _ := openIndex(t, 7)
	v := strValue("conc")
	if err := ix.Set(v, 0); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			it, ok, err := ix.Lookup(v)
			if err != nil || !ok {
				t.Errorf("Lookup concorrente: (%v,%v)", ok, err)
				return
			}
			last := types.RowID(0)
			first := true
			for it.Next() {
				if !first && it.RowID() <= last {
					t.Errorf("iterador fora de ordem: %d após %d", it.RowID(), last)
					return
				}
				last, first = it.RowID(), false
			}
			it.Close()
		}
	}()

	// rowIds crescentes cruzando ~3 fronteiras de grow (8KiB→64KiB de words).
	for r := types.RowID(0); r <= 500_000; r += 1000 {
		if err := ix.Set(v, r); err != nil {
			t.Fatalf("Set(%d): %v", r, err)
		}
	}
	close(done)
	wg.Wait()

	rows, ok := collect(t, ix, v)
	if !ok || len(rows) != 501 {
		t.Fatalf("esperado 501 rows ao final, obtido (%d,%v)", len(rows), ok)
	}
}

func strValue(s string) types.Value {
	return types.Value{Type: types.TypeString, Bytes: []byte(s)}
}

func openIndex(t *testing.T, fieldID types.FieldID) (*Index, string) {
	t.Helper()
	dir := t.TempDir()
	ix, err := Open(dir, fieldID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	return ix, dir
}

func collect(t *testing.T, ix *Index, v types.Value) ([]types.RowID, bool) {
	t.Helper()
	it, ok, err := ix.Lookup(v)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !ok {
		return nil, false
	}
	defer it.Close()
	var rows []types.RowID
	for it.Next() {
		rows = append(rows, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return rows, true
}

func TestSetLookupOrdenado(t *testing.T) {
	ix, _ := openIndex(t, 7)
	stop := strValue("Stop")
	// Fora de ordem e cruzando fronteiras de word (63/64).
	for _, r := range []types.RowID{1000, 1, 64, 63} {
		if err := ix.Set(stop, r); err != nil {
			t.Fatalf("Set(%d): %v", r, err)
		}
	}
	rows, ok := collect(t, ix, stop)
	if !ok {
		t.Fatal("valor conhecido deveria devolver ok=true")
	}
	want := []types.RowID{1, 63, 64, 1000}
	if !slices.Equal(rows, want) {
		t.Fatalf("esperado %v (ordenado), obtido %v", want, rows)
	}
}

func TestLookupValorDesconhecido(t *testing.T) {
	ix, _ := openIndex(t, 7)
	if _, ok, err := ix.Lookup(strValue("nunca-visto")); ok || err != nil {
		t.Fatalf("valor desconhecido: esperado (false,nil), obtido (%v,%v)", ok, err)
	}
}

func TestClearRemoveBit(t *testing.T) {
	ix, _ := openIndex(t, 7)
	v := strValue("Start")
	for _, r := range []types.RowID{5, 6, 7} {
		if err := ix.Set(v, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := ix.Clear(v, 6); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	rows, _ := collect(t, ix, v)
	if !slices.Equal(rows, []types.RowID{5, 7}) {
		t.Fatalf("pós-clear: %v", rows)
	}
	// Clear de valor desconhecido e de rowId além do bitmap: no-ops.
	if err := ix.Clear(strValue("inexistente"), 1); err != nil {
		t.Fatalf("Clear de valor desconhecido: %v", err)
	}
	if err := ix.Clear(v, 1<<40); err != nil {
		t.Fatalf("Clear além do bitmap: %v", err)
	}
}

func TestValoresDistintosNaoColidem(t *testing.T) {
	ix, _ := openIndex(t, 3)
	i64Five := types.Value{Type: types.TypeInt64, I64: 5}
	strFive := strValue("5")
	if err := ix.Set(i64Five, 10); err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(strFive, 20); err != nil {
		t.Fatal(err)
	}
	if rows, _ := collect(t, ix, i64Five); !slices.Equal(rows, []types.RowID{10}) {
		t.Fatalf("int64 5: %v", rows)
	}
	if rows, _ := collect(t, ix, strFive); !slices.Equal(rows, []types.RowID{20}) {
		t.Fatalf("string \"5\": %v", rows)
	}
}

func TestGrowComRowIdAlto(t *testing.T) {
	ix, _ := openIndex(t, 1)
	v := strValue("big")
	const bigRow = 1_000_000
	if err := ix.Set(v, 3); err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(v, bigRow); err != nil {
		t.Fatalf("Set(%d): %v", bigRow, err)
	}
	rows, _ := collect(t, ix, v)
	if !slices.Equal(rows, []types.RowID{3, bigRow}) {
		t.Fatalf("%v", rows)
	}
}

func TestArquivosNoDisco(t *testing.T) {
	ix, dir := openIndex(t, 42)
	if err := ix.Set(strValue("primeiro"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "values.dict")); err != nil {
		t.Fatalf("values.dict: %v", err)
	}
	bm := filepath.Join(dir, "00000001.bm")
	raw, err := os.ReadFile(bm)
	if err != nil {
		t.Fatalf("bitmap do valueID 1: %v", err)
	}
	if !bytes.Equal(raw[0:4], []byte("NDBB")) {
		t.Fatalf("magic: %q", raw[0:4])
	}
	if raw[4] != 1 {
		t.Fatalf("version: %d", raw[4])
	}
	if got := binary.LittleEndian.Uint16(raw[6:8]); got != 42 {
		t.Fatalf("fieldID no header: %d", got)
	}
	if got := binary.LittleEndian.Uint32(raw[8:12]); got != 1 {
		t.Fatalf("valueID no header: %d", got)
	}
}

func TestReopenPreservaBitsEValueIDs(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	vA, vB := strValue("A"), strValue("B")
	for _, r := range []types.RowID{2, 9} {
		if err := ix.Set(vA, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := ix.Set(vB, 5); err != nil {
		t.Fatal(err)
	}
	if err := ix.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}

	ix2, err := Open(dir, 7)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ix2.Close()
	if rows, ok := collect(t, ix2, vA); !ok || !slices.Equal(rows, []types.RowID{2, 9}) {
		t.Fatalf("A pós-reopen: (%v,%v)", rows, ok)
	}
	if rows, ok := collect(t, ix2, vB); !ok || !slices.Equal(rows, []types.RowID{5}) {
		t.Fatalf("B pós-reopen: (%v,%v)", rows, ok)
	}
	// valueID de A continua 1: novo Set não pode criar arquivo novo.
	if err := ix2.Set(vA, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000003.bm")); !os.IsNotExist(err) {
		t.Fatal("Set de valor conhecido não pode alocar novo valueID")
	}
}

func TestValorConhecidoComBitmapAusente(t *testing.T) {
	// Crash entre o fsync do values.dict e a criação do .bm: valor
	// conhecido com bitmap vazio (o replay do tail restaura os bits).
	dir := t.TempDir()
	ix, err := Open(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(strValue("orfao"), 8); err != nil {
		t.Fatal(err)
	}
	ix.Close()
	if err := os.Remove(filepath.Join(dir, "00000001.bm")); err != nil {
		t.Fatal(err)
	}

	ix2, err := Open(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer ix2.Close()
	rows, ok := collect(t, ix2, strValue("orfao"))
	if !ok {
		t.Fatal("valor está no dict: ok deveria ser true")
	}
	if len(rows) != 0 {
		t.Fatalf("bitmap ausente = vazio; obtido %v", rows)
	}
}

func TestDictComTailRasgado(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Set(strValue("integra"), 1); err != nil {
		t.Fatal(err)
	}
	ix.Close()

	path := filepath.Join(dir, "values.dict")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x09, 0x03, 0x00, 'p'}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	ix2, err := Open(dir, 7)
	if err != nil {
		t.Fatalf("reopen com tail rasgado: %v", err)
	}
	defer ix2.Close()
	if rows, ok := collect(t, ix2, strValue("integra")); !ok || !slices.Equal(rows, []types.RowID{1}) {
		t.Fatalf("prefixo válido: (%v,%v)", rows, ok)
	}
	if ix2.Values() != 1 {
		t.Fatalf("Values: esperado 1, obtido %d", ix2.Values())
	}
}

func TestValueNullRejeitado(t *testing.T) {
	ix, _ := openIndex(t, 7)
	if err := ix.Set(types.Value{Type: types.TypeNull}, 1); err == nil {
		t.Fatal("TypeNull nunca é indexado (NULL = campo ausente)")
	}
}
