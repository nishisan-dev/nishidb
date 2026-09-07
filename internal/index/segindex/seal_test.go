package segindex_test

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestSelarPreservaExatamenteOQueOWIPRespondia é a propriedade central da
// selagem: a conversão muda o layout, não o conjunto. Ela roda sobre um corpus
// com as três formas que quebram implementações diferentes — valor com uma
// posting só, valor com muitas, e rowIds fora de ordem (o caso do
// materializado).
func TestSelarPreservaExatamenteOQueOWIPRespondia(t *testing.T) {
	dir := t.TempDir()
	corpus := map[types.FieldID]map[uint64][]types.RowID{
		7: {100: {50, 10, 30, 40}, 200: {25}},
		9: {300: {11, 12, 13}},
	}
	before := map[[2]uint64][]types.RowID{}
	for field, values := range corpus {
		w, err := segindex.OpenWIP(dir, 42, field, 16)
		if err != nil {
			t.Fatal(err)
		}
		for h, rows := range values {
			for _, r := range rows {
				if err := w.Set(h, r); err != nil {
					t.Fatal(err)
				}
			}
		}
		for h := range values {
			before[[2]uint64{uint64(field), h}] = slices.Clone(w.Lookup(h))
		}
		closeWIP(t, w)
	}

	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	s, err := segindex.OpenSealed(segindex.SealedPath(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	for key, want := range before {
		field, h := types.FieldID(key[0]), key[1]
		it, ok := s.Lookup(field, h)
		if !ok {
			t.Fatalf("Lookup(%d, %d) devolveu ok=false depois da selagem", field, h)
		}
		var got []types.RowID
		for it.Next() {
			got = append(got, it.RowID())
		}
		if !slices.Equal(got, want) {
			t.Errorf("Lookup(%d, %d) = %v, quero %v", field, h, got, want)
		}
	}
}

// TestSelarDesligaOsWIPs: um .widx que sobrevive à selagem seria adotado pela
// próxima abertura como se o segmento ainda fosse o corrente, e ele passaria a
// receber postings que o .sidx nunca teria.
func TestSelarDesligaOsWIPs(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.OpenWIP(dir, 42, 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	closeWIP(t, w)
	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatal(err)
	}
	fields, err := segindex.ListWIP(dir, 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 0 {
		t.Errorf("ListWIP = %v, quero vazio", fields)
	}
}

// TestSelarEIdempotente cobre a janela de crash entre o rename do .sidx e o
// unlink dos .widx — que é exatamente o estado "tem .sidx e não tem .widx".
func TestSelarEIdempotente(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.OpenWIP(dir, 42, 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	closeWIP(t, w)
	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(segindex.SealedPath(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatalf("segunda selagem: %v", err)
	}
	second, err := os.ReadFile(segindex.SealedPath(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Error("a segunda selagem alterou o arquivo — ela deveria ser no-op")
	}
}
