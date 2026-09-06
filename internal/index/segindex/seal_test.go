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
	antes := map[[2]uint64][]types.RowID{}
	for campo, valores := range corpus {
		w, err := segindex.AbreWIP(dir, 42, campo, 16)
		if err != nil {
			t.Fatal(err)
		}
		for h, rows := range valores {
			for _, r := range rows {
				if err := w.Set(h, r); err != nil {
					t.Fatal(err)
				}
			}
		}
		for h := range valores {
			antes[[2]uint64{uint64(campo), h}] = slices.Clone(w.Lookup(h))
		}
		fechaWIP(t, w)
	}

	if err := segindex.Sela(dir, 42); err != nil {
		t.Fatalf("Sela: %v", err)
	}

	s, err := segindex.AbreSelado(segindex.CaminhoSelado(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Fecha(); err != nil {
			t.Errorf("Fecha: %v", err)
		}
	}()
	for chave, want := range antes {
		campo, h := types.FieldID(chave[0]), chave[1]
		it, ok := s.Lookup(campo, h)
		if !ok {
			t.Fatalf("Lookup(%d, %d) devolveu ok=false depois da selagem", campo, h)
		}
		var got []types.RowID
		for it.Next() {
			got = append(got, it.RowID())
		}
		if !slices.Equal(got, want) {
			t.Errorf("Lookup(%d, %d) = %v, quero %v", campo, h, got, want)
		}
	}
}

// TestSelarDesligaOsWIPs: um .widx que sobrevive à selagem seria adotado pela
// próxima abertura como se o segmento ainda fosse o corrente, e ele passaria a
// receber postings que o .sidx nunca teria.
func TestSelarDesligaOsWIPs(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.AbreWIP(dir, 42, 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	fechaWIP(t, w)
	if err := segindex.Sela(dir, 42); err != nil {
		t.Fatal(err)
	}
	campos, err := segindex.ListaWIP(dir, 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(campos) != 0 {
		t.Errorf("ListaWIP = %v, quero vazio", campos)
	}
}

// TestSelarEIdempotente cobre a janela de crash entre o rename do .sidx e o
// unlink dos .widx — que é exatamente o estado "tem .sidx e não tem .widx".
func TestSelarEIdempotente(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.AbreWIP(dir, 42, 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	fechaWIP(t, w)
	if err := segindex.Sela(dir, 42); err != nil {
		t.Fatal(err)
	}
	primeiro, err := os.ReadFile(segindex.CaminhoSelado(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	if err := segindex.Sela(dir, 42); err != nil {
		t.Fatalf("segunda selagem: %v", err)
	}
	segundo, err := os.ReadFile(segindex.CaminhoSelado(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(primeiro, segundo) {
		t.Error("a segunda selagem alterou o arquivo — ela deveria ser no-op")
	}
}
