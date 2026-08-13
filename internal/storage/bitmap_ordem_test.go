package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Os FieldIDs de um dataset novo precisam depender só do SPEC, não da ordem em
// que o chamador listou os campos indexados. O Open resolve BitmapFieldNames
// por LookupOrCreate na ordem do slice recebido, e essa ordem queima os
// primeiros IDs do dicionário: dois datasets criados do mesmo YAML com a lista
// em ordens diferentes ficavam com mapeamentos nome→ID diferentes PARA SEMPRE.
//
// O dano não é local: `SELECT *` sai em ordem de FieldID, e o contrato do
// driver JDBC do M3 mapeia POSIÇÃO para valor. Produção e staging criados do
// mesmo arquivo devolveriam colunas em posições diferentes.
func TestOrdemDosNomesDeBitmapNaoMudaOsFieldIDs(t *testing.T) {
	nomes := []string{"zeta", "alfa", "meio", "beta", "omega", "gama"}
	invertido := make([]string, len(nomes))
	for i, n := range nomes {
		invertido[len(nomes)-1-i] = n
	}

	ids := func(lista []string) map[string]types.FieldID {
		t.Helper()
		st, err := storage.Open(storage.Options{
			Dir:              filepath.Join(t.TempDir(), "ds"),
			BitmapFieldNames: lista,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		out := make(map[string]types.FieldID, len(nomes))
		for _, n := range nomes {
			id, err := st.FieldDict().LookupOrCreate([]byte(n))
			if err != nil {
				t.Fatal(err)
			}
			out[n] = id
		}
		return out
	}

	a, b := ids(nomes), ids(invertido)
	for _, n := range nomes {
		if a[n] != b[n] {
			t.Errorf("campo %q ficou com ID %d num dataset e %d no outro", n, a[n], b[n])
		}
	}
}

// O Open não pode reordenar o slice do chamador: ele é do chamador, e mutá-lo
// é efeito colateral invisível de uma função que promete só abrir o dataset.
func TestOpenNaoReordenaOSliceDoChamador(t *testing.T) {
	nomes := []string{"zeta", "alfa", "beta"}
	original := append([]string(nil), nomes...)

	st, err := storage.Open(storage.Options{
		Dir:              filepath.Join(t.TempDir(), "ds"),
		BitmapFieldNames: nomes,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for i := range original {
		if nomes[i] != original[i] {
			t.Fatalf("BitmapFieldNames foi reordenado no lugar: %v, want %v", nomes, original)
		}
	}
}
