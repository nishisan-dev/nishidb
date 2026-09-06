package rowindex

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func abreEscritas(t *testing.T, dir string, minRows uint64) *Escritas {
	t.Helper()
	e, err := OpenEscritas(filepath.Join(dir, "expiry.idx"), minRows)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// Zero é DESCONHECIDO, nunca "vencido em 1970". A distinção é o que impede a
// primeira varredura de apagar uma row viva cuja entrada ainda não existe.
func TestEntradaNuncaGravadaEhDesconhecidaENaoVencida(t *testing.T) {
	e := abreEscritas(t, t.TempDir(), 16)
	for _, id := range []uint64{0, 1, 15} {
		if ms, conhecido := e.Get(types.RowID(id)); conhecido || ms != 0 {
			t.Errorf("rowID %d: ms=%d conhecido=%v; quer 0,false", id, ms, conhecido)
		}
	}
}

func TestSetEGetPreservamOInstante(t *testing.T) {
	e := abreEscritas(t, t.TempDir(), 4)
	const ms = 1787878442123
	if err := e.Set(types.RowID(2), ms); err != nil {
		t.Fatal(err)
	}
	got, conhecido := e.Get(types.RowID(2))
	if !conhecido || got != ms {
		t.Fatalf("ms=%d conhecido=%v; quer %d,true", got, conhecido, ms)
	}
	// Vizinhos não são afetados.
	if _, conhecido := e.Get(types.RowID(1)); conhecido {
		t.Error("rowID 1 ficou conhecido sem ter sido gravado")
	}
}

func TestEscritasCresceAlemDaCapacidadeInicial(t *testing.T) {
	e := abreEscritas(t, t.TempDir(), 2)
	const ms = 1787878442123
	if err := e.Set(types.RowID(5000), ms); err != nil {
		t.Fatal(err)
	}
	if got, conhecido := e.Get(types.RowID(5000)); !conhecido || got != ms {
		t.Fatalf("ms=%d conhecido=%v", got, conhecido)
	}
}

func TestEscritasSobrevivemAReabertura(t *testing.T) {
	dir := t.TempDir()
	const ms = 1787878442123
	e := abreEscritas(t, dir, 4)
	if err := e.Set(types.RowID(3), ms); err != nil {
		t.Fatal(err)
	}
	if err := e.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e2 := abreEscritas(t, dir, 4)
	if got, conhecido := e2.Get(types.RowID(3)); !conhecido || got != ms {
		t.Fatalf("depois de reabrir: ms=%d conhecido=%v", got, conhecido)
	}
}

// Magic própria: abrir um sequences.idx como expiry.idx leria sequences como
// instantes de escrita, e a expiração agiria sobre números sem significado.
func TestMagicDeEscritasNaoAceitaOutroIndice(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.idx")
	s, err := OpenSequences(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenEscritas(path, 4); err == nil {
		t.Fatal("um sequences.idx foi aceito como expiry.idx")
	}
}
