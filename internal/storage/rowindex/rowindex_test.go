package rowindex

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestPackUnpackRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		seg   uint32
		off   uint32
		flags uint8
	}{
		{"minimo", 1, 64, 0},
		{"tipico", 127, 9283712, 0},
		{"offset_max", 1, (1 << 30) - 1, 0},
		{"segment_max", math.MaxUint32, 64, 0},
		{"flags_max", 42, 100, 3},
		{"tombstone", 7, 4096, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := Pack(c.seg, c.off, c.flags)
			if p == 0 {
				t.Fatal("packed válido nunca pode ser 0 (0 = sem location)")
			}
			seg, off, flags := Unpack(p)
			if seg != c.seg || off != c.off || flags != c.flags {
				t.Fatalf("roundtrip: esperado (%d,%d,%d), obtido (%d,%d,%d)",
					c.seg, c.off, c.flags, seg, off, flags)
			}
		})
	}
}

func TestPackPanicaComLimitesEstourados(t *testing.T) {
	cases := []struct {
		name  string
		seg   uint32
		off   uint32
		flags uint8
	}{
		{"offset_30bits", 1, 1 << 30, 0},
		{"flags_2bits", 1, 64, 4},
		{"segment_zero", 0, 64, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("esperado panic")
				}
			}()
			Pack(c.seg, c.off, c.flags)
		})
	}
}

func openLocations(t *testing.T, minRows uint64) (*Locations, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "locations.idx")
	l, err := OpenLocations(path, minRows)
	if err != nil {
		t.Fatalf("OpenLocations: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, path
}

func TestLocationsHeaderGolden(t *testing.T) {
	_, path := openLocations(t, 1024)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[0:4], []byte("NDBL")) {
		t.Fatalf("magic: %q", raw[0:4])
	}
	if raw[4] != 1 {
		t.Fatalf("version: %d", raw[4])
	}
	if raw[5] != 8 {
		t.Fatalf("entrySize: %d", raw[5])
	}
}

func TestLocationsPublishGetViaOverlay(t *testing.T) {
	l, _ := openLocations(t, 1024)
	p := Pack(1, 64, 0)
	l.Publish(10, p)
	got, ok := l.Get(10)
	if !ok || got != p {
		t.Fatalf("Get pós-Publish: esperado (%#x,true), obtido (%#x,%v)", p, got, ok)
	}
	if l.OverlayLen() != 1 {
		t.Fatalf("OverlayLen: esperado 1, obtido %d", l.OverlayLen())
	}
}

func TestLocationsGetAusente(t *testing.T) {
	l, _ := openLocations(t, 1024)
	if _, ok := l.Get(999); ok {
		t.Fatal("rowId sem location deveria devolver ok=false")
	}
	if _, ok := l.Get(1 << 40); ok {
		t.Fatal("rowId além do arquivo deveria devolver ok=false")
	}
}

func TestLocationsApplyOverlayMoveParaMmap(t *testing.T) {
	l, _ := openLocations(t, 1024)
	p1, p2 := Pack(1, 64, 0), Pack(2, 128, 0)
	l.Publish(5, p1)
	l.Publish(700, p2)
	if err := l.ApplyOverlay(); err != nil {
		t.Fatalf("ApplyOverlay: %v", err)
	}
	if l.OverlayLen() != 0 {
		t.Fatalf("overlay deveria estar vazio pós-apply, tem %d", l.OverlayLen())
	}
	for _, c := range []struct {
		row types.RowID
		p   uint64
	}{{5, p1}, {700, p2}} {
		if got, ok := l.Get(c.row); !ok || got != c.p {
			t.Fatalf("Get(%d) pós-apply: (%#x,%v)", c.row, got, ok)
		}
	}
}

func TestLocationsUpdateSobrescreve(t *testing.T) {
	l, _ := openLocations(t, 1024)
	l.Publish(3, Pack(1, 64, 0))
	if err := l.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	// Nova versão publicada no overlay tem precedência sobre o mmap.
	p2 := Pack(2, 4096, 0)
	l.Publish(3, p2)
	if got, ok := l.Get(3); !ok || got != p2 {
		t.Fatalf("overlay deveria ter precedência: (%#x,%v)", got, ok)
	}
}

func TestLocationsApplyCresceArquivo(t *testing.T) {
	l, path := openLocations(t, 64)
	p := Pack(9, 64, 0)
	const bigRow = 1_000_000
	l.Publish(bigRow, p)
	if err := l.ApplyOverlay(); err != nil {
		t.Fatalf("ApplyOverlay com grow: %v", err)
	}
	if got, ok := l.Get(bigRow); !ok || got != p {
		t.Fatalf("Get(%d): (%#x,%v)", bigRow, got, ok)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() < 64+(bigRow+1)*8 {
		t.Fatalf("arquivo não cresceu: %d bytes", st.Size())
	}
}

func TestLocationsSetDirect(t *testing.T) {
	l, _ := openLocations(t, 1024)
	p := Pack(3, 200, 0)
	if err := l.SetDirect(50, p); err != nil {
		t.Fatal(err)
	}
	if got, ok := l.Get(50); !ok || got != p {
		t.Fatalf("Get pós-SetDirect: (%#x,%v)", got, ok)
	}
	if l.OverlayLen() != 0 {
		t.Fatal("SetDirect não passa pelo overlay")
	}
}

func TestLocationsReopenPersisteAplicadoDescartaOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "locations.idx")
	l, err := OpenLocations(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	applied, volatile := Pack(1, 64, 0), Pack(2, 128, 0)
	l.Publish(1, applied)
	if err := l.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	l.Publish(2, volatile) // fica só no overlay (heap)
	if err := l.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := OpenLocations(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if got, ok := l2.Get(1); !ok || got != applied {
		t.Fatalf("entrada aplicada deveria persistir: (%#x,%v)", got, ok)
	}
	if _, ok := l2.Get(2); ok {
		t.Fatal("entrada só de overlay não pode sobreviver ao restart (semântica do design)")
	}
}

func TestLocationsConcorrencia(t *testing.T) {
	l, _ := openLocations(t, 1024)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for r := types.RowID(0); r < 200; r++ {
				if p, ok := l.Get(r); ok {
					if seg, _, _ := Unpack(p); seg == 0 {
						t.Errorf("Get(%d) devolveu packed inválido %#x", r, p)
						return
					}
				}
			}
		}
	}()
	for i := 0; i < 50; i++ {
		for r := types.RowID(0); r < 200; r++ {
			l.Publish(r, Pack(uint32(i+1), uint32(64+r), 0))
		}
		if err := l.ApplyOverlay(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestSequencesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sequences.idx")
	s, err := OpenSequences(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Set(10, 7455409733); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(11, -5); err != nil {
		t.Fatal(err)
	}
	if got := s.Get(10); got != 7455409733 {
		t.Fatalf("Get(10): %d", got)
	}
	if got := s.Get(11); got != -5 {
		t.Fatalf("Get(11): %d", got)
	}
	if got := s.Get(999); got != 0 {
		t.Fatalf("rowId sem sequence deveria ser 0, obtido %d", got)
	}
	if got := s.Get(1 << 40); got != 0 {
		t.Fatalf("rowId além do arquivo deveria ser 0, obtido %d", got)
	}
}

func TestSequencesGrowEReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sequences.idx")
	s, err := OpenSequences(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	const bigRow = 500_000
	if err := s.Set(bigRow, 123456789); err != nil {
		t.Fatal(err)
	}
	if err := s.Msync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenSequences(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Get(bigRow); got != 123456789 {
		t.Fatalf("persistência: esperado 123456789, obtido %d", got)
	}

	raw := make([]byte, 6)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ReadAt(raw, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[0:4], []byte("NDBQ")) {
		t.Fatalf("magic: %q", raw[0:4])
	}
}
