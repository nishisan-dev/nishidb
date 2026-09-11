package rowindex

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestApplyOverlayFalhaParcialPreservaEntradas(t *testing.T) {
	// Deferido da re-revisão S5-S9: a idempotência sob falha parcial do
	// ApplyOverlay (elogiada na revisão) precisa de teste. Um store falha
	// no meio do lote: nada se perde, o restante volta ao overlay e a
	// retentativa completa o trabalho.
	l, _ := openLocations(t, 1024)
	for r := types.RowID(0); r < 5; r++ {
		l.Publish(r, Pack(1, uint32(64+8*r), 0))
	}
	calls := 0
	l.SetStoreErrHookForTest(func(types.RowID) error {
		calls++
		if calls == 3 {
			return errors.New("disco cheio simulado")
		}
		return nil
	})
	if err := l.ApplyOverlay(); err == nil {
		t.Fatal("apply com store falhando deveria devolver erro")
	}
	for r := types.RowID(0); r < 5; r++ {
		if _, ok := l.Get(r); !ok {
			t.Fatalf("row %d perdida após falha parcial", r)
		}
	}
	if got := l.OverlayLen(); got != 3 {
		t.Fatalf("3 entradas não aplicadas deveriam voltar ao overlay, tem %d", got)
	}

	l.SetStoreErrHookForTest(nil)
	if err := l.ApplyOverlay(); err != nil {
		t.Fatalf("retentativa: %v", err)
	}
	if got := l.OverlayLen(); got != 0 {
		t.Fatalf("overlay deveria esvaziar na retentativa, tem %d", got)
	}
	for r := types.RowID(0); r < 5; r++ {
		if _, ok := l.Get(r); !ok {
			t.Fatalf("row %d perdida após retentativa", r)
		}
	}
}

func TestPackUnpackRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		sec   uint32
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
			p := Pack(c.sec, c.off, c.flags)
			if p == 0 {
				t.Fatal("packed válido nunca pode ser 0 (0 = sem location)")
			}
			sec, off, flags := Unpack(p)
			if sec != c.sec || off != c.off || flags != c.flags {
				t.Fatalf("roundtrip: esperado (%d,%d,%d), obtido (%d,%d,%d)",
					c.sec, c.off, c.flags, sec, off, flags)
			}
		})
	}
}

func TestPackPanicaComLimitesEstourados(t *testing.T) {
	cases := []struct {
		name  string
		sec   uint32
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
			Pack(c.sec, c.off, c.flags)
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

func TestLocationNuncaFicaInvisivelDuranteApply(t *testing.T) {
	// I6: rows criadas desde o último checkpoint existem SÓ no overlay
	// (não há valor antigo no mmap). Durante o ApplyOverlay elas precisam
	// continuar visíveis a leitores — o lote em aplicação fica exposto até
	// a última entrada estar no mmap. O hook verifica deterministicamente
	// no meio do apply.
	l, _ := openLocations(t, 1024)
	rows := []types.RowID{1, 2, 3, 4, 5}
	for _, r := range rows {
		l.Publish(r, Pack(1, uint32(64+8*r), 0))
	}
	l.SetApplyHookForTest(func() {
		for _, r := range rows {
			if _, ok := l.Get(r); !ok {
				t.Errorf("row %d invisível durante o apply (janela overlay→mmap)", r)
			}
		}
	})
	if err := l.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, ok := l.Get(r); !ok {
			t.Fatalf("row %d perdida após o apply", r)
		}
	}
}

func TestApplyOverlayComPublishConcorrenteNaoAborta(t *testing.T) {
	// I6: a invariante do design (§10.2) é que Publish e ApplyOverlay rodam
	// no mesmo goroutine (o writer). Este teste garante que uma violação
	// acidental dessa invariante nunca pode ABORTAR o processo com
	// "fatal error: concurrent map iteration and map write" (incapturável):
	// o swap do map sob Lock elimina a iteração de map vivo por construção.
	l, _ := openLocations(t, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			for r := types.RowID(0); r < 100; r++ {
				l.Publish(r, Pack(uint32(i%100+1), uint32(64+r), 0))
			}
		}
	}()
	for i := 0; i < 200; i++ {
		if err := l.ApplyOverlay(); err != nil {
			t.Fatal(err)
		}
	}
	<-done
	if err := l.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	for r := types.RowID(0); r < 100; r++ {
		if _, ok := l.Get(r); !ok {
			t.Fatalf("row %d perdeu a location", r)
		}
	}
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
