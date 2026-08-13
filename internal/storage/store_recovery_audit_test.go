package storage

// Auditoria da S12: cenários que o conjunto principal de testes de recovery
// não cobria e que exercitam caminhos onde o recovery pode DESTRUIR dados ou
// deixar o dataset permanentemente inacessível.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func audManifestPath(dir string) string {
	return filepath.Join(dir, "checkpoint", "manifest.bin")
}

func audSegPath(dir string, id uint32) string {
	return filepath.Join(dir, "data", fmt.Sprintf("segment-%08d.dat", id))
}

// audOpen abre sem registrar cleanup: estes testes fecham explicitamente
// para simular o crash.
func audOpen(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.SegmentSize == 0 {
		opts.SegmentSize = 1 << 20
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func TestReplayReinsereKeyComPrimaryAtrasDasLocations(t *testing.T) {
	// Janela real do §10.2: o ApplyOverlay grava as locations no mmap ANTES
	// do msync da primary. Um crash entre os dois deixa as locations À
	// FRENTE da primary. Se o replay só inserisse na primary as rows que
	// ainda não têm location, a key ficaria inalcançável por GetByKey para
	// sempre — visível por rowId, invisível por key.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "base", 1, statusPayload(t, s, "base", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	idxPath := filepath.Join(dir, "indexes", "primary.idx")
	primariaEmS, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatal(err)
	}

	mustPut(t, s, "tail-1", 1, statusPayload(t, s, "tail-1", "Start", 1), nil)
	mustPut(t, s, "tail-2", 2, statusPayload(t, s, "tail-2", "Start", 2), nil)
	// Índices em disco à frente do manifest (crash entre os passos 3 e 4).
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Regride SÓ a primary ao estado de S: as locations do tail continuam no
	// mmap, a primary não conhece as keys do tail.
	if err := os.WriteFile(idxPath, primariaEmS, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := audOpen(t, opts)
	defer s2.Close()
	for _, k := range []string{"base", "tail-1", "tail-2"} {
		rec, _, ok, err := s2.GetByKey([]byte(k), nil)
		if err != nil || !ok {
			t.Fatalf("GetByKey(%q) após replay com primary atrasada: (ok=%v, err=%v)", k, ok, err)
		}
		if string(rec.Key) != k {
			t.Fatalf("GetByKey(%q) devolveu a key %q", k, rec.Key)
		}
	}
	// E a reinserção não pode ter duplicado slots da key que já estava lá.
	if used := s2.Stats().PrimaryLoadFactor; used <= 0 {
		t.Fatal("load factor deveria ser positivo")
	}
	novo, created := mustPut(t, s2, "tail-1", 9, statusPayload(t, s2, "tail-1", "Stop", 9), nil)
	if created {
		t.Fatalf("a key do tail deveria ser reconhecida como existente, virou row nova %d", novo)
	}
}

func TestOpenAposCrashAntesDoPrimeiroManifest(t *testing.T) {
	// O Open cria o segmento 1 (header + fsync do dir) ANTES de gravar o
	// manifest inicial. Um crash nessa janela deixa "segmento sem manifest";
	// tratar isso como corrupção faria o dataset nascer inacessível, sem
	// nunca ter tido um único record.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(audManifestPath(dir)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(audSegPath(dir, 1)); err != nil {
		t.Fatalf("o cenário exige o segmento 1 já criado: %v", err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("dataset sem records não pode ficar inacessível: %v", err)
	}
	defer s2.Close()
	mustPut(t, s2, "pos-crash", 1, statusPayload(t, s2, "pos-crash", "Start", 1), nil)
	if _, _, ok, err := s2.GetByKey([]byte("pos-crash"), nil); !ok || err != nil {
		t.Fatalf("escrita após o recomeço: (ok=%v, err=%v)", ok, err)
	}
}

func TestOpenComDurableOffsetAlemDoSegmento(t *testing.T) {
	// Manifest declarando durável um offset que o arquivo não tem: o
	// OpenWriter chama Truncate(offset), e ftruncate ALÉM do fim ESTENDE o
	// arquivo com zeros — o writer passaria a escrever depois de um buraco.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "do", 1, statusPayload(t, s, "do", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	m, err := checkpoint.Load(audManifestPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	tamanhoOriginal := fileSizeAud(t, audSegPath(dir, 1))
	m.DurableOffset = uint64(tamanhoOriginal) + 4096
	if err := checkpoint.Write(audManifestPath(dir), m); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("durableOffset além do arquivo deveria ser fatal, obtido %v", err)
	}
	if got := fileSizeAud(t, audSegPath(dir, 1)); got != tamanhoOriginal {
		t.Fatalf("o Open não pode ter mexido no segmento: %d → %d", tamanhoOriginal, got)
	}
}

func TestOpenComDurableOffsetAbaixoDoHeader(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "dh", 1, statusPayload(t, s, "dh", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	m, err := checkpoint.Load(audManifestPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	m.DurableOffset = segment.HeaderSize - 1
	if err := checkpoint.Write(audManifestPath(dir), m); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("durableOffset dentro do header deveria ser fatal, obtido %v", err)
	}
}

func TestErrosFataisDeDatasetSaoClassificaveis(t *testing.T) {
	// Todo caminho "não dá para recuperar sozinho" precisa ser distinguível
	// de um erro de I/O transitório pelo caller (e pelos testes).
	novoDataset := func(t *testing.T) (string, Options) {
		t.Helper()
		dir := t.TempDir()
		opts := Options{Dir: dir}
		s := audOpen(t, opts)
		mustPut(t, s, "x", 1, statusPayload(t, s, "x", "Start", 1), nil)
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		return dir, opts
	}

	t.Run("manifest_ausente_com_dados", func(t *testing.T) {
		dir, opts := novoDataset(t)
		if err := os.Remove(audManifestPath(dir)); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("obtido %v", err)
		}
	})

	t.Run("manifest_corrompido", func(t *testing.T) {
		dir, opts := novoDataset(t)
		raw, err := os.ReadFile(audManifestPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 0xFF
		if err := os.WriteFile(audManifestPath(dir), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("obtido %v", err)
		}
	})

	t.Run("segmento_do_manifest_removido", func(t *testing.T) {
		dir, opts := novoDataset(t)
		if err := os.Remove(audSegPath(dir, 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("obtido %v", err)
		}
	})
}

func TestOpenComBuracoNaCadeiaDeSegmentos(t *testing.T) {
	// Segmento intermediário ausente: o replay não pode pular os records
	// dele em silêncio.
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 400}
	s := audOpen(t, opts)
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("bur-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if got := s.Stats().CurrentSegmentID; got < 3 {
		t.Fatalf("o cenário exige 3+ segmentos, obtido %d", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(audSegPath(dir, 2)); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("buraco na cadeia deveria ser fatal, obtido %v", err)
	}
}

func TestReplayUsaOMesmoCodePathDoWritePath(t *testing.T) {
	// Regra do §15A (Option A): replay e escrita compartilham a orquestração,
	// senão as duas divergem em silêncio. O observável é a igualdade dos
	// tallies: gravar N records e replayá-los tem que produzir a mesma
	// contabilidade de liveness que gravá-los com checkpoint no fim.
	monta := func(t *testing.T, comCheckpointNoFim bool) []checkpoint.SegmentInfo {
		t.Helper()
		dir := t.TempDir()
		opts := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}
		s := audOpen(t, opts)
		for i := 0; i < 4; i++ {
			k := fmt.Sprintf("cp-%d", i)
			mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
		}
		// Dois updates: viram garbage no mesmo segmento.
		for _, k := range []string{"cp-0", "cp-1"} {
			prev, _, ok, err := s.GetByKey([]byte(k), nil)
			if err != nil || !ok {
				t.Fatal(err)
			}
			mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)
		}
		if comCheckpointNoFim {
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			defer s.Close()
		} else {
			// Sem checkpoint: tudo volta pelo replay do tail no reopen.
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2 := audOpen(t, opts)
			defer s2.Close()
		}
		m, err := checkpoint.Load(audManifestPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		return m.Segments
	}

	escrito := monta(t, true)
	replayado := monta(t, false)
	if len(escrito) != len(replayado) {
		t.Fatalf("número de segmentos: escrito=%d replayado=%d", len(escrito), len(replayado))
	}
	for i := range escrito {
		e, r := escrito[i], replayado[i]
		if e.TotalRecords != r.TotalRecords || e.LiveRecords != r.LiveRecords || e.LiveBytes != r.LiveBytes {
			t.Fatalf("tally do segmento %d divergiu entre escrita e replay:\n  escrito  = %+v\n  replayado= %+v", e.ID, e, r)
		}
	}
}

func fileSizeAud(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}
