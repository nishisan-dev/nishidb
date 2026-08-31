package storage

// Important 1 e 4 e os Minors 2 e 4 da revisão da S12: a janela de crash que
// o replayValidator protege, a classificação de erro do manifest, o replay de
// tombstone e o caráter advisory dos campos de primary no manifest.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestReplayNaoDuplicaSlotComPrimaryAFrenteDasLocations(t *testing.T) {
	// IMPORTANT 1: janela simétrica da já testada — crash entre msyncPrimary
	// e msyncLocations deixa a PRIMARY à frente. É a janela que o
	// replayValidator protege: sem ele, o validador resolve o slot lendo
	// locations[rid], não encontra a location, responde "não é a row", o
	// Insert segue o probe e grava a MESMA key num segundo slot.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "base", 1, statusPayload(t, s, "base", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	locPath := filepath.Join(dir, "indexes", "locations.idx")
	locsEmS, err := os.ReadFile(locPath)
	if err != nil {
		t.Fatal(err)
	}

	const novas = 3
	for i := 0; i < novas; i++ {
		k := fmt.Sprintf("pf-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Regride SÓ as locations: a primary conhece as keys do tail, as
	// locations não.
	if err := os.WriteFile(locPath, locsEmS, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := audOpen(t, opts)
	defer s2.Close()
	for i := 0; i < novas; i++ {
		mustGetAud(t, s2, fmt.Sprintf("pf-%d", i))
	}
	mustGetAud(t, s2, "base")
	// A prova do finding: sem o replayValidator o replay grava slots
	// duplicados e Used() infla.
	if used := s2.PrimaryUsedForTest(); used != novas+1 {
		t.Fatalf("primary com slot duplicado: Used()=%d, esperado %d (1 base + %d do tail)", used, novas+1, novas)
	}
}

func TestErroTransitorioDoManifestNaoEhDatasetCorrompido(t *testing.T) {
	// IMPORTANT 4: só ErrCorruptManifest justifica afirmar corrupção. Um
	// EACCES/EIO é transitório, e a resposta operacional a "dataset
	// corrompido" (restaurar backup, reconstruir) é destrutiva.
	if os.Geteuid() == 0 {
		t.Skip("root ignora permissões de arquivo")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "tr", 1, statusPayload(t, s, "tr", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(audManifestPath(dir), 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(audManifestPath(dir), 0o644)

	_, err := Open(opts)
	if err == nil {
		t.Fatal("manifest ilegível deveria falhar")
	}
	if errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("falha transitória de leitura não pode ser afirmada como dataset corrompido: %v", err)
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("o erro de I/O original deveria ser preservado: %v", err)
	}
}

func TestReplayDeTombstone(t *testing.T) {
	// MINOR 2: nenhum cenário de recovery replayava record com
	// FlagTombstone — é o caminho do delete perdido num crash.
	t.Run("tombstone_no_tail_nao_ressuscita", func(t *testing.T) {
		dir := t.TempDir()
		opts := Options{Dir: dir}
		s := audOpen(t, opts)
		id, _ := mustPut(t, s, "del", 1, statusPayload(t, s, "del", "Start", 1), nil)
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		prev, _, ok, err := s.GetByKey([]byte("del"), nil)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if _, _, err := s.Put([]byte("del"), 2, types.FlagTombstone, statusPayload(t, s, "del", "Stop", 2), &prev); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}

		s2 := audOpen(t, opts)
		defer s2.Close()
		if _, _, ok, _ := s2.GetByKey([]byte("del"), nil); ok {
			t.Fatal("o tombstone do tail não pode ressuscitar a row no replay")
		}
		if _, _, ok, _ := s2.GetByRowID(s2.AbreSnapshot(), id, nil); ok {
			t.Fatal("GetByRowID deveria respeitar o tombstone replayado")
		}
		if rows := collectRows(t, s2.ScanLive(s2.AbreSnapshot())); len(rows) != 0 {
			t.Fatalf("ScanLive não pode listar row tombstoned: %v", rows)
		}
	})

	t.Run("ressurreicao_no_tail_apos_tombstone_checkpointado", func(t *testing.T) {
		dir := t.TempDir()
		opts := Options{Dir: dir}
		s := audOpen(t, opts)
		id, _ := mustPut(t, s, "res", 1, statusPayload(t, s, "res", "Start", 1), nil)
		prev, _, ok, err := s.GetByKey([]byte("res"), nil)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if _, _, err := s.Put([]byte("res"), 2, types.FlagTombstone, statusPayload(t, s, "res", "Stop", 2), &prev); err != nil {
			t.Fatal(err)
		}
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		// Ressuscita no tail (sem prev: a key está tombstoned, o lookup a
		// trata como ausente).
		if _, _, err := s.Put([]byte("res"), 3, 0, statusPayload(t, s, "res", "Start", 3), nil); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}

		s2 := audOpen(t, opts)
		defer s2.Close()
		rec, _, ok, err := s2.GetByKey([]byte("res"), nil)
		if err != nil || !ok {
			t.Fatalf("a ressurreição do tail deveria valer pós-replay: (ok=%v, err=%v)", ok, err)
		}
		if rec.Sequence != 3 {
			t.Fatalf("estado ressuscitado: %+v (row original %d)", rec, id)
		}
	})
}

func TestAdvisoryDoManifestNaoViraErro(t *testing.T) {
	// MINOR 4: PrimarySlotCount divergente é advisory (§14.15) — o header do
	// primary.idx prevalece. Sem teste, um refactor pode transformar o log em
	// erro e travar datasets legítimos pós-rehash.
	//
	// O HashKind era a outra metade deste teste e saiu do manifest na v3: o
	// header do primary.idx já RECUSA um kind desconhecido, o que fazia da cópia
	// no manifest uma segunda declaração do mesmo fato. O que se perde no dia em
	// que houver um segundo kind está anotado em internal/index/primary, junto de
	// onde esse segundo kind seria declarado.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "adv", 1, statusPayload(t, s, "adv", "Start", 1), nil)
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
	m.PrimarySlotCount *= 4 // como se um rehash tivesse acontecido pós-checkpoint
	if err := checkpoint.Write(audManifestPath(dir), m); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("divergência advisory não pode impedir a abertura: %v", err)
	}
	defer s2.Close()
	mustGetAud(t, s2, "adv")
}
