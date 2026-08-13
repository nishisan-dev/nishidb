package storage

// G5' da SC: acesso exclusivo ao diretório do dataset.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDatasetLockImpedeSegundaAbertura(t *testing.T) {
	// Dois processos no mesmo diretório teriam dois overlays de location,
	// dois contadores de rowId e dois checkpoints concorrentes. Com o CLI
	// isso vira alcançável por operação humana normal (ingest + query).
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)

	_, err := Open(opts)
	if !errors.Is(err, ErrDatasetLocked) {
		t.Fatalf("segunda abertura deveria ser recusada, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "pid") {
		t.Fatalf("o erro precisa apontar quem detém o dataset: %v", err)
	}

	// Fechar libera: o dataset volta a ser abrível.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("após o Close o dataset deveria abrir: %v", err)
	}
	defer s2.Close()
	mustPut(t, s2, "lk", 1, statusPayload(t, s2, "lk", "Start", 1), nil)
}

func TestLockEhLiberadoQuandoOpenFalha(t *testing.T) {
	// O lock é adquirido antes de tudo; um Open que falha depois disso não
	// pode deixar o dataset travado até o processo morrer. A prova é que a
	// segunda tentativa devolve o MESMO erro de diagnóstico, não "locked".
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "lf", 1, statusPayload(t, s, "lf", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrompe o manifest: o Open falhará DEPOIS de pegar o lock.
	path := filepath.Join(dir, "checkpoint", "manifest.bin")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	primeiro := func() error {
		_, err := Open(opts)
		return err
	}
	if err := primeiro(); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("esperado ErrDatasetCorrupt, obtido %v", err)
	}
	segundo := func() error {
		_, err := Open(opts)
		return err
	}
	err = segundo()
	if errors.Is(err, ErrDatasetLocked) {
		t.Fatalf("o lock vazou no caminho de erro do Open: %v", err)
	}
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("a segunda tentativa deveria repetir o diagnóstico real, obtido %v", err)
	}
}

func TestLockNaoImpedeReaberturaSequencial(t *testing.T) {
	// O ciclo normal dos testes e do CLI: abrir, fechar, reabrir várias
	// vezes no mesmo diretório.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	for i := 0; i < 3; i++ {
		s, err := Open(opts)
		if err != nil {
			t.Fatalf("abertura %d: %v", i, err)
		}
		mustPut(t, s, "seq", int64(i), statusPayload(t, s, "seq", "Start", int64(i)), nil)
		if err := s.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if rec := mustGetAud(t, s, "seq"); rec.Sequence != 2 {
		t.Fatalf("estado após 3 ciclos: %+v", rec)
	}
}
