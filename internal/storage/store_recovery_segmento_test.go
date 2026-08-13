package storage

// Important 2 da revisão da S12: segmento de 0 bytes (crash entre o
// O_CREATE|O_EXCL e o writeHeader do openNew) travava o dataset para sempre,
// e repará-lo cegamente perderia records de um segmento zerado por fora.

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestOpenComSegmentoDeZeroBytes(t *testing.T) {
	t.Run("recovery_adota_e_repara", func(t *testing.T) {
		dir := t.TempDir()
		opts := Options{Dir: dir, SegmentSize: 400}
		s := audOpen(t, opts)
		mustPut(t, s, "z-0", 0, statusPayload(t, s, "z-0", "Start", 0), nil)
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(audSegPath(dir, 2), nil, 0o644); err != nil {
			t.Fatal(err)
		}

		s2 := audOpen(t, opts)
		defer s2.Close()
		mustGetAud(t, s2, "z-0")
		if got := s2.Stats().CurrentSegmentID; got != 2 {
			t.Fatalf("o segmento reparado deveria ser o corrente, obtido %d", got)
		}
		// O header foi completado: escrever e reler funciona.
		mustPut(t, s2, "z-1", 1, statusPayload(t, s2, "z-1", "Start", 1), nil)
		mustGetAud(t, s2, "z-1")
	})

	t.Run("cold_start_repara", func(t *testing.T) {
		dir := t.TempDir()
		opts := Options{Dir: dir}
		s := audOpen(t, opts)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(audManifestPath(dir)); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(audSegPath(dir, 1), nil, 0o644); err != nil {
			t.Fatal(err)
		}

		s2, err := Open(opts)
		if err != nil {
			t.Fatalf("cold start com segmento de 0 bytes: %v", err)
		}
		defer s2.Close()
		mustPut(t, s2, "cs", 1, statusPayload(t, s2, "cs", "Start", 1), nil)
		mustGetAud(t, s2, "cs")
	})

	t.Run("segmento_intermediario_zerado_eh_fatal", func(t *testing.T) {
		// O reparo só é legítimo no ÚLTIMO segmento: um arquivo sem header
		// no MEIO da cadeia é buraco, não rotação interrompida. Sem a guarda,
		// o caminho de reparo assume o fim válido no header e o Truncate do
		// OpenWriter ZERA um segmento íntegro em silêncio — dano pior que
		// simplesmente não detectar.
		dir := t.TempDir()
		opts := Options{Dir: dir, SegmentSize: 400}
		s := audOpen(t, opts)
		for i := 0; i < 12; i++ {
			k := fmt.Sprintf("mid-%d", i)
			mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
		}
		if got := s.Stats().CurrentSegmentID; got < 3 {
			t.Fatalf("o cenário exige 3+ segmentos, obtido %d", got)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		// Sem checkpoint após os puts: o manifest inicial só conhece o
		// segmento 1, então o 2 não é "encolhido" — é intermediário sem
		// header, que é o caso desta guarda.
		if err := os.WriteFile(audSegPath(dir, 2), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		tamanhos := map[uint32]int64{1: fileSizeAud(t, audSegPath(dir, 1)), 3: fileSizeAud(t, audSegPath(dir, 3))}

		if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("segmento intermediário sem header deveria ser fatal, obtido %v", err)
		}
		for id, want := range tamanhos {
			if got := fileSizeAud(t, audSegPath(dir, id)); got != want {
				t.Fatalf("o Open recusado não pode truncar nada: segmento %d %d → %d", id, want, got)
			}
		}
	})

	t.Run("segmento_do_manifest_zerado_eh_fatal", func(t *testing.T) {
		// Salvaguarda: um segmento COM dados zerado por truncamento externo é
		// byte a byte indistinguível do órfão de rotação. Reparar apagaria a
		// evidência e perderia os records em silêncio.
		dir := t.TempDir()
		opts := Options{Dir: dir}
		s := audOpen(t, opts)
		mustPut(t, s, "perdido", 1, statusPayload(t, s, "perdido", "Start", 1), nil)
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(audSegPath(dir, 1), nil, 0o644); err != nil {
			t.Fatal(err)
		}

		if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("segmento do manifest zerado deveria ser fatal, obtido %v", err)
		}
	})
}
