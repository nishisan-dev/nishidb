package storage

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// A carência grava no ÍNDICE de prazos, não no payload. Este teste fixa essa
// distinção, porque confundi-la leva a medir a coisa errada: uma consulta
// "_writtenAt IS NULL" continua devolvendo a entidade depois da carência, e
// isso NÃO significa que o coletor não rodou.
func TestCarenciaGravaNoIndiceENaoNoPayload(t *testing.T) {
	s, _ := storeComExpiracao(t, t.TempDir(), 90*24*time.Hour)
	id, _ := mustPut(t, s, "legada", 1, payloadSemEscrita(t, s, "legada"), nil)

	if _, conhecido := s.escritas.Get(id); conhecido {
		t.Fatal("nasceu com prazo")
	}
	r, err := s.ExpiraUmaPassada(context.Background(), limitesDeTeste())
	if err != nil {
		t.Fatal(err)
	}
	if r.Carencias != 1 {
		t.Fatalf("carencias=%d: %+v", r.Carencias, r)
	}
	// O ÍNDICE agora conhece o prazo...
	if _, conhecido := s.escritas.Get(id); !conhecido {
		t.Fatal("a carência não chegou ao índice")
	}
	// ...e o PAYLOAD continua sem o campo.
	rec, _, ok, err := s.GetByRowID(s.presente(), id, nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, tem := instanteDeEscrita(rec.Payload, s.campoDeEscrita); tem {
		t.Fatal("a carência escreveu no payload; ela deve escrever só no índice")
	}
	_ = types.RowID(0)
}
