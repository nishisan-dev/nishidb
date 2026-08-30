package storage

// Critical 1 e Important 3 da revisão da S12: o replay derivava o "estado
// anterior" de uma location que pode estar À FRENTE do ponto S, e as duas
// consequências disso — posting perdida e liveness decrementada de record vivo.

import (
	"fmt"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestReplayRestauraPostingComBitmapAtrasDasLocations(t *testing.T) {
	// CRITICAL 1. O §10.2 msynca as locations (passo 3) antes do manifest
	// (passo 4), então após um crash é NORMAL a location já apontar para o
	// record do tail enquanto o índice não conhece a posting. Derivar o prev
	// dessa location fazia o replay comparar o record consigo mesmo: diff
	// vazio, Set suprimido, posting nunca restaurada. Observável: row visível
	// por GetByKey com o estado novo e o Lookup devolvendo VAZIO — query
	// indexada incompleta após restart.
	//
	// A montagem deixou de regredir o índice a um backup do ponto S, e o motivo
	// é que não existe mais o que regredir: o índice por segmento não é
	// durabilizado no checkpoint e é reconstruído em toda abertura (spec §2.5).
	// O crash que o teste ainda encena é o que importa — locations duráveis à
	// frente do manifest —, e é ele que decide se o replay do tail repõe a
	// posting.
	dir := t.TempDir()
	opts := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}
	s := audOpen(t, opts)
	id, _ := mustPut(t, s, "bm", 1, statusPayload(t, s, "bm", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	prev, _, ok, err := s.GetByKey([]byte("bm"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	mustPut(t, s, "bm", 2, statusPayload(t, s, "bm", "Stop", 2), &prev)
	// Crash com as locations do tail já duráveis e o manifest ainda no ponto S.
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := audOpen(t, opts)
	defer s2.Close()

	if rec := mustGetAud(t, s2, "bm"); rec.Sequence != 2 {
		t.Fatalf("o estado novo deveria estar visível: %+v", rec)
	}
	it, err := s2.LookupBitmap(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, it)
	if len(rows) != 1 || rows[0] != id {
		t.Fatalf("a posting de Stop deveria ter sido restaurada pelo replay, obtido %v", rows)
	}
	exigeBitmapCompleto(t, s2)
}

func TestReplayNaoDecrementaLivenessDeRecordVivo(t *testing.T) {
	// IMPORTANT 3, mesma raiz do Critical 1: com a location apontando para o
	// próprio record replayado, o decremento de liveness usava esse "estado
	// anterior" e marcava como garbage um record VIVO.
	//
	// Escopo do que este teste prova: as rows aqui são todas CRIADAS dentro
	// do tail, e para elas os três modos batem exatamente. Uma row que já
	// existia em S e recebe update no tail com location à frente cai no
	// resíduo documentado em replayRecord — LiveRecords superestimado, que é
	// o lado seguro. Este teste NÃO cobre esse caso; quem o mede, e prova que
	// ele não converge, é o TestResiduoDeLivenessDoReplayNaoConverge.
	monta := func(t *testing.T, modo string) []checkpoint.SegmentInfo {
		t.Helper()
		dir := t.TempDir()
		opts := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}
		s := audOpen(t, opts)
		for i := 0; i < 4; i++ {
			k := fmt.Sprintf("tl-%d", i)
			mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
		}
		for _, k := range []string{"tl-0", "tl-1"} {
			prev, _, ok, err := s.GetByKey([]byte(k), nil)
			if err != nil || !ok {
				t.Fatal(err)
			}
			mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)
		}
		switch modo {
		case "escrita":
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			defer s.Close()
		case "replay":
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2 := audOpen(t, opts)
			defer s2.Close()
			exigeBitmapCompleto(t, s2)
		case "replay_overlay_aplicado":
			// Checkpoint parcial: locations à frente do manifest.
			if err := s.PartialCheckpointForTest(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2 := audOpen(t, opts)
			defer s2.Close()
			exigeBitmapCompleto(t, s2)
		}
		m, err := checkpoint.Load(audManifestPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		return m.Segments
	}

	escrita := monta(t, "escrita")
	for _, modo := range []string{"replay", "replay_overlay_aplicado"} {
		got := monta(t, modo)
		if len(got) != len(escrita) {
			t.Fatalf("modo %s: número de segmentos %d, esperado %d", modo, len(got), len(escrita))
		}
		for i := range escrita {
			e, g := escrita[i], got[i]
			if e.TotalRecords != g.TotalRecords || e.LiveRecords != g.LiveRecords || e.LiveBytes != g.LiveBytes {
				t.Fatalf("modo %s divergiu da escrita no segmento %d:\n  escrita = %+v\n  obtido  = %+v", modo, e.ID, e, g)
			}
		}
	}
}
