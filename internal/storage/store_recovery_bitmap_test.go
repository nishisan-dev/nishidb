package storage

// Critical 1 e Important 3 da revisão da S12: o replay derivava o "estado
// anterior" de uma location que pode estar À FRENTE do ponto S, e as duas
// consequências disso — posting perdida e liveness decrementada de record
// vivo. Traz também o teste-guarda de propriedade do bitmap, usado ao final
// de todos os cenários de recovery.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// exigeBitmapCompleto verifica a propriedade da qual a spec §44 depende:
// para toda row viva, todo campo indexado do payload CORRENTE aparece nas
// postings do seu valor.
//
// É uma propriedade, e não um caso de teste, por causa da assimetria do
// §14.16: falso positivo o executor filtra na revalidação, falso negativo é
// invisível — nenhuma camada acima recupera um candidato que o índice não
// devolveu, e a query volta silenciosamente incompleta.
//
// Limite: cobre as rows alcançáveis pelo ScanLive. Uma row cuja location se
// perdeu não é varrida aqui — isso é responsabilidade das asserções de
// visibilidade por key de cada cenário.
func exigeBitmapCompleto(t *testing.T, s *Store) {
	t.Helper()
	// Sem índice declarado o guarda não verifica nada. Falhar aqui evita que
	// ele vire um no-op invisível se algum cenário perder a configuração de
	// bitmap — uma rede que passa sempre é pior que rede nenhuma.
	if len(s.bitmaps) == 0 {
		t.Fatal("guarda de bitmap chamado num Store sem índice: o cenário precisa declarar BitmapFields, senão a asserção não verifica nada")
	}
	it := s.ScanLive()
	defer it.Close()
	var buf []byte
	for it.Next() {
		id := it.RowID()
		rec, b, vivo, err := s.GetByRowID(id, buf)
		buf = b
		if err != nil {
			t.Fatalf("guarda de bitmap: GetByRowID(%d): %v", id, err)
		}
		if !vivo {
			continue
		}
		var r encoding.FieldReader
		r.Reset(rec.Payload)
		for {
			fid, v, ok := r.Next()
			if !ok {
				break
			}
			if _, indexado := s.bitmaps[fid]; !indexado {
				continue
			}
			postings, err := s.LookupBitmap(fid, v)
			if err != nil {
				t.Fatalf("guarda de bitmap: LookupBitmap(%d): %v", fid, err)
			}
			achou := false
			for postings.Next() {
				if postings.RowID() == id {
					achou = true
					break
				}
			}
			postings.Close()
			if !achou {
				t.Fatalf("FALSO NEGATIVO no bitmap: a row %d tem o campo %d=%q no payload corrente, mas não aparece nas postings desse valor",
					id, fid, v.Bytes)
			}
		}
		if err := r.Err(); err != nil {
			t.Fatalf("guarda de bitmap: payload da row %d: %v", id, err)
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("guarda de bitmap: ScanLive: %v", err)
	}
}

// copiaDir copia um diretório raso (arquivos apenas) para dst, substituindo
// o destino. Usado para regredir índices ao estado do último checkpoint.
func copiaDir(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.RemoveAll(dst); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func bitmapDir(dir string, field types.FieldID) string {
	return filepath.Join(dir, "indexes", "bitmap", fmt.Sprintf("%05d", field))
}

func TestReplayRestauraPostingComBitmapAtrasDasLocations(t *testing.T) {
	// CRITICAL 1. O §10.2 msynca as locations (passo 3) ANTES dos bitmaps e
	// antes do manifest (passo 4), então após um crash é NORMAL a location
	// já apontar para o record do tail enquanto a posting não chegou ao
	// disco. Derivar o prev dessa location faz o replay comparar o record
	// consigo mesmo: diff vazio, Set suprimido, posting nunca restaurada.
	// Observável: row visível por GetByKey com o estado novo e LookupBitmap
	// devolvendo VAZIO — query indexada incompleta após restart.
	dir := t.TempDir()
	opts := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}
	s := audOpen(t, opts)
	id, _ := mustPut(t, s, "bm", 1, statusPayload(t, s, "bm", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Estado dos bitmaps no ponto S, para regredir depois.
	backup := filepath.Join(t.TempDir(), "bm-em-S")
	copiaDir(t, bitmapDir(dir, fldStatus), backup)

	prev, _, ok, err := s.GetByKey([]byte("bm"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	mustPut(t, s, "bm", 2, statusPayload(t, s, "bm", "Stop", 2), &prev)
	// Crash entre msyncLocations e msyncBitmaps: as locations do tail já
	// estão duráveis…
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// …e as postings, não.
	copiaDir(t, backup, bitmapDir(dir, fldStatus))

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
	// o lado seguro. Este teste NÃO cobre esse caso.
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
