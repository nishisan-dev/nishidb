package storage

import (
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestCheckpointEscreveManifestCoerente(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, CamposIndexados: []types.FieldID{fldStatus}})
	const n = 8
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("cp-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", 0), nil)
	}

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatalf("Load do manifest: %v", err)
	}
	if m.Epoch != 2 { // 1 = manifest da criação; 2 = este checkpoint
		t.Fatalf("Epoch: %d", m.Epoch)
	}
	if m.NextRowID != n || m.RowCount != n {
		t.Fatalf("contadores: %+v", *m)
	}
	if m.CurrentSegment != 1 || len(m.Segments) != 1 || m.Segments[0].ID != 1 {
		t.Fatalf("segmentos: %+v", *m)
	}
	// Stats de liveness com os totais que o writer conhece (refinamento
	// live/garbage é S14; sem compaction, live == total).
	seg := m.Segments[0]
	if seg.TotalRecords != n || seg.LiveRecords != n {
		t.Fatalf("records do segmento: %+v", seg)
	}
	if seg.LiveBytes == 0 || seg.LiveBytes > uint64(seg.SizeBytes) {
		t.Fatalf("liveBytes fora da faixa: %+v", seg)
	}
	if m.DurableOffset <= 64 {
		t.Fatalf("DurableOffset deveria estar após o header: %d", m.DurableOffset)
	}
	if m.PrimarySlotCount != 1<<20 || m.PrimaryUsed != n {
		t.Fatalf("primary no manifest: %+v", *m)
	}
	if m.FieldDictValidBytes <= 8 {
		t.Fatalf("FieldDictValidBytes: %d", m.FieldDictValidBytes)
	}
	// O manifest não descreve mais o conjunto indexado, e a asserção que estava
	// aqui saiu com ele. Quem descreve o índice é o próprio arquivo dentro do
	// segmento: o .sidx tem uma seção por campo, e "qual campo este dataset
	// indexa" se lê de lá — não de uma segunda cópia no manifest, que o primeiro
	// checkpoint atrasado já deixaria divergente.
}

func TestCheckpointEsvaziaOverlayEMantemLeitura(t *testing.T) {
	s := openStore(t, Options{})
	const n = 5
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("ov-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", 0), nil)
	}
	if got := s.Stats().OverlayLen; got != n {
		t.Fatalf("OverlayLen pré-checkpoint: %d", got)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats().OverlayLen; got != 0 {
		t.Fatalf("OverlayLen pós-checkpoint: %d", got)
	}
	// Leituras seguem funcionando (agora via mmap) e o scan não muda.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("ov-%d", i)
		if _, _, ok, err := s.GetByKey(s.presente(), []byte(k), nil); err != nil || !ok {
			t.Fatalf("GetByKey(%q) pós-checkpoint: (%v,%v)", k, ok, err)
		}
	}
	if rows := collectRows(t, s.ScanLive(s.presente())); !slices.Equal(rows, []types.RowID{0, 1, 2, 3, 4}) {
		t.Fatalf("ScanLive pós-checkpoint: %v", rows)
	}
}

func TestCheckpointIncrementaEpochEAceitaNovosPuts(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir})
	mustPut(t, s, "a", 1, statusPayload(t, s, "a", "Start", 0), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "b", 2, statusPayload(t, s, "b", "Start", 0), nil)
	if got := s.Stats().OverlayLen; got != 1 {
		t.Fatalf("OverlayLen entre checkpoints: %d", got)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != 3 || m.RowCount != 2 { // 1 da criação + 2 explícitos
		t.Fatalf("%+v", *m)
	}
}

func TestCheckpointComLeitoresConcorrentes(t *testing.T) {
	s := openStore(t, Options{})
	key := []byte("cp-conc")
	mustPut(t, s, "cp-conc", 0, statusPayload(t, s, "cp-conc", "Start", 0), nil)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var buf []byte
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, b, ok, err := s.GetByKey(s.presente(), key, buf); err != nil || !ok {
				t.Errorf("leitura durante checkpoint: (%v,%v)", ok, err)
				return
			} else {
				buf = b
			}
		}
	}()
	var prev types.Record
	var buf []byte
	var ok bool
	var err error
	for i := int64(1); i <= 50; i++ {
		prev, buf, ok, err = s.GetByKey(s.presente(), key, buf)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if _, _, err := s.Put(key, i, 0, statusPayload(t, s, "cp-conc", "Start", i), &prev); err != nil {
			t.Fatal(err)
		}
		if i%10 == 0 {
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
}
