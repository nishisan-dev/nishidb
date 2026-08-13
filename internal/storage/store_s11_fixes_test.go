package storage

// Testes do fix round consolidado S10+S11: C2 (ordem de durabilidade do
// checkpoint), I1 (liveness real por segmento), I2 (writer serializado).

import (
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestCheckpointOrdemDeDurabilidade(t *testing.T) {
	// S11-C2: trava a SEQUÊNCIA do protocolo §10.2 — fdatasync do segmento
	// ANTES do apply do overlay, os 4 msyncs depois, manifest POR ÚLTIMO.
	// As mutações da revisão (inverter Sync↔ApplyOverlay; remover msyncs;
	// remover fsync do manifest) falham este teste.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	var trace []string
	s.SetCheckpointTraceForTest(func(step string) { trace = append(trace, step) })

	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("ord-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", 0), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	want := []string{"segSync", "applyOverlay", "msyncPrimary", "msyncLocations", "msyncSequences", "msyncBitmaps", "manifest"}
	if !slices.Equal(trace, want) {
		t.Fatalf("ordem de durabilidade violada:\nesperado %v\nobtido   %v", want, trace)
	}
}

// A ordem interna do checkpoint.Write (write→fsync→rename→dirsync) é
// travada por TestWriteOrdemDeDurabilidade no próprio pacote checkpoint.

func TestLivenessDecrementaEmUpdate(t *testing.T) {
	// S11-I1: update torna o record anterior garbage imediatamente — o
	// manifest tem que refletir live < total, sem depender de compaction.
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir})
	// 3 creates + 2 updates na mesma key = 5 records, 3 vivos.
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("lv-%d", i)
		mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	}
	for seq := int64(2); seq <= 3; seq++ {
		prev, _, ok, err := s.GetByKey([]byte("lv-0"), nil)
		if err != nil || !ok {
			t.Fatal(err)
		}
		mustPut(t, s, "lv-0", seq, statusPayload(t, s, "lv-0", "Stop", seq), &prev)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	seg := m.Segments[0]
	if seg.TotalRecords != 5 {
		t.Fatalf("TotalRecords: esperado 5, obtido %d", seg.TotalRecords)
	}
	if seg.LiveRecords != 3 {
		t.Fatalf("LiveRecords: esperado 3 (2 updates viraram garbage), obtido %d", seg.LiveRecords)
	}
	if seg.LiveBytes == 0 || seg.LiveBytes >= uint64(seg.SizeBytes) {
		t.Fatalf("LiveBytes deveria excluir o garbage: %+v", seg)
	}
}

func TestCheckpointConcorrenteComPutSerializa(t *testing.T) {
	// S11-I2: writer-only deixou de ser convenção — o mutex do writer
	// serializa Put×Checkpoint; a violação da convenção não pode corromper
	// (antes: concurrent map read/write no segTally = fatal error).
	s := openStore(t, Options{})
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
			if err := s.Checkpoint(); err != nil {
				t.Errorf("Checkpoint concorrente: %v", err)
				return
			}
		}
	}()
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("ser-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	close(stop)
	wg.Wait()
	if st := s.Stats(); st.RowCount != 200 {
		t.Fatalf("RowCount: %d", st.RowCount)
	}
}
