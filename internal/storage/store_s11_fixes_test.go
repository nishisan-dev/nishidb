package storage

// Testes do fix round consolidado S10+S11: C2 (ordem de durabilidade do
// checkpoint), I1 (liveness real por segmento), I2 (writer serializado).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

	// msyncHash entra ENTRE msyncBitmaps e manifest, e a posição é o que se
	// trava aqui: o §10.2 exige que todo índice esteja durável antes do
	// manifest, porque é o manifest que declara o ponto S como recuperável. Um
	// msync depois dele declararia durável um índice ainda em página suja, e a
	// recovery seguinte confiaria na declaração.
	//
	// O passo aparece mesmo neste dataset, que não tem índice hash algum, e
	// isso é deliberado: um passo emitido só quando há conteúdo tornaria a
	// SEQUÊNCIA dependente da configuração do dataset, e este teste passaria a
	// provar o dataset dele em vez do protocolo.
	want := []string{"segSync", "applyOverlay", "msyncPrimary", "msyncLocations", "msyncSequences", "msyncBitmaps", "msyncHash", "manifest"}
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

func TestErroDeAppendNaRotacaoEhSticky(t *testing.T) {
	// Round 2 N1: o Append embute o fdatasync do segmento selado e o fsync
	// de diretório da rotação — erro ali é de durabilidade e NÃO pode
	// escapar do modo falho (fsyncgate no caminho mais quente). Decisão:
	// TODO erro do Append é sticky.
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 400})
	mustPut(t, s, "rot-0", 0, statusPayload(t, s, "rot-0", "Start", 0), nil)

	// Bloqueia a criação do segmento 2: escrever no segmento corrente
	// continua ok; a primeira ROTAÇÃO falha no Append (openNew → EACCES).
	dataDir := filepath.Join(dir, "data")
	if err := os.Chmod(dataDir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dataDir, 0o755)

	var appendErr error
	for i := 1; i <= 20 && appendErr == nil; i++ {
		k := fmt.Sprintf("rot-%d", i)
		_, _, appendErr = s.Put([]byte(k), int64(i), 0, statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if appendErr == nil {
		t.Fatal("rotação com data/ somente-leitura deveria falhar o Append")
	}
	// Sticky: o writer não aceita mais nada até reopen.
	if err := os.Chmod(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Put([]byte("rot-3"), 3, 0, statusPayload(t, s, "rot-3", "Start", 3), nil); err == nil ||
		!strings.Contains(err.Error(), "modo falho") {
		t.Fatalf("Put pós-falha de Append deveria ser sticky, obtido %v", err)
	}
	if err := s.Checkpoint(); err == nil || !strings.Contains(err.Error(), "modo falho") {
		t.Fatalf("Checkpoint pós-falha de Append: %v", err)
	}
}

func TestErroToleradoNoClearContabilizaEPreservaCommit(t *testing.T) {
	// Round 2 N4: erro na fase de Clear (pós-commit) é tolerado — o Put
	// SUCEDE, o estado novo fica visível, o contador registra e a posting
	// antiga sobra como falso positivo (filtrado pela revalidação §44).
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	id, _ := mustPut(t, s, "cl", 1, statusPayload(t, s, "cl", "Start", 1), nil)

	s.SetClearErrHookForTest(func(types.FieldID) error { return errors.New("clear falho simulado") })
	prev, _, ok, err := s.GetByKey([]byte("cl"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	got, created, err := s.Put([]byte("cl"), 2, 0, statusPayload(t, s, "cl", "Stop", 2), &prev)
	if err != nil || created || got != id {
		t.Fatalf("Put com Clear falho deveria suceder: (%d,%v,%v)", got, created, err)
	}
	if n := s.ClearFailuresForTest(); n != 1 {
		t.Fatalf("clearFailures: esperado 1, obtido %d", n)
	}
	rec, _, ok, err := s.GetByKey([]byte("cl"), nil)
	if err != nil || !ok || rec.Sequence != 2 {
		t.Fatalf("estado novo deveria estar visível: (%+v,%v,%v)", rec, ok, err)
	}
	// Falso positivo residual: a posting antiga permanece (Clear falhou)…
	itStart, err := s.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStart); !slices.Equal(rows, []types.RowID{id}) {
		t.Fatalf("posting antiga deveria sobrar como falso positivo: %v", rows)
	}
	// …e a nova existe (Set rodou pré-commit).
	itStop, err := s.LookupBitmap(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStop); !slices.Equal(rows, []types.RowID{id}) {
		t.Fatalf("posting nova: %v", rows)
	}
	// Writer segue utilizável: erro tolerado não é sticky.
	s.SetClearErrHookForTest(nil)
	mustPut(t, s, "cl2", 1, statusPayload(t, s, "cl2", "Start", 1), nil)
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
