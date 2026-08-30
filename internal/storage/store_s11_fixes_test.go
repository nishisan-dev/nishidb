package storage

// Testes do fix round consolidado S10+S11: C2 (ordem de durabilidade do
// checkpoint), I1 (liveness real por segmento), I2 (writer serializado).

import (
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
	s := openStore(t, Options{CamposIndexados: []types.FieldID{fldStatus}})
	var trace []string
	s.SetCheckpointTraceForTest(func(step string) { trace = append(trace, step) })

	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("ord-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", 0), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// O que continua travado é a REGRA do §10.2: todo índice durável antes do
	// manifest, porque é o manifest que declara o ponto S como recuperável. Um
	// msync depois dele declararia durável um índice ainda em página suja, e a
	// recovery seguinte confiaria na declaração.
	//
	// Os dois passos do índice secundário — msyncBitmaps e msyncHash — SAÍRAM da
	// sequência, e a ausência é a propriedade, não uma perda: o índice por
	// segmento não é durabilizado no checkpoint porque o .widx nunca é confiado
	// através de um restart (spec §2.5). Ele é reconstruído a partir dos próprios
	// records, o que elimina a invariante de ordem contra o durableOffset e a
	// classe inteira de janelas de crash do índice secundário. Um passo que
	// voltasse aqui seria o sinal de que essa decisão foi desfeita.
	want := []string{"segSync", "applyOverlay", "msyncPrimary", "msyncLocations", "msyncSequences", "manifest"}
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

// O TestErroToleradoNoClearContabilizaEPreservaCommit (Round 2 N4) saiu daqui, e
// não foi migrado, porque o que ele media deixou de existir: a fase de CLEAR.
//
// Ele afirmava que um erro pós-commit era TOLERADO — o Put sucedia, o contador
// clearFailures registrava, a posting antiga sobrava como falso positivo e a
// nova existia. No índice por segmento não há Clear (spec §5.2), então não há
// erro pós-commit a tolerar nem contador a manter: o passo 8 do applyRecord
// simplesmente não existe.
//
// As duas metades que continuam sendo propriedade do produto sobreviveram, e sem
// encenação nenhuma: que a posting do valor antigo permanece como falso positivo
// e que a do valor novo existe é o que o TestPostingOrfaEhSupersetEOExecutorFiltra
// e o TestUpdateRemoveAPostingDoValorAntigo afirmam com um update comum.

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
