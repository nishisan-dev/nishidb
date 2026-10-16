package storage

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// Os testes deste arquivo provam o piso de durabilidade do dicionário de
// grupos: o checkpoint grava no manifest (seção 0x0002) o tamanho do
// grupos.dict que ele sabe fsyncado, e a abertura que encontra um prefixo
// válido MENOR reconstrói o contador em vez de adotá-lo com órfãs zeradas.

// floorDataset cria um dataset com três grupos checkpointados, fecha, e
// devolve o caminho do grupos.dict e o piso que o manifest gravou.
func floorDataset(t *testing.T, dir string, def counterOfTest) (dictPath string, floor int64) {
	t.Helper()
	s, rel := storeWithCounters(t, dir, def)
	for i, olt := range []string{"A", "B", "C", "C"} {
		k := keyOfServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, olt, true, rel.agora()), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	dictPath = filepath.Join(dir, "indexes", diretorioOfCounters, def.name, "grupos.dict")
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(dictPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Counters) != 1 || m.Counters[0].DictValidBytes != st.Size() {
		t.Fatalf("o manifest gravou o piso %+v; quer o tamanho fsyncado do grupos.dict, %d", m.Counters, st.Size())
	}
	return dictPath, m.Counters[0].DictValidBytes
}

// TestCounterDictBelowTheDurableFloorIsRebuilt: o dicionário que perdeu bytes
// abaixo do piso — aqui, a última entrada cortada no meio — faz a abertura
// reconstruir o contador, com log nomeado, em vez de truncar a cauda e
// adotá-lo: adotado, o vetor apontaria para o grupo C, que o dicionário já não
// tem, e a dobra zeraria as duas linhas como órfãs, sem erro.
func TestCounterDictBelowTheDurableFloorIsRebuilt(t *testing.T) {
	dir := t.TempDir()
	def := byOLT("aut", true)
	dictPath, floor := floorDataset(t, dir, def)
	if err := os.Truncate(dictPath, floor-1); err != nil {
		t.Fatal(err)
	}

	var record recordOfLog
	opts := optionsWithCounters(dir, def)
	opts.Log = logging.New(&record, logging.FormatText, slog.LevelInfo)
	s := openStore(t, opts)
	if n := s.CountersBuiltForTest(); n != 1 {
		t.Fatalf("a abertura reconstruiu %d contadores; o dicionário abaixo do piso tem de ser reconstruído, não adotado", n)
	}
	if n := counterChamado(t, s, "aut").Stats().Orphans; n != 0 {
		t.Fatalf("%d órfãs: o contador foi adotado com o dicionário abaixo do piso", n)
	}
	exigeTotal(t, s, "aut", "C", 2, 2)
	if out := record.String(); !strings.Contains(out, "counter.dict_below_durable_floor") || !strings.Contains(out, "counter=aut") {
		t.Fatalf("a reconstrução pelo piso tem de sair no log com o nome do evento e do contador; log:\n%s", out)
	}
}

// TestCounterDictAboveTheDurableFloorIsAdopted: bytes ALÉM do piso são o
// normal — appends depois do último checkpoint — e a cauda rasgada além dele é
// truncada como sempre, sem reconstruir.
func TestCounterDictAboveTheDurableFloorIsAdopted(t *testing.T) {
	dir := t.TempDir()
	def := byOLT("aut", true)
	dictPath, floor := floorDataset(t, dir, def)
	f, err := os.OpenFile(dictPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x05, 0x00, 'x'}); err != nil { // entrada rasgada
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ := storeWithCounters(t, dir, def)
	if n := s.CountersBuiltForTest(); n != 0 {
		t.Fatalf("a abertura reconstruiu %d contadores; cauda além do piso não é perda", n)
	}
	exigeTotal(t, s, "aut", "C", 2, 2)
	if st, err := os.Stat(dictPath); err != nil || st.Size() != floor {
		t.Fatalf("a cauda rasgada além do piso tinha de ser truncada no piso %d: %v, %v", floor, st.Size(), err)
	}
}

// TestManifestWithoutTheFloorOpensAsBefore: o manifest de um binário anterior
// não tem a seção 0x0002, e a abertura segue o comportamento de antes — sem
// piso, a cauda é truncada e o contador é adotado. As órfãs que isso deixa são
// a perda silenciosa que o piso existe para pegar; aqui elas provam que, sem
// a seção, nada mudou.
func TestManifestWithoutTheFloorOpensAsBefore(t *testing.T) {
	dir := t.TempDir()
	def := byOLT("aut", true)
	dictPath, floor := floorDataset(t, dir, def)
	manifestPath := filepath.Join(dir, "checkpoint", "manifest.bin")
	m, err := checkpoint.Load(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	m.Counters[0].DictValidBytes = 0
	if err := checkpoint.Write(manifestPath, m); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(dictPath, floor-1); err != nil {
		t.Fatal(err)
	}
	s, _ := storeWithCounters(t, dir, def)
	if n := s.CountersBuiltForTest(); n != 0 {
		t.Fatalf("sem piso no manifest a abertura tinha de adotar como antes; reconstruiu %d", n)
	}
	if n := counterChamado(t, s, "aut").Stats().Orphans; n != 2 {
		t.Fatalf("órfãs = %d; sem piso, as duas linhas do grupo perdido viram órfãs como antes", n)
	}
}
