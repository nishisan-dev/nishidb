package storage_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

func sourceOfTest() checkpoint.Source {
	return checkpoint.Source{
		Topic:     "radius-accounting",
		GroupID:   "nishidb-radius",
		Bootstrap: checkpoint.CanonicalBootstrap([]string{"kafka01:9092"}),
	}
}

func offsetsOfTest(o0, o1 int64) checkpoint.SourceOffsets {
	return checkpoint.SourceOffsets{
		Source:  sourceOfTest(),
		Offsets: map[int32]int64{0: o0, 1: o1},
	}
}

func pathOffsets(dir string) string {
	return filepath.Join(dir, "checkpoint", checkpoint.OffsetsFileName)
}

// Dataset sem Kafka não pode ganhar um arquivo de posição: um kafka.offsets
// vazio num dataset alimentado por `nishidb ingest` seria estado inventado.
func TestDatasetSemKafkaNaoCriaOArquivoDeOffsets(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pathOffsets(dir)); !os.IsNotExist(err) {
		t.Errorf("kafka.offsets foi criado num dataset sem fonte Kafka")
	}
	if _, ok, err := st.SourceOffsets(); ok || err != nil {
		t.Errorf("SourceOffsets = (_, %v, %v), quer (_, false, nil)", ok, err)
	}
}

func TestOffsetsSobrevivemAoCheckpointEAoReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	quer := offsetsOfTest(42, 918273)
	if err := st.SetSourceOffsets(quer); err != nil {
		t.Fatal(err)
	}
	if err := st.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// O Open faz um checkpoint IMEDIATO. Se os offsets não fossem semeados
	// antes dele, esse checkpoint os apagaria — e o sintoma seria um consumer
	// que recomeça do zero a cada reinício, sem erro nenhum.
	st2, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	has, ok, err := st2.SourceOffsets()
	if err != nil || !ok {
		t.Fatalf("SourceOffsets após reopen = (_, %v, %v)", ok, err)
	}
	if !reflect.DeepEqual(has, quer) {
		t.Errorf("offsets após reopen:\n tem: %+v\nquer: %+v", has, quer)
	}
}

// O invariante central do M2. São dois arquivos e dois renames sem transação:
// a única proteção é a ORDEM, e é ela que decide entre reprocessar (inócuo) e
// perder para sempre.
func TestOffsetsSaoGravadosDEPOISDoManifest(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SetSourceOffsets(offsetsOfTest(1, 2)); err != nil {
		t.Fatal(err)
	}

	var passos []string
	st.SetCheckpointTraceForTest(func(s string) { passos = append(passos, s) })
	defer st.SetCheckpointTraceForTest(nil)

	if err := st.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	iManifest, iOffsets := -1, -1
	for i, p := range passos {
		switch p {
		case "manifest":
			iManifest = i
		case "sourceOffsets":
			iOffsets = i
		}
	}
	if iManifest < 0 || iOffsets < 0 {
		t.Fatalf("passos = %v, quer conter manifest e sourceOffsets", passos)
	}
	if iOffsets < iManifest {
		t.Errorf("offsets gravados ANTES do manifest (%v): um crash entre os dois deixaria "+
			"a posição adiante dos dados, e os eventos no meio nunca mais seriam lidos", passos)
	}
}

// Crash entre os dois renames: o manifest novo já está no disco e o
// kafka.offsets ainda é o antigo. O resultado tem de ser posição ATRÁS dos
// dados — reprocessamento, não buraco.
func TestCrashEntreManifestEOffsetsDeixaAPosicaoAtras(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	antigo := offsetsOfTest(10, 10)
	if err := st.SetSourceOffsets(antigo); err != nil {
		t.Fatal(err)
	}
	if err := st.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Simula o avanço do consumo seguido de crash: o writer publica a posição
	// nova, mas o processo morre depois do rename do manifest e antes do
	// rename dos offsets. O arquivo em disco continua sendo o antigo.
	if err := st.SetSourceOffsets(offsetsOfTest(200, 200)); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	has, ok, err := st2.SourceOffsets()
	if err != nil || !ok {
		t.Fatalf("SourceOffsets = (_, %v, %v)", ok, err)
	}
	for p, off := range has.Offsets {
		if off > antigo.Offsets[p] {
			t.Errorf("partição %d retomou em %d, adiante do último ponto durável %d", p, off, antigo.Offsets[p])
		}
	}
}

// §51 põe "start queries" ANTES de "seek Kafka offsets". Um kafka.offsets
// ilegível não torna os dados inacessíveis, e derrubar a abertura por causa
// dele derrubaria também as consultas, que não têm nada a ver com o problema.
func TestOffsetsCorrompidosNaoImpedemAAberturaMasSaoReportados(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetSourceOffsets(offsetsOfTest(7, 7)); err != nil {
		t.Fatal(err)
	}
	if err := st.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(pathOffsets(dir))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff // quebra o CRC
	if err := os.WriteFile(pathOffsets(dir), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatalf("a abertura falhou por causa dos offsets: %v", err)
	}
	defer st2.Close()

	_, ok, err := st2.SourceOffsets()
	if ok {
		t.Error("offsets corrompidos foram devolvidos como válidos")
	}
	if !errors.Is(err, checkpoint.ErrCorruptOffsets) {
		t.Errorf("erro = %v, quer ErrCorruptOffsets", err)
	}
	// Consultar tem de continuar funcionando.
	if st2.Stats().RowCount != 0 {
		t.Errorf("Stats inconsistente com offsets corrompidos")
	}
}

// O chamador mantém o mapa vivo entre lotes. Compartilhar a referência faria
// o checkpoint gravar uma posição mais nova que a dos dados que ele acabou de
// tornar duráveis — a inversão que o §48 proíbe.
func TestSetSourceOffsetsCopiaOMapa(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	vivo := offsetsOfTest(5, 5)
	if err := st.SetSourceOffsets(vivo); err != nil {
		t.Fatal(err)
	}
	vivo.Offsets[0] = 999 // o consumer segue avançando

	has, _, err := st.SourceOffsets()
	if err != nil {
		t.Fatal(err)
	}
	if has.Offsets[0] != 5 {
		t.Errorf("offset da partição 0 = %d, quer 5: o Store compartilhou o mapa com o chamador", has.Offsets[0])
	}
}
