package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

// goldenStateHex é o `state` de sampleFeedState gerado por um oráculo
// INDEPENDENTE do código (script Python com CRC32C bit a bit). Leitura:
//
//	4e444254              magic "NDBT"
//	01 01 0000            versão 1, flags = clean, reservado
//	101112...1f           epoch crua
//	4d00000000000000      Sequence = 77
//	1400000000000000      Floor = 20
//	0010000000000000      EndPos = 4096
//	02000000              storeCount = 2
//	0600 6576656e7473     "events"
//	03000000              Segment = 3
//	0000100000000000      Offset = 1048576
//	e903000000000000      NextRowID = 1001
//	0000000000000000      RowIDBase = 0
//	0e00 6576656e74735f686973746f7279   "events_history"
//	00000000              Segment = 0
//	4000000000000000      Offset = 64
//	0500000000000000      NextRowID = 5
//	0200000000000000      RowIDBase = 2
//	766dbca5              crc32c de tudo acima
const goldenStateHex = "4e44425401010000101112131415161718191a1b1c1d1e1f4d00000000000000140000000000000000100000000000000200000006006576656e7473030000000000100000000000e90300000000000000000000000000000e006576656e74735f686973746f727900000000400000000000000005000000000000000200000000000000766dbca5"

func sampleFeedState() feedState {
	return feedState{
		Epoch:    [16]byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f},
		Sequence: 77,
		Floor:    20,
		EndPos:   4096,
		Clean:    true,
		Fingerprint: Fingerprint{Stores: []StorePosition{
			{Name: "events", Segment: 3, Offset: 1 << 20, NextRowID: 1001},
			{Name: "events_history", Segment: 0, Offset: 64, NextRowID: 5, RowIDBase: 2},
		}},
	}
}

func goldenState(t *testing.T) []byte {
	t.Helper()
	b, err := hex.DecodeString(goldenStateHex)
	if err != nil {
		t.Fatalf("golden inválido: %v", err)
	}
	return b
}

// withCRC refaz o CRC de um corpo mutado: fabrica a incoerência estrutural
// que o CRC, sozinho, não enxerga.
func withCRC(body []byte) []byte {
	return binary.LittleEndian.AppendUint32(append([]byte(nil), body...), crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
}

func TestFeedStateGoldenBytes(t *testing.T) {
	want := goldenState(t)
	got, err := encodeFeedState(sampleFeedState())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("state divergiu do golden:\n got %x\nwant %x", got, want)
	}

	// O arquivo em disco é exatamente o golden: writeFeedState não acrescenta
	// nem transforma nada.
	dir := t.TempDir()
	if err := writeFeedState(dir, sampleFeedState()); err != nil {
		t.Fatal(err)
	}
	onDisk, err := os.ReadFile(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, want) {
		t.Fatalf("arquivo state divergiu do golden:\n got %x\nwant %x", onDisk, want)
	}
	back, ok, err := readFeedState(dir)
	if err != nil || !ok {
		t.Fatalf("readFeedState = ok %v, err %v", ok, err)
	}
	if back.Epoch != sampleFeedState().Epoch || back.Sequence != 77 || back.Floor != 20 || back.EndPos != 4096 || !back.Clean ||
		!back.Fingerprint.Equal(sampleFeedState().Fingerprint) {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestFeedStateRoundTripsNotCleanAndEmptyFingerprint(t *testing.T) {
	dir := t.TempDir()
	st := feedState{Epoch: [16]byte{1}, Sequence: 0, Floor: 0, EndPos: 0, Clean: false}
	if err := writeFeedState(dir, st); err != nil {
		t.Fatal(err)
	}
	back, ok, err := readFeedState(dir)
	if err != nil || !ok {
		t.Fatalf("readFeedState = ok %v, err %v", ok, err)
	}
	if back.Clean || len(back.Fingerprint.Stores) != 0 || back.Epoch != st.Epoch {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestFeedStateAbsentFileIsNotAnError(t *testing.T) {
	st, ok, err := readFeedState(t.TempDir())
	if err != nil || ok {
		t.Fatalf("arquivo ausente: ok = %v, err = %v; esperado false e nil", ok, err)
	}
	if st.Sequence != 0 || st.Clean {
		t.Fatalf("estado de arquivo ausente deveria ser zero, veio %+v", st)
	}
}

func TestFeedStateRejectsBadCRC(t *testing.T) {
	good := goldenState(t)
	// Um byte trocado em CADA posição do arquivo, o do próprio CRC inclusive.
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x01
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "state"), bad, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := readFeedState(dir); !errors.Is(err, errCorruptState) || ok {
			t.Fatalf("byte %d trocado: ok = %v, err = %v; esperado errCorruptState", i, ok, err)
		}
	}
}

func TestFeedStateRejectsMalformedFiles(t *testing.T) {
	good := goldenState(t)
	body := good[:len(good)-4]
	mutateBody := func(f func(b []byte) []byte) []byte { return withCRC(f(append([]byte(nil), body...))) }
	cases := []struct {
		name string
		src  []byte
	}{
		{"empty", nil},
		{"wrong magic with valid crc", mutateBody(func(b []byte) []byte { b[0] = 'X'; return b })},
		{"future version with valid crc", mutateBody(func(b []byte) []byte { b[4] = 2; return b })},
		{"unknown flag bit with valid crc", mutateBody(func(b []byte) []byte { b[5] = 0x03; return b })},
		{"reserved used with valid crc", mutateBody(func(b []byte) []byte { b[6] = 1; return b })},
		{"store count above the bytes present", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[48:], 3); return b })},
		{"store count absurdly large", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[48:], 0xFFFFFFFF); return b })},
		{"store count below the stores present", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[48:], 1); return b })},
		{"name length past the body", mutateBody(func(b []byte) []byte { binary.LittleEndian.PutUint16(b[52:], 0xFFFF); return b })},
		{"trailing byte before the crc", mutateBody(func(b []byte) []byte { return append(b, 0) })},
	}
	for n := 0; n < len(good); n++ {
		cases = append(cases, struct {
			name string
			src  []byte
		}{"truncated to " + strconv.Itoa(n), good[:n]})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeFeedState(c.src); !errors.Is(err, errCorruptState) {
				t.Fatalf("err = %v, esperado errCorruptState", err)
			}
		})
	}
}

func TestFeedStateReadSurfacesIOErrorsInsteadOfReportingAbsent(t *testing.T) {
	dir := t.TempDir()
	// Um diretório no lugar do arquivo: ReadFile falha com erro que NÃO é
	// "não existe", e tratá-lo como ausente reiniciaria o feed por engano.
	if err := os.Mkdir(filepath.Join(dir, "state"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := readFeedState(dir); err == nil || ok {
		t.Fatalf("ok = %v, err = %v; esperado erro de leitura", ok, err)
	}
}

func TestFeedStateWriteRejectsNameThatDoesNotFitItsPrefix(t *testing.T) {
	st := sampleFeedState()
	st.Fingerprint.Stores[0].Name = strings.Repeat("n", 65536)
	if err := writeFeedState(t.TempDir(), st); err == nil {
		t.Fatalf("writeFeedState aceitou nome de 65536 bytes")
	}
}

func TestFeedStateWriteReplacesAtomicallyAndLeavesNoTemporary(t *testing.T) {
	dir := t.TempDir()
	first := sampleFeedState()
	if err := writeFeedState(dir, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Clean = false
	second.Sequence = 99
	if err := writeFeedState(dir, second); err != nil {
		t.Fatal(err)
	}
	back, ok, err := readFeedState(dir)
	if err != nil || !ok || back.Sequence != 99 || back.Clean {
		t.Fatalf("depois do segundo write: %+v ok=%v err=%v", back, ok, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "state" {
		t.Fatalf("o diretório deveria conter só `state`, tem %v", entries)
	}
}

// TestFeedStateCrashBeforeRenameKeepsTheOldStateWhole: a queda em qualquer
// ponto ANTES do rename deixa o estado anterior inteiro e legível (é o que
// "gravado por atomicfile" promete), e depois do rename o novo inteiro.
func TestFeedStateCrashBeforeRenameKeepsTheOldStateWhole(t *testing.T) {
	for _, crashAfter := range []string{atomicfile.StepWrite, atomicfile.StepFsync} {
		t.Run("crash after "+crashAfter, func(t *testing.T) {
			dir := t.TempDir()
			old := sampleFeedState()
			if err := writeFeedState(dir, old); err != nil {
				t.Fatal(err)
			}
			next := old
			next.Clean = false
			next.Sequence = 500
			crash := errors.New("simulated crash")
			err := writeFeedStateHook(dir, next, func(step string) error {
				if step == crashAfter {
					return crash
				}
				return nil
			})
			if !errors.Is(err, crash) {
				t.Fatalf("err = %v, esperado a queda simulada", err)
			}
			back, ok, err := readFeedState(dir)
			if err != nil || !ok || back.Sequence != 77 || !back.Clean {
				t.Fatalf("o estado antigo deveria estar inteiro: %+v ok=%v err=%v", back, ok, err)
			}
		})
	}
	t.Run("crash after rename", func(t *testing.T) {
		dir := t.TempDir()
		next := sampleFeedState()
		next.Sequence = 500
		crash := errors.New("simulated crash")
		err := writeFeedStateHook(dir, next, func(step string) error {
			if step == atomicfile.StepRename {
				return crash
			}
			return nil
		})
		if !errors.Is(err, crash) {
			t.Fatalf("err = %v", err)
		}
		back, ok, err := readFeedState(dir)
		if err != nil || !ok || back.Sequence != 500 {
			t.Fatalf("depois do rename o estado novo deveria estar inteiro: %+v ok=%v err=%v", back, ok, err)
		}
	})
}

func TestFingerprintEqualComparesPositionsInOrder(t *testing.T) {
	a := Fingerprint{Stores: []StorePosition{{Name: "a", Segment: 1, Offset: 2, NextRowID: 3}, {Name: "b"}}}
	same := Fingerprint{Stores: []StorePosition{{Name: "a", Segment: 1, Offset: 2, NextRowID: 3}, {Name: "b"}}}
	if !a.Equal(same) {
		t.Fatalf("impressões idênticas deveriam ser iguais")
	}
	for name, other := range map[string]Fingerprint{
		"different offset":  {Stores: []StorePosition{{Name: "a", Segment: 1, Offset: 9, NextRowID: 3}, {Name: "b"}}},
		"different rowid":   {Stores: []StorePosition{{Name: "a", Segment: 1, Offset: 2, NextRowID: 4}, {Name: "b"}}},
		"different segment": {Stores: []StorePosition{{Name: "a", Segment: 2, Offset: 2, NextRowID: 3}, {Name: "b"}}},
		"different floor":   {Stores: []StorePosition{{Name: "a", Segment: 1, Offset: 2, NextRowID: 3, RowIDBase: 1}, {Name: "b"}}},
		"swapped order":     {Stores: []StorePosition{{Name: "b"}, {Name: "a", Segment: 1, Offset: 2, NextRowID: 3}}},
		"missing store":     {Stores: []StorePosition{{Name: "a", Segment: 1, Offset: 2, NextRowID: 3}}},
		"empty":             {},
	} {
		if a.Equal(other) {
			t.Fatalf("%s: impressões diferentes foram tratadas como iguais", name)
		}
	}
	if !(Fingerprint{}).Equal(Fingerprint{Stores: []StorePosition{}}) {
		t.Fatalf("nil e vazio deveriam ser iguais")
	}
}

// O retrato offline do feed só sai do `state` com a marca limpa: é aí que a
// posição gravada é a que a próxima subida retoma. Sem a marca (a vida que
// caiu) e sem `state` não há retrato; o ilegível é erro. O tamanho em disco
// é a soma dos arquivos de segmento, e só deles.
func TestReadClosedStatusOnlyTrustsACleanState(t *testing.T) {
	dir := t.TempDir()
	if _, ok, err := ReadClosedStatus(dir); ok || err != nil {
		t.Fatalf("sem state: ok=%v err=%v; quero ausente sem erro", ok, err)
	}
	st := sampleFeedState()
	if err := writeFeedState(dir, st); err != nil {
		t.Fatal(err)
	}
	for name, size := range map[string]int{segmentFileName(21): 300, segmentFileName(50): 200, "lixo.tmp": 999} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := ReadClosedStatus(dir)
	if err != nil || !ok {
		t.Fatalf("state limpo: ok=%v err=%v", ok, err)
	}
	want := Status{Epoch: "101112131415161718191a1b1c1d1e1f", Produced: 77, AvailableAfter: 20, EndPos: 4096, DiskBytes: 500, DiskSegments: 2}
	if got != want {
		t.Fatalf("retrato = %+v\nquero    %+v", got, want)
	}
	st.Clean = false
	if err := writeFeedState(dir, st); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadClosedStatus(dir); ok || err != nil {
		t.Fatalf("state sem marca limpa: ok=%v err=%v; quero sem retrato", ok, err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFileName), []byte("NDBT"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadClosedStatus(dir); ok || !errors.Is(err, errCorruptState) {
		t.Fatalf("state ilegível: ok=%v err=%v; quero o erro de estado corrompido", ok, err)
	}
}
