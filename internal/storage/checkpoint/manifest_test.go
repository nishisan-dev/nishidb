package checkpoint

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func sampleManifest() Manifest {
	return Manifest{
		Epoch:           7,
		CreatedAtUnixMs: 1765488490000,
		NextRowID:       4183927,
		RowCount:        4183900,
		CurrentSegment:  3,
		DurableOffset:   9283712,
		Segments: []SegmentInfo{
			{ID: 1, SizeBytes: 512 << 20},
			{ID: 2, SizeBytes: 512 << 20},
			{ID: 3, SizeBytes: 9283712 + 64},
		},
		PrimarySlotCount:    1 << 24,
		PrimaryUsed:         4183900,
		HashKind:            1,
		FieldDictValidBytes: 1024,
		Bitmaps: []BitmapInfo{
			{FieldID: 2, ValuesDictValidBytes: 96},
			{FieldID: 7, ValuesDictValidBytes: 48},
		},
	}
}

func TestManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := sampleManifest()
	if err := Write(path, &want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Epoch != want.Epoch || got.CreatedAtUnixMs != want.CreatedAtUnixMs ||
		got.NextRowID != want.NextRowID || got.RowCount != want.RowCount ||
		got.CurrentSegment != want.CurrentSegment || got.DurableOffset != want.DurableOffset ||
		got.PrimarySlotCount != want.PrimarySlotCount || got.PrimaryUsed != want.PrimaryUsed ||
		got.HashKind != want.HashKind || got.FieldDictValidBytes != want.FieldDictValidBytes {
		t.Fatalf("campos escalares diferem:\nwant %+v\ngot  %+v", want, *got)
	}
	if len(got.Segments) != 3 || got.Segments[2] != want.Segments[2] {
		t.Fatalf("segments: %+v", got.Segments)
	}
	if len(got.Bitmaps) != 2 || got.Bitmaps[0] != want.Bitmaps[0] || got.Bitmaps[1] != want.Bitmaps[1] {
		t.Fatalf("bitmaps: %+v", got.Bitmaps)
	}
}

func TestManifestVazioRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := Manifest{Epoch: 1, CurrentSegment: 1, DurableOffset: 64, HashKind: 1}
	if err := Write(path, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 1 || len(got.Segments) != 0 || len(got.Bitmaps) != 0 {
		t.Fatalf("%+v", *got)
	}
}

func TestManifestGoldenHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	m := sampleManifest()
	if err := Write(path, &m); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[0:4], []byte("NDBM")) {
		t.Fatalf("magic: %q", raw[0:4])
	}
	if raw[4] != 1 {
		t.Fatalf("version: %d", raw[4])
	}
}

func TestManifestAusente(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "manifest.bin"))
	if !errors.Is(err, ErrNoManifest) {
		t.Fatalf("esperado ErrNoManifest, obtido %v", err)
	}
}

func TestManifestCorrompido(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.bin")
	m := sampleManifest()
	if err := Write(path, &m); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{"crc", append(bytes.Clone(raw[:len(raw)-1]), raw[len(raw)-1]^0xFF)},
		{"campo", func() []byte { c := bytes.Clone(raw); c[24] ^= 0xFF; return c }()},
		{"truncado", raw[:len(raw)-5]},
		{"magic", func() []byte { c := bytes.Clone(raw); c[0] = 'X'; return c }()},
		{"vazio", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := filepath.Join(dir, "bad-"+c.name+".bin")
			if err := os.WriteFile(p, c.data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(p); !errors.Is(err, ErrCorruptManifest) {
				t.Fatalf("esperado ErrCorruptManifest, obtido %v", err)
			}
		})
	}
}

func TestWriteEhAtomico(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.bin")
	m1 := sampleManifest()
	if err := Write(path, &m1); err != nil {
		t.Fatal(err)
	}
	m2 := sampleManifest()
	m2.Epoch = 8
	m2.NextRowID = 5000000
	if err := Write(path, &m2); err != nil {
		t.Fatal(err)
	}
	// Sem arquivo temporário sobrando e conteúdo novo íntegro.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "manifest.bin" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("sobras no diretório: %v", names)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 8 || got.NextRowID != 5000000 {
		t.Fatalf("conteúdo antigo sobreviveu: %+v", *got)
	}
}

func TestManifestMuitosSegmentos(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	m := Manifest{Epoch: 1, CurrentSegment: 200, HashKind: 1}
	for i := uint32(1); i <= 200; i++ {
		m.Segments = append(m.Segments, SegmentInfo{ID: i, SizeBytes: int64(i) * 1000})
	}
	if err := Write(path, &m); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Segments) != 200 || got.Segments[199].SizeBytes != 200000 {
		t.Fatalf("segments: len=%d", len(got.Segments))
	}
	_ = types.RowID(0) // mantém o import de types coeso com o restante do pacote
}
