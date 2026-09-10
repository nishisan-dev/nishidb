package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
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
			{ID: 1, SizeBytes: 512 << 20, TotalRecords: 700000, LiveRecords: 650000, LiveBytes: 480 << 20},
			{ID: 2, SizeBytes: 512 << 20, TotalRecords: 700001, LiveRecords: 700001, LiveBytes: 512 << 20},
			{ID: 3, SizeBytes: 9283712 + 64, TotalRecords: 12000, LiveRecords: 12000, LiveBytes: 9283712},
		},
		PrimarySlotCount:    1 << 24,
		PrimaryUsed:         4183900,
		FieldDictValidBytes: 1024,
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
		got.FieldDictValidBytes != want.FieldDictValidBytes {
		t.Fatalf("campos escalares diferem:\nwant %+v\ngot  %+v", want, *got)
	}
	if len(got.Segments) != 3 || got.Segments[2] != want.Segments[2] {
		t.Fatalf("segments: %+v", got.Segments)
	}
}

// TestOsDoisCamposReservadosSaoGravadosComoZero trava a decisão da v3 no lugar
// em que ela é observável: os bytes.
//
// hashKind e bitmapCount continuam ocupando as mesmas posições — tirá-los
// obrigaria a reescrever o parser do corpo e a condenar todo manifest já
// gravado —, mas nada mais os preenche. Gravar um valor neles voltaria a
// publicar um fato que o produto não mantém, e é assim que nasce a próxima
// leitura errada.
func TestOsDoisCamposReservadosSaoGravadosComoZero(t *testing.T) {
	m := sampleManifest()
	raw := m.encode()
	// hashKind vem logo depois de primarySlotCount e primaryUsed, que por sua
	// vez vêm depois do último segmento. O offset é calculado, e não literal:
	// um literal deixaria de apontar para o campo no dia em que o registro por
	// segmento crescesse, e o teste passaria a afirmar sobre outro byte.
	off := 56 + len(m.Segments)*36 + 8 + 8
	if raw[off] != 0 {
		t.Errorf("hashKind gravado como %d; a v3 o reserva em zero", raw[off])
	}
	if bm := binary.LittleEndian.Uint16(raw[off+1+8:]); bm != 0 {
		t.Errorf("bitmapCount gravado como %d; a v3 o reserva em zero", bm)
	}
}

func TestManifestVazioRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	want := Manifest{Epoch: 1, CurrentSegment: 1, DurableOffset: 64}
	if err := Write(path, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 1 || len(got.Segments) != 0 {
		t.Fatalf("%+v", *got)
	}
}

// TestManifestReservaACapacidadeDoLayoutQueGrava trava as duas metades da
// mesma conta: quantos bytes o registro por segmento ocupa e se a reserva do
// make cobre isso.
//
// O layout do topo do arquivo declara 36 bytes por segmento, e o encode grava
// esses 36 desde que os stats de liveness da S14 entraram — mas a capacidade
// do make ficou em 12, o valor de antes. Não corrompia nada (append cresce
// sozinho), e é justamente por isso que passou: nenhum teste olhava para a
// reserva, então o desperdício de uma realocação em TODO manifest com
// segmento não tinha como aparecer no vermelho.
//
// A reserva é observável pela capacidade do slice devolvido: append não
// realoca enquanto o que vem cabe, então cap() igual à fórmula é a prova de
// que a reserva cobriu o encode inteiro. Um registro que cresça de novo sem
// que a fórmula acompanhe quebra este teste — que é exatamente o alarme que
// faltou da primeira vez.
func TestManifestReservaACapacidadeDoLayoutQueGrava(t *testing.T) {
	base := Manifest{Epoch: 1, CurrentSegment: 1}
	withoutSegment := len(base.encode())
	withOne := base
	withOne.Segments = []SegmentInfo{
		{ID: 1, SizeBytes: 4096, TotalRecords: 9, LiveRecords: 8, LiveBytes: 3000},
	}
	if bytesPerSegment := len(withOne.encode()) - withoutSegment; bytesPerSegment != 36 {
		t.Errorf("o encode grava %d bytes por segmento; o layout declara 36", bytesPerSegment)
	}

	m := sampleManifest()
	b := m.encode()
	reserved := 96 + len(m.Segments)*36 + TLVTailSize(&m)
	if len(b) > reserved {
		t.Fatalf("o encode produziu %d bytes com reserva de %d: a fórmula não cobre o layout", len(b), reserved)
	}
	if cap(b) != reserved {
		t.Errorf("cap = %d com reserva de %d: o append realocou no caminho do checkpoint", cap(b), reserved)
	}

	// A cauda TLV entrou na fórmula pela MESMA razão que os 36 bytes por
	// segmento: ela é conteúdo novo no encode, e uma reserva que a ignorasse
	// faria o append realocar em todo checkpoint. O termo é TLVTailSize,
	// e não um literal, porque duplicar a conta foi exatamente o que deixou a
	// capacidade em 12 quando o layout já gravava 36.
	withSections := sampleManifest()
	withSections.StoreMode, withSections.RowIDBase = 1, 500
	withSections.Schedule = &ScheduleInfo{FallbackDurationMs: 60_000, BlockShift: 6, Levels: 4}
	bs := withSections.encode()
	reservedWithSections := 96 + len(withSections.Segments)*36 + TLVTailSize(&withSections)
	if len(bs) > reservedWithSections {
		t.Fatalf("com cauda o encode produziu %d bytes com reserva de %d", len(bs), reservedWithSections)
	}
	if cap(bs) != reservedWithSections {
		t.Errorf("cap = %d com reserva de %d: o append realocou com a cauda TLV", cap(bs), reservedWithSections)
	}
	if growth := len(bs) - len(b); growth != TLVTailSize(&withSections)-TLVTailSize(&m) {
		t.Errorf("as seções a mais ocuparam %d bytes; TLVTailSize declara %d",
			growth, TLVTailSize(&withSections)-TLVTailSize(&m))
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
	// A versão é v3 SEMPRE, e não mais condicional à existência de cauda: a
	// seção do índice por segmento é incondicional, e é a versão — não uma tag —
	// que impede um binário anterior de abrir este dataset. Quem trava esse
	// "sempre" é o TestTodoManifestSaiNaV3.
	if raw[4] != manifestV3 {
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
	m := Manifest{Epoch: 1, CurrentSegment: 200}
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
}
