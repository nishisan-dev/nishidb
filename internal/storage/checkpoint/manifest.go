// Package checkpoint implementa o manifest do NishiDB (design §9): o
// snapshot consistente que amarra segmentos, índices e dicionários, gravado
// atomicamente ao final de cada checkpoint (§10.2).
package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrNoManifest indica dataset sem manifest (cold start).
var ErrNoManifest = errors.New("checkpoint: manifest ausente")

// ErrCorruptManifest indica manifest ilegível (CRC, magic, truncado).
var ErrCorruptManifest = errors.New("checkpoint: manifest corrompido")

// Layout do manifest.bin (little-endian):
//
//	0   4  magic "NDBM"
//	4   1  version = 1
//	5   3  reserved
//	8   8  epoch (u64)
//	16  8  createdAtUnixMs (i64)
//	24  8  nextRowID (u64)
//	32  8  rowCount (u64)
//	40  4  currentSegment (u32)
//	44  8  durableOffset (u64)
//	52  4  segmentCount (u32)
//	    por segmento: id u32 | sizeBytes i64 | totalRecords u64 |
//	                  liveRecords u64 | liveBytes u64  (36 bytes)
//	    primarySlotCount u64 | primaryUsed u64 | hashKind u8
//	    fieldDictValidBytes i64
//	    bitmapCount u16 | por bitmap: fieldID u16 | valuesDictValidBytes i64
//	    kafkaCount u32 (=0, reservado p/ M3)
//	    crc32c u32 (sobre tudo acima)
const (
	manifestVersion = 1
)

var (
	manifestMagic = [4]byte{'N', 'D', 'B', 'M'}
	castagnoli    = crc32.MakeTable(crc32.Castagnoli)
)

// SegmentInfo descreve um segmento no manifest, com as estatísticas de
// liveness do design §9 rastreadas pelo writer (updates decrementam o
// segmento do record anterior).
//
// Unidades: SizeBytes é o tamanho do ARQUIVO (inclui o header de 64 bytes
// do segmento); TotalRecords conta todos os records já gravados no segmento,
// vivos ou não.
//
// LiveRecords e LiveBytes NÃO são a contagem exata dos records vivos: são um
// LIMITE SUPERIOR dela. Podem ficar ACIMA do real, nunca abaixo. Quem os
// consumir — a compaction do M4 é o consumidor previsto, no minimumLiveRatio
// do §14.16 — precisa lê-los como "no máximo isto está vivo": a assimetria é
// deliberada e é de segurança, porque superestimar liveness apenas ADIA uma
// compaction, enquanto subestimá-la faria a compaction descartar dado vivo.
//
// De onde vem o excesso: do replay do tail no recovery. Quando a row já
// existia no ponto do último checkpoint e o tail traz um update cuja location
// durável já aponta para o record NOVO, o replay não tem como saber qual era
// o estado anterior e não decrementa o record que o update tornou garbage
// (o porquê está em replayRecord, internal/storage/store_recovery.go). Esse
// record órfão fica contado como vivo de forma PERMANENTE: o decremento do
// write path identifica o garbage pela location da row, que já nomeia o
// record do tail, então cada update seguinte desconta o anterior a ELE e
// nunca o órfão. Só uma varredura do segmento — a compaction — reconstrói o
// tally.
//
// O resíduo está medido e travado em TestResiduoDeLivenessDoReplayNaoConverge
// (internal/storage/store_liveness_residuo_test.go), que também prova que ele
// não converge com checkpoints posteriores. Débito 38 em docs/debitos.md.
type SegmentInfo struct {
	ID           uint32
	SizeBytes    int64
	TotalRecords uint64
	LiveRecords  uint64
	LiveBytes    uint64
}

// BitmapInfo descreve um índice bitmap no manifest.
type BitmapInfo struct {
	FieldID              types.FieldID
	ValuesDictValidBytes int64
}

// Manifest é o estado amarrado por um checkpoint.
type Manifest struct {
	Epoch               uint64
	CreatedAtUnixMs     int64
	NextRowID           uint64
	RowCount            uint64
	CurrentSegment      uint32
	DurableOffset       uint64
	Segments            []SegmentInfo
	PrimarySlotCount    uint64
	PrimaryUsed         uint64
	HashKind            uint8
	FieldDictValidBytes int64
	Bitmaps             []BitmapInfo
}

func (m *Manifest) encode() []byte {
	// 36 por segmento, que é o que o laço abaixo grava desde os stats de
	// liveness da S14. A capacidade estava em 12 e ficou para trás: não
	// corrompia nada, mas fazia o append realocar em todo manifest com
	// segmento — desperdício silencioso no caminho do checkpoint.
	b := make([]byte, 0, 96+len(m.Segments)*36+len(m.Bitmaps)*10)
	b = append(b, manifestMagic[:]...)
	b = append(b, manifestVersion, 0, 0, 0)
	b = binary.LittleEndian.AppendUint64(b, m.Epoch)
	b = binary.LittleEndian.AppendUint64(b, uint64(m.CreatedAtUnixMs))
	b = binary.LittleEndian.AppendUint64(b, m.NextRowID)
	b = binary.LittleEndian.AppendUint64(b, m.RowCount)
	b = binary.LittleEndian.AppendUint32(b, m.CurrentSegment)
	b = binary.LittleEndian.AppendUint64(b, m.DurableOffset)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Segments)))
	for _, s := range m.Segments {
		b = binary.LittleEndian.AppendUint32(b, s.ID)
		b = binary.LittleEndian.AppendUint64(b, uint64(s.SizeBytes))
		b = binary.LittleEndian.AppendUint64(b, s.TotalRecords)
		b = binary.LittleEndian.AppendUint64(b, s.LiveRecords)
		b = binary.LittleEndian.AppendUint64(b, s.LiveBytes)
	}
	b = binary.LittleEndian.AppendUint64(b, m.PrimarySlotCount)
	b = binary.LittleEndian.AppendUint64(b, m.PrimaryUsed)
	b = append(b, m.HashKind)
	b = binary.LittleEndian.AppendUint64(b, uint64(m.FieldDictValidBytes))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(m.Bitmaps)))
	for _, bm := range m.Bitmaps {
		b = binary.LittleEndian.AppendUint16(b, uint16(bm.FieldID))
		b = binary.LittleEndian.AppendUint64(b, uint64(bm.ValuesDictValidBytes))
	}
	b = binary.LittleEndian.AppendUint32(b, 0) // kafkaCount (M3)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// reader é um cursor de decodificação com detecção de truncamento.
type reader struct {
	b   []byte
	pos int
	bad bool
}

func (r *reader) take(n int) []byte {
	if r.bad || r.pos+n > len(r.b) {
		r.bad = true
		return make([]byte, n)
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v
}

func (r *reader) u16() uint16 { return binary.LittleEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *reader) u8() uint8   { return r.take(1)[0] }

func decode(raw []byte) (*Manifest, error) {
	if len(raw) < 8 || [4]byte(raw[0:4]) != manifestMagic {
		return nil, fmt.Errorf("%w: magic inválida", ErrCorruptManifest)
	}
	if raw[4] != manifestVersion {
		return nil, fmt.Errorf("%w: version desconhecida %d", ErrCorruptManifest, raw[4])
	}
	if len(raw) < 12 {
		return nil, fmt.Errorf("%w: truncado", ErrCorruptManifest)
	}
	body, tail := raw[:len(raw)-4], raw[len(raw)-4:]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(tail) {
		return nil, fmt.Errorf("%w: CRC32C não confere", ErrCorruptManifest)
	}

	r := &reader{b: body, pos: 8}
	m := &Manifest{}
	m.Epoch = r.u64()
	m.CreatedAtUnixMs = int64(r.u64())
	m.NextRowID = r.u64()
	m.RowCount = r.u64()
	m.CurrentSegment = r.u32()
	m.DurableOffset = r.u64()
	segCount := r.u32()
	// Sanidade contra contagens absurdas antes de alocar (36 B/segmento).
	if int(segCount) > len(body)/36 {
		return nil, fmt.Errorf("%w: segmentCount %d impossível", ErrCorruptManifest, segCount)
	}
	m.Segments = make([]SegmentInfo, 0, segCount)
	for i := uint32(0); i < segCount; i++ {
		m.Segments = append(m.Segments, SegmentInfo{
			ID:           r.u32(),
			SizeBytes:    int64(r.u64()),
			TotalRecords: r.u64(),
			LiveRecords:  r.u64(),
			LiveBytes:    r.u64(),
		})
	}
	m.PrimarySlotCount = r.u64()
	m.PrimaryUsed = r.u64()
	m.HashKind = r.u8()
	m.FieldDictValidBytes = int64(r.u64())
	bmCount := r.u16()
	m.Bitmaps = make([]BitmapInfo, 0, bmCount)
	for i := uint16(0); i < bmCount; i++ {
		m.Bitmaps = append(m.Bitmaps, BitmapInfo{
			FieldID:              types.FieldID(r.u16()),
			ValuesDictValidBytes: int64(r.u64()),
		})
	}
	if kafka := r.u32(); kafka != 0 {
		return nil, fmt.Errorf("%w: seção kafka inesperada (%d)", ErrCorruptManifest, kafka)
	}
	if r.bad || r.pos != len(body) {
		return nil, fmt.Errorf("%w: tamanho inconsistente (pos=%d body=%d)", ErrCorruptManifest, r.pos, len(body))
	}
	return m, nil
}

// writeTrace é o seam de teste da ordem de durabilidade do Write (nil em
// produção).
var writeTrace func(step string)

// step executa a operação e, em sucesso, emite o evento — amarrar o trace à
// execução garante que remover a operação também remove o evento (poder de
// detecção contra a mutação "remove o Sync, mantém o emit").
func step(name string, op func() error) error {
	if err := op(); err != nil {
		return err
	}
	if writeTrace != nil {
		writeTrace(name)
	}
	return nil
}

// Write grava o manifest atomicamente.
func Write(path string, m *Manifest) error {
	return escreveAtomico(path, m.encode())
}

// escreveAtomico grava tmp → fsync → rename → fsync do diretório (a política
// de rename durável do design §9/M18).
//
// É compartilhado pelo manifest.bin e pelo kafka.offsets de propósito: este é
// o único caminho de durabilidade PROVADO do projeto (a ordem está travada
// por TestWriteOrdemDeDurabilidade), e um segundo arquivo com uma segunda
// cópia da sequência seria uma cópia que diverge na primeira correção.
func escreveAtomico(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: criar %s: %w", tmp, err)
	}
	if err := step("write", func() error {
		_, werr := f.Write(data)
		return werr
	}); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: escrever %s: %w", tmp, err)
	}
	if err := step("fsync", f.Sync); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := step("rename", func() error {
		return os.Rename(tmp, path)
	}); err != nil {
		return fmt.Errorf("checkpoint: rename de %s: %w", filepath.Base(path), err)
	}
	return step("dirsync", func() error {
		return syncDir(filepath.Dir(path))
	})
}

// syncDir força a durabilidade da entrada de diretório após um rename.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("checkpoint: fsync do diretório %s: %w", dir, err)
	}
	return nil
}

// Load lê e valida o manifest. Ausência devolve ErrNoManifest (cold start).
func Load(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNoManifest, path)
	}
	if err != nil {
		return nil, err
	}
	return decode(raw)
}
