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
//	4   1  version = 1 (sem cauda) ou 2 (com cauda TLV)
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
//	    [cauda TLV, só na v2 — ver abaixo]
//	    crc32c u32 (sobre tudo acima)
//
// A CAUDA TLV é o ponto de extensão do formato, e existe para que uma seção
// nova (a primeira é o conjunto de índices hash) não obrigue a reescrever o
// corpo nem a condenar todo manifest já gravado. Cada registro é
// `tag u16 | len u32 | len bytes`, e a tag carrega a política de
// compatibilidade no bit mais alto:
//
//	tag <  0x8000  IGNORÁVEL: um binário que não a conhece pula os bytes e
//	               segue. Serve para informação puramente descritiva.
//	tag >= 0x8000  CRÍTICA: um binário que não a conhece RECUSA o manifest.
//	               Serve para o que muda o significado do dataset — a seção
//	               de hashes é 0x8001 porque abrir o dataset ignorando-a
//	               faria o índice parar de ser atualizado e reaparecer
//	               INCOMPLETO, que é o mesmo modo de falha que o
//	               AllowBitmapDrop existe para tornar deliberado.
//
// O byte de versão acompanha: um manifest SEM cauda continua saindo como v1,
// byte a byte igual ao que a versão anterior do binário gravava. É isso que
// preserva o rollback — enquanto ninguém declarar índice hash, um binário
// antigo abre o dataset normalmente. A partir da primeira cauda o manifest é
// v2, e o binário antigo recusa com "version desconhecida", que é a resposta
// certa: ele não sabe manter o índice que o dataset tem.
const (
	// manifestV1 é o corpo sem cauda; manifestV2 é o corpo IDÊNTICO mais a
	// cauda TLV. Não há v2 sem cauda: emitir a versão nova com cauda vazia
	// quebraria o rollback sem nada em troca.
	manifestV1 = 1
	manifestV2 = 2

	// tagCritica é o bit que separa as duas políticas acima.
	tagCritica = 0x8000
	// tagHashes é a seção do conjunto de índices hash (§ índice hash físico).
	tagHashes = 0x8001
	// tagHistorico é a seção do MODO do dataset e da marca-d'água de
	// retenção. Ela é crítica pelo pior modo de falha que este formato tem:
	// um binário que a ignorasse abriria um log append-only de eventos como
	// se fosse estado corrente materializado, e leria uma tabela em que cada
	// entidade aparece dezenas de vezes como se cada linha fosse uma entidade.
	// Não é índice incompleto — é a resposta errada, com o CRC batendo.
	tagHistorico = 0x8002

	// tlvHeaderSize é `tag u16 | len u32`.
	tlvHeaderSize = 6
	// hashInfoSize é `fieldID u16 | slotCount u64`, o mesmo tamanho de uma
	// entrada de bitmap.
	hashInfoSize = 10

	// historicoInfoSize é `modo u8 | rowIDBase u64`.
	historicoInfoSize = 9
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

// HashInfo descreve um índice hash secundário no manifest.
//
// SlotCount é ADVISORY, pela mesma razão do PrimarySlotCount: a fonte de
// verdade é o header do arquivo do índice, e um rehash pós-checkpoint deixa o
// manifest para trás — divergência é esperada, não erro. O que o manifest
// carrega de load-bearing aqui é o CONJUNTO de FieldIDs: é ele que permite
// recusar uma abertura que abandone um índice existente ou que declare um
// índice novo sobre um dataset que já tem linhas.
type HashInfo struct {
	FieldID   types.FieldID
	SlotCount uint64
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
	// Hashes é a primeira seção da cauda TLV. Vazia mantém o manifest na v1,
	// byte a byte igual ao que o binário anterior gravava.
	Hashes []HashInfo

	// Modo é a engine do dataset: 0 materializado, 1 histórico. O tipo é
	// uint8 cru e não um enum do storage porque este pacote descreve um
	// FORMATO — importar o vocabulário de quem o consome inverteria a
	// dependência para ganhar um nome.
	Modo uint8
	// RowIDBase é a marca-d'água de retenção do dataset histórico: rowId
	// abaixo dela foi purgado e não é mais visível. Zero no dataset
	// materializado, e é isso que o mantém na v1.
	RowIDBase uint64
}

// temHistorico diz se a seção 0x8002 precisa ser emitida. O dataset
// materializado tem os dois campos zerados e continua gravando um manifest v1
// byte a byte igual ao de antes desta seção existir — o rollback segue
// possível para quem nunca declarou `history`.
func (m *Manifest) temHistorico() bool { return m.Modo != 0 || m.RowIDBase != 0 }

// TamanhoDaCaudaTLV devolve quantos bytes a cauda ocupa neste manifest, e
// ZERO quando não há seção nenhuma — que é o caso em que o manifest continua
// sendo v1.
//
// É exportado porque é metade da fórmula de reserva do encode, e a outra
// metade vive no teste que trava a reserva. Deixar a conta duplicada foi
// exatamente o defeito que deixou a capacidade em 12 quando o layout já
// gravava 36 por segmento.
func TamanhoDaCaudaTLV(m *Manifest) int {
	n := 0
	if len(m.Hashes) > 0 {
		// tag+len do registro, o u16 de contagem e as entradas.
		n += tlvHeaderSize + 2 + len(m.Hashes)*hashInfoSize
	}
	if m.temHistorico() {
		n += tlvHeaderSize + historicoInfoSize
	}
	return n
}

// versao decide o byte 4. Ver o comentário de layout: v1 enquanto não houver
// cauda, para que o rollback continue possível.
func (m *Manifest) versao() uint8 {
	if TamanhoDaCaudaTLV(m) == 0 {
		return manifestV1
	}
	return manifestV2
}

func (m *Manifest) encode() []byte {
	// 36 por segmento, que é o que o laço abaixo grava desde os stats de
	// liveness da S14. A capacidade estava em 12 e ficou para trás: não
	// corrompia nada, mas fazia o append realocar em todo manifest com
	// segmento — desperdício silencioso no caminho do checkpoint.
	b := make([]byte, 0, 96+len(m.Segments)*36+len(m.Bitmaps)*10+TamanhoDaCaudaTLV(m))
	b = append(b, manifestMagic[:]...)
	b = append(b, m.versao(), 0, 0, 0)
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
	b = m.appendCauda(b)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// appendCauda grava os registros TLV. Nada é emitido quando não há seção: um
// registro de comprimento zero já bastaria para levar o manifest à v2 e matar
// o rollback sem entregar informação nenhuma.
func (m *Manifest) appendCauda(b []byte) []byte {
	// Ordem CRESCENTE de tag, e não a ordem em que as seções foram inventadas:
	// o manifest é comparado byte a byte pelos testes de formato, e uma ordem
	// que dependesse do histórico do produto mudaria o arquivo sem que o
	// conteúdo mudasse.
	if len(m.Hashes) > 0 {
		b = binary.LittleEndian.AppendUint16(b, tagHashes)
		b = binary.LittleEndian.AppendUint32(b, uint32(2+len(m.Hashes)*hashInfoSize))
		b = binary.LittleEndian.AppendUint16(b, uint16(len(m.Hashes)))
		for _, h := range m.Hashes {
			b = binary.LittleEndian.AppendUint16(b, uint16(h.FieldID))
			b = binary.LittleEndian.AppendUint64(b, h.SlotCount)
		}
	}
	if m.temHistorico() {
		b = binary.LittleEndian.AppendUint16(b, tagHistorico)
		b = binary.LittleEndian.AppendUint32(b, historicoInfoSize)
		b = append(b, m.Modo)
		b = binary.LittleEndian.AppendUint64(b, m.RowIDBase)
	}
	return b
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
	if raw[4] != manifestV1 && raw[4] != manifestV2 {
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
	if err := decodeCauda(r, m); err != nil {
		return nil, err
	}
	if r.bad || r.pos != len(body) {
		return nil, fmt.Errorf("%w: tamanho inconsistente (pos=%d body=%d)", ErrCorruptManifest, r.pos, len(body))
	}
	return m, nil
}

// decodeCauda percorre os registros TLV até o fim do corpo. Um manifest v1
// simplesmente não entra no laço: o corpo acaba no kafkaCount.
//
// A checagem de comprimento é feita ANTES de consumir o payload de propósito.
// O `take` do reader devolve zeros e marca `bad` quando estoura, o que
// bastaria para reprovar no fim — mas com um len adulterado para perto de
// 4 GiB o laço só descobriria isso depois de tentar avançar o cursor por
// aquele tanto, e um manifest corrompido não pode custar mais que a leitura
// dos bytes que ele de fato tem.
func decodeCauda(r *reader, m *Manifest) error {
	for r.pos < len(r.b) && !r.bad {
		tag := r.u16()
		n := r.u32()
		if r.bad || uint64(r.pos)+uint64(n) > uint64(len(r.b)) {
			return fmt.Errorf("%w: registro TLV de tag %#04x declara %d bytes, além do fim do manifest", ErrCorruptManifest, tag, n)
		}
		payload := r.take(int(n))
		switch {
		case tag == tagHashes:
			if err := decodeHashes(payload, m); err != nil {
				return err
			}
		case tag == tagHistorico:
			if err := decodeHistorico(payload, m); err != nil {
				return err
			}
		case tag&tagCritica != 0:
			// Ignorar uma seção crítica desconhecida é o modo de falha que
			// esta política existe para impedir: o dataset teria um índice
			// que esta abertura não mantém, e ele reapareceria INCOMPLETO.
			return fmt.Errorf("%w: seção crítica %#04x desconhecida por esta versão; o dataset foi escrito por um binário mais novo",
				ErrCorruptManifest, tag)
		}
		// Tag ignorável desconhecida: os bytes já foram pulados acima.
	}
	return nil
}

// decodeHashes lê a seção 0x8001.
func decodeHashes(payload []byte, m *Manifest) error {
	hr := &reader{b: payload}
	n := int(hr.u16())
	if hr.bad || 2+n*hashInfoSize != len(payload) {
		return fmt.Errorf("%w: seção de hashes declara %d entradas em %d bytes", ErrCorruptManifest, n, len(payload))
	}
	m.Hashes = make([]HashInfo, 0, n)
	for i := 0; i < n; i++ {
		m.Hashes = append(m.Hashes, HashInfo{
			FieldID:   types.FieldID(hr.u16()),
			SlotCount: hr.u64(),
		})
	}
	if hr.bad {
		return fmt.Errorf("%w: seção de hashes truncada", ErrCorruptManifest)
	}
	return nil
}

// decodeHistorico lê a seção 0x8002.
func decodeHistorico(payload []byte, m *Manifest) error {
	if len(payload) != historicoInfoSize {
		return fmt.Errorf("%w: seção de histórico tem %d bytes, esperado %d",
			ErrCorruptManifest, len(payload), historicoInfoSize)
	}
	hr := &reader{b: payload}
	m.Modo = hr.take(1)[0]
	m.RowIDBase = hr.u64()
	if hr.bad {
		return fmt.Errorf("%w: seção de histórico truncada", ErrCorruptManifest)
	}
	return nil
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
