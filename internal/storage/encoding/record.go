package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrInvalidPayload indica violação dos invariantes de payload materializado
// (ordem crescente de FieldID, sem duplicatas).
var ErrInvalidPayload = errors.New("encoding: payload viola invariantes")

// RecordHeaderSize é o tamanho fixo do header de record em bytes.
//
// Layout (little-endian):
//
//	0   4  magic "NDBR"
//	4   1  formatVersion = 1
//	5   1  flags
//	6   2  keyLength (u16)
//	8   8  rowId (u64)
//	16  8  sequence (i64)
//	24  4  fieldCount (u32)
//	28  4  payloadLength (u32)
//	32  4  crc32c sobre bytes [4..32) ++ key ++ payload
//	36  ...key ++ payload
const RecordHeaderSize = 36

// Tetos absolutos do formato de record. São limites do FORMATO (aplicados
// em ParseHeader antes de CRC ou de qualquer alocação do caller), não de
// configuração: o Store pode configurar limites menores, nunca maiores.
const (
	// MaxKeySize é o teto absoluto da serviceKey em bytes.
	MaxKeySize = 1 << 10 // 1 KiB
	// MaxRecordSize é o teto absoluto do record completo (header+key+payload).
	MaxRecordSize = 4 << 20 // 4 MiB
)

const recordFormatVersion = 1

var recordMagic = [4]byte{'N', 'D', 'B', 'R'}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Header é a visão decodificada do header fixo de um record.
type Header struct {
	Flags      uint8
	KeyLen     uint16
	RowID      types.RowID
	Sequence   int64
	FieldCount uint32
	PayloadLen uint32
	CRC        uint32
}

// TotalSize devolve o tamanho total do record (header + key + payload).
func (h Header) TotalSize() int {
	return RecordHeaderSize + int(h.KeyLen) + int(h.PayloadLen)
}

// BuildRecord codifica um record completo ao final de dst.
//
// O fieldCount gravado no header é derivado POR CONSTRUÇÃO de
// ValidatePayload(payload) — nunca aceito de fora: um fieldCount errado
// ficaria durável e protegido por CRC, indetectável para sempre. A mesma
// chamada valida os invariantes do payload (ordem, duplicatas, tags).
// Os tetos absolutos do formato (MaxKeySize/MaxRecordSize) são aplicados
// aqui, espelhando o ParseHeader.
func BuildRecord(dst []byte, rowID types.RowID, seq int64, flags uint8, key, payload []byte) ([]byte, error) {
	fieldCount, err := ValidateRecord(key, payload)
	if err != nil {
		return dst, err
	}
	base := len(dst)
	dst = append(dst, recordMagic[:]...)
	dst = append(dst, recordFormatVersion, flags)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(key)))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(rowID))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(seq))
	dst = binary.LittleEndian.AppendUint32(dst, fieldCount)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	dst = binary.LittleEndian.AppendUint32(dst, 0) // placeholder do CRC
	dst = append(dst, key...)
	dst = append(dst, payload...)

	crc := crc32.Update(0, castagnoli, dst[base+4:base+32])
	crc = crc32.Update(crc, castagnoli, dst[base+RecordHeaderSize:])
	binary.LittleEndian.PutUint32(dst[base+32:base+36], crc)
	return dst, nil
}

// ValidateRecord aplica as checagens de FORMATO de um record — invariantes
// do payload (ordem, duplicatas, tags), teto de key e teto de tamanho total
// — e devolve o fieldCount derivado.
//
// Existe separada do BuildRecord para que o caller possa rejeitar um record
// ANTES de assumir custos irreversíveis. No Store, reservar um rowId é
// irreversível por construção (ids nunca são reutilizados, §14 risco 9):
// validar depois de reservar queima um id por evento inválido, e com fonte
// ruidosa isso infla os arrays densos indexados por rowId.
func ValidateRecord(key, payload []byte) (uint32, error) {
	fieldCount, err := ValidatePayload(payload)
	if err != nil {
		return 0, err
	}
	if len(key) > MaxKeySize {
		return 0, fmt.Errorf("%w: key de %d bytes excede o teto %d", ErrInvalidPayload, len(key), MaxKeySize)
	}
	if total := RecordHeaderSize + int64(len(key)) + int64(len(payload)); total > MaxRecordSize {
		return 0, fmt.Errorf("%w: record de %d bytes excede o teto %d", ErrInvalidPayload, total, MaxRecordSize)
	}
	return fieldCount, nil
}

// ParseHeader decodifica o header fixo (36 bytes) validando magic, version
// e os tetos absolutos de comprimento (MaxKeySize/MaxRecordSize) — sem
// validar CRC (a validação completa exige key+payload — ParseRecord).
// Callers podem alocar com base nos comprimentos devolvidos: eles nunca
// excedem MaxRecordSize.
func ParseHeader(buf []byte) (Header, error) {
	if len(buf) < RecordHeaderSize {
		return Header{}, fmt.Errorf("%w: header de record truncado (%d bytes)", ErrCorrupt, len(buf))
	}
	if [4]byte(buf[0:4]) != recordMagic {
		return Header{}, fmt.Errorf("%w: magic de record inválida %q", ErrCorrupt, buf[0:4])
	}
	if buf[4] != recordFormatVersion {
		return Header{}, fmt.Errorf("%w: formatVersion desconhecida %d", ErrCorrupt, buf[4])
	}
	h := Header{
		Flags:      buf[5],
		KeyLen:     binary.LittleEndian.Uint16(buf[6:8]),
		RowID:      types.RowID(binary.LittleEndian.Uint64(buf[8:16])),
		Sequence:   int64(binary.LittleEndian.Uint64(buf[16:24])),
		FieldCount: binary.LittleEndian.Uint32(buf[24:28]),
		PayloadLen: binary.LittleEndian.Uint32(buf[28:32]),
		CRC:        binary.LittleEndian.Uint32(buf[32:36]),
	}
	if h.KeyLen > MaxKeySize {
		return Header{}, fmt.Errorf("%w: keyLength %d excede o teto %d", ErrCorrupt, h.KeyLen, MaxKeySize)
	}
	// Aritmética em int64: sem overflow mesmo em GOARCH de 32 bits.
	if total := RecordHeaderSize + int64(h.KeyLen) + int64(h.PayloadLen); total > MaxRecordSize {
		return Header{}, fmt.Errorf("%w: record de %d bytes excede o teto %d", ErrCorrupt, total, MaxRecordSize)
	}
	return h, nil
}

// ParseRecord valida magic, version, comprimentos e CRC32C, devolvendo a
// visão zero-copy do record sobre buf. Bytes excedentes após o total
// declarado são ignorados (buf pode ser uma janela maior do segmento).
func ParseRecord(buf []byte) (types.Record, error) {
	h, err := ParseHeader(buf)
	if err != nil {
		return types.Record{}, err
	}
	total := h.TotalSize()
	if len(buf) < total {
		return types.Record{}, fmt.Errorf("%w: record declara %d bytes, buffer tem %d", ErrCorrupt, total, len(buf))
	}
	crc := crc32.Update(0, castagnoli, buf[4:32])
	crc = crc32.Update(crc, castagnoli, buf[RecordHeaderSize:total])
	if crc != h.CRC {
		return types.Record{}, fmt.Errorf("%w: CRC32C não confere (esperado %#x, calculado %#x)", ErrCorrupt, h.CRC, crc)
	}
	keyEnd := RecordHeaderSize + int(h.KeyLen)
	return types.Record{
		RowID:    h.RowID,
		Sequence: h.Sequence,
		Flags:    h.Flags,
		Key:      buf[RecordHeaderSize:keyEnd:keyEnd],
		Payload:  buf[keyEnd:total:total],
	}, nil
}

// ValidatePayload verifica os invariantes de payload materializado (ordem
// crescente de FieldID, sem duplicatas, sem TypeNull, bem-formado) e devolve
// o número de campos. É o gate usado pelo Store.Put.
func ValidatePayload(payload []byte) (uint32, error) {
	var (
		r     FieldReader
		count uint32
		last  types.FieldID
	)
	r.Reset(payload)
	for {
		id, _, ok := r.Next()
		if !ok {
			if err := r.Err(); err != nil {
				return 0, err
			}
			return count, nil
		}
		if count > 0 && id <= last {
			return 0, fmt.Errorf("%w: fieldID %d após %d (ordem crescente exigida)", ErrInvalidPayload, id, last)
		}
		last = id
		count++
	}
}
