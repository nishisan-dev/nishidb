// Package encoding implementa o wire format de campos e records do NishiDB
// (spec §31-33): campos codificados como FIELD_ID (u16 LE) + TYPE (u8) +
// [LENGTH uvarint apenas para tipos variáveis] + VALUE.
//
// Invariantes do payload materializado: campos em ordem crescente de FieldID,
// sem duplicatas e sem TypeNull (NULL = campo ausente). A ordenação é
// responsabilidade de quem monta o payload e é validada pelo Store no Put.
package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrCorrupt indica payload malformado ou truncado.
var ErrCorrupt = errors.New("encoding: payload corrompido")

const (
	varWidth     = -1 // tipo de largura variável (LENGTH uvarint + bytes)
	invalidWidth = -2 // tag desconhecida ou reservada (TypeNull)
)

// fieldWidth devolve a largura fixa em bytes do tipo, varWidth para tipos
// variáveis ou invalidWidth para tags que nunca aparecem em payload.
func fieldWidth(t types.FieldType) int {
	switch t {
	case types.TypeInt64, types.TypeDouble, types.TypeTimestamp,
		types.TypeDateTime, types.TypeTime:
		return 8
	case types.TypeBool:
		return 1
	case types.TypeIPv4:
		return 4
	case types.TypeIPv6:
		return 16
	case types.TypeIPv4Prefix:
		return 5
	case types.TypeIPv6Prefix:
		return 17
	case types.TypeString, types.TypeBytes, types.TypeJSON:
		return varWidth
	default:
		return invalidWidth
	}
}

// AppendField codifica um campo ao final de dst e devolve o slice resultante.
//
// Value inválida (TypeNull, tag desconhecida, largura fixa errada em Bytes,
// bool fora de 0/1) devolve ErrInvalidPayload com dst INTACTO. O erro é
// obrigatório e não um panic porque o materializer converte dados EXTERNOS
// sob política de log-and-skip por evento (§101): um panic não é capturado
// por ela, então um único evento com Value malformada derrubaria o loop de
// ingestão inteiro.
//
// A validação acontece ANTES de qualquer escrita justamente para que um
// caller que ignore o erro não fique com um payload meio codificado.
func AppendField(dst []byte, id types.FieldID, v types.Value) ([]byte, error) {
	w := fieldWidth(v.Type)
	switch {
	case w == invalidWidth:
		return dst, fmt.Errorf("%w: tipo %#x não é codificável (TypeNull e tags desconhecidas nunca entram em payload)",
			ErrInvalidPayload, v.Type)
	case v.Type == types.TypeBool && v.I64 != 0 && v.I64 != 1:
		return dst, fmt.Errorf("%w: bool com valor %d (esperado 0 ou 1)", ErrInvalidPayload, v.I64)
	case w > 0 && w != 8 && v.Type != types.TypeBool && len(v.Bytes) != w:
		return dst, fmt.Errorf("%w: tipo %#x exige %d bytes, recebeu %d", ErrInvalidPayload, v.Type, w, len(v.Bytes))
	}

	dst = binary.LittleEndian.AppendUint16(dst, uint16(id))
	dst = append(dst, byte(v.Type))
	switch {
	case v.Type == types.TypeBool:
		return append(dst, byte(v.I64)), nil
	case w == 8:
		return binary.LittleEndian.AppendUint64(dst, uint64(v.I64)), nil
	case w == varWidth:
		dst = binary.AppendUvarint(dst, uint64(len(v.Bytes)))
		return append(dst, v.Bytes...), nil
	default:
		return append(dst, v.Bytes...), nil
	}
}

// FieldReader é um cursor zero-copy sobre um payload de campos codificados.
// Os Bytes das Values devolvidas são subslices do payload.
type FieldReader struct {
	payload []byte
	pos     int
	// start é o offset do campo devolvido pelo último Next, ou -1 quando não
	// há campo corrente (antes do primeiro Next, no fim ou após erro).
	start int
	err   error
}

// Reset posiciona o reader no início do payload e limpa o estado de erro.
func (r *FieldReader) Reset(payload []byte) {
	r.payload = payload
	r.pos = 0
	r.start = -1
	r.err = nil
}

// RawField devolve os bytes brutos do campo corrente — id, tag, length e
// valor —, subslice do payload válido até a próxima chamada de Next ou
// Reset. Existe para o merge copiar um campo não tocado sem decodificar e
// recodificar. Devolve nil quando não há campo corrente.
func (r *FieldReader) RawField() []byte {
	if r.start < 0 || r.start >= r.pos {
		return nil
	}
	return r.payload[r.start:r.pos:r.pos]
}

// Err devolve o erro de corrupção encontrado, ou nil em fim limpo.
func (r *FieldReader) Err() error { return r.err }

func (r *FieldReader) fail(format string, args ...any) (types.FieldID, types.Value, bool) {
	r.err = fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, args...)...)
	r.start = -1
	return 0, types.Value{}, false
}

// Next devolve o próximo campo. ok=false indica fim do payload ou erro
// (distinguíveis via Err).
func (r *FieldReader) Next() (types.FieldID, types.Value, bool) {
	if r.err != nil || r.pos >= len(r.payload) {
		r.start = -1
		return 0, types.Value{}, false
	}
	r.start = r.pos
	if len(r.payload)-r.pos < 3 {
		return r.fail("header de campo truncado no offset %d", r.pos)
	}
	id := types.FieldID(binary.LittleEndian.Uint16(r.payload[r.pos:]))
	tag := types.FieldType(r.payload[r.pos+2])
	r.pos += 3

	w := fieldWidth(tag)
	if w == invalidWidth {
		return r.fail("tag de tipo inválida %#x no offset %d", tag, r.pos-1)
	}

	length := w
	if w == varWidth {
		l, n := binary.Uvarint(r.payload[r.pos:])
		if n <= 0 {
			return r.fail("uvarint de length inválido no offset %d", r.pos)
		}
		r.pos += n
		if l > uint64(len(r.payload)-r.pos) {
			return r.fail("length %d além do fim do payload no offset %d", l, r.pos)
		}
		length = int(l)
	} else if length > len(r.payload)-r.pos {
		return r.fail("valor de largura fixa truncado no offset %d", r.pos)
	}

	val := types.Value{Type: tag}
	raw := r.payload[r.pos : r.pos+length : r.pos+length]
	r.pos += length

	switch {
	case tag == types.TypeBool:
		if raw[0] > 1 {
			return r.fail("byte de bool inválido %d", raw[0])
		}
		val.I64 = int64(raw[0])
	case w == 8:
		val.I64 = int64(binary.LittleEndian.Uint64(raw))
	default:
		val.Bytes = raw
	}
	return id, val, true
}

// fieldHeaderSize é o cabeçalho de todo campo: FIELD_ID u16 + TYPE u8.
const fieldHeaderSize = 3

// OverwriteInt64 reescreve, NO LUGAR, o valor do campo corrente — o devolvido
// pelo último Next —, que precisa ser int64. O payload sob o reader é
// mutado; nenhum byte muda de posição, porque o int64 tem largura fixa.
//
// Existe para o materializer decidir o prazo sobre o payload JÁ montado e só
// então gravá-lo: o campo entra com um valor provisório, o merge roda, e o
// valor decidido sobre o resultado substitui os 8 bytes sem remontar nada.
//
// Campo corrente ausente ou de outro tipo devolve ErrInvalidPayload com o
// payload intacto.
func (r *FieldReader) OverwriteInt64(v int64) error {
	raw := r.RawField()
	if len(raw) != fieldHeaderSize+8 || types.FieldType(raw[2]) != types.TypeInt64 {
		return fmt.Errorf("%w: current field is not an int64 (%d raw bytes)", ErrInvalidPayload, len(raw))
	}
	binary.LittleEndian.PutUint64(raw[fieldHeaderSize:], uint64(v))
	return nil
}
