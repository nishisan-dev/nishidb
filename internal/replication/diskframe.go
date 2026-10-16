package replication

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"math"
	"slices"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Formato em disco do feed de replicação (tudo little-endian).
//
// O feed é um log de SEGMENTOS; cada segmento é um cabeçalho fixo de 64 bytes
// seguido de QUADROS, um por lote fechado. O formato é de disco: mudar um byte
// aqui invalida feeds já gravados, e por isso os golden tests
// (TestFrameGoldenBytes, TestSegmentHeaderGoldenBytes) fixam os bytes com um
// literal gerado fora do código.
//
// Cabeçalho de segmento (64 bytes):
//
//	0   4  magic "NDBF"
//	4   2  versão = 1
//	6   2  reservado (zero)
//	8  16  epoch crua (a do Stream é o hex desses 16 bytes)
//	24  8  FirstSequence: From do primeiro quadro do segmento
//	32  8  StartPos: posição acumulada (EndPos do quadro anterior) ao abrir o segmento
//	40 24  zeros
//
// Quadro:
//
//	0   4  magic "NDFR"
//	4   4  bodyLen u32
//	8   4  crc32c do corpo
//	12     corpo:
//	         From u64 | To u64 | At i64 (unix ns) | EndPos u64
//	         reasonLen u8 | reason
//	         count u32
//	         count × entrada:
//	           Sequence u64 | Operation u8 | tableLen u16 | table
//	           At i64 | RowID u64 | RecordSequence i64 | Flags u8
//	           keyLen u32 | key | payloadLen u32 | payload
//
// EndPos é a posição acumulada, em bytes de quadro (cabeçalho incluído, o
// cabeçalho de SEGMENTO não conta), desde o início da geração, do FIM deste
// quadro. É o que dá o atraso em bytes: posição produzida menos a do cursor.
const (
	segmentHeaderSize = 64
	frameHeaderSize   = 12

	segmentVersion = 1

	// frameFixedBody é o corpo de um quadro sem razão e sem entradas:
	// From, To, At, EndPos, reasonLen e count.
	frameFixedBody = 8 + 8 + 8 + 8 + 1 + 4
	// entryFixedBytes é o que uma entrada ocupa com table, key e payload
	// vazios. Serve de sanidade ANTES de alocar o slice de entradas: sem ela,
	// um count adulterado (com o CRC refeito) faria o decoder pedir a memória
	// do processo antes de chegar ao fim do corpo.
	entryFixedBytes = 8 + 1 + 2 + 8 + 8 + 8 + 1 + 4 + 4

	maxFrameReason = math.MaxUint8
	maxFrameTable  = math.MaxUint16

	// noTime codifica o time.Time zero. O zero de time.Time (ano 1) não cabe
	// em UnixNano: o resultado é indefinido e, na prática, uma data qualquer
	// do século XIX que voltaria do disco como instante válido.
	noTime = math.MinInt64
)

var (
	frameMagic   = [4]byte{'N', 'D', 'F', 'R'}
	segmentMagic = [4]byte{'N', 'D', 'B', 'F'}

	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// errCorruptFrame é o único erro do decoder de quadro: truncado, magic errado,
// CRC que não bate ou corpo incoerente são o mesmo fato para quem lê (o
// quadro não é confiável). Quem precisa distinguir "rasgado no fim" de
// "corrompido no meio" decide pela POSIÇÃO do quadro no log, não pelo erro.
var errCorruptFrame = errors.New("replication: corrupt frame")

// errFrameTooLarge é um lote com campo que não cabe no prefixo de tamanho do
// quadro (razão > 255 bytes, tabela > 65.535, key, payload ou corpo >
// MaxUint32).
var errFrameTooLarge = errors.New("replication: frame field too large for its length prefix")

// errCorruptSegment é o cabeçalho de segmento ilegível ou de versão que este
// binário não conhece.
var errCorruptSegment = errors.New("replication: corrupt segment header")

// errBadEpoch é a epoch que não é hex de exatamente 16 bytes.
var errBadEpoch = errors.New("replication: epoch is not 16 hex-encoded bytes")

// frame é um quadro decodificado. Entries, Key, Payload e Reason pertencem ao
// chamador: nada aponta para o buffer de leitura.
//
// O quadro NÃO grava Batch.Epoch (está no cabeçalho do segmento), Batch.bytes
// nem Batch.Messages: batch() os reconstrói na leitura. Messages vale
// To − From + 1, o número de publicações que o intervalo cobriu (cada
// Publish consome uma sequence, mesmo as coalescidas).
type frame struct {
	From, To, EndPos uint64
	At               time.Time
	Reason           string
	Entries          []Entry
}

// batch reconstrói o Batch que o quadro representa, com a epoch do segmento
// em que ele foi lido (hex, como a do Stream). Messages = To − From + 1 e
// bytes é a mesma estimativa de memória que o Stream usa (entryBytes), pois o
// cache é contabilizado em bytes de memória, não de disco.
func (f frame) batch(epoch string) *Batch {
	b := &Batch{
		Epoch: epoch, From: f.From, To: f.To, At: f.At, Reason: f.Reason,
		Messages: int(f.To - f.From + 1), Entries: f.Entries,
	}
	for i := range f.Entries {
		b.bytes += entryBytes(f.Entries[i])
	}
	return b
}

// segmentHeader é a identidade de um segmento: a geração a que pertence, a
// primeira sequence e a posição acumulada em que ele começa.
type segmentHeader struct {
	Epoch                   [16]byte
	FirstSequence, StartPos uint64
}

func encodeTime(t time.Time) int64 {
	if t.IsZero() {
		return noTime
	}
	return t.UnixNano()
}

func decodeTime(n int64) time.Time {
	if n == noTime {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// frameSize devolve o tamanho exato, em bytes, que appendFrame produz para o
// lote. O chamador precisa dele ANTES de codificar: o EndPos gravado no quadro
// é a posição do fim dele, que é a posição de início somada ao tamanho.
func frameSize(b *Batch) int {
	n := frameHeaderSize + frameFixedBody + len(b.Reason)
	for i := range b.Entries {
		e := &b.Entries[i]
		n += entryFixedBytes + len(e.Table) + len(e.Record.Key) + len(e.Record.Payload)
	}
	return n
}

// appendFrame codifica o lote como um quadro, acrescenta a dst e devolve o
// slice resultante (reatribua sempre, como append). endPos é a posição
// acumulada do FIM do quadro, ou seja, início + frameSize(b).
//
// Razão acima de 255 bytes, tabela acima de 65.535, key ou payload acima de
// MaxUint32 ou corpo acima de MaxUint32 não cabem no prefixo de tamanho:
// gravá-los truncaria o prefixo e o quadro seguinte ficaria ilegível. Nesses
// casos devolve errFrameTooLarge e dst intacto, sem gravar nada. (As razões são
// um conjunto fechado e o nome da tabela é validado pelo schema, então só um
// bug ou um registro monstruoso chega aqui; o gravador do feed trata como
// lacuna, nunca como pânico.)
//
// Não usa fmt nem encoding/json: é o caminho por lote do gravador.
func appendFrame(dst []byte, b *Batch, endPos uint64) ([]byte, error) {
	if err := checkFrameFits(b); err != nil {
		return dst, err
	}
	dst = slices.Grow(dst, frameSize(b))
	start := len(dst)
	dst = append(dst, frameMagic[:]...)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0) // bodyLen e crc, preenchidos no fim
	dst = binary.LittleEndian.AppendUint64(dst, b.From)
	dst = binary.LittleEndian.AppendUint64(dst, b.To)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(encodeTime(b.At)))
	dst = binary.LittleEndian.AppendUint64(dst, endPos)
	dst = append(dst, byte(len(b.Reason)))
	dst = append(dst, b.Reason...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b.Entries)))
	for i := range b.Entries {
		e := &b.Entries[i]
		dst = binary.LittleEndian.AppendUint64(dst, e.Sequence)
		dst = append(dst, byte(e.Operation))
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(e.Table)))
		dst = append(dst, e.Table...)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(encodeTime(e.At)))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Record.RowID))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Record.Sequence))
		dst = append(dst, e.Record.Flags)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Record.Key)))
		dst = append(dst, e.Record.Key...)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Record.Payload)))
		dst = append(dst, e.Record.Payload...)
	}
	body := dst[start+frameHeaderSize:]
	binary.LittleEndian.PutUint32(dst[start+4:], uint32(len(body)))
	binary.LittleEndian.PutUint32(dst[start+8:], crc32.Checksum(body, castagnoli))
	return dst, nil
}

// checkFrameFits confere que todo campo cabe no seu prefixo de tamanho. Key e
// payload têm prefixo de 4 bytes, mas o limite do corpo inteiro (também 4
// bytes) já os cobre: um campo acima de MaxUint32 estoura o corpo.
func checkFrameFits(b *Batch) error {
	if len(b.Reason) > maxFrameReason {
		return errFrameTooLarge
	}
	body := int64(frameFixedBody) + int64(len(b.Reason))
	for i := range b.Entries {
		e := &b.Entries[i]
		if len(e.Table) > maxFrameTable {
			return errFrameTooLarge
		}
		body += entryFixedBytes + int64(len(e.Table)) + int64(len(e.Record.Key)) + int64(len(e.Record.Payload))
		if body > math.MaxUint32 {
			return errFrameTooLarge
		}
	}
	return nil
}

// frameReader percorre o corpo de um quadro. Estourar o corpo marca bad em vez
// de entrar em pânico: um quadro truncado com CRC certo é dado ruim, e o
// decoder só precisa checar bad uma vez, no fim de cada trecho.
type frameReader struct {
	b   []byte
	off int
	bad bool
}

func (r *frameReader) take(n int) []byte {
	if r.bad || n < 0 || n > len(r.b)-r.off {
		r.bad = true
		return nil
	}
	s := r.b[r.off : r.off+n]
	r.off += n
	return s
}

func (r *frameReader) u8() uint8 {
	if s := r.take(1); s != nil {
		return s[0]
	}
	return 0
}

func (r *frameReader) u16() uint16 {
	if s := r.take(2); s != nil {
		return binary.LittleEndian.Uint16(s)
	}
	return 0
}

func (r *frameReader) u32() uint32 {
	if s := r.take(4); s != nil {
		return binary.LittleEndian.Uint32(s)
	}
	return 0
}

func (r *frameReader) u64() uint64 {
	if s := r.take(8); s != nil {
		return binary.LittleEndian.Uint64(s)
	}
	return 0
}

// decodeFrame decodifica o quadro no início de src e devolve quantos bytes ele
// ocupa. src pode ter mais quadros depois; só o primeiro é lido.
//
// Key e Payload de TODAS as entradas são copiados para UMA arena alocada por
// quadro, e nunca apontam para src: o leitor reaproveita o buffer de leitura,
// e um alias mudaria debaixo do consumidor. Cada fatia da arena tem a
// capacidade limitada ao próprio tamanho, para que um append numa key não
// escreva no payload vizinho. A arena tem o tamanho do corpo (limite
// superior do que key e payload somam), o que evita uma passada extra só para
// medir.
//
// Qualquer defeito (truncado, magic errado, bodyLen além do resto, CRC que não
// bate, corpo incoerente) devolve errCorruptFrame.
func decodeFrame(src []byte) (f frame, n int, err error) {
	if len(src) < frameHeaderSize || [4]byte(src[0:4]) != frameMagic {
		return frame{}, 0, errCorruptFrame
	}
	bodyLen := int64(binary.LittleEndian.Uint32(src[4:8]))
	if bodyLen < frameFixedBody || bodyLen > int64(len(src)-frameHeaderSize) {
		return frame{}, 0, errCorruptFrame
	}
	body := src[frameHeaderSize : frameHeaderSize+int(bodyLen)]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(src[8:12]) {
		return frame{}, 0, errCorruptFrame
	}

	r := frameReader{b: body}
	f.From = r.u64()
	f.To = r.u64()
	f.At = decodeTime(int64(r.u64()))
	f.EndPos = r.u64()
	f.Reason = string(r.take(int(r.u8())))
	count := r.u32()
	if r.bad || f.From > f.To || uint64(count)*entryFixedBytes > uint64(len(body)-r.off) {
		return frame{}, 0, errCorruptFrame
	}

	arena := make([]byte, 0, len(body))
	grab := func(data []byte) []byte {
		if len(data) == 0 {
			return nil
		}
		o := len(arena)
		arena = append(arena, data...)
		return arena[o : o+len(data) : o+len(data)]
	}
	f.Entries = make([]Entry, count)
	prevTable := ""
	for i := range f.Entries {
		e := &f.Entries[i]
		e.Sequence = r.u64()
		e.Operation = Operation(r.u8())
		table := r.take(int(r.u16()))
		// As entradas de um lote repetem poucas tabelas: reaproveitar a string
		// da anterior (comparação sem alocar) evita uma string por entrada.
		if string(table) != prevTable {
			prevTable = string(table)
		}
		e.Table = prevTable
		e.At = decodeTime(int64(r.u64()))
		e.Record = types.Record{
			RowID:    types.RowID(r.u64()),
			Sequence: int64(r.u64()),
			Flags:    r.u8(),
		}
		e.Record.Key = grab(r.take(int(r.u32())))
		e.Record.Payload = grab(r.take(int(r.u32())))
		if r.bad {
			return frame{}, 0, errCorruptFrame
		}
	}
	if r.off != len(body) {
		return frame{}, 0, errCorruptFrame
	}
	return f, frameHeaderSize + int(bodyLen), nil
}

// appendSegmentHeader acrescenta o cabeçalho de 64 bytes de h a dst.
func appendSegmentHeader(dst []byte, h segmentHeader) []byte {
	dst = slices.Grow(dst, segmentHeaderSize)
	dst = append(dst, segmentMagic[:]...)
	dst = binary.LittleEndian.AppendUint16(dst, segmentVersion)
	dst = append(dst, 0, 0)
	dst = append(dst, h.Epoch[:]...)
	dst = binary.LittleEndian.AppendUint64(dst, h.FirstSequence)
	dst = binary.LittleEndian.AppendUint64(dst, h.StartPos)
	return append(dst, make([]byte, segmentHeaderSize-40)...)
}

// decodeSegmentHeader lê os 64 primeiros bytes de src. Recusa (com
// errCorruptSegment) magic errado, versão desconhecida, bytes reservados ou de
// preenchimento diferentes de zero e FirstSequence zero (as sequences do feed
// começam em 1, e o piso é FirstSequence - 1).
//
// Preenchimento fora de zero é recusado porque uma versão futura que o usasse
// teria bumpado a versão: byte inesperado ali é lixo, não extensão.
func decodeSegmentHeader(src []byte) (segmentHeader, error) {
	if len(src) < segmentHeaderSize || [4]byte(src[0:4]) != segmentMagic {
		return segmentHeader{}, errCorruptSegment
	}
	if binary.LittleEndian.Uint16(src[4:6]) != segmentVersion || src[6] != 0 || src[7] != 0 {
		return segmentHeader{}, errCorruptSegment
	}
	for _, c := range src[40:segmentHeaderSize] {
		if c != 0 {
			return segmentHeader{}, errCorruptSegment
		}
	}
	var h segmentHeader
	copy(h.Epoch[:], src[8:24])
	h.FirstSequence = binary.LittleEndian.Uint64(src[24:32])
	h.StartPos = binary.LittleEndian.Uint64(src[32:40])
	if h.FirstSequence == 0 {
		return segmentHeader{}, errCorruptSegment
	}
	return h, nil
}

// epochBytes converte a epoch do Stream (hex de 16 bytes) nos 16 bytes crus
// que o cabeçalho de segmento guarda.
func epochBytes(s string) ([16]byte, error) {
	var out [16]byte
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != len(out) {
		return out, errBadEpoch
	}
	copy(out[:], raw)
	return out, nil
}

// epochString é o inverso de epochBytes.
func epochString(e [16]byte) string {
	return hex.EncodeToString(e[:])
}
