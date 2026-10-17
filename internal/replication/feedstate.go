package replication

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

// stateFileName é o arquivo de estado do feed, ao lado dos segmentos.
const stateFileName = "state"

// stateVersion é a versão do formato do arquivo de estado.
const stateVersion = 1

// stateFlagClean é o bit 0 do byte de flags: o feed foi fechado de forma
// limpa depois de gravar tudo.
const stateFlagClean = 1

// storePositionMinBytes é o que uma StorePosition ocupa com nome vazio. Serve
// de sanidade ANTES de alocar: sem ela um count adulterado (com o CRC refeito)
// faria o decoder pedir a memória do processo.
const storePositionMinBytes = 2 + 4 + 8 + 8 + 8

var stateMagic = [4]byte{'N', 'D', 'B', 'T'}

// errCorruptState é o arquivo de estado ilegível: truncado, magic errado,
// versão desconhecida, CRC que não bate ou corpo incoerente. Quem lê trata
// como queda (o feed nasce de novo), nunca como estado parcial.
var errCorruptState = errors.New("replication: corrupt feed state")

// StorePosition é a posição de escrita de UM Store no instante do fechamento
// do feed: o segmento corrente, o offset do fim do último record válido, o
// próximo RowID a alocar e o piso de retenção (RowIDBase).
//
// O piso entra porque a retenção da tabela histórica o avança e aposenta
// segmentos SEM gravar record: segmento, offset e NextRowID ficam idênticos, e
// só o piso revela que linhas já publicadas pelo feed deixaram de existir.
// Zero fora do modo histórico.
type StorePosition struct {
	Name      string
	Segment   uint32
	Offset    int64
	NextRowID uint64
	RowIDBase uint64
}

// Fingerprint é a impressão dos Stores (base e, quando publicada, a histórica)
// gravada no estado do feed. Na subida, depois da recovery dos Stores, ela é
// comparada com a posição recuperada: qualquer diferença quer dizer que
// alguém mexeu no Store fora do feed (comando offline, backup restaurado,
// record que o fsync final não levou ao disco), e a diferença é tratada como
// queda.
//
// A ordem das posições é significativa e a comparação é por igualdade exata.
type Fingerprint struct {
	Stores []StorePosition
}

// feedState é o conteúdo do arquivo `state`: a identidade da geração, o
// cursor produzido, o piso, a posição em bytes, a marca de fechamento limpo e
// a impressão dos Stores.
type feedState struct {
	Epoch                   [16]byte
	Sequence, Floor, EndPos uint64
	Clean                   bool
	Fingerprint             Fingerprint
}

// Layout do arquivo `state` (little-endian):
//
//	0   4  magic "NDBT"
//	4   1  versão = 1
//	5   1  flags (bit 0 = clean)
//	6   2  reservado (zero)
//	8  16  epoch crua
//	24  8  Sequence
//	32  8  Floor
//	40  8  EndPos
//	48  4  storeCount u32
//	       por Store:
//	         nameLen u16 | name | Segment u32 | Offset i64 | NextRowID u64 | RowIDBase u64
//	    4  crc32c sobre tudo acima
//
// O formato é de disco e tem golden test (TestFeedStateGoldenBytes) com o
// literal gerado fora do código.
func encodeFeedState(st feedState) ([]byte, error) {
	size := 48 + 4 + 4
	for _, s := range st.Fingerprint.Stores {
		if len(s.Name) > math.MaxUint16 {
			return nil, errors.New("replication: store name in fingerprint longer than 65535 bytes")
		}
		size += storePositionMinBytes + len(s.Name)
	}
	b := make([]byte, 0, size)
	b = append(b, stateMagic[:]...)
	flags := byte(0)
	if st.Clean {
		flags |= stateFlagClean
	}
	b = append(b, stateVersion, flags, 0, 0)
	b = append(b, st.Epoch[:]...)
	b = binary.LittleEndian.AppendUint64(b, st.Sequence)
	b = binary.LittleEndian.AppendUint64(b, st.Floor)
	b = binary.LittleEndian.AppendUint64(b, st.EndPos)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(st.Fingerprint.Stores)))
	for _, s := range st.Fingerprint.Stores {
		b = binary.LittleEndian.AppendUint16(b, uint16(len(s.Name)))
		b = append(b, s.Name...)
		b = binary.LittleEndian.AppendUint32(b, s.Segment)
		b = binary.LittleEndian.AppendUint64(b, uint64(s.Offset))
		b = binary.LittleEndian.AppendUint64(b, s.NextRowID)
		b = binary.LittleEndian.AppendUint64(b, s.RowIDBase)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli)), nil
}

func decodeFeedState(src []byte) (feedState, error) {
	var st feedState
	if len(src) < 48+4+4 || [4]byte(src[0:4]) != stateMagic {
		return st, errCorruptState
	}
	body, crc := src[:len(src)-4], binary.LittleEndian.Uint32(src[len(src)-4:])
	if crc32.Checksum(body, castagnoli) != crc {
		return st, errCorruptState
	}
	flags := src[5]
	if src[4] != stateVersion || flags&^stateFlagClean != 0 || src[6] != 0 || src[7] != 0 {
		return st, errCorruptState
	}
	st.Clean = flags&stateFlagClean != 0
	copy(st.Epoch[:], src[8:24])
	st.Sequence = binary.LittleEndian.Uint64(src[24:32])
	st.Floor = binary.LittleEndian.Uint64(src[32:40])
	st.EndPos = binary.LittleEndian.Uint64(src[40:48])
	count := int(binary.LittleEndian.Uint32(src[48:52]))
	rest := body[52:]
	if count > len(rest)/storePositionMinBytes {
		return feedState{}, errCorruptState
	}
	if count > 0 {
		st.Fingerprint.Stores = make([]StorePosition, 0, count)
	}
	for i := 0; i < count; i++ {
		if len(rest) < 2 {
			return feedState{}, errCorruptState
		}
		nameLen := int(binary.LittleEndian.Uint16(rest))
		rest = rest[2:]
		if len(rest) < nameLen+4+8+8+8 {
			return feedState{}, errCorruptState
		}
		var sp StorePosition
		sp.Name = string(rest[:nameLen])
		rest = rest[nameLen:]
		sp.Segment = binary.LittleEndian.Uint32(rest)
		sp.Offset = int64(binary.LittleEndian.Uint64(rest[4:]))
		sp.NextRowID = binary.LittleEndian.Uint64(rest[12:])
		sp.RowIDBase = binary.LittleEndian.Uint64(rest[20:])
		rest = rest[28:]
		st.Fingerprint.Stores = append(st.Fingerprint.Stores, sp)
	}
	if len(rest) != 0 {
		return feedState{}, errCorruptState
	}
	return st, nil
}

// Equal diz se duas impressões são idênticas (mesmas posições, na mesma
// ordem).
func (f Fingerprint) Equal(o Fingerprint) bool {
	return slices.Equal(f.Stores, o.Stores)
}

// writeFeedState grava o estado em <dir>/state por atomicfile (tmp, fsync,
// rename, fsync do diretório): quem lê depois de uma queda vê o estado antigo
// inteiro ou o novo inteiro. O diretório precisa existir.
func writeFeedState(dir string, st feedState) error {
	return writeFeedStateHook(dir, st, nil)
}

// writeFeedStateHook é writeFeedState com o seam de falha do atomicfile (hook
// nil em produção): os testes de queda interrompem a gravação entre os passos
// e conferem que o estado antigo sobrevive inteiro.
func writeFeedStateHook(dir string, st feedState, hook atomicfile.Hook) error {
	b, err := encodeFeedState(st)
	if err != nil {
		return err
	}
	return atomicfile.WriteWithHook(filepath.Join(dir, stateFileName), b, 0o644, hook)
}

// readFeedState lê <dir>/state. Arquivo ausente é a primeira subida do feed e
// devolve ok=false sem erro; qualquer outra falha de leitura, ou um conteúdo
// que não passa no CRC, devolve erro (nunca um estado parcial).
func readFeedState(dir string) (st feedState, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if errors.Is(err, os.ErrNotExist) {
		return feedState{}, false, nil
	}
	if err != nil {
		return feedState{}, false, err
	}
	st, err = decodeFeedState(b)
	if err != nil {
		return feedState{}, false, err
	}
	return st, true, nil
}

// FeedDir devolve o diretório do feed em disco de um dataset (segmentos e
// `state`), a partir do DatasetRootDir dele. É a fonte ÚNICA do caminho: a
// abertura do servidor e a leitura offline do CLI partem dela.
func FeedDir(datasetRoot string) string {
	return filepath.Join(datasetRoot, "replication", "feed")
}

// ReadClosedStatus é o retrato do feed fechado, lido do disco sem abri-lo —
// para quem consulta com o servidor parado (`nishidb query --config`). Só lê:
// não toma lock, não religa o log, não apaga órfão.
//
// Devolve ok=true SÓ com o `state` presente e com a marca limpa: é aí que a
// posição gravada (epoch, sequence, piso e posição em bytes) é a que a
// próxima subida retoma. Um `state` sem a marca limpa é o da subida de uma
// vida que caiu ou fechou sem a impressão, e a sequence dele parou no Resume:
// publicá-la como "produced" mentiria, e a próxima subida abre outra geração
// de qualquer jeito. Ausente ou sem marca limpa devolvem ok=false sem erro;
// ilegível devolve o erro.
//
// Com ok=true, DiskBytes e DiskSegments são o tamanho dos arquivos de
// segmento do diretório (cabeçalhos incluídos, como no feed aberto).
// BufferedBytes e os contadores da vida (entradas, lotes, invalidações,
// erros de disco) ficam zerados: não há processo, e quem os lê na
// `_meta.replication` os publica como NULL.
func ReadClosedStatus(dir string) (Status, bool, error) {
	st, found, err := readFeedState(dir)
	if err != nil || !found || !st.Clean {
		return Status{}, false, err
	}
	out := Status{
		Epoch:          epochString(st.Epoch),
		Produced:       st.Sequence,
		AvailableAfter: st.Floor,
		EndPos:         st.EndPos,
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Status{}, false, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := parseSegmentName(e.Name()); !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return Status{}, false, err
		}
		out.DiskBytes += info.Size()
		out.DiskSegments++
	}
	return out, true, nil
}
