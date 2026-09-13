package users

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
)

var (
	stateMagic = [4]byte{'N', 'D', 'B', 'U'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

const stateVersion = 1

// errStateAbsent é a primeira subida: não há tabela ainda. Não sai do pacote —
// quem abre a tabela recebe a credencial de bootstrap em vez deste erro.
var errStateAbsent = errors.New("users: user table absent")

// Tamanho mínimo de uma linha, usado como sanidade ANTES de alocar: sem isso
// um count adulterado faria o decodificador pedir a memória do processo antes
// de chegar ao fim do corpo.
const minBytesPerUser = 2 + 2 + 1 + 8 + 2 + 8 + 2 + 8 + (1 + 4 + 4 + 1 + saltBytes + keyBytes)

// Layout do users.state (little-endian):
//
//	0   4  magic "NDBU"
//	4   1  version = 1
//	5   3  reservado
//	8   4  userCount u32
//	       por usuário, em ordem crescente de nome:
//	         usernameLen u16 | username
//	         roleLen u16     | role
//	              1          | flags (bit 0 = bootstrap)
//	              8          | createdAt i64 (unix ms)
//	         createdByLen u16| createdBy
//	              8          | updatedAt i64 (unix ms)
//	         updatedByLen u16| updatedBy
//	              8          | passwordChangedAt i64 (unix ms)
//	              1          | algoritmo do hash
//	              4          | timeCost u32
//	              4          | memoryKiB u32
//	              1          | threads u8
//	             16          | sal
//	             32          | chave derivada
//	    4  crc32c sobre tudo acima
//
// A ordem por nome é canônica: duas tabelas iguais produzem arquivos idênticos
// byte a byte, e o golden test fixa o formato de disco.
func encodeState(users []User) []byte {
	b := make([]byte, 0, 16+len(users)*(minBytesPerUser+32))
	b = append(b, stateMagic[:]...)
	b = append(b, stateVersion, 0, 0, 0)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(users)))
	for _, u := range users {
		b = appendText(b, u.Username)
		b = appendText(b, u.Role)
		var flags byte
		if u.Bootstrap {
			flags |= 1
		}
		b = append(b, flags)
		b = binary.LittleEndian.AppendUint64(b, uint64(u.CreatedAt.UnixMilli()))
		b = appendText(b, u.CreatedBy)
		b = binary.LittleEndian.AppendUint64(b, uint64(u.UpdatedAt.UnixMilli()))
		b = appendText(b, u.UpdatedBy)
		b = binary.LittleEndian.AppendUint64(b, uint64(u.PasswordChangedAt.UnixMilli()))
		b = append(b, u.Hash.Algorithm)
		b = binary.LittleEndian.AppendUint32(b, u.Hash.TimeCost)
		b = binary.LittleEndian.AppendUint32(b, u.Hash.MemoryKiB)
		b = append(b, u.Hash.Threads)
		b = append(b, u.Hash.Salt[:]...)
		b = append(b, u.Hash.Key[:]...)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func appendText(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

// decodeState valida o arquivo inteiro, inclusive as invariantes da tabela:
// nome e papel válidos, sem nome repetido, em ordem, e ao menos um ADMIN. Um
// arquivo que passa no CRC mas viola uma delas não foi escrito por este
// código, e é tratado como corrompido.
func decodeState(raw []byte) ([]User, error) {
	if len(raw) < 16 {
		return nil, fmt.Errorf("%w: truncated header (%d bytes)", ErrCorruptUsers, len(raw))
	}
	body, tail := raw[:len(raw)-4], raw[len(raw)-4:]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(tail) {
		return nil, fmt.Errorf("%w: crc mismatch", ErrCorruptUsers)
	}
	if [4]byte(body[:4]) != stateMagic {
		return nil, fmt.Errorf("%w: bad magic", ErrCorruptUsers)
	}
	if body[4] != stateVersion {
		return nil, fmt.Errorf("%w: unknown version %d", ErrCorruptUsers, body[4])
	}
	r := reader{buf: body, pos: 12}
	count := binary.LittleEndian.Uint32(body[8:12])
	if int(count) > (len(body)-12)/minBytesPerUser {
		return nil, fmt.Errorf("%w: userCount %d impossible for %d bytes", ErrCorruptUsers, count, len(body))
	}
	users := make([]User, 0, count)
	admins := 0
	for i := uint32(0); i < count; i++ {
		var u User
		u.Username = r.text()
		u.Role = r.text()
		flags := r.u8()
		u.Bootstrap = flags&1 != 0
		u.CreatedAt = time.UnixMilli(int64(r.u64()))
		u.CreatedBy = r.text()
		u.UpdatedAt = time.UnixMilli(int64(r.u64()))
		u.UpdatedBy = r.text()
		u.PasswordChangedAt = time.UnixMilli(int64(r.u64()))
		u.Hash.Algorithm = r.u8()
		u.Hash.TimeCost = r.u32()
		u.Hash.MemoryKiB = r.u32()
		u.Hash.Threads = r.u8()
		copy(u.Hash.Salt[:], r.bytes(saltBytes))
		copy(u.Hash.Key[:], r.bytes(keyBytes))
		if r.bad {
			return nil, fmt.Errorf("%w: truncated user record %d", ErrCorruptUsers, i)
		}
		if flags&^1 != 0 {
			return nil, fmt.Errorf("%w: unknown flags 0x%02x in user record %d", ErrCorruptUsers, flags, i)
		}
		if err := ValidateUsername(u.Username); err != nil {
			return nil, fmt.Errorf("%w: user record %d: %w", ErrCorruptUsers, i, err)
		}
		if err := ValidateRole(u.Role); err != nil {
			return nil, fmt.Errorf("%w: user %q: %w", ErrCorruptUsers, u.Username, err)
		}
		if err := u.Hash.validate(); err != nil {
			return nil, fmt.Errorf("%w: user %q: %v", ErrCorruptUsers, u.Username, err)
		}
		if n := len(users); n > 0 && strings.Compare(users[n-1].Username, u.Username) >= 0 {
			return nil, fmt.Errorf("%w: user %q out of order or repeated", ErrCorruptUsers, u.Username)
		}
		if u.Role == config.RoleAdmin {
			admins++
		}
		users = append(users, u)
	}
	if r.pos != len(body) {
		return nil, fmt.Errorf("%w: inconsistent size (pos=%d body=%d)", ErrCorruptUsers, r.pos, len(body))
	}
	if admins == 0 {
		return nil, fmt.Errorf("%w: no ADMIN in the table", ErrCorruptUsers)
	}
	return users, nil
}

// loadState lê o users.state. Ausência devolve errStateAbsent, distinto de
// corrupção: "nunca existiu" é a primeira subida, "existia e não dá para ler"
// é recusa.
func loadState(path string) ([]User, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errStateAbsent
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %w", ErrCorruptUsers, path, err)
	}
	users, err := decodeState(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return slices.Clip(users), nil
}

type reader struct {
	buf []byte
	pos int
	bad bool
}

func (r *reader) take(n int) []byte {
	if r.bad || n < 0 || r.pos+n > len(r.buf) {
		r.bad = true
		return nil
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b
}

func (r *reader) u8() uint8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (r *reader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (r *reader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (r *reader) text() string {
	b := r.take(int(r.u16()))
	return string(b)
}

func (r *reader) u16() uint16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

func (r *reader) bytes(n int) []byte {
	return r.take(n)
}
