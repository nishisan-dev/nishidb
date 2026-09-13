package users

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"reflect"
	"testing"
	"time"
)

func sampleUser(name, role string) User {
	u := User{
		Username:          name,
		Role:              role,
		CreatedAt:         time.UnixMilli(1_700_000_000_000),
		CreatedBy:         "bootstrap",
		UpdatedAt:         time.UnixMilli(1_700_000_001_000),
		UpdatedBy:         "admin",
		PasswordChangedAt: time.UnixMilli(1_700_000_002_000),
		Hash: PasswordHash{
			Algorithm: algorithmArgon2id,
			TimeCost:  2,
			MemoryKiB: 19 * 1024,
			Threads:   1,
		},
	}
	for i := range u.Hash.Salt {
		u.Hash.Salt[i] = byte(i)
	}
	for i := range u.Hash.Key {
		u.Hash.Key[i] = byte(0xA0 + i)
	}
	return u
}

func TestEstadoIdaEVoltaPreservaTodasAsColunas(t *testing.T) {
	admin := sampleUser("admin", "ADMIN")
	admin.Bootstrap = true
	users := []User{admin, sampleUser("leitor", "READ")}
	got, err := decodeState(encodeState(users))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, users) {
		t.Errorf("ida e volta divergiu:\nquer %+v\nobteve %+v", users, got)
	}
}

// O golden é montado À MÃO, campo a campo, a partir do layout documentado em
// state.go — nunca a partir de encodeState. Um golden gerado pela própria
// função que ele fiscaliza passaria com qualquer mudança de formato.
func TestEstadoGoldenDeBytesFixaOFormatoDeDisco(t *testing.T) {
	u := sampleUser("admin", "ADMIN")
	u.Bootstrap = true

	var want []byte
	want = append(want, 'N', 'D', 'B', 'U', 1, 0, 0, 0)
	want = append(want, 1, 0, 0, 0) // userCount
	want = append(want, 5, 0, 'a', 'd', 'm', 'i', 'n')
	want = append(want, 5, 0, 'A', 'D', 'M', 'I', 'N')
	want = append(want, 0x01) // flags: bootstrap
	want = binary.LittleEndian.AppendUint64(want, 1_700_000_000_000)
	want = append(want, 9, 0, 'b', 'o', 'o', 't', 's', 't', 'r', 'a', 'p')
	want = binary.LittleEndian.AppendUint64(want, 1_700_000_001_000)
	want = append(want, 5, 0, 'a', 'd', 'm', 'i', 'n')
	want = binary.LittleEndian.AppendUint64(want, 1_700_000_002_000)
	want = append(want, 1)                      // argon2id
	want = append(want, 2, 0, 0, 0)             // timeCost
	want = append(want, 0x00, 0x4C, 0x00, 0x00) // memoryKiB = 19456
	want = append(want, 1)                      // threads
	for i := 0; i < 16; i++ {
		want = append(want, byte(i))
	}
	for i := 0; i < 32; i++ {
		want = append(want, byte(0xA0+i))
	}
	want = binary.LittleEndian.AppendUint32(want, crc32.Checksum(want, crc32.MakeTable(crc32.Castagnoli)))

	if got := encodeState([]User{u}); !bytes.Equal(got, want) {
		t.Errorf("formato de disco mudou:\nquer   %x\nobteve %x", want, got)
	}
}

func TestEstadoCorrompidoEhRecusado(t *testing.T) {
	good := encodeState([]User{sampleUser("admin", "ADMIN"), sampleUser("leitor", "READ")})
	reseal := func(body []byte) []byte {
		body = append([]byte(nil), body...)
		return binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))
	}
	body := good[:len(good)-4]

	cases := []struct {
		name string
		raw  []byte
	}{
		{"vazio", nil},
		{"crc", append(append([]byte(nil), body...), 0, 0, 0, 0)},
		{"magic", reseal(append([]byte("XXXX"), body[4:]...))},
		{"versao", func() []byte { b := append([]byte(nil), body...); b[4] = 9; return reseal(b) }()},
		{"contagem impossivel", func() []byte {
			b := append([]byte(nil), body...)
			binary.LittleEndian.PutUint32(b[8:12], 1<<30)
			return reseal(b)
		}()},
		{"byte a mais", reseal(append(append([]byte(nil), body...), 0))},
		{"truncado", reseal(body[:len(body)-10])},
		{"sem admin", encodeState([]User{sampleUser("leitor", "READ")})},
		{"fora de ordem", encodeState([]User{sampleUser("zeca", "ADMIN"), sampleUser("ana", "READ")})},
		{"repetido", encodeState([]User{sampleUser("admin", "ADMIN"), sampleUser("admin", "READ")})},
		{"papel invalido", encodeState([]User{sampleUser("admin", "ADMIN"), sampleUser("b", "WRITE")})},
		{"nome invalido", encodeState([]User{sampleUser("Admin", "ADMIN")})},
		{"memoria absurda", func() []byte {
			u := sampleUser("admin", "ADMIN")
			u.Hash.MemoryKiB = 1 << 31
			return encodeState([]User{u})
		}()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeState(c.raw); !errors.Is(err, ErrCorruptUsers) {
				t.Errorf("erro = %v, quer ErrCorruptUsers", err)
			}
		})
	}
}
