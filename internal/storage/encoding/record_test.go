package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func buildTestPayload(t *testing.T) ([]byte, uint32) {
	t.Helper()
	var p []byte
	p = AppendField(p, 1, types.Value{Type: types.TypeString, Bytes: []byte("CMP-V0001PPCLP-013")})
	p = AppendField(p, 3, types.Value{Type: types.TypeInt64, I64: 7455409733})
	p = AppendField(p, 9, types.Value{Type: types.TypeIPv4, Bytes: []byte{177, 1, 2, 3}})
	return p, 3
}

func TestBuildRecordParseRecordRoundTrip(t *testing.T) {
	payload, fc := buildTestPayload(t)
	key := []byte("CMP-V0001PPCLP-013")

	rec := BuildRecord(nil, 4183927, 7455409733, 0, key, payload, fc)

	got, err := ParseRecord(rec)
	if err != nil {
		t.Fatalf("ParseRecord: %v", err)
	}
	if got.RowID != 4183927 {
		t.Fatalf("rowId: esperado 4183927, obtido %d", got.RowID)
	}
	if got.Sequence != 7455409733 {
		t.Fatalf("sequence: esperado 7455409733, obtido %d", got.Sequence)
	}
	if got.Flags != 0 {
		t.Fatalf("flags: esperado 0, obtido %d", got.Flags)
	}
	if !bytes.Equal(got.Key, key) {
		t.Fatalf("key: esperado %q, obtido %q", key, got.Key)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatal("payload difere do original")
	}
}

func TestBuildRecordLayoutGolden(t *testing.T) {
	// Fixa o layout em disco: mudanças aqui exigem bump de formatVersion.
	key := []byte("ab")
	payload := AppendField(nil, 2, types.Value{Type: types.TypeBool, I64: 1})
	rec := BuildRecord(nil, 0x1122334455667788, -2, types.FlagTombstone, key, payload, 1)

	if len(rec) != RecordHeaderSize+len(key)+len(payload) {
		t.Fatalf("tamanho: esperado %d, obtido %d", RecordHeaderSize+len(key)+len(payload), len(rec))
	}
	if !bytes.Equal(rec[0:4], []byte("NDBR")) {
		t.Fatalf("magic: %q", rec[0:4])
	}
	if rec[4] != 1 {
		t.Fatalf("formatVersion: esperado 1, obtido %d", rec[4])
	}
	if rec[5] != types.FlagTombstone {
		t.Fatalf("flags: esperado %d, obtido %d", types.FlagTombstone, rec[5])
	}
	if kl := binary.LittleEndian.Uint16(rec[6:8]); kl != 2 {
		t.Fatalf("keyLength: esperado 2, obtido %d", kl)
	}
	if id := binary.LittleEndian.Uint64(rec[8:16]); id != 0x1122334455667788 {
		t.Fatalf("rowId: obtido %#x", id)
	}
	if seq := int64(binary.LittleEndian.Uint64(rec[16:24])); seq != -2 {
		t.Fatalf("sequence: esperado -2, obtido %d", seq)
	}
	if fc := binary.LittleEndian.Uint32(rec[24:28]); fc != 1 {
		t.Fatalf("fieldCount: esperado 1, obtido %d", fc)
	}
	if pl := binary.LittleEndian.Uint32(rec[28:32]); pl != uint32(len(payload)) {
		t.Fatalf("payloadLength: esperado %d, obtido %d", len(payload), pl)
	}
	if !bytes.Equal(rec[RecordHeaderSize:RecordHeaderSize+2], key) {
		t.Fatal("key não está no offset 36")
	}
}

func TestParseHeader(t *testing.T) {
	payload, fc := buildTestPayload(t)
	key := []byte("k1")
	rec := BuildRecord(nil, 77, 100, 0, key, payload, fc)

	h, err := ParseHeader(rec[:RecordHeaderSize])
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if h.RowID != 77 || h.Sequence != 100 || h.Flags != 0 {
		t.Fatalf("header: %+v", h)
	}
	if h.KeyLen != uint16(len(key)) || h.PayloadLen != uint32(len(payload)) || h.FieldCount != fc {
		t.Fatalf("comprimentos: %+v", h)
	}
	if h.TotalSize() != len(rec) {
		t.Fatalf("TotalSize: esperado %d, obtido %d", len(rec), h.TotalSize())
	}
}

func TestParseRecordDetectaCorrupcao(t *testing.T) {
	payload, fc := buildTestPayload(t)
	rec := BuildRecord(nil, 1, 2, 0, []byte("key"), payload, fc)

	flip := func(off int) []byte {
		c := bytes.Clone(rec)
		c[off] ^= 0xFF
		return c
	}
	cases := []struct {
		name string
		buf  []byte
	}{
		{"magic_corrompida", flip(0)},
		{"version_desconhecida", flip(4)},
		{"flags_corrompida", flip(5)},
		{"rowid_corrompido", flip(8)},
		{"sequence_corrompida", flip(16)},
		{"crc_corrompido", flip(32)},
		{"key_corrompida", flip(RecordHeaderSize)},
		{"payload_corrompido", flip(len(rec) - 1)},
		{"truncado_no_header", rec[:RecordHeaderSize-1]},
		{"truncado_no_payload", rec[:len(rec)-1]},
		{"vazio", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := ParseRecord(c.buf); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("esperado ErrCorrupt, obtido %v", err)
			}
		})
	}
}

func TestParseHeaderRejeitaComprimentosInsanos(t *testing.T) {
	payload, fc := buildTestPayload(t)
	rec := BuildRecord(nil, 1, 2, 0, []byte("key"), payload, fc)

	// payloadLength absurdo: ParseHeader aceita (só valida forma), mas o
	// caller usa MaxRecordSize; aqui garantimos que ParseRecord rejeita
	// quando o buffer não contém o total declarado.
	c := bytes.Clone(rec)
	binary.LittleEndian.PutUint32(c[28:32], math.MaxUint32)
	if _, err := ParseRecord(c); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("esperado ErrCorrupt, obtido %v", err)
	}
}

func TestValidatePayload(t *testing.T) {
	ordered, fc := buildTestPayload(t)

	unordered := AppendField(nil, 5, types.Value{Type: types.TypeInt64, I64: 1})
	unordered = AppendField(unordered, 2, types.Value{Type: types.TypeInt64, I64: 2})

	dup := AppendField(nil, 5, types.Value{Type: types.TypeInt64, I64: 1})
	dup = AppendField(dup, 5, types.Value{Type: types.TypeInt64, I64: 2})

	withNull := append(bytes.Clone(ordered), 0xFF, 0x00, byte(types.TypeNull))

	t.Run("valido", func(t *testing.T) {
		got, err := ValidatePayload(ordered)
		if err != nil || got != fc {
			t.Fatalf("esperado (%d, nil), obtido (%d, %v)", fc, got, err)
		}
	})
	t.Run("vazio_valido", func(t *testing.T) {
		if got, err := ValidatePayload(nil); err != nil || got != 0 {
			t.Fatalf("esperado (0, nil), obtido (%d, %v)", got, err)
		}
	})
	t.Run("fora_de_ordem", func(t *testing.T) {
		if _, err := ValidatePayload(unordered); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("esperado ErrInvalidPayload, obtido %v", err)
		}
	})
	t.Run("duplicata", func(t *testing.T) {
		if _, err := ValidatePayload(dup); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("esperado ErrInvalidPayload, obtido %v", err)
		}
	})
	t.Run("null_reservado", func(t *testing.T) {
		if _, err := ValidatePayload(withNull); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("esperado ErrCorrupt (tag reservada), obtido %v", err)
		}
	})
}

func TestBuildRecordReutilizaBuffer(t *testing.T) {
	payload, fc := buildTestPayload(t)
	buf := make([]byte, 0, 1024)
	rec1 := BuildRecord(buf, 1, 1, 0, []byte("a"), payload, fc)
	rec2 := BuildRecord(rec1[:0], 2, 2, 0, []byte("b"), payload, fc)
	got, err := ParseRecord(rec2)
	if err != nil {
		t.Fatalf("ParseRecord após reuso: %v", err)
	}
	if got.RowID != 2 || !bytes.Equal(got.Key, []byte("b")) {
		t.Fatalf("record reusado incorreto: %+v", got)
	}
}

func BenchmarkBuildRecord(b *testing.B) {
	var payload []byte
	payload = AppendField(payload, 1, types.Value{Type: types.TypeString, Bytes: []byte("CMP-V0001PPCLP-013")})
	payload = AppendField(payload, 3, types.Value{Type: types.TypeInt64, I64: 7455409733})
	key := []byte("CMP-V0001PPCLP-013")
	buf := make([]byte, 0, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = BuildRecord(buf[:0], 1, int64(i), 0, key, payload, 2)
	}
	_ = buf
}
