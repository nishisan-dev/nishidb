package encoding

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func fieldCases() []struct {
	name string
	id   types.FieldID
	val  types.Value
} {
	return []struct {
		name string
		id   types.FieldID
		val  types.Value
	}{
		{"int64_negativo", 1, types.Value{Type: types.TypeInt64, I64: -42}},
		{"int64_max", 2, types.Value{Type: types.TypeInt64, I64: math.MaxInt64}},
		{"int64_min", 3, types.Value{Type: types.TypeInt64, I64: math.MinInt64}},
		{"double", 4, types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(3.14159))}},
		{"double_neg_zero", 5, types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(math.Copysign(0, -1)))}},
		{"bool_true", 6, types.Value{Type: types.TypeBool, I64: 1}},
		{"bool_false", 7, types.Value{Type: types.TypeBool, I64: 0}},
		{"timestamp", 8, types.Value{Type: types.TypeTimestamp, I64: 1765488490000}},
		{"ipv4", 9, types.Value{Type: types.TypeIPv4, Bytes: []byte{177, 1, 2, 3}}},
		{"ipv6", 10, types.Value{Type: types.TypeIPv6, Bytes: bytes.Repeat([]byte{0xAB}, 16)}},
		{"ipv4_prefix", 11, types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{10, 0, 0, 0, 24}}},
		{"ipv6_prefix", 12, types.Value{Type: types.TypeIPv6Prefix, Bytes: append(bytes.Repeat([]byte{0x20}, 16), 64)}},
		{"string", 13, types.Value{Type: types.TypeString, Bytes: []byte("CMP-V0001PPCLP-013")}},
		{"string_vazia", 14, types.Value{Type: types.TypeString, Bytes: nil}},
		{"string_longa", 15, types.Value{Type: types.TypeString, Bytes: bytes.Repeat([]byte{'x'}, 300)}},
		{"bytes", 16, types.Value{Type: types.TypeBytes, Bytes: []byte{0x00, 0xFF, 0x10}}},
		{"json", 17, types.Value{Type: types.TypeJSON, Bytes: []byte(`{"a":[1,2],"b":"c"}`)}},
	}
}

func assertValueEqual(t *testing.T, want, got types.Value) {
	t.Helper()
	if got.Type != want.Type {
		t.Fatalf("tipo: esperado %#x, obtido %#x", want.Type, got.Type)
	}
	switch want.Type {
	case types.TypeInt64, types.TypeDouble, types.TypeBool, types.TypeTimestamp:
		if got.I64 != want.I64 {
			t.Fatalf("I64: esperado %d, obtido %d", want.I64, got.I64)
		}
	default:
		if !bytes.Equal(got.Bytes, want.Bytes) {
			t.Fatalf("Bytes: esperado %v, obtido %v", want.Bytes, got.Bytes)
		}
	}
}

func TestAppendFieldRoundTripPorTipo(t *testing.T) {
	for _, c := range fieldCases() {
		t.Run(c.name, func(t *testing.T) {
			payload := AppendField(nil, c.id, c.val)

			var r FieldReader
			r.Reset(payload)
			id, v, ok := r.Next()
			if !ok {
				t.Fatalf("Next devolveu ok=false; err=%v", r.Err())
			}
			if id != c.id {
				t.Fatalf("fieldID: esperado %d, obtido %d", c.id, id)
			}
			assertValueEqual(t, c.val, v)

			if _, _, ok := r.Next(); ok {
				t.Fatal("esperado fim do payload após um campo")
			}
			if err := r.Err(); err != nil {
				t.Fatalf("fim limpo esperado, obtido err=%v", err)
			}
		})
	}
}

func TestAppendFieldPayloadComTodosOsTipos(t *testing.T) {
	cases := fieldCases()
	var payload []byte
	for _, c := range cases {
		payload = AppendField(payload, c.id, c.val)
	}

	var r FieldReader
	r.Reset(payload)
	for _, c := range cases {
		id, v, ok := r.Next()
		if !ok {
			t.Fatalf("campo %q: ok=false; err=%v", c.name, r.Err())
		}
		if id != c.id {
			t.Fatalf("campo %q: fieldID esperado %d, obtido %d", c.name, c.id, id)
		}
		assertValueEqual(t, c.val, v)
	}
	if _, _, ok := r.Next(); ok {
		t.Fatal("esperado fim do payload")
	}
	if err := r.Err(); err != nil {
		t.Fatalf("fim limpo esperado, obtido err=%v", err)
	}
}

func TestFieldReaderZeroCopy(t *testing.T) {
	payload := AppendField(nil, 1, types.Value{Type: types.TypeString, Bytes: []byte("abc")})

	var r FieldReader
	r.Reset(payload)
	_, v, ok := r.Next()
	if !ok {
		t.Fatalf("ok=false; err=%v", r.Err())
	}
	// Mutação no payload deve refletir em v.Bytes (subslice, não cópia).
	payload[len(payload)-3] = 'X'
	if v.Bytes[0] != 'X' {
		t.Fatal("v.Bytes deveria ser subslice do payload (zero-copy)")
	}
}

func TestFieldReaderPayloadVazio(t *testing.T) {
	var r FieldReader
	r.Reset(nil)
	if _, _, ok := r.Next(); ok {
		t.Fatal("payload vazio deveria devolver ok=false")
	}
	if err := r.Err(); err != nil {
		t.Fatalf("payload vazio é fim limpo; obtido err=%v", err)
	}
}

func TestFieldReaderCorrompido(t *testing.T) {
	str := AppendField(nil, 7, types.Value{Type: types.TypeString, Bytes: []byte("hello")})
	i64 := AppendField(nil, 3, types.Value{Type: types.TypeInt64, I64: 99})

	cases := []struct {
		name    string
		payload []byte
	}{
		{"header_truncado", str[:2]},
		{"valor_fixo_truncado", i64[:len(i64)-1]},
		{"varlen_truncado", str[:len(str)-1]},
		{"tag_invalida", []byte{0x01, 0x00, 0xFF}},
		{"tag_null_reservada", []byte{0x01, 0x00, 0x00}},
		{"uvarint_sem_fim", []byte{0x01, 0x00, byte(types.TypeString), 0x80}},
		{"uvarint_alem_do_fim", []byte{0x01, 0x00, byte(types.TypeString), 0x0A, 'a', 'b'}},
		{"bool_byte_invalido", []byte{0x01, 0x00, byte(types.TypeBool), 0x02}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var r FieldReader
			r.Reset(c.payload)
			for {
				if _, _, ok := r.Next(); !ok {
					break
				}
			}
			if !errors.Is(r.Err(), ErrCorrupt) {
				t.Fatalf("esperado ErrCorrupt, obtido %v", r.Err())
			}
		})
	}
}

func TestAppendFieldPanicaComValorInvalido(t *testing.T) {
	cases := []struct {
		name string
		val  types.Value
	}{
		{"tipo_null", types.Value{Type: types.TypeNull}},
		{"tipo_desconhecido", types.Value{Type: types.FieldType(0xFF)}},
		{"ipv4_largura_errada", types.Value{Type: types.TypeIPv4, Bytes: []byte{1, 2, 3}}},
		{"ipv6_largura_errada", types.Value{Type: types.TypeIPv6, Bytes: []byte{1, 2}}},
		{"prefix_largura_errada", types.Value{Type: types.TypeIPv4Prefix, Bytes: []byte{1, 2, 3, 4}}},
		{"bool_fora_de_0_1", types.Value{Type: types.TypeBool, I64: 2}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("esperado panic com Value inválido")
				}
			}()
			AppendField(nil, 1, c.val)
		})
	}
}

func BenchmarkFieldReader(b *testing.B) {
	var payload []byte
	for _, c := range fieldCases() {
		payload = AppendField(payload, c.id, c.val)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var r FieldReader
	for i := 0; i < b.N; i++ {
		r.Reset(payload)
		for {
			if _, _, ok := r.Next(); !ok {
				break
			}
		}
		if r.Err() != nil {
			b.Fatal(r.Err())
		}
	}
}
