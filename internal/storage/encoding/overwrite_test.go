package encoding

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// overwritePayload monta o payload de três campos com o valor dado no campo
// 2, o int64. É também o oráculo da reescrita: o payload reescrito tem de ser
// byte a byte o que AppendField monta com o valor novo.
func overwritePayload(t *testing.T, field2 int64) []byte {
	t.Helper()
	var out []byte
	var err error
	for _, f := range []struct {
		id types.FieldID
		v  types.Value
	}{
		{1, types.Value{Type: types.TypeString, Bytes: []byte("abc")}},
		{2, types.Value{Type: types.TypeInt64, I64: field2}},
		{3, types.Value{Type: types.TypeTimestamp, I64: 9}},
	} {
		if out, err = AppendField(out, f.id, f.v); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// A reescrita produz EXATAMENTE o payload que AppendField monta com o valor
// novo: o campo 2 muda e nenhum outro byte. A comparação é do payload inteiro
// porque uma escrita deslocada (no cabeçalho, ou além do valor) corromperia
// a tag ou o vizinho, e uma releitura campo a campo pararia no erro sem ver o
// campo — passando em silêncio.
func TestOverwriteInt64RewritesOnlyTheCurrentValue(t *testing.T) {
	p := overwritePayload(t, 7)
	want := overwritePayload(t, math.MinInt64)
	var r FieldReader
	r.Reset(p)
	r.Next()
	if id, _, _ := r.Next(); id != 2 {
		t.Fatalf("campo corrente %d, quero 2", id)
	}
	if err := r.OverwriteInt64(math.MinInt64); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, want) {
		t.Errorf("payload reescrito = %x, quero %x", p, want)
	}
	r.Reset(p)
	seen := false
	for {
		id, v, ok := r.Next()
		if !ok {
			break
		}
		if id == 2 {
			seen = true
			if v.I64 != math.MinInt64 {
				t.Errorf("campo 2 = %d depois da reescrita", v.I64)
			}
		}
	}
	if err := r.Err(); err != nil {
		t.Errorf("releitura do payload reescrito: %v", err)
	}
	if !seen {
		t.Error("a releitura não alcançou o campo 2")
	}
}

// Campo de outro tipo, mesmo de largura 8, é recusado intacto: o timestamp é
// também 8 bytes, e reescrevê-lo como int64 trocaria o significado do dado.
func TestOverwriteInt64RefusesOtherTypesAndLeavesPayloadIntact(t *testing.T) {
	p := overwritePayload(t, 7)
	want := overwritePayload(t, 7)
	var r FieldReader
	r.Reset(p)
	for _, wantID := range []types.FieldID{1, 3} {
		for {
			id, _, ok := r.Next()
			if !ok {
				t.Fatalf("campo %d não encontrado", wantID)
			}
			if id == wantID {
				break
			}
		}
		if err := r.OverwriteInt64(1); !errors.Is(err, ErrInvalidPayload) {
			t.Errorf("campo %d: err = %v, quero ErrInvalidPayload", wantID, err)
		}
	}
	r.Next() // fim: sem campo corrente
	if err := r.OverwriteInt64(1); !errors.Is(err, ErrInvalidPayload) {
		t.Errorf("sem campo corrente: err = %v, quero ErrInvalidPayload", err)
	}
	if !bytes.Equal(p, want) {
		t.Error("a recusa mexeu no payload")
	}
}
