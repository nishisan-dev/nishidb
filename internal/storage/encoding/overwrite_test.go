package encoding

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func overwritePayload(t *testing.T) []byte {
	t.Helper()
	var out []byte
	var err error
	for _, f := range []struct {
		id types.FieldID
		v  types.Value
	}{
		{1, types.Value{Type: types.TypeString, Bytes: []byte("abc")}},
		{2, types.Value{Type: types.TypeInt64, I64: 7}},
		{3, types.Value{Type: types.TypeTimestamp, I64: 9}},
	} {
		if out, err = AppendField(out, f.id, f.v); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// O valor reescrito é lido de volta pelo mesmo reader, e os campos vizinhos
// não mudam um byte — a largura fixa é o que dispensa remontar o payload.
func TestOverwriteInt64RewritesOnlyTheCurrentValue(t *testing.T) {
	p := overwritePayload(t)
	want := overwritePayload(t)
	var r FieldReader
	r.Reset(p)
	r.Next()
	if id, _, _ := r.Next(); id != 2 {
		t.Fatalf("campo corrente %d, quero 2", id)
	}
	if err := r.OverwriteInt64(math.MinInt64); err != nil {
		t.Fatal(err)
	}
	r.Reset(p)
	for {
		id, v, ok := r.Next()
		if !ok {
			break
		}
		if id == 2 && v.I64 != math.MinInt64 {
			t.Errorf("campo 2 = %d depois da reescrita", v.I64)
		}
	}
	if len(p) != len(want) || !bytes.Equal(p[:4+3], want[:4+3]) {
		t.Errorf("os bytes antes do campo reescrito mudaram")
	}
	tail := len(want) - 11
	if !bytes.Equal(p[tail:], want[tail:]) {
		t.Errorf("o campo seguinte mudou")
	}
}

// Campo de outro tipo, mesmo de largura 8, é recusado intacto: o timestamp é
// também 8 bytes, e reescrevê-lo como int64 trocaria o significado do dado.
func TestOverwriteInt64RefusesOtherTypesAndLeavesPayloadIntact(t *testing.T) {
	p := overwritePayload(t)
	want := overwritePayload(t)
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
