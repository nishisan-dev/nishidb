package fakestorage_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// writerUnderTest é a superfície de ESCRITA comum ao dublê e ao real. As
// asserções de compilação garantem assinaturas idênticas; este teste garante
// o que elas não alcançam — o COMPORTAMENTO nas bordas.
type writerUnderTest interface {
	Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error)
	GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error)
}

var (
	_ writerUnderTest = (*fakestorage.Store)(nil)
	_ writerUnderTest = (*storage.Store)(nil)
)

// openReal abre um Store real e REGISTRA o campo usado nos testes.
//
// O registro é necessário porque o real valida cada fieldID do payload contra
// o próprio dicionário — divergência de comportamento com o dublê, que não tem
// dicionário e por isso não valida (registrada no report da I1).
func openReal(t *testing.T, fields ...string) *storage.Store {
	t.Helper()
	st, err := storage.Open(storage.Options{Dir: filepath.Join(t.TempDir(), "ds")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for _, name := range fields {
		if _, err := st.FieldDict().LookupOrCreate([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// Os tetos de FORMATO valem igual nos dois. Um dublê que aceitasse o que o
// real recusa deixaria o teste do consumidor passar no desenvolvimento e a
// ingestão falhar em produção — que é o modo de falha mais caro possível,
// porque o sinal chega depois do deploy.
//
// Os dois lados do limite são exercitados de propósito: um dublê que recusasse
// TUDO também "passaria" num teste que só verificasse a recusa.
func TestParidadeDeTetosDeFormato(t *testing.T) {
	fieldID := types.FieldID(1)
	payloadOf := func(n int) []byte {
		p, err := encoding.AppendField(nil, fieldID,
			types.Value{Type: types.TypeBytes, Bytes: make([]byte, n)})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	small := payloadOf(8)

	cases := []struct {
		name     string
		key      []byte
		payload  []byte
		accepted bool
		msg      string
	}{
		{"key no teto", make([]byte, encoding.MaxKeySize), small, true, ""},
		{"key acima do teto", make([]byte, encoding.MaxKeySize+1), small, false, "excede o teto"},
		{"record acima do teto", []byte("k"), payloadOf(encoding.MaxRecordSize), false, "excede o teto"},
		{"key vazia", nil, small, false, "key vazia"},
	}

	stores := map[string]writerUnderTest{"fake": fakestorage.New(fakestorage.Indexes{}), "real": openReal(t, "campo")}
	for name, st := range stores {
		for _, tc := range cases {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				// Key distinta por caso: no caminho aceito, reusar a key faria
				// o segundo Put virar update e mascarar o resultado.
				key := tc.key
				if len(key) > 0 {
					key = append(append([]byte(nil), key...), byte(len(tc.name)))
					if len(tc.key) == encoding.MaxKeySize {
						key = key[:encoding.MaxKeySize] // mantém EXATAMENTE o teto
					}
				}
				_, _, err := st.Put(key, 1, 0, tc.payload, nil)
				if tc.accepted {
					if err != nil {
						t.Fatalf("%s recusou o que está DENTRO do teto: %v", name, err)
					}
					return
				}
				if err == nil {
					t.Fatalf("%s aceitou o que está FORA do teto", name)
				}
				if !errors.Is(err, types.ErrInvalidArgument) && !errors.Is(err, encoding.ErrInvalidPayload) {
					t.Errorf("%s: erro = %v, want ErrInvalidArgument ou ErrInvalidPayload", name, err)
				}
				if !strings.Contains(err.Error(), tc.msg) {
					t.Errorf("%s: mensagem = %q, want conter %q", name, err, tc.msg)
				}
			})
		}
	}
}

// O teto do valor INDEXADO também é do formato (values.dict) e vale nos dois:
// o real recusa antes de tornar o record durável, porque um valor grande
// demais num campo indexado impediria o dataset de reabrir.
func TestParidadeDoTetoDeValorIndexado(t *testing.T) {
	large, err := encoding.AppendField(nil, types.FieldID(1),
		types.Value{Type: types.TypeBytes, Bytes: make([]byte, types.MaxIndexedValueBytes+1)})
	if err != nil {
		t.Fatal(err)
	}

	fake := fakestorage.New(fakestorage.Indexes{Fields: []types.FieldID{1}})
	realStore, err := storage.Open(storage.Options{
		Dir: filepath.Join(t.TempDir(), "ds"), IndexedFieldsByName: []string{"campo"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer realStore.Close()

	for name, st := range map[string]writerUnderTest{"fake": fake, "real": realStore} {
		t.Run(name, func(t *testing.T) {
			_, _, err := st.Put([]byte("k"), 1, 0, large, nil)
			if err == nil {
				t.Fatalf("%s aceitou valor acima do teto em campo indexado", name)
			}
			if !strings.Contains(err.Error(), "teto") {
				t.Errorf("%s: mensagem = %q", name, err)
			}
		})
	}
}
