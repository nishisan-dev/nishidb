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

// escritor é a superfície de ESCRITA comum ao dublê e ao real. As asserções
// de compilação garantem assinaturas idênticas; este teste garante o que elas
// não alcançam — o COMPORTAMENTO nas bordas.
type escritor interface {
	Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error)
	GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error)
}

var (
	_ escritor = (*fakestorage.Store)(nil)
	_ escritor = (*storage.Store)(nil)
)

// abreReal abre um Store real e REGISTRA o campo usado nos testes.
//
// O registro é necessário porque o real valida cada fieldID do payload contra
// o próprio dicionário — divergência de comportamento com o dublê, que não tem
// dicionário e por isso não valida (registrada no report da I1).
func abreReal(t *testing.T, campos ...string) *storage.Store {
	t.Helper()
	st, err := storage.Open(storage.Options{Dir: filepath.Join(t.TempDir(), "ds")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for _, nome := range campos {
		if _, err := st.FieldDict().LookupOrCreate([]byte(nome)); err != nil {
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
	campoID := types.FieldID(1)
	payloadDe := func(n int) []byte {
		p, err := encoding.AppendField(nil, campoID,
			types.Value{Type: types.TypeBytes, Bytes: make([]byte, n)})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	pequeno := payloadDe(8)

	casos := []struct {
		nome    string
		key     []byte
		payload []byte
		aceita  bool
		msg     string
	}{
		{"key no teto", make([]byte, encoding.MaxKeySize), pequeno, true, ""},
		{"key acima do teto", make([]byte, encoding.MaxKeySize+1), pequeno, false, "excede o teto"},
		{"record acima do teto", []byte("k"), payloadDe(encoding.MaxRecordSize), false, "excede o teto"},
		{"key vazia", nil, pequeno, false, "key vazia"},
	}

	stores := map[string]escritor{"fake": fakestorage.New(fakestorage.Indexes{}), "real": abreReal(t, "campo")}
	for nome, st := range stores {
		for _, tc := range casos {
			t.Run(nome+"/"+tc.nome, func(t *testing.T) {
				// Key distinta por caso: no caminho aceito, reusar a key faria
				// o segundo Put virar update e mascarar o resultado.
				key := tc.key
				if len(key) > 0 {
					key = append(append([]byte(nil), key...), byte(len(tc.nome)))
					if len(tc.key) == encoding.MaxKeySize {
						key = key[:encoding.MaxKeySize] // mantém EXATAMENTE o teto
					}
				}
				_, _, err := st.Put(key, 1, 0, tc.payload, nil)
				if tc.aceita {
					if err != nil {
						t.Fatalf("%s recusou o que está DENTRO do teto: %v", nome, err)
					}
					return
				}
				if err == nil {
					t.Fatalf("%s aceitou o que está FORA do teto", nome)
				}
				if !errors.Is(err, types.ErrInvalidArgument) && !errors.Is(err, encoding.ErrInvalidPayload) {
					t.Errorf("%s: erro = %v, want ErrInvalidArgument ou ErrInvalidPayload", nome, err)
				}
				if !strings.Contains(err.Error(), tc.msg) {
					t.Errorf("%s: mensagem = %q, want conter %q", nome, err, tc.msg)
				}
			})
		}
	}
}

// O teto do valor INDEXADO também é do formato (values.dict) e vale nos dois:
// o real recusa antes de tornar o record durável, porque um valor grande
// demais num campo indexado impediria o dataset de reabrir.
func TestParidadeDoTetoDeValorIndexado(t *testing.T) {
	grande, err := encoding.AppendField(nil, types.FieldID(1),
		types.Value{Type: types.TypeBytes, Bytes: make([]byte, types.MaxIndexedValueBytes+1)})
	if err != nil {
		t.Fatal(err)
	}

	fake := fakestorage.New(fakestorage.Indexes{Campos: []types.FieldID{1}})
	real, err := storage.Open(storage.Options{
		Dir: filepath.Join(t.TempDir(), "ds"), CamposIndexadosPorNome: []string{"campo"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer real.Close()

	for nome, st := range map[string]escritor{"fake": fake, "real": real} {
		t.Run(nome, func(t *testing.T) {
			_, _, err := st.Put([]byte("k"), 1, 0, grande, nil)
			if err == nil {
				t.Fatalf("%s aceitou valor acima do teto em campo indexado", nome)
			}
			if !strings.Contains(err.Error(), "teto") {
				t.Errorf("%s: mensagem = %q", nome, err)
			}
		})
	}
}
