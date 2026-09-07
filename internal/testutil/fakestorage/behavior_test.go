package fakestorage_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// storeUnderTest é a superfície que o bloco de query consome. O MESMO roteiro
// roda no dublê e no real: asserção de assinatura garante que compilam iguais,
// e só um roteiro compartilhado garante que se COMPORTAM iguais.
type storeUnderTest interface {
	Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error)
	GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error)
	GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error)
	ScanLive(snap types.Snapshot) types.RowIDIterator
	OpenSnapshot() types.Snapshot
	Lookup(field types.FieldID, v types.Value) (types.RowIDIterator, error)
	Indexed(field types.FieldID) bool
}

var (
	_ storeUnderTest = (*fakestorage.Store)(nil)
	_ storeUnderTest = (*storage.Store)(nil)
)

// O roteiro cobre o que o executor observa: identidade de rowId, update sobre
// a mesma key, indexação de postings, liveness e os erros de argumento.
func TestComportamentoDoDubleEDoRealCoincidem(t *testing.T) {
	const (
		statusID       = types.FieldID(1) // indexado
		nameID         = types.FieldID(2) // não indexado
		withoutIndexID = types.FieldID(3)
	)
	str := func(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
	payload := func(status, name string) []byte {
		p, err := encoding.AppendField(nil, statusID, str(status))
		if err != nil {
			t.Fatal(err)
		}
		if p, err = encoding.AppendField(p, nameID, str(name)); err != nil {
			t.Fatal(err)
		}
		return p
	}

	newReal := func(t *testing.T) storeUnderTest {
		st, err := storage.Open(storage.Options{
			Dir: filepath.Join(t.TempDir(), "ds"),
			// A ordem dos nomes define os IDs: status=1, name=2, third=3.
			CamposIndexadosPorNome: []string{"status"},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		for _, n := range []string{"name", "third"} {
			if _, err := st.FieldDict().LookupOrCreate([]byte(n)); err != nil {
				t.Fatal(err)
			}
		}
		return st
	}

	stores := map[string]func(t *testing.T) storeUnderTest{
		"fake": func(*testing.T) storeUnderTest {
			return fakestorage.New(fakestorage.Indexes{Fields: []types.FieldID{statusID}})
		},
		"real": newReal,
	}

	for name, newStore := range stores {
		t.Run(name, func(t *testing.T) {
			st := newStore(t)

			// 1. Primeiro Put cria, e o rowId é 0-based nos dois.
			id, created, err := st.Put([]byte("A"), 1, 0, payload("Start", "ze"), nil)
			if err != nil || !created {
				t.Fatalf("Put: id=%d criado=%v err=%v", id, created, err)
			}
			if id != 0 {
				t.Errorf("primeiro rowId = %d, want 0", id)
			}

			// 2. Put na MESMA key atualiza, sem criar id novo.
			id2, created2, err := st.Put([]byte("A"), 2, 0, payload("Stop", "ze"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if created2 {
				t.Error("update marcou created=true")
			}
			if id2 != id {
				t.Errorf("update mudou o rowId: %d → %d", id, id2)
			}

			// 3. O estado corrente é o do último Put.
			rec, _, ok, err := st.GetByKey(st.OpenSnapshot(), []byte("A"), nil)
			if err != nil || !ok {
				t.Fatalf("GetByKey: ok=%v err=%v", ok, err)
			}
			if !bytes.Equal(rec.Payload, payload("Stop", "ze")) {
				t.Error("GetByKey não devolveu o estado do último Put")
			}
			if rec.Sequence != 2 {
				t.Errorf("Sequence = %d, want 2", rec.Sequence)
			}

			// 4. A indexação: o valor NOVO passa a apontar a row, e o ANTIGO
			// CONTINUA apontando — o índice por segmento não tem fase de CLEAR
			// (spec §5.2), porque a posting vive no índice do segmento em que o
			// record está e morre quando aquele segmento é aposentado.
			//
			// Esta asserção INVERTEU junto com o dublê, e a inversão é a parte
			// que mais importa deste arquivo: um dublê que limpasse a posting
			// antiga ficaria MAIS PRECISO QUE O REAL, e as dezenas de testes que
			// o consomem como fonte de candidatos passariam a afirmar, em verde,
			// uma estreiteza de superconjunto que o produto não tem. O dublê
			// existe para reproduzir o real, inclusive no que o real faz de
			// feio; a divergência só apareceria na integração.
			if ids := collect(t, st, statusID, str("Start")); len(ids) != 1 || ids[0] != id {
				t.Errorf("posting do valor antigo = %v, want [%d]: ela sobrevive como candidato, e o §44 a descarta", ids, id)
			}
			if ids := collect(t, st, statusID, str("Stop")); len(ids) != 1 || ids[0] != id {
				t.Errorf("posting do valor novo = %v, want [%d]", ids, id)
			}

			// 5. Valor nunca visto devolve iterador VAZIO, não erro.
			if ids := collect(t, st, statusID, str("Jamais")); len(ids) != 0 {
				t.Errorf("valor nunca visto = %v", ids)
			}

			// 6. Campo sem índice bitmap é ErrInvalidArgument nos dois.
			if _, err := st.Lookup(nameID, str("ze")); !errors.Is(err, types.ErrInvalidArgument) {
				t.Errorf("Lookup em campo sem índice: %v", err)
			}
			if st.Indexed(nameID) || st.Indexed(withoutIndexID) {
				t.Error("Indexado true para campo sem índice")
			}
			if !st.Indexed(statusID) {
				t.Error("Indexado false para o campo indexado")
			}

			// 7. rowId inexistente: ok=false SEM erro (não é falha, é ausência).
			if _, _, ok, err := st.GetByRowID(st.OpenSnapshot(), types.RowID(9999), nil); ok || err != nil {
				t.Errorf("GetByRowID inexistente: ok=%v err=%v", ok, err)
			}
			// Key inexistente idem.
			if _, _, ok, err := st.GetByKey(st.OpenSnapshot(), []byte("NAO-EXISTE"), nil); ok || err != nil {
				t.Errorf("GetByKey inexistente: ok=%v err=%v", ok, err)
			}

			// 8. Tombstone pelas FLAGS: a row some das leituras, e o posting
			// continua (é o superset que o executor tem de filtrar).
			if _, _, err := st.Put([]byte("A"), 3, types.FlagTombstone, payload("Stop", "ze"), nil); err != nil {
				t.Fatal(err)
			}
			if _, _, ok, _ := st.GetByKey(st.OpenSnapshot(), []byte("A"), nil); ok {
				t.Error("row tombstoned continuou visível no GetByKey")
			}
			if _, _, ok, _ := st.GetByRowID(st.OpenSnapshot(), id, nil); ok {
				t.Error("row tombstoned continuou visível no GetByRowID")
			}
			if ids := scanLive(t, st); len(ids) != 0 {
				t.Errorf("ScanLive devolveu row morta: %v", ids)
			}

			// 9. Key vazia é recusada.
			if _, _, err := st.Put(nil, 1, 0, payload("Start", "x"), nil); !errors.Is(err, types.ErrInvalidArgument) {
				t.Errorf("Put com key vazia: %v", err)
			}
		})
	}
}

// ScanLive devolve rowIds em ordem CRESCENTE nos dois — pré-condição das
// interseções e uniões do executor.
func TestOrdemDoScanLiveCoincide(t *testing.T) {
	str := func(s string) types.Value { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
	p, err := encoding.AppendField(nil, types.FieldID(1), str("v"))
	if err != nil {
		t.Fatal(err)
	}

	realStore, err := storage.Open(storage.Options{Dir: filepath.Join(t.TempDir(), "ds")})
	if err != nil {
		t.Fatal(err)
	}
	defer realStore.Close()
	if _, err := realStore.FieldDict().LookupOrCreate([]byte("campo")); err != nil {
		t.Fatal(err)
	}

	for name, st := range map[string]storeUnderTest{
		"fake": fakestorage.New(fakestorage.Indexes{}),
		"real": realStore,
	} {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 20; i++ {
				if _, _, err := st.Put([]byte{'K', byte('a' + i)}, 1, 0, p, nil); err != nil {
					t.Fatal(err)
				}
			}
			ids := scanLive(t, st)
			if len(ids) != 20 {
				t.Fatalf("ScanLive = %d rows, want 20", len(ids))
			}
			for i := 1; i < len(ids); i++ {
				if ids[i] <= ids[i-1] {
					t.Fatalf("ordem quebrada em %d: %v", i, ids)
				}
			}
		})
	}
}

func collect(t *testing.T, st storeUnderTest, f types.FieldID, v types.Value) []types.RowID {
	t.Helper()
	it, err := st.Lookup(f, v)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	defer it.Close()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func scanLive(t *testing.T, st storeUnderTest) []types.RowID {
	t.Helper()
	it := st.ScanLive(st.OpenSnapshot())
	defer it.Close()
	var out []types.RowID
	for it.Next() {
		out = append(out, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
