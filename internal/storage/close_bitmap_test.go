package storage_test

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A QUARTA rota de leitura. O quiesce da I2 cobriu Put, GetByKey, GetByRowID e
// ScanLive, e o teste daquela tarefa asseriu "toda operação após o Close" — mas
// o helper `abre` não configura índice bitmap, então `LookupBitmap` estava fora
// do alcance da asserção. A invariante valia em três rotas e falhava na quarta.
//
// O iterador de bitmap é o mais exposto de todos: ele SOBREVIVE à chamada que o
// criou (é lazy por construção) e lê o mmap a cada Next. É a fonte de candidatos
// de 4 dos 6 planos do executor.

func abreComBitmap(t *testing.T, campo string) (*storage.Store, types.FieldID) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ds")
	st, err := storage.Open(storage.Options{Dir: dir, BitmapFieldNames: []string{campo}})
	if err != nil {
		t.Fatal(err)
	}
	id, err := st.FieldDict().LookupOrCreate([]byte(campo))
	if err != nil {
		t.Fatal(err)
	}
	return st, id
}

func povoa(t *testing.T, st *storage.Store, campo types.FieldID, valor string, rows int) {
	t.Helper()
	p, err := encoding.AppendField(nil, campo,
		types.Value{Type: types.TypeString, Bytes: []byte(valor)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if _, _, err := st.Put([]byte{'K', byte(i), byte(i >> 8)}, 1, 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// O caso reproduzido pela revisão final: iterador de bitmap EM VOO quando o
// Close acontece. Antes da correção isto entrava em panic no bounds check do
// mmap ("offset N fora dos limites (size=0)"), porque o Close zera o tamanho da
// região e o iterador continuava lendo.
func TestIteradorDeBitmapEmVooNaoEntraEmPanicNoClose(t *testing.T) {
	st, campo := abreComBitmap(t, "status")
	povoa(t, st, campo, "Stop", 4096)

	it, err := st.LookupBitmap(campo, types.Value{Type: types.TypeString, Bytes: []byte("Stop")})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	// Consome só o começo: o resto do bitmap continua por ler quando o Close
	// desmapeia a região.
	if !it.Next() {
		t.Fatal("o índice não devolveu nenhuma row")
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Nenhum panic, e o motivo da parada é reportado — a mesma disciplina do
	// liveIterator: o iterador reentra no Store a cada avanço.
	for it.Next() {
	}
	if !errors.Is(it.Err(), storage.ErrDatasetClosed) {
		t.Errorf("Err() = %v, want ErrDatasetClosed", it.Err())
	}
	if err := it.Close(); err != nil {
		t.Errorf("Close do iterador = %v, want nil", err)
	}
}

// Iterador de bitmap criado DEPOIS do Close: sem panic, sem row, com o motivo.
func TestLookupBitmapAposCloseDevolveErro(t *testing.T) {
	st, campo := abreComBitmap(t, "status")
	povoa(t, st, campo, "Stop", 64)

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LookupBitmap(campo, types.Value{Type: types.TypeString, Bytes: []byte("Stop")}); !errors.Is(err, storage.ErrDatasetClosed) {
		t.Errorf("LookupBitmap após Close = %v, want ErrDatasetClosed", err)
	}
}

// A corrida de verdade, sob -race: leitores varrendo bitmaps enquanto o Close
// acontece. É o cenário do servidor do M3 — Store de vida longa e query em
// streaming —, onde o CLI não tem a ordem de defer a favor.
func TestCloseConcorrenteComIteradoresDeBitmap(t *testing.T) {
	st, campo := abreComBitmap(t, "status")
	povoa(t, st, campo, "Stop", 4096)
	valor := types.Value{Type: types.TypeString, Bytes: []byte("Stop")}

	const leitores = 6
	var wg sync.WaitGroup
	for range leitores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 2_000 {
				it, err := st.LookupBitmap(campo, valor)
				if err != nil {
					if !errors.Is(err, storage.ErrDatasetClosed) {
						t.Errorf("LookupBitmap: %v", err)
					}
					return
				}
				for it.Next() {
				}
				if err := it.Err(); err != nil && !errors.Is(err, storage.ErrDatasetClosed) {
					t.Errorf("iteração: %v", err)
					it.Close()
					return
				}
				it.Close()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := st.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	wg.Wait()
}

// Valor nunca visto devolve iterador vazio, e o vazio também precisa sobreviver
// ao Close — ele é o mesmo tipo devolvido nas outras rotas.
func TestIteradorVazioDeBitmapSobreviveAoClose(t *testing.T) {
	st, campo := abreComBitmap(t, "status")
	povoa(t, st, campo, "Stop", 8)

	it, err := st.LookupBitmap(campo, types.Value{Type: types.TypeString, Bytes: []byte("Start")})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if it.Next() {
		t.Error("iterador de valor inexistente devolveu row")
	}
	if err := it.Err(); err != nil {
		t.Errorf("Err() = %v, want nil (não houve o que interromper)", err)
	}
	it.Close()
}
