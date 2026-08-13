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

// F-1 (§14.21): fechar com operação em voo produzia PANIC — o mmap era
// desmapeado sob os pés da operação, e o bounds check continuava aprovando
// offsets contra o tamanho antigo. Panic não é capturável pela política de
// log-and-skip da ingestão, então derrubava o processo inteiro.
//
// O CLI abre e fecha o dataset A CADA COMANDO, então o que era corrida rara
// virou caminho normal — é por isso que este defeito bloqueava a I2.
//
// O contrato novo: operação após o Close devolve ErrDatasetClosed. Erro, não
// panic, porque o chamador tem política para erro.

func abre(t *testing.T, campos ...string) (*storage.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ds")
	st, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range campos {
		if _, err := st.FieldDict().LookupOrCreate([]byte(c)); err != nil {
			t.Fatal(err)
		}
	}
	return st, dir
}

func payloadDe(t *testing.T, v string) []byte {
	t.Helper()
	p, err := encoding.AppendField(nil, types.FieldID(1),
		types.Value{Type: types.TypeString, Bytes: []byte(v)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// O caso do writer: Put em voo enquanto o Close acontece.
func TestCloseConcorrenteComPutNaoEntraEmPanic(t *testing.T) {
	st, _ := abre(t, "campo")
	p := payloadDe(t, "v")

	var (
		wg      sync.WaitGroup
		fechado sync.WaitGroup
	)
	fechado.Add(1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Escreve até o Close cortar. Nenhuma iteração pode entrar em panic;
		// depois do Close, toda tentativa devolve ErrDatasetClosed.
		visto := false
		for i := 0; i < 20_000; i++ {
			_, _, err := st.Put([]byte{'K', byte(i), byte(i >> 8)}, 1, 0, p, nil)
			if err == nil {
				continue
			}
			if !errors.Is(err, storage.ErrDatasetClosed) {
				t.Errorf("Put devolveu erro inesperado: %v", err)
				return
			}
			visto = true
			break
		}
		if !visto {
			t.Log("o Close não alcançou o writer nesta execução (aceitável: é corrida)")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer fechado.Done()
		if err := st.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	wg.Wait()

	// Depois do Close, TODA operação devolve o erro — nenhuma entra em panic.
	if _, _, err := st.Put([]byte("Z"), 1, 0, p, nil); !errors.Is(err, storage.ErrDatasetClosed) {
		t.Errorf("Put após Close = %v, want ErrDatasetClosed", err)
	}
	if _, _, _, err := st.GetByKey([]byte("K"), nil); !errors.Is(err, storage.ErrDatasetClosed) {
		t.Errorf("GetByKey após Close = %v, want ErrDatasetClosed", err)
	}
	if _, _, _, err := st.GetByRowID(0, nil); !errors.Is(err, storage.ErrDatasetClosed) {
		t.Errorf("GetByRowID após Close = %v, want ErrDatasetClosed", err)
	}
	if err := st.Sync(); !errors.Is(err, storage.ErrDatasetClosed) {
		t.Errorf("Sync após Close = %v, want ErrDatasetClosed", err)
	}
	if err := st.Checkpoint(); !errors.Is(err, storage.ErrDatasetClosed) {
		t.Errorf("Checkpoint após Close = %v, want ErrDatasetClosed", err)
	}
}

// O caso dos LEITORES, que o design registrou como a outra metade do defeito:
// o godoc promete leitura concorrente com o writer, e elas morriam igual.
func TestCloseConcorrenteComLeiturasNaoEntraEmPanic(t *testing.T) {
	st, _ := abre(t, "campo")
	p := payloadDe(t, "v")
	for i := 0; i < 500; i++ {
		if _, _, err := st.Put([]byte{'K', byte(i), byte(i >> 8)}, 1, 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}

	const leitores = 6
	var wg sync.WaitGroup
	for l := 0; l < leitores; l++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5_000; i++ {
				// Varredura completa: é a leitura que mais tempo passa dentro
				// do mmap, e a que o Close tinha mais chance de atropelar.
				it := st.ScanLive()
				for it.Next() {
					if _, _, _, err := st.GetByRowID(it.RowID(), nil); err != nil {
						if errors.Is(err, storage.ErrDatasetClosed) {
							it.Close()
							return
						}
						t.Errorf("GetByRowID: %v", err)
						it.Close()
						return
					}
				}
				if err := it.Err(); err != nil {
					if !errors.Is(err, storage.ErrDatasetClosed) {
						t.Errorf("iteração: %v", err)
					}
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

	// O iterador criado DEPOIS do Close não itera e reporta o motivo.
	it := st.ScanLive()
	if it.Next() {
		t.Error("ScanLive após Close devolveu row")
	}
	if !errors.Is(it.Err(), storage.ErrDatasetClosed) {
		t.Errorf("Err() = %v, want ErrDatasetClosed", it.Err())
	}
}

// O lock do dataset só é devolvido DEPOIS do quiesce: enquanto houver escrita
// em voo, outro processo não pode assumir o diretório. Aqui o segundo "outro
// processo" é um segundo Open no mesmo diretório.
func TestLockSoEhLiberadoDepoisDoQuiesce(t *testing.T) {
	st, dir := abre(t, "campo")

	// Com o dataset aberto, o segundo Open falha.
	if _, err := storage.Open(storage.Options{Dir: dir}); err == nil {
		t.Fatal("segundo Open deveria falhar com o dataset travado")
	} else if !errors.Is(err, storage.ErrDatasetLocked) {
		t.Errorf("erro = %v, want ErrDatasetLocked", err)
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Depois do Close o dataset é assumível — e íntegro.
	st2, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reabrir após Close: %v", err)
	}
	defer st2.Close()
}

// Close é idempotente: o CLI e os defers do chamador o chamam mais de uma vez
// sem que isso vire erro nem duplo release do lock.
func TestCloseEhIdempotente(t *testing.T) {
	st, dir := abre(t, "campo")
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Errorf("segundo Close = %v, want nil", err)
	}
	st2, err := storage.Open(storage.Options{Dir: dir})
	if err != nil {
		t.Fatalf("o dataset ficou inacessível após dois Close: %v", err)
	}
	st2.Close()
}
