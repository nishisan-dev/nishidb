package server_test

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// specComHash declara uma coluna com índice hash — o caso do piloto, em que a
// dimensão de consulta mais valiosa (`node`) é hash.
const specComHash = `version: 1
name: d
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    node:
      type: string
      index: hash
`

// TestServidorAbreODatasetComOsCamposDeHashDeclarados prova, pelo caminho de
// subida REAL, que o servidor passa schema.CamposComHash ao Store.
//
// A asserção não olha para dentro do servidor: ela olha para o DISCO. Depois da
// subida, reabrir o mesmo diretório SEM declarar o campo tem de ser recusado —
// e só é recusado porque o manifest que o servidor gravou nomeia o índice. Um
// servidor que esquecesse de passar a lista criaria o dataset sem o índice, a
// reabertura passaria, e o defeito só apareceria como query lenta em produção.
//
// É a metade do modo de falha cruzado servidor↔CLI que schema.CamposComHash
// existe para impedir. A outra metade — o `nishidb query` recusando abrir um
// dataset que o servidor criou COM o índice — é o mesmo erro visto do outro
// lado, e cai na mesma guarda.
func TestServidorAbreODatasetComOsCamposDeHashDeclarados(t *testing.T) {
	a := novoAmbiente(t)
	a.escreveDataset("d", specComHash)

	cfg := a.carrega()
	srv, err := server.Novo(cfg, server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	if err := srv.Fecha(); err != nil {
		t.Fatalf("Fecha: %v", err)
	}

	dir := cfg.CaminhoDoDataset("d")
	if _, err := storage.Open(storage.Options{Dir: dir}); !errors.Is(err, storage.ErrDatasetCorrupt) {
		t.Fatalf("reabrir sem o campo de hash devia ser recusado (o servidor não criou o índice?): %v", err)
	}
	st, err := storage.Open(storage.Options{Dir: dir, HashFieldNames: []string{"node"}})
	if err != nil {
		t.Fatalf("reabrir declarando o campo: %v", err)
	}
	defer func() { _ = st.Close() }()
	f, ok := st.FieldDict().Lookup([]byte("node"))
	if !ok {
		t.Fatal("o campo node não foi registrado no dicionário do dataset")
	}
	if !st.HasHash(f) {
		t.Fatal("o Store reabriu sem a capability de hash no campo declarado")
	}
}
