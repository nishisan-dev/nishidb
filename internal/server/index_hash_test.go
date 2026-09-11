package server_test

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// specWithHash declara uma coluna indexada — o caso do piloto, em que a
// dimensão de consulta mais valiosa (`node`) precisa de índice. A grafia
// mudou com a D5 (`index: true`, não mais `index: hash`), e a função que ele
// exercita virou uma só — schema.IndexedFieldNames, no lugar do par
// CamposComBitmap/CamposComHash —, mas o nome do fixture e o caminho continuam
// os mesmos.
const specWithHash = `version: 1
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
      index: true
`

// TestServidorAbreODatasetComOsCamposDeHashDeclarados prova, pelo caminho de
// subida REAL, que o servidor passa schema.IndexedFieldNames ao Store.
//
// A asserção não olha para dentro do servidor: ela olha para o DISCO. O oráculo
// é o ARQUIVO DE ÍNDICE do segmento corrente — o Open cria um .widx por campo
// indexado antes de aceitar o primeiro Put —, e ele só existe se a lista tiver
// chegado ao Store. Um servidor que esquecesse de passá-la criaria o dataset sem
// índice nenhum, e o defeito só apareceria como query lenta em produção.
//
// O oráculo ERA outro: reabrir sem declarar o campo tinha de ser recusado,
// porque o manifest nomeava o índice. Aquela guarda saiu junto com o motor
// global — com o índice por segmento, abrir sem o campo é legítimo e a consulta
// enumera —, então a prova mudou de lugar e foi para o arquivo, que é onde o
// efeito de passar a lista sempre esteve.
//
// É a metade do modo de falha cruzado servidor↔CLI que schema.IndexedFieldNames
// existe para impedir; a outra metade é o mesmo erro visto do lado do CLI.
func TestServidorAbreODatasetComOsCamposDeHashDeclarados(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("d", specWithHash)

	cfg := a.load()
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dir := cfg.DatasetRootDir("d")
	dataDir := filepath.Join(dir, "data")

	// O que o SERVIDOR deixou em disco, lido ANTES de qualquer reabertura: a
	// ordem importa porque toda abertura descarta os .widx herdados e cria os
	// seus (spec §2.5), de modo que reabrir declarando o campo e só então olhar
	// para o arquivo mediria a reabertura, não o servidor.
	segs, err := segindex.SegmentsWithWIP(dataDir)
	if err != nil {
		t.Fatalf("SegmentsWithWIP: %v", err)
	}
	var deixados []types.FieldID
	for _, sec := range segs {
		fields, err := segindex.ListWIP(dataDir, sec)
		if err != nil {
			t.Fatalf("ListWIP(%d): %v", sec, err)
		}
		deixados = append(deixados, fields...)
	}

	st, err := storage.Open(storage.Options{Dir: dir, IndexedFieldsByName: []string{"node"}})
	if err != nil {
		t.Fatalf("reabrir declarando o campo: %v", err)
	}
	defer func() { _ = st.Close() }()
	// O id do campo vem do dicionário do dataset — que é append-only, então o
	// id que o servidor usou é o mesmo que sai daqui —, e não de uma constante
	// posicional.
	f, ok := st.FieldDict().Lookup([]byte("node"))
	if !ok {
		t.Fatal("o campo node não foi registrado no dicionário do dataset")
	}
	if !st.Indexed(f) {
		t.Fatal("o Store reabriu sem a capability de índice no campo declarado")
	}
	if !slices.Contains(deixados, f) {
		t.Fatalf("o servidor não criou índice para o campo node (id %d); os .widx que ele deixou cobrem %v", f, deixados)
	}
}
