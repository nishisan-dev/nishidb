package storage

// R-A, R-C e item 14 da SC: o conjunto de campos indexados é validado contra
// o manifest na abertura, os campos podem ser declarados por nome, e um
// segmento que encolheu por fora é fatal.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestBitmapFieldNamesResolvePeloDicionario(t *testing.T) {
	// R-C: os FieldIDs nascem de schema.Compile, que precisa do dicionário —
	// e o dicionário vive dentro do Store. Sem declarar por nome, o
	// bootstrap exigiria abrir sem índices, compilar, fechar e reabrir.
	dir := t.TempDir()
	opts := Options{Dir: dir, BitmapFieldNames: []string{"id", "status", "counter"}}
	s := audOpen(t, opts)
	defer s.Close()

	// A ordem de criação no dicionário fixa os IDs 1..3.
	for _, want := range []types.FieldID{fldID, fldStatus, fldCounter} {
		if !s.HasBitmap(want) {
			t.Fatalf("campo %d declarado por nome deveria ter bitmap; conjunto=%v", want, s.BitmapFields())
		}
	}
	mustPut(t, s, "bn", 1, statusPayload(t, s, "bn", "Start", 1), nil)
	it, err := s.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 1 {
		t.Fatalf("o índice resolvido por nome deveria indexar: %v", rows)
	}
}

func TestBitmapFieldsEBitmapFieldNamesSaoUniao(t *testing.T) {
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir})
	// Registra os campos para que os nomes resolvam para 1..3.
	statusPayload(t, s, "x", "Start", 0)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}

	opts := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}, BitmapFieldNames: []string{"id", "status"}}
	s2 := audOpen(t, opts)
	defer s2.Close()
	// "status" aparece dos dois lados: entra uma vez só.
	if got := s2.BitmapFields(); len(got) != 2 {
		t.Fatalf("união deveria ter 2 campos distintos, obtido %v", got)
	}
}

func TestAberturaSemCampoDoManifestEhRejeitada(t *testing.T) {
	// R-A: o índice ficaria em disco parando de receber sets/clears; se o
	// campo voltasse depois, a query indexada devolveria resultado
	// incompleto sem nenhum sintoma.
	dir := t.TempDir()
	comBitmap := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}
	s := audOpen(t, comBitmap)
	mustPut(t, s, "ra", 1, statusPayload(t, s, "ra", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("abrir sem o campo indexado deveria falhar, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "AllowBitmapDrop") {
		t.Fatalf("o erro precisa dizer como prosseguir de propósito: %v", err)
	}

	// Opt-out explícito destrava.
	s2, err := Open(Options{Dir: dir, AllowBitmapDrop: true})
	if err != nil {
		t.Fatalf("AllowBitmapDrop deveria permitir: %v", err)
	}
	defer s2.Close()
	if s2.HasBitmap(fldStatus) {
		t.Fatal("o campo foi dropado nesta abertura")
	}
}

func TestIndiceNovoEmDatasetComRowsEhRejeitado(t *testing.T) {
	// Terceiro caminho do mesmo dano: um bitmap criado agora num dataset que
	// já tem rows cobre apenas os records futuros — as rows antigas somem da
	// query indexada. Backfill não existe no M1.
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir})
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("bf-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}})
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("índice novo sobre rows existentes deveria falhar, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "3 rows") {
		t.Fatalf("o erro precisa dizer quantas rows ficariam de fora: %v", err)
	}
}

func TestIndiceNovoEmDatasetVazioEhPermitido(t *testing.T) {
	// Sem rows não há nada a backfillar: declarar o índice é seguro.
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(Options{Dir: dir, BitmapFieldNames: []string{"id", "status", "counter"}})
	if err != nil {
		t.Fatalf("dataset vazio deveria aceitar índice novo: %v", err)
	}
	defer s2.Close()
	mustPut(t, s2, "vz", 1, statusPayload(t, s2, "vz", "Start", 1), nil)
	it, err := s2.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 1 {
		t.Fatalf("postings: %v", rows)
	}
}

func TestSegmentoQueEncolheuEhFatal(t *testing.T) {
	// Item 14: segmentos só crescem. Um arquivo menor que o registrado no
	// manifest encolheu por fora, e os records que faltam são
	// irrecuperáveis — replayar o prefixo mutilado como se fosse o dataset
	// inteiro perderia dados em silêncio.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	for i := 0; i < 6; i++ {
		k := fmt.Sprintf("sh-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Trunca para um tamanho INTERMEDIÁRIO: maior que o header, menor que o
	// registrado. Não é tail rasgado — é perda de records já checkpointados.
	tamanho := fileSizeAud(t, audSegPath(dir, 1))
	if err := os.Truncate(audSegPath(dir, 1), tamanho/2); err != nil {
		t.Fatal(err)
	}

	_, err := Open(opts)
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("segmento encolhido deveria ser fatal, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "encolheu") {
		t.Fatalf("o erro precisa apontar truncamento externo: %v", err)
	}
}
