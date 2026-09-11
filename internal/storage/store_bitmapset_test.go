package storage

// R-C e item 14 da SC: os campos indexados podem ser declarados por NOME, a
// união dos dois caminhos é o conjunto do dataset, e um segmento que encolheu
// por fora é fatal.
//
// O R-A saiu daqui junto com o motor global. Ele exigia que o conjunto indexado
// fosse validado contra o manifest na abertura, e as duas recusas que ele
// produzia — abrir sem um campo que o dataset declara indexado, e declarar um
// campo novo sobre um dataset povoado — existiam para impedir um índice global
// que cobrisse só um pedaço do dataset e devolvesse MENOS linhas do que existem.
// Com o índice por segmento, segmento sem a seção do campo não devolve menos:
// ele ENUMERA as linhas dele como candidatas, e o resultado é o mesmo, só mais
// caro. A recusa passou a ser obstáculo sem dano a evitar, e o manifest deixou
// de carregar o conjunto contra o qual ela decidia.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestCamposIndexadosPorNomeResolvemPeloDicionario(t *testing.T) {
	// R-C: os FieldIDs nascem de schema.Compile, que precisa do dicionário —
	// e o dicionário vive dentro do Store. Sem declarar por nome, o
	// bootstrap exigiria abrir sem índices, compilar, fechar e reabrir.
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFieldsByName: []string{"id", "status", "counter"}}
	s := audOpen(t, opts)
	defer s.Close()

	// A ordem de criação no dicionário fixa os IDs 1..3.
	for _, want := range []types.FieldID{fldID, fldStatus, fldCounter} {
		if !s.Indexed(want) {
			t.Fatalf("campo %d declarado por nome deveria ser indexado; conjunto=%v", want, s.FieldsIndexados())
		}
	}
	mustPut(t, s, "bn", 1, statusPayload(t, s, "bn", "Start", 1), nil)
	it, err := s.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 1 {
		t.Fatalf("o índice resolvido por nome deveria indexar: %v", rows)
	}
}

func TestCamposIndexadosPorIDEPorNomeSaoUniao(t *testing.T) {
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

	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}, IndexedFieldsByName: []string{"id", "status"}}
	s2 := audOpen(t, opts)
	defer s2.Close()
	// "status" aparece dos dois lados: entra uma vez só.
	if got := s2.FieldsIndexados(); len(got) != 2 {
		t.Fatalf("união deveria ter 2 campos distintos, obtido %v", got)
	}
}

// TestIndiceNovoEmDatasetPovoadoEhAceitoEResponde é o que substitui as duas
// recusas do R-A, e ele afirma a coisa mais forte que se pode afirmar aqui: a
// consulta sobre o campo recém-declarado devolve o MESMO conjunto que o full
// scan filtrado, inclusive para as linhas gravadas ANTES da declaração.
//
// É a resposta da spec à pergunta que travava o débito 22. Ela sai correta de
// graça porque o contrato do Lookup sempre foi superconjunto com revalidação: um
// segmento sem a seção do campo enumera as linhas dele como candidatas, e o
// executor filtra. A parte de CUSTO — os segmentos antigos enumerarem
// indefinidamente — está a meio caminho: a pendência passou a ser por (segmento,
// campo), e um .sidx sem a seção do campo declarado entra na fila de
// reconstrução, mas nada CONSOME essa fila ainda, e é por isso que o débito 22
// segue aberto. Quem prova o agendamento é
// TestAcrescentarIndiceEmDatasetPovoadoAgendaReconstrucao; o que este teste
// guarda é a corretude, que vale ANTES de a reconstrução alcançar o segmento.
func TestIndiceNovoEmDatasetPovoadoEhAceitoEResponde(t *testing.T) {
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

	s2, err := Open(Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}})
	if err != nil {
		t.Fatalf("declarar índice sobre dataset povoado deveria ser aceito: %v", err)
	}
	defer s2.Close()

	it, err := s2.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	candidatos := collectRows(t, it)
	// Asserção CRUA sobre o índice, ao lado da de produto: sem ela, um Lookup
	// que devolvesse zero candidatos passaria — a revalidação decide contra o
	// payload e o full scan também devolveria vazio se o índice estivesse
	// quebrado de outra forma.
	exigeCandidatos(t, "índice declarado sobre dataset povoado", candidatos, []types.RowID{0, 1, 2})
	if got := lookupRevalidado(t, s2, fldStatus, strVal("Start")); len(got) != 3 {
		t.Fatalf("pós-revalidação deveriam vir as 3 linhas anteriores à declaração: %v", got)
	}
}

// TestAbrirSemUmCampoAntesIndexadoEhAceito é a outra metade do R-A que caiu. O
// dano que a recusa evitava era o índice global parar de receber sets e
// reaparecer INCOMPLETO se o campo voltasse; no índice por segmento os segmentos
// escritos no intervalo simplesmente não têm a seção do campo, e enumeram.
func TestAbrirSemUmCampoAntesIndexadoEhAceito(t *testing.T) {
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}})
	mustPut(t, s, "ra", 1, statusPayload(t, s, "ra", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("abrir sem o campo antes indexado deveria ser aceito: %v", err)
	}
	defer s2.Close()
	if s2.Indexed(fldStatus) {
		t.Fatal("o campo não foi declarado nesta abertura")
	}
}

func TestIndiceNovoEmDatasetVazioEhPermitido(t *testing.T) {
	// Sem rows não há nada a backfillar: declarar o índice é seguro.
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(Options{Dir: dir, IndexedFieldsByName: []string{"id", "status", "counter"}})
	if err != nil {
		t.Fatalf("dataset vazio deveria aceitar índice novo: %v", err)
	}
	defer s2.Close()
	mustPut(t, s2, "vz", 1, statusPayload(t, s2, "vz", "Start", 1), nil)
	it, err := s2.Lookup(fldStatus, strVal("Start"))
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

	// Trunca para um size INTERMEDIÁRIO: maior que o header, menor que o
	// registrado. Não é tail rasgado — é perda de records já checkpointados.
	size := fileSizeAud(t, audSecPath(dir, 1))
	if err := os.Truncate(audSecPath(dir, 1), size/2); err != nil {
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
