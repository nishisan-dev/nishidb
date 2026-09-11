package storage

// Round final da SC: as lacunas de mutação apontadas pelo verdito (W1, W2,
// W6/X6, W7, X1), o F-4 e o F-5.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestValorIndexavelGiganteEhBarradoAntesDoAppend(t *testing.T) {
	// F-5, elevado a fatal por reprodução: um valor de campo indexado acima
	// do teto do values.dict falhava só na fase de Set, DEPOIS do Append. O
	// record ficava durável, o replay do tail tentava reindexá-lo a cada
	// abertura e falhava sempre — um único evento externo, com configuração
	// default, deixava o dataset INABRÍVEL para sempre.
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}
	s := audOpen(t, opts)
	mustPut(t, s, "ok", 1, statusPayload(t, s, "ok", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	gigante := strings.Repeat("v", 70000)
	_, _, err := s.Put([]byte("bomba"), 2, 0, statusPayload(t, s, "bomba", gigante, 2), nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("valor indexável acima do teto deveria ser rejeitado como argumento inválido, obtido %v", err)
	}
	// A mensagem nomeia o campo pelo NOME, não só pelo id: quem lê o log é
	// o operador, que conhece "status" e não o fieldID 2.
	if !strings.Contains(err.Error(), `"status"`) {
		t.Fatalf("a mensagem precisa nomear o campo: %v", err)
	}
	if errors.Is(err, ErrWriterFailed) {
		t.Fatalf("evento ruim não pode matar o writer: %v", err)
	}
	// O writer segue vivo: a ingestão não para por um evento sujo.
	mustPut(t, s, "depois", 3, statusPayload(t, s, "depois", "Stop", 3), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// E o dataset continua abrível — nenhum record irreindexável foi gravado.
	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("o dataset deveria seguir abrível: %v", err)
	}
	defer s2.Close()
	mustGetAud(t, s2, "ok")
	mustGetAud(t, s2, "depois")
	if _, _, ok, _ := s2.GetByKey(s2.presente(), []byte("bomba"), nil); ok {
		t.Fatal("o evento rejeitado não pode ter sido gravado")
	}
	exigeBitmapCompleto(t, s2)
}

func TestTetoDoValorIndexavelNaFronteira(t *testing.T) {
	// R2: o teto é do u16 do values.dict — 65535 cabe, 65536 não. Um teste
	// só com "70000 falha" ficaria verde com o limite em qualquer lugar.
	s := openStore(t, Options{IndexedFields: []types.FieldID{fldStatus}})

	noLimit := strings.Repeat("v", types.MaxIndexedValueBytes)
	if _, _, err := s.Put([]byte("limite"), 1, 0, statusPayload(t, s, "limite", noLimit, 1), nil); err != nil {
		t.Fatalf("valor de exatamente %d bytes deveria ser aceito: %v", types.MaxIndexedValueBytes, err)
	}
	// E fica de fato indexado: aceitar sem indexar seria falso negativo.
	it, err := s.Lookup(fldStatus, strVal(noLimit))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 1 {
		t.Fatalf("o valor no limite deveria estar indexado: %v", rows)
	}

	umAMais := strings.Repeat("v", types.MaxIndexedValueBytes+1)
	if _, _, err := s.Put([]byte("acima"), 2, 0, statusPayload(t, s, "acima", umAMais, 2), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("um byte acima do teto deveria ser recusado, obtido %v", err)
	}
}

func TestFailWriterEmbrulhaSentinelaNaChamadaQueMata(t *testing.T) {
	// F-4: é a chamada que mata o writer que deixa o phantom commit. Se ela
	// devolvesse o erro cru, o consumidor sob log-and-skip concluiria que o
	// writer está vivo e que nada foi gravado — as duas coisas falsas.
	s := openStore(t, Options{IndexedFields: []types.FieldID{fldStatus}})
	s.SetSetErrHookForTest(func(types.FieldID) error { return errors.New("set falho simulado") })

	_, _, err := s.Put([]byte("mata"), 1, 0, statusPayload(t, s, "mata", "Start", 1), nil)
	if !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("a chamada que MATA o writer precisa carregar o sentinela, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "set falho simulado") {
		t.Fatalf("a causa original deveria ser preservada: %v", err)
	}
}

func TestEncolhimentoDeUmByteJaEhFatal(t *testing.T) {
	// W2: o teste anterior truncava pela metade, então uma tolerância de 64
	// bytes sobrevivia. E a substring discrimina esta guarda da guarda de
	// durableOffset, que devolveria o mesmo sentinela por outra causa.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	for i := 0; i < 6; i++ {
		k := fmt.Sprintf("um-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	size := fileSizeAud(t, audSecPath(dir, 1))
	if err := os.Truncate(audSecPath(dir, 1), size-1); err != nil {
		t.Fatal(err)
	}

	_, err := Open(opts)
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("1 byte a menos já é perda de record durável, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "encolheu") {
		t.Fatalf("precisa ser a guarda de encolhimento, não outra: %v", err)
	}
}

func TestEncolhimentoDeSegmentoSeladoEhFatal(t *testing.T) {
	// W2, parte que importa mais: para um segmento SELADO o item 14 é a
	// ÚNICA guarda — a de durableOffset só vale para o segmento corrente.
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 400}
	s := audOpen(t, opts)
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("sel-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if got := s.Stats().CurrentSegmentID; got < 3 {
		t.Fatalf("o cenário exige 3+ segmentos, obtido %d", got)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	size := fileSizeAud(t, audSecPath(dir, 1))
	if err := os.Truncate(audSecPath(dir, 1), size-64); err != nil {
		t.Fatal(err)
	}

	_, err := Open(opts)
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("segmento selado encolhido deveria ser fatal, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "encolheu") {
		t.Fatalf("precisa ser a guarda de encolhimento: %v", err)
	}
}

// Os dois testes que estavam aqui — TestAllowBitmapDropNaoAutorizaIndiceSemBackfill
// e TestUmaUnicaRowJaBloqueiaIndiceNovo — saíram com as guardas que eles
// exercitavam, e nenhum dos dois afirmava mais nada além da RECUSA.
//
// O primeiro provava que o opt-out da direção 1 (abrir sem um campo indexado)
// não relaxava a direção 2 (redeclarar o campo depois), fechando o ciclo
// drop → escrever → re-adicionar. O segundo provava que UMA linha já bastava
// para bloquear, e que a mensagem dizia quantas ficariam de fora.
//
// As duas direções eram sobre o mesmo dano: um índice GLOBAL que cobrisse só
// parte do dataset devolveria menos linhas do que existem. O índice por segmento
// não tem como produzi-lo — segmento sem a seção do campo enumera as linhas dele
// como candidatas —, e o que substitui as duas recusas é o
// TestIndiceNovoEmDatasetPovoadoEhAceitoEResponde, que afirma o oposto: a
// consulta devolve inclusive as linhas gravadas antes de o campo ser declarado.

func TestCamposIndexadosRejeitaFieldIDZeroENomeVazio(t *testing.T) {
	// W6/X6: um índice no campo 0 ENTRA no manifest, e a direção 1 passa a
	// exigi-lo em toda abertura — dataset refém de um índice que nunca
	// indexaria nada (o fieldID 0 é reservado).
	if _, err := Open(Options{Dir: t.TempDir(), IndexedFields: []types.FieldID{0}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("fieldID 0 deveria ser rejeitado, obtido %v", err)
	}
	if _, err := Open(Options{Dir: t.TempDir(), IndexedFieldsByName: []string{""}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nome vazio deveria ser rejeitado, obtido %v", err)
	}
}

func TestCamposIndexadosDevolveEmOrdemCrescente(t *testing.T) {
	// X1: o doc promete ordem crescente e a fonte é um map, cuja iteração é
	// aleatória por construção.
	s := openStore(t, Options{IndexedFields: []types.FieldID{fldCounter, fldStatus, fldID}})
	got := s.FieldsIndexados()
	if len(got) != 3 {
		t.Fatalf("esperado 3 campos, obtido %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("FieldsIndexados deveria vir em ordem crescente: %v", got)
		}
	}
}
