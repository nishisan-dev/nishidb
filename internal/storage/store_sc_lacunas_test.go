package storage

// Round final da SC: as lacunas de mutação apontadas pelo verdito (W1, W2,
// W6/X6, W7, X1), o F-4 e o F-5.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/bitmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestValorIndexavelGiganteEhBarradoAntesDoAppend(t *testing.T) {
	// F-5, elevado a fatal por reprodução: um valor de campo indexado acima
	// do teto do values.dict falhava só na fase de Set, DEPOIS do Append. O
	// record ficava durável, o replay do tail tentava reindexá-lo a cada
	// abertura e falhava sempre — um único evento externo, com configuração
	// default, deixava o dataset INABRÍVEL para sempre.
	dir := t.TempDir()
	opts := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}
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
	if _, _, ok, _ := s2.GetByKey([]byte("bomba"), nil); ok {
		t.Fatal("o evento rejeitado não pode ter sido gravado")
	}
	exigeBitmapCompleto(t, s2)
}

func TestTetoDoValorIndexavelNaFronteira(t *testing.T) {
	// R2: o teto é do u16 do values.dict — 65535 cabe, 65536 não. Um teste
	// só com "70000 falha" ficaria verde com o limite em qualquer lugar.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})

	noLimite := strings.Repeat("v", bitmap.MaxValueBytes)
	if _, _, err := s.Put([]byte("limite"), 1, 0, statusPayload(t, s, "limite", noLimite, 1), nil); err != nil {
		t.Fatalf("valor de exatamente %d bytes deveria ser aceito: %v", bitmap.MaxValueBytes, err)
	}
	// E fica de fato indexado: aceitar sem indexar seria falso negativo.
	it, err := s.LookupBitmap(fldStatus, strVal(noLimite))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 1 {
		t.Fatalf("o valor no limite deveria estar indexado: %v", rows)
	}

	umAMais := strings.Repeat("v", bitmap.MaxValueBytes+1)
	if _, _, err := s.Put([]byte("acima"), 2, 0, statusPayload(t, s, "acima", umAMais, 2), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("um byte acima do teto deveria ser recusado, obtido %v", err)
	}
}

func TestFailWriterEmbrulhaSentinelaNaChamadaQueMata(t *testing.T) {
	// F-4: é a chamada que mata o writer que deixa o phantom commit. Se ela
	// devolvesse o erro cru, o consumidor sob log-and-skip concluiria que o
	// writer está vivo e que nada foi gravado — as duas coisas falsas.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
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
	tamanho := fileSizeAud(t, audSegPath(dir, 1))
	if err := os.Truncate(audSegPath(dir, 1), tamanho-1); err != nil {
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
	tamanho := fileSizeAud(t, audSegPath(dir, 1))
	if err := os.Truncate(audSegPath(dir, 1), tamanho-64); err != nil {
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

func TestAllowBitmapDropNaoAutorizaIndiceSemBackfill(t *testing.T) {
	// W1: a mensagem da direção 1 INSTRUI o operador a ligar
	// AllowBitmapDrop, e o checkpoint do fim do Open já apaga o campo do
	// manifest. Se a flag também relaxasse a direção 2, o ciclo
	// drop → escrever → re-adicionar devolveria um índice incompleto — que
	// é exatamente o que o R-A existe para impedir.
	dir := t.TempDir()
	comBitmap := Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}}
	s := audOpen(t, comBitmap)
	mustPut(t, s, "w1", 1, statusPayload(t, s, "w1", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Abre dropando o índice e grava mais uma row: o intervalo em que o
	// índice ficou parado agora tem conteúdo.
	s2, err := Open(Options{Dir: dir, AllowBitmapDrop: true})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s2, "w1-b", 2, statusPayload(t, s2, "w1-b", "Stop", 2), nil)
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-declarar o campo, mesmo com a flag ligada, tem que ser recusado.
	if _, err := Open(Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}, AllowBitmapDrop: true}); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("AllowBitmapDrop não pode autorizar índice sem backfill, obtido %v", err)
	}
}

func TestUmaUnicaRowJaBloqueiaIndiceNovo(t *testing.T) {
	// W7: o teste anterior usava 3 rows e asseria "3 rows", deixando passar
	// qualquer limiar entre 1 e 3.
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir})
	mustPut(t, s, "unica", 1, statusPayload(t, s, "unica", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Options{Dir: dir, BitmapFields: []types.FieldID{fldStatus}})
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("uma única row já ficaria de fora do índice, obtido %v", err)
	}
	if !strings.Contains(err.Error(), "1 rows") {
		t.Fatalf("a mensagem precisa dizer quantas rows ficariam de fora: %v", err)
	}
}

func TestBitmapFieldsRejeitaFieldIDZeroENomeVazio(t *testing.T) {
	// W6/X6: um bitmap no campo 0 ENTRA no manifest, e a direção 1 passa a
	// exigi-lo em toda abertura — dataset refém de um índice que nunca
	// indexaria nada (o fieldID 0 é reservado).
	if _, err := Open(Options{Dir: t.TempDir(), BitmapFields: []types.FieldID{0}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("fieldID 0 deveria ser rejeitado, obtido %v", err)
	}
	if _, err := Open(Options{Dir: t.TempDir(), BitmapFieldNames: []string{""}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nome vazio deveria ser rejeitado, obtido %v", err)
	}
}

func TestBitmapFieldsDevolveEmOrdemCrescente(t *testing.T) {
	// X1: o doc promete ordem crescente e a fonte é um map, cuja iteração é
	// aleatória por construção.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldCounter, fldStatus, fldID}})
	got := s.BitmapFields()
	if len(got) != 3 {
		t.Fatalf("esperado 3 campos, obtido %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("BitmapFields deveria vir em ordem crescente: %v", got)
		}
	}
}
