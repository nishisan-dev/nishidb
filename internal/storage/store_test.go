package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Ordem de criação dos campos nos testes: "id"=1, "status"=2, "counter"=3.
// BitmapFields usa IDs (contrato do brief); o dicionário é determinístico
// na ordem de LookupOrCreate, fixada pelo statusPayload.
const (
	fldID      types.FieldID = 1
	fldStatus  types.FieldID = 2
	fldCounter types.FieldID = 3
)

func openStore(t testing.TB, opts Options) *Store {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	if opts.SegmentSize == 0 {
		opts.SegmentSize = 1 << 20
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// mustAppendField codifica um campo que o teste sabe ser válido.
func mustAppendField(tb testing.TB, dst []byte, id types.FieldID, v types.Value) []byte {
	tb.Helper()
	out, err := encoding.AppendField(dst, id, v)
	if err != nil {
		tb.Fatalf("AppendField(%d, %#x): %v", id, v.Type, err)
	}
	return out
}

func statusPayload(t testing.TB, s *Store, key, status string, counter int64) []byte {
	t.Helper()
	for _, name := range []string{"id", "status", "counter"} {
		if _, err := s.FieldDict().LookupOrCreate([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	var p []byte
	p = mustAppendField(t, p, fldID, types.Value{Type: types.TypeString, Bytes: []byte(key)})
	p = mustAppendField(t, p, fldStatus, types.Value{Type: types.TypeString, Bytes: []byte(status)})
	p = mustAppendField(t, p, fldCounter, types.Value{Type: types.TypeInt64, I64: counter})
	return p
}

func strVal(s string) types.Value {
	return types.Value{Type: types.TypeString, Bytes: []byte(s)}
}

func collectRows(t *testing.T, it types.RowIDIterator) []types.RowID {
	t.Helper()
	defer it.Close()
	var rows []types.RowID
	for it.Next() {
		rows = append(rows, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func mustPut(t testing.TB, s *Store, key string, seq int64, payload []byte, prev *types.Record) (types.RowID, bool) {
	t.Helper()
	id, created, err := s.Put([]byte(key), seq, 0, payload, prev)
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return id, created
}

func TestPutCreateEGetByKeyRoundTrip(t *testing.T) {
	s := openStore(t, Options{})
	key := "CMP-V0001PPCLP-013"
	payload := statusPayload(t, s, key, "Start", 100)

	rowID, created := mustPut(t, s, key, 7455409733, payload, nil)
	if !created {
		t.Fatal("primeira escrita deveria criar")
	}

	rec, _, ok, err := s.GetByKey([]byte(key), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey: (%v,%v)", ok, err)
	}
	if rec.RowID != rowID || rec.Sequence != 7455409733 || rec.Flags != 0 {
		t.Fatalf("record: %+v", rec)
	}
	if !bytes.Equal(rec.Key, []byte(key)) || !bytes.Equal(rec.Payload, payload) {
		t.Fatal("key/payload diferem do gravado")
	}
}

func TestGetByKeyAusente(t *testing.T) {
	s := openStore(t, Options{})
	if _, _, ok, err := s.GetByKey([]byte("nao-existe"), nil); ok || err != nil {
		t.Fatalf("esperado (false,nil), obtido (%v,%v)", ok, err)
	}
}

func TestPutUpdateMantemRowIDETrocaEstado(t *testing.T) {
	s := openStore(t, Options{})
	id1, _ := mustPut(t, s, "upd", 1, statusPayload(t, s, "upd", "Start", 1), nil)

	prev, buf, ok, err := s.GetByKey([]byte("upd"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	id2, created := mustPut(t, s, "upd", 2, statusPayload(t, s, "upd", "Stop", 2), &prev)
	if created || id2 != id1 {
		t.Fatalf("update: esperado (%d,false), obtido (%d,%v)", id1, id2, created)
	}

	// Leitura por rowId com buffer reutilizado vê o estado novo.
	rec, buf2, ok, err := s.GetByRowID(id1, buf)
	if err != nil || !ok {
		t.Fatalf("GetByRowID: (%v,%v)", ok, err)
	}
	if rec.Sequence != 2 {
		t.Fatalf("estado novo não visível: seq=%d", rec.Sequence)
	}
	// Reuso do buffer devolvido em nova leitura.
	rec, _, ok, err = s.GetByRowID(id1, buf2)
	if err != nil || !ok || rec.Sequence != 2 {
		t.Fatalf("releitura com buf reutilizado: (%+v,%v,%v)", rec, ok, err)
	}
}

func TestPutComPrevConfiaNoRowIDSemRefazerFind(t *testing.T) {
	// Prova de que o Put NÃO refaz Find quando prev é fornecido: um prev da
	// MESMA key com rowId forjado (999, inexistente) faz o novo estado
	// aterrissar na row 999. Se o Put refizesse o Find, o registro iria
	// para a row verdadeira da key. (prev de OUTRA key é rejeitado pela
	// guarda — ver TestPutRejeitaPrevDeOutraKey.)
	s := openStore(t, Options{})
	idA, _ := mustPut(t, s, "key-a", 1, statusPayload(t, s, "key-a", "Start", 1), nil)

	prevA, _, ok, err := s.GetByKey([]byte("key-a"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	forged := prevA
	forged.RowID = 999
	got, created := mustPut(t, s, "key-a", 2, statusPayload(t, s, "key-a", "Stop", 2), &forged)
	if created || got != 999 {
		t.Fatalf("Put deveria confiar no rowId forjado 999, obtido (%d,%v)", got, created)
	}
	rec, _, ok, err := s.GetByRowID(999, nil)
	if err != nil || !ok || rec.Sequence != 2 || !bytes.Equal(rec.Key, []byte("key-a")) {
		t.Fatalf("estado deveria estar na row 999: (%+v,%v,%v)", rec, ok, err)
	}
	// A row original não foi tocada (nenhum re-Find aconteceu).
	if rec, _, ok, _ := s.GetByRowID(idA, nil); !ok || rec.Sequence != 1 {
		t.Fatalf("row original alterada: (%+v,%v)", rec, ok)
	}
}

func TestPutSemPrevParaKeyExistenteFazFallback(t *testing.T) {
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	mustPut(t, s, "fb", 1, statusPayload(t, s, "fb", "Start", 1), nil)

	// prev=nil com key existente: o Store lê o prev sozinho e não cria rowId
	// novo. É ISSO que este teste mede, e a asserção segue intacta.
	id2, created := mustPut(t, s, "fb", 2, statusPayload(t, s, "fb", "Stop", 2), nil)
	if created {
		t.Fatal("fallback não pode criar rowId novo")
	}
	// IGUALDADE, e não superconjunto: um dataset, uma row, um segmento — não há
	// falso positivo possível em Stop, e afrouxar aqui seria apagar asserção sem
	// que o desenho novo peça.
	itStop, err := s.LookupBitmap(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStop); !slices.Equal(rows, []types.RowID{id2}) {
		t.Fatalf("bitmap Stop: %v", rows)
	}

	// A asserção que ESTE teste tinha em Start — "o bitmap deveria esvaziar" —
	// media o diff do motor global, que não existe mais: o índice por segmento
	// não tem fase de CLEAR (spec §5.2), então a posting de Start sobrevive como
	// candidato. O que o produto devolve é o resultado pós-revalidação, e é ele
	// que continua vazio.
	if rows := lookupRevalidado(t, s, fldStatus, strVal("Start")); len(rows) != 0 {
		t.Fatalf("depois da revalidação, status=Start deveria ser vazio, tem %v", rows)
	}
}

func TestBitmapDiffEmUpdate(t *testing.T) {
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	for i, k := range []string{"k1", "k2", "k3"} {
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", 0), nil)
	}
	prev, _, _, err := s.GetByKey([]byte("k2"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "k2", 10, statusPayload(t, s, "k2", "Stop", 1), &prev)

	// Em Start a asserção passa de igualdade para SUPERCONJUNTO, e a razão é a
	// §5.2: sem fase de CLEAR, a row 1 — que era Start e virou Stop — sobrevive
	// como candidato até o segmento ser compactado. Exigir [0 2] aqui seria
	// medir a estreiteza do motor global. O que o produto devolve é a linha de
	// baixo, pós-revalidação, e essa continua exata.
	itStart, err := s.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	exigeCandidatos(t, "Start", collectRows(t, itStart), []types.RowID{0, 2})
	if rows := lookupRevalidado(t, s, fldStatus, strVal("Start")); !slices.Equal(rows, []types.RowID{0, 2}) {
		t.Fatalf("Start pós-revalidação: esperado [0 2], obtido %v", rows)
	}

	// Em Stop a IGUALDADE é preservada, e de propósito: nenhuma linha deixou de
	// ser Stop, então não há falso positivo que a explique. Afrouxá-la aqui
	// junto com a de cima teria apagado uma asserção que o desenho novo não
	// exige apagar.
	itStop, err := s.LookupBitmap(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStop); !slices.Equal(rows, []types.RowID{1}) {
		t.Fatalf("Stop: esperado [1], obtido %v", rows)
	}

	// A→A (mesmo valor): pertinência inalterada. Preservado integralmente — é a
	// guarda contra um Set que se confundisse com update de valor.
	prev2, _, _, err := s.GetByKey([]byte("k1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "k1", 20, statusPayload(t, s, "k1", "Start", 9), &prev2)
	if rows := lookupRevalidado(t, s, fldStatus, strVal("Start")); !slices.Equal(rows, []types.RowID{0, 2}) {
		t.Fatalf("Start pós A→A: %v", rows)
	}
}

func TestLookupBitmapCampoNaoIndexadoEValorDesconhecido(t *testing.T) {
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	mustPut(t, s, "x", 1, statusPayload(t, s, "x", "Start", 0), nil)

	if _, err := s.LookupBitmap(999, strVal("qualquer")); err == nil {
		t.Fatal("campo sem índice bitmap deveria falhar")
	}
	it, err := s.LookupBitmap(fldStatus, strVal("nunca-visto"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 0 {
		t.Fatalf("valor desconhecido = iterador vazio, obtido %v", rows)
	}
}

func TestScanLiveOrdemTombstoneEOverlay(t *testing.T) {
	s := openStore(t, Options{})
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("scan-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", 0), nil)
	}
	// Row 2 vira tombstone (o caminho existe desde já; a Fase 9 o usará).
	prev, _, _, err := s.GetByKey([]byte("scan-2"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Put([]byte("scan-2"), 10, types.FlagTombstone, nil, &prev); err != nil {
		t.Fatalf("Put tombstone: %v", err)
	}

	// Todas as rows estão só no overlay (nenhum checkpoint ainda): o scan
	// precisa enxergá-las mesmo assim, em ordem crescente, pulando a 2.
	rows := collectRows(t, s.ScanLive())
	if !slices.Equal(rows, []types.RowID{0, 1, 3, 4}) {
		t.Fatalf("ScanLive: esperado [0 1 3 4], obtido %v", rows)
	}
}

func TestGetTombstonedNaoEhLive(t *testing.T) {
	s := openStore(t, Options{})
	id, _ := mustPut(t, s, "morta", 1, statusPayload(t, s, "morta", "Start", 0), nil)
	prev, _, _, err := s.GetByKey([]byte("morta"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Put([]byte("morta"), 2, types.FlagTombstone, nil, &prev); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := s.GetByRowID(id, nil); ok {
		t.Fatal("GetByRowID de tombstone deveria ser ok=false")
	}
	if _, _, ok, _ := s.GetByKey([]byte("morta"), nil); ok {
		t.Fatal("GetByKey de tombstone deveria ser ok=false")
	}
	if _, _, ok, _ := s.GetByRowID(9999, nil); ok {
		t.Fatal("rowId inexistente deveria ser ok=false")
	}
}

func TestSequence(t *testing.T) {
	s := openStore(t, Options{})
	id, _ := mustPut(t, s, "seq", 42, statusPayload(t, s, "seq", "Start", 0), nil)
	if got, pres := s.Sequence(id); pres != types.PresencaViva || got != 42 {
		t.Fatalf("Sequence: (%d,%v)", got, pres)
	}
	if _, pres := s.Sequence(777); pres != types.PresencaAusente {
		t.Fatal("rowId inexistente não tem sequence")
	}
}

func TestRotacaoDeSegmentoNoPut(t *testing.T) {
	// Segmento minúsculo força rotação; tudo continua legível e o Stats
	// reflete os segmentos criados.
	s := openStore(t, Options{SegmentSize: 400})
	for i := 0; i < 6; i++ {
		k := fmt.Sprintf("rot-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	for i := 0; i < 6; i++ {
		k := fmt.Sprintf("rot-%d", i)
		rec, _, ok, err := s.GetByKey([]byte(k), nil)
		if err != nil || !ok || !bytes.Equal(rec.Key, []byte(k)) {
			t.Fatalf("GetByKey(%q) pós-rotação: (%v,%v)", k, ok, err)
		}
	}
	if st := s.Stats(); st.SegmentsTotal < 2 {
		t.Fatalf("esperado ≥2 segmentos, Stats=%+v", st)
	}
}

func TestErrosDePut(t *testing.T) {
	s := openStore(t, Options{})
	okPayload := statusPayload(t, s, "e", "Start", 0)

	unordered := mustAppendField(t, nil, 5, types.Value{Type: types.TypeInt64, I64: 1})
	unordered = mustAppendField(t, unordered, 2, types.Value{Type: types.TypeInt64, I64: 2})

	if _, _, err := s.Put(nil, 1, 0, okPayload, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("key vazia: esperado ErrInvalidArgument, obtido %v", err)
	}
	if _, _, err := s.Put([]byte("k"), 1, 0, unordered, nil); !errors.Is(err, encoding.ErrInvalidPayload) {
		t.Fatalf("payload desordenado: esperado ErrInvalidPayload, obtido %v", err)
	}
	bigKey := bytes.Repeat([]byte{'k'}, encoding.MaxKeySize+1)
	if _, _, err := s.Put(bigKey, 1, 0, okPayload, nil); err == nil {
		t.Fatal("key acima de encoding.MaxKeySize deveria falhar")
	}
}

func TestOpenRejeitaSegmentSizeInvalido(t *testing.T) {
	_, err := Open(Options{Dir: t.TempDir(), SegmentSize: 2 << 30})
	if err == nil {
		t.Fatal("SegmentSize acima do teto deveria falhar no Open")
	}
}

func TestSyncEveryPutSmoke(t *testing.T) {
	s := openStore(t, Options{SyncEveryPut: true})
	mustPut(t, s, "durable", 1, statusPayload(t, s, "durable", "Start", 0), nil)
	if _, _, ok, err := s.GetByKey([]byte("durable"), nil); err != nil || !ok {
		t.Fatalf("(%v,%v)", ok, err)
	}
}

func TestStatsCoerente(t *testing.T) {
	s := openStore(t, Options{})
	const n = 10
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("st-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", 0), nil)
	}
	st := s.Stats()
	if st.RowCount != n {
		t.Fatalf("RowCount: %d", st.RowCount)
	}
	if st.OverlayLen != n {
		t.Fatalf("OverlayLen (sem checkpoint): esperado %d, obtido %d", n, st.OverlayLen)
	}
	if st.CurrentSegmentID != 1 || st.SegmentsTotal != 1 {
		t.Fatalf("segmentos: %+v", st)
	}
	if st.PrimaryLoadFactor <= 0 || st.PrimaryLoadFactor >= 1 {
		t.Fatalf("PrimaryLoadFactor fora de (0,1): %v", st.PrimaryLoadFactor)
	}
}

// TestStatsContaBytesDeDiscoEDeLiveness cobre os dois campos de bytes: sem
// série de disco não há como ver a curva chegar, e o §86 dá 2 a 3 dias de
// margem até o ENOSPC — curto demais para descobrir pelo alerta do FS.
//
// O que este teste NÃO prova é a troca da derivação de SegmentsTotal: com um
// segmento só, derivar do ID corrente devolveria o mesmo 1, e a asserção
// passaria nas duas implementações. Quem separa as duas é o
// TestStatsParaDeContarSegmentoAposentado, e só ele.
func TestStatsContaBytesDeDiscoEDeLiveness(t *testing.T) {
	s := openStore(t, Options{})
	const n = 100
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	// O checkpoint é o ponto em que o Store relista os arquivos; sem ele o
	// snapshot ainda descreve o disco de antes dos Puts, e é assim de
	// propósito (a nota do campo StorageBytes).
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	got := s.Stats()
	if got.StorageBytes <= 0 {
		t.Errorf("StorageBytes = %d, queria > 0", got.StorageBytes)
	}
	if got.SegmentsTotal != 1 {
		t.Errorf("SegmentsTotal = %d, queria 1", got.SegmentsTotal)
	}
	// O tally é limite SUPERIOR do que está vivo (débitos 38 e 45), mas
	// nunca pode passar do tamanho do arquivo que o contém: se passar, o
	// que quebrou não foi a precisão, foi a contabilidade. A comparação só
	// vale LOGO APÓS um checkpoint, que é quando os dois números descrevem o
	// mesmo instante — o tally anda a cada Put e o tamanho só no checkpoint,
	// então entre os dois o vivo passa do disco por construção.
	if got.LiveBytesUpperBound == 0 || got.LiveBytesUpperBound > uint64(got.StorageBytes) {
		t.Errorf("LiveBytesUpperBound = %d fora de (0, %d]", got.LiveBytesUpperBound, got.StorageBytes)
	}
}

// TestStatsParaDeContarSegmentoAposentado é o teste que separa a contagem
// real da derivação antiga (`SegmentsTotal = CurrentSegmentID`, com a
// premissa "IDs monotônicos desde 1, sem remoção no M1"). Nenhum cenário de
// dataset que só CRESCE distingue as duas: enquanto ninguém remove segmento,
// a contagem e o ID corrente são o mesmo número. O que as separa é a
// aposentadoria, e é para ela que a Task existe.
//
// A aposentadoria é montada como a decisão D5 do §53-55 a fará: rename para
// nome não canônico, que o ListSegments ignora de propósito
// (segment.go:252). O arquivo continua em disco — o manifest vigente ainda
// pode nomeá-lo — e some do dataset no checkpoint seguinte.
//
// A asserção decisiva é SegmentsTotal < CurrentSegmentID. Sob a derivação
// antiga os dois são iguais em TODO cenário possível, então essa comparação
// é falsa por construção lá e verdadeira aqui.
func TestStatsParaDeContarSegmentoAposentado(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const n = 400
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("ap-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	antes := s.Stats()
	if antes.SegmentsTotal < 2 {
		t.Fatalf("o cenário precisa de um segmento SELADO para aposentar: %+v", antes)
	}
	if antes.SegmentsTotal != antes.CurrentSegmentID {
		t.Fatalf("sem remoção os dois têm de coincidir; %+v", antes)
	}

	alvo := filepath.Join(dir, "data", "segment-00000001.dat")
	info, err := os.Stat(alvo)
	if err != nil {
		t.Fatalf("stat do segmento a aposentar: %v", err)
	}
	if err := os.Rename(alvo, alvo+".retired"); err != nil {
		t.Fatalf("aposentar por rename: %v", err)
	}
	// O checkpoint é o ponto de reconciliação: é ele que relista o diretório
	// e é por ele que a aposentadoria entra na contabilidade do Store.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint pós-aposentadoria: %v", err)
	}
	depois := s.Stats()

	if depois.CurrentSegmentID != antes.CurrentSegmentID {
		t.Fatalf("o ID corrente não regride: %d -> %d", antes.CurrentSegmentID, depois.CurrentSegmentID)
	}
	if depois.SegmentsTotal >= depois.CurrentSegmentID {
		t.Errorf("SegmentsTotal = %d ainda acompanha CurrentSegmentID = %d: a contagem continua derivada do ID",
			depois.SegmentsTotal, depois.CurrentSegmentID)
	}
	if depois.SegmentsTotal != antes.SegmentsTotal-1 {
		t.Errorf("SegmentsTotal = %d, queria %d (um segmento a menos)", depois.SegmentsTotal, antes.SegmentsTotal-1)
	}
	if depois.StorageBytes != antes.StorageBytes-info.Size() {
		t.Errorf("StorageBytes = %d, queria %d (%d - %d do arquivo aposentado)",
			depois.StorageBytes, antes.StorageBytes-info.Size(), antes.StorageBytes, info.Size())
	}
}

// TestStatsDeDatasetSemRecordContaOSegmentoInicial fixa o caso que a contagem
// pelo tally do writer perde: um dataset recém-provisionado tem o arquivo do
// segmento inicial em disco e nenhum record, e o tally só ganha entrada no
// applyRecord. Reportar `segments=0` com `current_segment=1` é snapshot
// internamente incoerente vazando para o /v1/stats e para a
// nishidb_segments_total — justo no painel que o M4 existe para alimentar, e
// justo no dataset que acabou de subir, quando ninguém ainda confia nele.
//
// Quem sabe QUAIS segmentos existem é o ListSegments, e o Open já faz um
// checkpoint antes de devolver o Store.
func TestStatsDeDatasetSemRecordContaOSegmentoInicial(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir})
	got := s.Stats()

	if got.CurrentSegmentID != 1 {
		t.Fatalf("CurrentSegmentID = %d, queria 1", got.CurrentSegmentID)
	}
	if got.SegmentsTotal != 1 {
		t.Errorf("SegmentsTotal = %d, queria 1: o arquivo do segmento inicial existe em disco", got.SegmentsTotal)
	}
	entradas, err := os.ReadDir(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("ler data/: %v", err)
	}
	var emDisco int64
	for _, e := range entradas {
		fi, err := e.Info()
		if err != nil {
			t.Fatalf("stat de %s: %v", e.Name(), err)
		}
		emDisco += fi.Size()
	}
	if emDisco <= 0 {
		t.Fatalf("data/ vazio: o cenário não é o que o teste descreve")
	}
	// Exatidão, e não "> 0": StorageBytes é tamanho de arquivo, então logo
	// após o checkpoint do Open ele tem de bater com o diretório byte a byte.
	if got.StorageBytes != emDisco {
		t.Errorf("StorageBytes = %d, queria %d (soma dos arquivos em data/)", got.StorageBytes, emDisco)
	}
	if got.LiveBytesUpperBound != 0 {
		t.Errorf("LiveBytesUpperBound = %d, queria 0: nenhum record foi escrito", got.LiveBytesUpperBound)
	}
}

func TestVisibilidadeLeitorConcorrente(t *testing.T) {
	// A location é o commit point: um leitor em loop nunca vê estado
	// parcial — a key sempre confere e a sequence nunca regride.
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	key := []byte("concurrent")
	mustPut(t, s, "concurrent", 0, statusPayload(t, s, "concurrent", "Start", 0), nil)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []byte
			var lastSeq int64 = -1
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec, b, ok, err := s.GetByKey(key, buf)
				buf = b
				if err != nil || !ok {
					t.Errorf("GetByKey concorrente: (%v,%v)", ok, err)
					return
				}
				if !bytes.Equal(rec.Key, key) {
					t.Error("key trocada em leitura concorrente")
					return
				}
				if rec.Sequence < lastSeq {
					t.Errorf("sequence regrediu: %d → %d", lastSeq, rec.Sequence)
					return
				}
				lastSeq = rec.Sequence
				// M5: payload e header do MESMO estado — o writer grava
				// counter == sequence, então divergência = estado parcial.
				var counter int64
				var fr encoding.FieldReader
				fr.Reset(rec.Payload)
				for {
					id, v, more := fr.Next()
					if !more {
						break
					}
					if id == fldCounter {
						counter = v.I64
					}
				}
				if counter != rec.Sequence {
					t.Errorf("estado parcial: counter=%d sequence=%d", counter, rec.Sequence)
					return
				}
			}
		}()
	}

	var prev types.Record
	var buf []byte
	var ok bool
	var err error
	for i := int64(1); i <= 400; i++ {
		prev, buf, ok, err = s.GetByKey(key, buf)
		if err != nil || !ok {
			t.Fatal(err)
		}
		status := "Start"
		if i%2 == 0 {
			status = "Stop"
		}
		if _, _, err := s.Put(key, i, 0, statusPayload(t, s, "concurrent", status, i), &prev); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

// TestStatsNaoBloqueiaAtrasDoCheckpoint fixa a propriedade que o godoc de
// Stats sempre prometeu e que a conta de disco tinha tirado dele: leitor
// concorrente NÃO espera o writer. O Checkpoint segura o wmu do início ao
// fim, e o histograma dele vai até 30s porque é esse o intervalo padrão do
// §78 — um Stats atrás desse lock apaga o /metrics e o /v1/stats exatamente
// sob a pressão que eles existem para mostrar. Num milestone cujo objetivo é
// tornar a curva de disco visível (§92), é a regressão mais cara possível.
//
// O cenário sai do seam de trace do checkpoint: o hook segura o passo
// msyncLocations, que roda DENTRO da seção crítica do writer, e o teste mede
// quanto o Stats demora com o checkpoint parado ali. Sem espelho atômico dos
// agregados, ele demora o checkpoint inteiro.
func TestStatsNaoBloqueiaAtrasDoCheckpoint(t *testing.T) {
	s := openStore(t, Options{})
	// Records o bastante para o snapshot ter o que somar: um Stats que
	// devolvesse zeros passaria no teste de tempo sem provar nada.
	const n = 200
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("blk-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}

	entrou := make(chan struct{})
	liberado := make(chan struct{})
	s.SetCheckpointTraceForTest(func(passo string) {
		if passo != "msyncLocations" {
			return
		}
		close(entrou)
		<-liberado // segura o checkpoint DENTRO da seção crítica do writer
	})
	// Desinstalar é higiene, não detalhe: o hook fecha um canal, e todo
	// caminho de saída deste teste já esperou o checkpoint terminar — deixar
	// o seam armado transformaria um checkpoint futuro em panic de
	// close-of-closed-channel.
	defer s.SetCheckpointTraceForTest(nil)
	cpFim := make(chan error, 1)
	go func() { cpFim <- s.Checkpoint() }()
	<-entrou

	pronto := make(chan Stats, 1)
	go func() { pronto <- s.Stats() }()
	select {
	case got := <-pronto:
		if got.SegmentsTotal == 0 || got.RowCount != n {
			t.Errorf("Stats respondeu rápido mas com snapshot vazio: %+v", got)
		}
	case <-time.After(2 * time.Second):
		close(liberado)
		<-cpFim
		t.Fatal("Stats bloqueou atrás do checkpoint: o painel cega justamente sob pressão")
	}
	close(liberado)
	if err := <-cpFim; err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}
