package storage

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

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

func openStore(t *testing.T, opts Options) *Store {
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

func statusPayload(t *testing.T, s *Store, key, status string, counter int64) []byte {
	t.Helper()
	for _, name := range []string{"id", "status", "counter"} {
		if _, err := s.FieldDict().LookupOrCreate([]byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	var p []byte
	p = encoding.AppendField(p, fldID, types.Value{Type: types.TypeString, Bytes: []byte(key)})
	p = encoding.AppendField(p, fldStatus, types.Value{Type: types.TypeString, Bytes: []byte(status)})
	p = encoding.AppendField(p, fldCounter, types.Value{Type: types.TypeInt64, I64: counter})
	return p
}

func strVal(s string) types.Value {
	return types.Value{Type: types.TypeString, Bytes: []byte(s)}
}

func collectRows(t *testing.T, it RowIDIterator) []types.RowID {
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

func mustPut(t *testing.T, s *Store, key string, seq int64, payload []byte, prev *types.Record) (types.RowID, bool) {
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

	// prev=nil com key existente: o Store lê o prev sozinho, não cria, e o
	// diff de bitmap enxerga o estado antigo.
	id2, created := mustPut(t, s, "fb", 2, statusPayload(t, s, "fb", "Stop", 2), nil)
	if created {
		t.Fatal("fallback não pode criar rowId novo")
	}
	itStart, err := s.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStart); len(rows) != 0 {
		t.Fatalf("bitmap Start deveria esvaziar, tem %v", rows)
	}
	itStop, err := s.LookupBitmap(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStop); !slices.Equal(rows, []types.RowID{id2}) {
		t.Fatalf("bitmap Stop: %v", rows)
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

	itStart, err := s.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStart); !slices.Equal(rows, []types.RowID{0, 2}) {
		t.Fatalf("Start: esperado [0 2], obtido %v", rows)
	}
	itStop, err := s.LookupBitmap(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStop); !slices.Equal(rows, []types.RowID{1}) {
		t.Fatalf("Stop: esperado [1], obtido %v", rows)
	}

	// A→A (mesmo valor): pertinência inalterada.
	prev2, _, _, err := s.GetByKey([]byte("k1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "k1", 20, statusPayload(t, s, "k1", "Start", 9), &prev2)
	itStart2, err := s.LookupBitmap(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStart2); !slices.Equal(rows, []types.RowID{0, 2}) {
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
	if got, ok := s.Sequence(id); !ok || got != 42 {
		t.Fatalf("Sequence: (%d,%v)", got, ok)
	}
	if _, ok := s.Sequence(777); ok {
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

	unordered := encoding.AppendField(nil, 5, types.Value{Type: types.TypeInt64, I64: 1})
	unordered = encoding.AppendField(unordered, 2, types.Value{Type: types.TypeInt64, I64: 2})

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
