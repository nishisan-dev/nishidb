package storage

// Testes da S12 — recovery/tail replay (design §10.3, spec §51-52 e os
// casos vinculantes do §14.12-15 e do brief).

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// reopen fecha (sem checkpoint — equivalente a crash para o estado em
// heap: overlay e tallies morrem) e reabre o dataset no mesmo diretório.
func reopen(t *testing.T, s *Store, opts Options) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	return s2
}

func TestRestartBasico_WriteRestartLookup(t *testing.T) {
	// O critério da Fase 1 (spec §113): write → restart → lookup by id.
	// N puts checkpointados + M puts só no overlay (perdidos no heap) —
	// o replay do tail recupera os M.
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("rb-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 3; i < 5; i++ {
		k := fmt.Sprintf("rb-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}

	s2 := reopen(t, s, opts)
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("rb-%d", i)
		rec, _, ok, err := s2.GetByKey(s2.presente(), []byte(k), nil)
		if err != nil || !ok || rec.Sequence != int64(i) {
			t.Fatalf("GetByKey(%q) pós-restart: (%+v,%v,%v)", k, rec, ok, err)
		}
		if rec2, _, ok, err := s2.GetByRowID(s2.presente(), rec.RowID, nil); err != nil || !ok || !bytes.Equal(rec2.Key, []byte(k)) {
			t.Fatalf("GetByRowID(%d): (%v,%v)", rec.RowID, ok, err)
		}
	}
	if rows := collectRows(t, s2.ScanLive(s2.presente())); !slices.Equal(rows, []types.RowID{0, 1, 2, 3, 4}) {
		t.Fatalf("ScanLive pós-restart: %v", rows)
	}
	it, err := s2.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, it); len(rows) != 5 {
		t.Fatalf("bitmap pós-restart: %v", rows)
	}
	if st := s2.Stats(); st.RowCount != 5 {
		t.Fatalf("RowCount pós-restart: %d", st.RowCount)
	}
	exigeBitmapCompleto(t, s2)
	// O writer está posicionado: escrever depois do restart funciona.
	mustPut(t, s2, "rb-nova", 99, statusPayload(t, s2, "rb-nova", "Start", 99), nil)
	if _, _, ok, _ := s2.GetByKey(s2.presente(), []byte("rb-nova"), nil); !ok {
		t.Fatal("put pós-restart deveria funcionar")
	}
	exigeBitmapCompleto(t, s2)
}

// tailRecordLens devolve os comprimentos dos records do tail construídos
// pelos statusPayload dos testes (mesma construção → mesmo tamanho).
func tailRecordLen(t *testing.T, s *Store, key, status string, counter int64) int64 {
	t.Helper()
	p := statusPayload(t, s, key, status, counter)
	rec, err := encoding.BuildRecord(nil, 0, 0, 0, []byte(key), p)
	if err != nil {
		t.Fatal(err)
	}
	return int64(len(rec))
}

func TestReopenTailIncompleto(t *testing.T) {
	// spec §104/§52: truncamento no meio do último record → descarta o
	// record cortado, ftruncate no fim válido, estados anteriores valem.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := openStore(t, opts)
	mustPut(t, s, "ti-0", 1, statusPayload(t, s, "ti-0", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Dois estados de ti-1 no tail: o corte no meio do 2º preserva o 1º.
	mustPut(t, s, "ti-1", 1, statusPayload(t, s, "ti-1", "Start", 1), nil)
	prev, _, _, err := s.GetByKey(s.presente(), []byte("ti-1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "ti-1", 2, statusPayload(t, s, "ti-1", "Stop", 2), &prev)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "data", "segment-00000001.dat")
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, st.Size()-10); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen com tail truncado: %v", err)
	}
	defer s2.Close()
	rec, _, ok, err := s2.GetByKey(s2.presente(), []byte("ti-1"), nil)
	if err != nil || !ok || rec.Sequence != 1 {
		t.Fatalf("ti-1 deveria voltar ao estado anterior ao corte: (%+v,%v,%v)", rec, ok, err)
	}
	// O lixo foi truncado: o arquivo termina no fim válido (o put pós-reopen
	// aterrissa exatamente ali).
	st2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Size() >= st.Size() {
		t.Fatalf("arquivo não truncado: %d → %d", st.Size(), st2.Size())
	}
}

func TestReopenCRCCorrompidoNoTailDescartaDali(t *testing.T) {
	// CRC inválido no meio do tail: replay para ALI; records anteriores do
	// tail valem; os posteriores (mesmo íntegros) são descartados (§52).
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := openStore(t, opts)
	mustPut(t, s, "cc-base", 1, statusPayload(t, s, "cc-base", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	tailStart := func() int64 {
		m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
		if err != nil {
			t.Fatal(err)
		}
		return int64(m.DurableOffset)
	}()

	len1 := tailRecordLen(t, s, "cc-1", "Start", 1)
	len2 := tailRecordLen(t, s, "cc-2", "Start", 2)
	mustPut(t, s, "cc-1", 1, statusPayload(t, s, "cc-1", "Start", 1), nil)
	mustPut(t, s, "cc-2", 2, statusPayload(t, s, "cc-2", "Start", 2), nil)
	mustPut(t, s, "cc-3", 3, statusPayload(t, s, "cc-3", "Start", 3), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "data", "segment-00000001.dat")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[tailStart+len1+len2-1] ^= 0xFF // corrompe o fim do record de cc-2
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if _, _, ok, _ := s2.GetByKey(s2.presente(), []byte("cc-1"), nil); !ok {
		t.Fatal("cc-1 (antes da corrupção) deveria estar visível")
	}
	for _, k := range []string{"cc-2", "cc-3"} {
		if _, _, ok, _ := s2.GetByKey(s2.presente(), []byte(k), nil); ok {
			t.Fatalf("%s (a partir da corrupção) deveria ter sido descartado", k)
		}
	}
	// Arquivo truncado no início do record corrompido.
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != tailStart+len1 {
		t.Fatalf("truncamento: esperado %d, obtido %d", tailStart+len1, st.Size())
	}
}

func TestReopenSegmentosPosCheckpointDescobertos(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 400}
	s := openStore(t, opts)
	mustPut(t, s, "sp-0", 0, statusPayload(t, s, "sp-0", "Start", 0), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	const n = 12 // força rotação para segmentos 2+
	for i := 1; i <= n; i++ {
		k := fmt.Sprintf("sp-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if s.Stats().CurrentSegmentID < 2 {
		t.Fatal("o cenário exige rotação pós-checkpoint")
	}

	s2 := reopen(t, s, opts)
	for i := 0; i <= n; i++ {
		k := fmt.Sprintf("sp-%d", i)
		if _, _, ok, err := s2.GetByKey(s2.presente(), []byte(k), nil); err != nil || !ok {
			t.Fatalf("GetByKey(%q): (%v,%v)", k, ok, err)
		}
	}
	if s2.Stats().CurrentSegmentID < 2 {
		t.Fatalf("writer deveria estar no último segmento: %+v", s2.Stats())
	}
}

func TestReopenSegmentoOrfaoAdotado(t *testing.T) {
	// Rotação interrompida: o segmento novo existe no dir (só header), fora
	// do manifest. A recovery o ADOTA (senão o O_EXCL da próxima rotação
	// falharia com EEXIST — M11).
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := openStore(t, opts)
	mustPut(t, s, "orf-0", 0, statusPayload(t, s, "orf-0", "Start", 0), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Forja o órfão: header do segmento 1 com o ID trocado para 2.
	seg1 := filepath.Join(dir, "data", "segment-00000001.dat")
	raw, err := os.ReadFile(seg1)
	if err != nil {
		t.Fatal(err)
	}
	hdr := bytes.Clone(raw[:64])
	hdr[8] = 2 // segmentID u64 LE: byte baixo
	if err := os.WriteFile(filepath.Join(dir, "data", "segment-00000002.dat"), hdr, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen com órfão: %v", err)
	}
	defer s2.Close()
	if _, _, ok, _ := s2.GetByKey(s2.presente(), []byte("orf-0"), nil); !ok {
		t.Fatal("estado anterior deveria sobreviver")
	}
	// O writer adotou o órfão: o próximo put aterrissa no segmento 2 sem
	// EEXIST, e o estado segue consistente.
	mustPut(t, s2, "orf-1", 1, statusPayload(t, s2, "orf-1", "Start", 1), nil)
	if st := s2.Stats(); st.CurrentSegmentID != 2 {
		t.Fatalf("writer deveria estar no órfão adotado: %+v", st)
	}
}

func TestPhantomCommitViraVisivelNoReplay(t *testing.T) {
	// §14.12: Put que devolveu ERRO pós-Append (sticky) deixa record
	// durável sem location. Após o restart, o replay o torna VISÍVEL —
	// semântica aceita e documentada. O seam de erro na fase de Set produz
	// o phantom com payload REPLAYÁVEL.
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)
	mustPut(t, s, "ph-ok", 1, statusPayload(t, s, "ph-ok", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	s.SetSetErrHookForTest(func(types.FieldID) error { return errors.New("set falho simulado") })
	if _, _, err := s.Put([]byte("ph-fantasma"), 7, 0, statusPayload(t, s, "ph-fantasma", "Start", 7), nil); err == nil {
		t.Fatal("o Put do phantom deveria falhar")
	}
	// Invisível agora, e o writer está sticky.
	if _, _, ok, _ := s.GetByKey(s.presente(), []byte("ph-fantasma"), nil); ok {
		t.Fatal("phantom não pode estar visível antes do restart")
	}
	if _, _, err := s.Put([]byte("ph-outra"), 1, 0, statusPayload(t, s, "ph-outra", "Start", 1), nil); err == nil ||
		!strings.Contains(err.Error(), "modo falho") {
		t.Fatalf("writer deveria estar sticky: %v", err)
	}

	s2 := reopen(t, s, opts)
	// Visível após o replay, com o estado do record órfão.
	rec, _, ok, err := s2.GetByKey(s2.presente(), []byte("ph-fantasma"), nil)
	if err != nil || !ok || rec.Sequence != 7 {
		t.Fatalf("phantom deveria estar visível pós-restart: (%+v,%v,%v)", rec, ok, err)
	}
	// E a recovery LIMPA o sticky (reopen é o remédio, §14.14).
	mustPut(t, s2, "ph-pos", 1, statusPayload(t, s2, "ph-pos", "Start", 1), nil)
	// O phantom foi materializado por um Put que morreu ANTES da fase de
	// Set: quem tem de ligar as postings dele é o replay.
	exigeBitmapCompleto(t, s2)
}

func TestEpochNaoRegride(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := openStore(t, opts) // auto-checkpoint da criação: epoch 1
	mustPut(t, s, "ep", 1, statusPayload(t, s, "ep", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil { // epoch 2
		t.Fatal(err)
	}
	s2 := reopen(t, s, opts) // auto-checkpoint da recovery: epoch 3
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != 3 {
		t.Fatalf("epoch pós-reopen deveria ser 3 (nunca regride), obtido %d", m.Epoch)
	}
	if err := s2.Checkpoint(); err != nil { // epoch 4
		t.Fatal(err)
	}
	m2, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if m2.Epoch != 4 {
		t.Fatalf("epoch: %d", m2.Epoch)
	}
}

func TestTalliesSobrevivemAoRestart(t *testing.T) {
	// Seeds §14.15: os tallies de liveness voltam do manifest — o cenário
	// da S11 (Total=5/Live=3) sobrevive a um restart sem tail.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := openStore(t, opts)
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("tl-%d", i)
		mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
	}
	for seq := int64(2); seq <= 3; seq++ {
		prev, _, _, err := s.GetByKey(s.presente(), []byte("tl-0"), nil)
		if err != nil {
			t.Fatal(err)
		}
		mustPut(t, s, "tl-0", seq, statusPayload(t, s, "tl-0", "Stop", seq), &prev)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	_ = reopen(t, s, opts) // auto-checkpoint regrava o manifest com os seeds
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	sec := m.Segments[0]
	if sec.TotalRecords != 5 || sec.LiveRecords != 3 {
		t.Fatalf("tallies pós-restart: %+v", sec)
	}
}

func TestManifestAusenteComDadosEhFatal(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := openStore(t, opts)
	mustPut(t, s, "fa", 1, statusPayload(t, s, "fa", "Start", 1), nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "checkpoint", "manifest.bin")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(opts); err == nil || !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("manifest ausente COM dados deveria ser fatal: %v", err)
	}
}

func TestDatasetNovoTemManifestDesdeACriacao(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := openStore(t, opts)
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatalf("manifest deveria existir desde a criação: %v", err)
	}
	if m.Epoch != 1 || m.CurrentSegment != 1 {
		t.Fatalf("manifest inicial: %+v", *m)
	}
	// Reabrir um dataset vazio (mas já criado) usa o caminho de recovery.
	s2 := reopen(t, s, opts)
	if st := s2.Stats(); st.RowCount != 0 || st.CurrentSegmentID != 1 {
		t.Fatalf("dataset vazio pós-reopen: %+v", st)
	}
}

func TestReopenBitmapConverge(t *testing.T) {
	// Transições do tail reconvergem os bitmaps: update A→B pós-checkpoint
	// perdido do heap volta pelo replay — query pós-restart = pré-crash.
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)
	id, _ := mustPut(t, s, "bc", 1, statusPayload(t, s, "bc", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	prev, _, _, err := s.GetByKey(s.presente(), []byte("bc"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "bc", 2, statusPayload(t, s, "bc", "Stop", 2), &prev)

	s2 := reopen(t, s, opts)
	// A convergência que este teste mede é a do estado NOVO: o update A→B
	// perdido do heap tem de voltar pelo replay. Essa asserção segue exata.
	itB, err := s2.Lookup(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itB); !slices.Equal(rows, []types.RowID{id}) {
		t.Fatalf("posting nova pós-replay: %v", rows)
	}
	// A posting ANTIGA já não é limpa pelo replay, porque não há Clear a
	// reaplicar (spec §5.2). Ela fica como candidato e some com o segmento; o
	// que o produto devolve — query pós-restart igual à pré-crash, que é a frase
	// do cabeçalho deste teste — é o resultado pós-revalidação.
	if rows := lookupRevalidado(t, s2, fldStatus, strVal("Start")); len(rows) != 0 {
		t.Fatalf("pós-revalidação, status=Start deveria ser vazio depois do replay: %v", rows)
	}
	exigeBitmapCompleto(t, s2)
}

func TestCrashEntreApplyEManifest(t *testing.T) {
	// Crash entre os passos 2-3 (apply+msync) e 4 (manifest) do checkpoint:
	// os índices em disco ficam À FRENTE do manifest. O replay re-aplica
	// S→tail por cima — idempotente (§10.2), sem duplicação.
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)
	id1, _ := mustPut(t, s, "ca-1", 1, statusPayload(t, s, "ca-1", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	prev, _, _, err := s.GetByKey(s.presente(), []byte("ca-1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "ca-1", 2, statusPayload(t, s, "ca-1", "Stop", 2), &prev)
	mustPut(t, s, "ca-2", 1, statusPayload(t, s, "ca-2", "Start", 1), nil)
	// Simula o checkpoint parcial: sync+apply+msync SEM manifest.
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}

	s2 := reopen(t, s, opts)
	rec, _, ok, err := s2.GetByKey(s2.presente(), []byte("ca-1"), nil)
	if err != nil || !ok || rec.Sequence != 2 {
		t.Fatalf("ca-1: (%+v,%v,%v)", rec, ok, err)
	}
	if _, _, ok, _ := s2.GetByKey(s2.presente(), []byte("ca-2"), nil); !ok {
		t.Fatal("ca-2 deveria estar visível")
	}
	if st := s2.Stats(); st.RowCount != 2 {
		t.Fatalf("sem duplicação: RowCount=%d", st.RowCount)
	}
	// O que este teste prova é a IDEMPOTÊNCIA do replay por cima de índices já
	// à frente do manifest — e as asserções que a medem (visibilidade das duas
	// keys, Sequence de ca-1 e RowCount==2, acima) seguem intactas.
	//
	// A asserção de que a posting antiga de ca-1 "está limpa" media o Clear
	// reaplicado pelo replay, que deixou de existir (§5.2). Sem Clear, uma
	// re-aplicação duplicada não teria como se manifestar apagando de menos;
	// ela se manifestaria como rowId REPETIDO no iterador — e é isso que a
	// asserção passa a cobrar, junto do resultado pós-revalidação.
	itA, err := s2.Lookup(fldStatus, strVal("Start"))
	if err != nil {
		t.Fatal(err)
	}
	exigeCandidatos(t, "Start", collectRows(t, itA), []types.RowID{id1 + 1})
	if rows := lookupRevalidado(t, s2, fldStatus, strVal("Start")); !slices.Equal(rows, []types.RowID{id1 + 1}) {
		t.Fatalf("pós-revalidação status=Start deveria ser só ca-2 (%d): %v", id1+1, rows)
	}
	exigeBitmapCompleto(t, s2)
}

func TestDoubleReopenEhIdempotente(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}
	s := openStore(t, opts)
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("dr-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "dr-4", 4, statusPayload(t, s, "dr-4", "Start", 4), nil)

	s2 := reopen(t, s, opts)
	manifestA := loadManifestAud(t, dir)
	s3 := reopen(t, s2, opts)
	manifestB := loadManifestAud(t, dir)
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("dr-%d", i)
		if _, _, ok, err := s3.GetByKey(s3.presente(), []byte(k), nil); err != nil || !ok {
			t.Fatalf("GetByKey(%q) no 2º reopen: (%v,%v)", k, ok, err)
		}
	}
	if rows := collectRows(t, s3.ScanLive(s3.presente())); !slices.Equal(rows, []types.RowID{0, 1, 2, 3, 4}) {
		t.Fatalf("ScanLive: %v", rows)
	}
	if st := s3.Stats(); st.RowCount != 5 {
		t.Fatalf("RowCount: %d", st.RowCount)
	}
	exigeBitmapCompleto(t, s3)

	// O reopen só pode avançar o epoch: todo o resto do ponto S — tallies
	// inclusive — tem que sair idêntico, senão o replay não é idempotente.
	if manifestB.Epoch != manifestA.Epoch+1 {
		t.Fatalf("epoch: %d → %d (esperado +1)", manifestA.Epoch, manifestB.Epoch)
	}
	manifestA.Epoch, manifestB.Epoch = 0, 0
	manifestA.CreatedAtUnixMs, manifestB.CreatedAtUnixMs = 0, 0
	if !reflect.DeepEqual(manifestA, manifestB) {
		t.Fatalf("o 2º reopen produziu um manifest diferente:\n  1º = %+v\n  2º = %+v", manifestA, manifestB)
	}
}

// loadManifestAud lê o manifest corrente do dataset.
func loadManifestAud(t *testing.T, dir string) *checkpoint.Manifest {
	t.Helper()
	m, err := checkpoint.Load(audManifestPath(dir))
	if err != nil {
		t.Fatalf("Load do manifest: %v", err)
	}
	return m
}
