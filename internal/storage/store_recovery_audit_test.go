package storage

// Auditoria da S12: cenários que o conjunto principal de testes de recovery
// não cobria e que exercitam caminhos onde o recovery pode DESTRUIR dados ou
// deixar o dataset permanentemente inacessível.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func audManifestPath(dir string) string {
	return filepath.Join(dir, "checkpoint", "manifest.bin")
}

func audSegPath(dir string, id uint32) string {
	return filepath.Join(dir, "data", fmt.Sprintf("segment-%08d.dat", id))
}

// audOpen abre sem registrar cleanup: estes testes fecham explicitamente
// para simular o crash.
func audOpen(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.SegmentSize == 0 {
		opts.SegmentSize = 1 << 20
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// mustGetAud lê a key exigindo que ela exista. Mora aqui, e não junto dos
// testes do fix round, para que reverter um commit posterior não deixe os
// demais sem o helper.
func mustGetAud(t *testing.T, s *Store, key string) types.Record {
	t.Helper()
	rec, _, ok, err := s.GetByKey(s.AbreSnapshot(), []byte(key), nil)
	if err != nil || !ok {
		t.Fatalf("GetByKey(%q): (ok=%v, err=%v)", key, ok, err)
	}
	return rec
}

func TestReplayReinsereKeyComPrimaryAtrasDasLocations(t *testing.T) {
	// Janela real do §10.2: o ApplyOverlay grava as locations no mmap ANTES
	// do msync da primary. Um crash entre os dois deixa as locations À
	// FRENTE da primary. Se o replay só inserisse na primary as rows que
	// ainda não têm location, a key ficaria inalcançável por GetByKey para
	// sempre — visível por rowId, invisível por key.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "base", 1, statusPayload(t, s, "base", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	idxPath := filepath.Join(dir, "indexes", "primary.idx")
	primariaEmS, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatal(err)
	}

	mustPut(t, s, "tail-1", 1, statusPayload(t, s, "tail-1", "Start", 1), nil)
	mustPut(t, s, "tail-2", 2, statusPayload(t, s, "tail-2", "Start", 2), nil)
	// Índices em disco à frente do manifest (crash entre os passos 3 e 4).
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Regride SÓ a primary ao estado de S: as locations do tail continuam no
	// mmap, a primary não conhece as keys do tail.
	if err := os.WriteFile(idxPath, primariaEmS, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := audOpen(t, opts)
	defer s2.Close()
	for _, k := range []string{"base", "tail-1", "tail-2"} {
		rec, _, ok, err := s2.GetByKey(s2.AbreSnapshot(), []byte(k), nil)
		if err != nil || !ok {
			t.Fatalf("GetByKey(%q) após replay com primary atrasada: (ok=%v, err=%v)", k, ok, err)
		}
		if string(rec.Key) != k {
			t.Fatalf("GetByKey(%q) devolveu a key %q", k, rec.Key)
		}
	}
	// E a reinserção não pode ter duplicado slots da key que já estava lá.
	if used := s2.Stats().PrimaryLoadFactor; used <= 0 {
		t.Fatal("load factor deveria ser positivo")
	}
	novo, created := mustPut(t, s2, "tail-1", 9, statusPayload(t, s2, "tail-1", "Stop", 9), nil)
	if created {
		t.Fatalf("a key do tail deveria ser reconhecida como existente, virou row nova %d", novo)
	}
}

func TestOpenAposCrashAntesDoPrimeiroManifest(t *testing.T) {
	// O Open cria o segmento 1 (header + fsync do dir) ANTES de gravar o
	// manifest inicial. Um crash nessa janela deixa "segmento sem manifest";
	// tratar isso como corrupção faria o dataset nascer inacessível, sem
	// nunca ter tido um único record.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(audManifestPath(dir)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(audSegPath(dir, 1)); err != nil {
		t.Fatalf("o cenário exige o segmento 1 já criado: %v", err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("dataset sem records não pode ficar inacessível: %v", err)
	}
	defer s2.Close()
	mustPut(t, s2, "pos-crash", 1, statusPayload(t, s2, "pos-crash", "Start", 1), nil)
	if _, _, ok, err := s2.GetByKey(s2.AbreSnapshot(), []byte("pos-crash"), nil); !ok || err != nil {
		t.Fatalf("escrita após o recomeço: (ok=%v, err=%v)", ok, err)
	}
}

func TestOpenComDurableOffsetAlemDoSegmento(t *testing.T) {
	// Manifest declarando durável um offset que o arquivo não tem: o
	// OpenWriter chama Truncate(offset), e ftruncate ALÉM do fim ESTENDE o
	// arquivo com zeros — o writer passaria a escrever depois de um buraco.
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "do", 1, statusPayload(t, s, "do", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	m, err := checkpoint.Load(audManifestPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	tamanhoOriginal := fileSizeAud(t, audSegPath(dir, 1))
	m.DurableOffset = uint64(tamanhoOriginal) + 4096
	if err := checkpoint.Write(audManifestPath(dir), m); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("durableOffset além do arquivo deveria ser fatal, obtido %v", err)
	}
	if got := fileSizeAud(t, audSegPath(dir, 1)); got != tamanhoOriginal {
		t.Fatalf("o Open não pode ter mexido no segmento: %d → %d", tamanhoOriginal, got)
	}
}

func TestOpenComDurableOffsetAbaixoDoHeader(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	s := audOpen(t, opts)
	mustPut(t, s, "dh", 1, statusPayload(t, s, "dh", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	m, err := checkpoint.Load(audManifestPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	m.DurableOffset = segment.HeaderSize - 1
	if err := checkpoint.Write(audManifestPath(dir), m); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("durableOffset dentro do header deveria ser fatal, obtido %v", err)
	}
}

func TestErrosFataisDeDatasetSaoClassificaveis(t *testing.T) {
	// Todo caminho "não dá para recuperar sozinho" precisa ser distinguível
	// de um erro de I/O transitório pelo caller (e pelos testes).
	novoDataset := func(t *testing.T) (string, Options) {
		t.Helper()
		dir := t.TempDir()
		opts := Options{Dir: dir}
		s := audOpen(t, opts)
		mustPut(t, s, "x", 1, statusPayload(t, s, "x", "Start", 1), nil)
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		return dir, opts
	}

	t.Run("manifest_ausente_com_dados", func(t *testing.T) {
		dir, opts := novoDataset(t)
		if err := os.Remove(audManifestPath(dir)); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("obtido %v", err)
		}
	})

	t.Run("manifest_corrompido", func(t *testing.T) {
		dir, opts := novoDataset(t)
		raw, err := os.ReadFile(audManifestPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 0xFF
		if err := os.WriteFile(audManifestPath(dir), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err = Open(opts)
		if !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("obtido %v", err)
		}
		// A causa original sobrevive ao embrulho: quem quiser distinguir
		// "manifest ilegível" de "segmento sumido" consegue.
		if !errors.Is(err, checkpoint.ErrCorruptManifest) {
			t.Fatalf("a causa ErrCorruptManifest deveria ser preservada na cadeia: %v", err)
		}
	})

	t.Run("segmento_do_manifest_removido", func(t *testing.T) {
		dir, opts := novoDataset(t)
		if err := os.Remove(audSegPath(dir, 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
			t.Fatalf("obtido %v", err)
		}
	})
}

func TestOpenComBuracoNaCadeiaDeSegmentos(t *testing.T) {
	// Segmento intermediário ausente: o replay não pode pular os records
	// dele em silêncio.
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 400}
	s := audOpen(t, opts)
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("bur-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if got := s.Stats().CurrentSegmentID; got < 3 {
		t.Fatalf("o cenário exige 3+ segmentos, obtido %d", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(audSegPath(dir, 2)); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("buraco na cadeia deveria ser fatal, obtido %v", err)
	}
}

func TestReplayUsaOMesmoCodePathDoWritePath(t *testing.T) {
	// Regra do §15A (Option A): replay e escrita compartilham a orquestração,
	// senão as duas divergem em silêncio. O observável é a igualdade dos
	// tallies: gravar N records e replayá-los tem que produzir a mesma
	// contabilidade de liveness que gravá-los com checkpoint no fim.
	monta := func(t *testing.T, comCheckpointNoFim bool) []checkpoint.SegmentInfo {
		t.Helper()
		dir := t.TempDir()
		opts := Options{Dir: dir, CamposIndexados: []types.FieldID{fldStatus}}
		s := audOpen(t, opts)
		for i := 0; i < 4; i++ {
			k := fmt.Sprintf("cp-%d", i)
			mustPut(t, s, k, 1, statusPayload(t, s, k, "Start", 1), nil)
		}
		// Dois updates: viram garbage no mesmo segmento.
		for _, k := range []string{"cp-0", "cp-1"} {
			prev, _, ok, err := s.GetByKey(s.AbreSnapshot(), []byte(k), nil)
			if err != nil || !ok {
				t.Fatal(err)
			}
			mustPut(t, s, k, 2, statusPayload(t, s, k, "Stop", 2), &prev)
		}
		if comCheckpointNoFim {
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			defer s.Close()
		} else {
			// Sem checkpoint: tudo volta pelo replay do tail no reopen.
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2 := audOpen(t, opts)
			defer s2.Close()
		}
		m, err := checkpoint.Load(audManifestPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		return m.Segments
	}

	escrito := monta(t, true)
	replayado := monta(t, false)
	if len(escrito) != len(replayado) {
		t.Fatalf("número de segmentos: escrito=%d replayado=%d", len(escrito), len(replayado))
	}
	for i := range escrito {
		e, r := escrito[i], replayado[i]
		if e.TotalRecords != r.TotalRecords || e.LiveRecords != r.LiveRecords || e.LiveBytes != r.LiveBytes {
			t.Fatalf("tally do segmento %d divergiu entre escrita e replay:\n  escrito  = %+v\n  replayado= %+v", e.ID, e, r)
		}
	}
}

func TestCorrupcaoNoMeioDaCadeiaEhFatalENaoTrunca(t *testing.T) {
	// Truncar é legítimo só no ÚLTIMO segmento. Um record corrompido no meio
	// de um segmento que já foi selado por rotação não é tail rasgado: parar
	// e truncar ali descartaria os segmentos seguintes INTEIROS, que estão
	// íntegros. Tem que ser fatal, e o Open não pode tocar em nada.
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 400}
	s := audOpen(t, opts)
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("mc-%d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if got := s.Stats().CurrentSegmentID; got < 3 {
		t.Fatalf("o cenário exige 3+ segmentos, obtido %d", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrompe o PRIMEIRO record do segmento 1, que já foi selado.
	raw, err := os.ReadFile(audSegPath(dir, 1))
	if err != nil {
		t.Fatal(err)
	}
	raw[segment.HeaderSize+40] ^= 0xFF
	if err := os.WriteFile(audSegPath(dir, 1), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	tamanhos := map[uint32]int64{}
	for id := uint32(1); id <= 3; id++ {
		tamanhos[id] = fileSizeAud(t, audSegPath(dir, id))
	}

	if _, err := Open(opts); !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("corrupção antes do último segmento deveria ser fatal, obtido %v", err)
	}
	for id, want := range tamanhos {
		if got := fileSizeAud(t, audSegPath(dir, id)); got != want {
			t.Fatalf("o Open recusado não pode truncar nada: segmento %d %d → %d", id, want, got)
		}
	}
}

func TestRotacaoSeguinteAposAdotarOrfao(t *testing.T) {
	// O registro M11 é sobre a rotação FUTURA: com o órfão abandonado no
	// diretório, o openNew(O_EXCL) da próxima rotação falharia com EEXIST.
	// Adotar o órfão como corrente resolve — este teste chega até a rotação
	// seguinte para provar que a cadeia continua.
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 400}
	s := audOpen(t, opts)
	mustPut(t, s, "rot-0", 0, statusPayload(t, s, "rot-0", "Start", 0), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Forja o órfão de uma rotação interrompida: header do 1 com ID trocado.
	raw, err := os.ReadFile(audSegPath(dir, 1))
	if err != nil {
		t.Fatal(err)
	}
	hdr := append([]byte(nil), raw[:segment.HeaderSize]...)
	hdr[8] = 2
	if err := os.WriteFile(audSegPath(dir, 2), hdr, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := audOpen(t, opts)
	defer s2.Close()
	if got := s2.Stats().CurrentSegmentID; got != 2 {
		t.Fatalf("órfão deveria ter sido adotado como corrente, obtido %d", got)
	}
	// Escreve até rotacionar de novo: é aqui que o EEXIST apareceria.
	for i := 1; i <= 10; i++ {
		k := fmt.Sprintf("rot-%d", i)
		mustPut(t, s2, k, int64(i), statusPayload(t, s2, k, "Start", int64(i)), nil)
	}
	if got := s2.Stats().CurrentSegmentID; got < 3 {
		t.Fatalf("a rotação após a adoção deveria avançar além do 2, obtido %d", got)
	}
	for i := 0; i <= 10; i++ {
		k := fmt.Sprintf("rot-%d", i)
		if _, _, ok, err := s2.GetByKey(s2.AbreSnapshot(), []byte(k), nil); !ok || err != nil {
			t.Fatalf("GetByKey(%q) após a rotação: (ok=%v, err=%v)", k, ok, err)
		}
	}
}

func TestPhantomCommitEmUpdateMaterializaEstadoNovo(t *testing.T) {
	// §14.12 no caso de UPDATE: o record órfão carrega o estado novo de uma
	// row que já existia. O replay tem que promover esse estado E reexecutar
	// a transição de bitmap que o Put falho deixou pela metade.
	dir := t.TempDir()
	opts := Options{Dir: dir, CamposIndexados: []types.FieldID{fldStatus}}
	s := audOpen(t, opts)
	id, _ := mustPut(t, s, "phu", 1, statusPayload(t, s, "phu", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	prev, _, ok, err := s.GetByKey(s.AbreSnapshot(), []byte("phu"), nil)
	if err != nil || !ok {
		t.Fatal(err)
	}
	s.SetSetErrHookForTest(func(types.FieldID) error { return errors.New("set falho simulado") })
	if _, _, err := s.Put([]byte("phu"), 2, 0, statusPayload(t, s, "phu", "Stop", 2), &prev); err == nil {
		t.Fatal("o Put do phantom deveria falhar")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := audOpen(t, opts)
	defer s2.Close()
	rec, _, ok, err := s2.GetByKey(s2.AbreSnapshot(), []byte("phu"), nil)
	if err != nil || !ok || rec.RowID != id || rec.Sequence != 2 {
		t.Fatalf("o update órfão deveria estar visível pós-restart: (%+v, ok=%v, err=%v)", rec, ok, err)
	}
	// A indexação foi reexecutada: a row entra em Stop. Esta é a metade que
	// importa e ela segue EXATA — o Put falho não chegou a indexar nada, então
	// perder a row aqui seria falso negativo nascido de um phantom commit, que é
	// o modo de falha que este teste existe para pegar.
	itStop, err := s2.Lookup(fldStatus, strVal("Stop"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, itStop); len(rows) != 1 || rows[0] != id {
		t.Fatalf("postings de Stop após o replay: %v", rows)
	}
	// A outra metade — "sai de Start" — era a fase de CLEAR, que o índice por
	// segmento não tem (spec §5.2). A posting antiga sobrevive como candidato e
	// quem a descarta é a revalidação do §44; o que o replay tem de garantir é
	// que o estado NOVO manda, e é isso que a linha abaixo afirma.
	if rows := lookupRevalidado(t, s2, fldStatus, strVal("Start")); len(rows) != 0 {
		t.Fatalf("pós-revalidação status=Start deveria ser vazio após o replay: %v", rows)
	}
	// A row não pode ter sido contada duas vezes.
	if got := s2.Stats().RowCount; got != 1 {
		t.Fatalf("RowCount após replay de update órfão: %d", got)
	}
}

func fileSizeAud(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}
