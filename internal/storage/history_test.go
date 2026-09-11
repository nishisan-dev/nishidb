package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// pairHistory abre o dataset base e a tabela irmã, compartilhando o
// dicionário — que é como o servidor os abre.
func pairHistory(t *testing.T) (base, hist *Store) {
	t.Helper()
	base = openStore(t, Options{})
	hist = openStore(t, Options{
		Dir:              t.TempDir(),
		Dataset:          "events_history",
		Mode:             HistoryMode,
		SharedDictionary: base.FieldDict(),
	})
	return base, hist
}

func TestApendaGravaUmaLinhaVisivel(t *testing.T) {
	base, hist := pairHistory(t)
	p := statusPayload(t, base, "a", "novo", 1)
	id, err := hist.Apenda(p)
	if err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	rec, _, ok, err := hist.GetByRowID(hist.presente(), id, nil)
	if err != nil || !ok {
		t.Fatalf("GetByRowID = (%v, %v)", ok, err)
	}
	if rec.RowID != id {
		t.Fatalf("RowID = %d, quer %d", rec.RowID, id)
	}
	if len(rec.Key) != 0 {
		t.Fatalf("a linha do histórico tem key %q; ela não devia ter nenhuma", rec.Key)
	}
}

// É a propriedade central da tabela: o mesmo identifier duas vezes são DUAS
// linhas. No dataset base seriam uma, mergeada.
func TestApendaNaoDeduplica(t *testing.T) {
	base, hist := pairHistory(t)
	p := statusPayload(t, base, "mesmo", "novo", 1)
	primeiro, err := hist.Apenda(p)
	if err != nil {
		t.Fatalf("primeiro Apenda: %v", err)
	}
	segundo, err := hist.Apenda(p)
	if err != nil {
		t.Fatalf("segundo Apenda: %v", err)
	}
	if primeiro == segundo {
		t.Fatalf("os dois eventos foram para a mesma row %d", primeiro)
	}
	if n := countVivas(t, hist); n != 2 {
		t.Fatalf("%d linhas vivas, quer 2", n)
	}
}

func countVivas(t *testing.T, s *Store) int {
	t.Helper()
	it := s.ScanLive(s.presente())
	defer func() { _ = it.Close() }()
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("ScanLive: %v", err)
	}
	return n
}

func TestPutNoHistoricoEhRecusado(t *testing.T) {
	base, hist := pairHistory(t)
	p := statusPayload(t, base, "a", "novo", 1)
	if _, _, err := hist.Put([]byte("a"), 0, 0, p, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Put = %v, quer ErrInvalidArgument", err)
	}
}

func TestApendaNoMaterializadoEhRecusado(t *testing.T) {
	base := openStore(t, Options{})
	p := statusPayload(t, base, "a", "novo", 1)
	if _, err := base.Apenda(p); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Apenda = %v, quer ErrInvalidArgument", err)
	}
}

// Sem o dicionário do base os FieldIDs seriam outros e todo record
// decodificaria colunas trocadas, sem erro e com o CRC batendo.
func TestModoHistoricoSemDicionarioCompartilhadoEhRecusado(t *testing.T) {
	_, err := Open(Options{Dir: t.TempDir(), SegmentSize: 1 << 20, Mode: HistoryMode})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open = %v, quer ErrInvalidArgument", err)
	}
}

// A expiração por inatividade renova o prazo a cada escrita, e no histórico
// nenhuma linha é reescrita: a regra nunca teria a segunda escrita.
func TestModoHistoricoComTTLAtivoEhRecusado(t *testing.T) {
	base := openStore(t, Options{})
	_, err := Open(Options{
		Dir: t.TempDir(), SegmentSize: 1 << 20, Mode: HistoryMode,
		SharedDictionary: base.FieldDict(),
		ActiveTTL:        true, WriteInstantFieldName: "_writtenAt", TTLFieldName: "_expiresIn",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open = %v, quer ErrInvalidArgument", err)
	}
}

// Abrir um histórico como materializado leria um log de eventos como se fosse
// estado corrente. O contrário é pior: sem primary, o primeiro Apenda
// duplicaria uma entidade de forma durável.
func TestModoGravadoEhImutavel(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist, err := Open(Options{Dir: dir, SegmentSize: 1 << 20, Mode: HistoryMode, SharedDictionary: base.FieldDict()})
	if err != nil {
		t.Fatalf("Open do histórico: %v", err)
	}
	if _, err := hist.Apenda(statusPayload(t, base, "a", "novo", 1)); err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	if err := hist.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := hist.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = Open(Options{Dir: dir, SegmentSize: 1 << 20, SharedDictionary: base.FieldDict()})
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("reabrir como materializado = %v, quer ErrDatasetCorrupt", err)
	}
}

func TestOHistoricoNaoCriaPrimaryNemSequences(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist := openStore(t, Options{Dir: dir, Mode: HistoryMode, SharedDictionary: base.FieldDict()})
	if _, err := hist.Apenda(statusPayload(t, base, "a", "novo", 1)); err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	if err := hist.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for _, name := range []string{"primary.idx", "sequences.idx", "expiry.idx", "prazos.idx"} {
		if _, err := os.Stat(filepath.Join(dir, "indexes", name)); !os.IsNotExist(err) {
			t.Errorf("o histórico criou %s (err = %v)", name, err)
		}
	}
	// A location continua, porque é ela que resolve rowId → bytes.
	if _, err := os.Stat(filepath.Join(dir, "indexes", "locations.idx")); err != nil {
		t.Errorf("o histórico não tem locations.idx: %v", err)
	}
}

// O replay do tail é o que repõe o que foi apendado depois do último
// checkpoint. No histórico ele não tem primary a reconciliar nem sequence a
// repor, e é justamente por isso que precisa de teste próprio.
func TestHistoricoRecuperaOTailSemCheckpoint(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist, err := Open(Options{Dir: dir, SegmentSize: 1 << 20, Mode: HistoryMode, SharedDictionary: base.FieldDict()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 5 {
		if _, err := hist.Apenda(statusPayload(t, base, "a", "novo", int64(i))); err != nil {
			t.Fatalf("Apenda %d: %v", i, err)
		}
	}
	// Fecha SEM checkpoint: tudo o que foi escrito vive só no tail.
	if err := hist.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	back := openStore(t, Options{Dir: dir, Mode: HistoryMode, SharedDictionary: base.FieldDict()})
	if n := countVivas(t, back); n != 5 {
		t.Fatalf("%d linhas depois do replay, quer 5", n)
	}
}

// A marca-d'água é durável: sem isso, o restart devolveria à consulta as
// linhas que a retenção já tinha tirado dela.
func TestMarcaDaguaSobreviveAoRestart(t *testing.T) {
	base := openStore(t, Options{})
	dir := t.TempDir()
	hist, err := Open(Options{Dir: dir, SegmentSize: 1 << 20, Mode: HistoryMode, SharedDictionary: base.FieldDict()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 4 {
		if _, err := hist.Apenda(statusPayload(t, base, "a", "novo", int64(i))); err != nil {
			t.Fatalf("Apenda %d: %v", i, err)
		}
	}
	hist.rowIDBase.Store(2)
	if n := countVivas(t, hist); n != 2 {
		t.Fatalf("%d linhas visíveis com a marca em 2, quer 2", n)
	}
	if err := hist.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := hist.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	back := openStore(t, Options{Dir: dir, Mode: HistoryMode, SharedDictionary: base.FieldDict()})
	if got := back.RowIDBase(); got != 2 {
		t.Fatalf("RowIDBase = %d depois do restart, quer 2", got)
	}
	if n := countVivas(t, back); n != 2 {
		t.Fatalf("%d linhas depois do restart, quer 2", n)
	}
}

// A linha purgada some da consulta por rowId também, e não só da varredura.
func TestLinhaAbaixoDaMarcaSomeDoGetByRowID(t *testing.T) {
	base, hist := pairHistory(t)
	id, err := hist.Apenda(statusPayload(t, base, "a", "novo", 1))
	if err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	hist.rowIDBase.Store(uint64(id) + 1)
	if _, _, ok, err := hist.GetByRowID(hist.presente(), id, nil); err != nil || ok {
		t.Fatalf("GetByRowID = (%v, %v), quer (false, nil)", ok, err)
	}
}

func TestSequenceDoHistoricoEhZero(t *testing.T) {
	base, hist := pairHistory(t)
	id, err := hist.Apenda(statusPayload(t, base, "a", "novo", 1))
	if err != nil {
		t.Fatalf("Apenda: %v", err)
	}
	seq, presenca := hist.Sequence(id)
	if seq != 0 {
		t.Errorf("Sequence = %d, quer 0", seq)
	}
	if presenca != types.PresenceLive {
		t.Errorf("presença = %v, quer viva", presenca)
	}
}
