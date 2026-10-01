package counter

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// syncObserver é o dublê do arquivo do dicionário que vê cada fsync: conta as
// chamadas, roda um observador ANTES de sincronizar (para o teste olhar o vetor
// naquele instante) e registra o tamanho do arquivo depois de cada fsync bem-
// sucedido — o prefixo que sobreviveria a uma queda de energia, medido do lado
// de fora, e não pela contabilidade do próprio Dict.
type syncObserver struct {
	dictFile
	path   string
	syncs  int
	before func()
	synced *int64
}

func (o *syncObserver) Sync() error {
	o.syncs++
	if o.before != nil {
		o.before()
	}
	if err := o.dictFile.Sync(); err != nil {
		return err
	}
	if o.synced != nil {
		st, err := os.Stat(o.path)
		if err != nil {
			return err
		}
		*o.synced = st.Size()
	}
	return nil
}

// TestApplyOverlaySyncsTheDictOnceAndBeforeTheFirstSet é a ordem estrutural do
// fsync no checkpoint: o ApplyOverlay sincroniza o dicionário UMA vez, e antes
// de gravar qualquer id no vetor — o dublê olha o vetor no instante do fsync e
// o encontra zerado. Um ApplyOverlay sem grupo novo desde o anterior não paga
// fsync nenhum.
func TestApplyOverlaySyncsTheDictOnceAndBeforeTheFirstSet(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", false, 100), false)
	obs := &syncObserver{}
	c.dict.wrapFileForTest(func(f dictFile) dictFile {
		obs.dictFile = f
		return obs
	})
	olts := []string{"A", "B", "C"}
	for r, olt := range olts {
		if err := c.Transition(types.RowID(r), payload(t, olt, 1, 1, false), false); err != nil {
			t.Fatal(err)
		}
	}
	if obs.syncs != 0 {
		t.Fatalf("%d fsyncs na internação de três grupos novos; o grupo novo não pode pagar fsync", obs.syncs)
	}
	obs.before = func() {
		for r := range olts {
			if v, _ := c.vector.Get(types.RowID(r)); v != WithoutGroup {
				t.Errorf("no instante do fsync do dicionário o vetor já tinha a linha %d (entrada %d): o id chegou ao vetor antes de ficar durável", r, v)
			}
		}
	}
	if err := c.ApplyOverlay(nil); err != nil {
		t.Fatal(err)
	}
	if obs.syncs != 1 {
		t.Fatalf("%d fsyncs no ApplyOverlay com o dicionário sujo; quer exatamente 1", obs.syncs)
	}
	for r := range olts {
		if v, _ := c.vector.Get(types.RowID(r)); v&^conditionBit != uint32(r+1) {
			t.Fatalf("linha %d no vetor = %d depois do ApplyOverlay, quer %d", r, v, r+1)
		}
	}
	// Só transição entre grupos que já existem: o dicionário continua limpo.
	if err := c.Transition(0, payload(t, "B", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(nil); err != nil {
		t.Fatal(err)
	}
	if obs.syncs != 1 {
		t.Fatalf("o ApplyOverlay sem grupo novo fez fsync (total %d); dicionário limpo não toca o arquivo", obs.syncs)
	}
}

// TestApplyOverlayWithAFailedDictSyncWritesNothing: o fsync que falha volta
// como ErrDictSync e NENHUM Set acontece — o vetor continua sem o id, e o
// overlay inteiro fica onde estava. É o que deixa o Store tratar o erro como
// writer falho sem que o vetor tenha recebido id não durável.
func TestApplyOverlayWithAFailedDictSyncWritesNothing(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", false, 100), false)
	c.dict.wrapFileForTest(func(f dictFile) dictFile {
		return &failingSync{dictFile: f, err: errors.New("EIO simulado")}
	})
	for r, olt := range []string{"A", "B"} {
		if err := c.Transition(types.RowID(r), payload(t, olt, 1, 1, false), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.ApplyOverlay(nil); !errors.Is(err, ErrDictSync) {
		t.Fatalf("ApplyOverlay com fsync falho = %v, quer ErrDictSync", err)
	}
	for r := types.RowID(0); r < 2; r++ {
		if v, _ := c.vector.Get(r); v != WithoutGroup {
			t.Fatalf("a linha %d foi ao vetor (entrada %d) com o fsync do dicionário falho", r, v)
		}
	}
	if n := c.OverlayLen(); n != 2 {
		t.Fatalf("overlay com %d entradas depois do fsync falho, quer as 2 intactas", n)
	}
}

// TestProcessCrashTailIsMadeDurableBeforeTheVectorReferencesIt é o cenário que
// a abertura sincronizada fecha. O processo 1 interna três grupos e morre antes
// do checkpoint: a cauda do dicionário fica no page cache. O processo 2 a lê
// como se estivesse em disco, o replay resolve as linhas aos MESMOS ids, e o
// checkpoint leva os ids ao vetor sem nada a sincronizar no dicionário. Uma
// queda de energia depois — simulada truncando o grupos.dict no último tamanho
// que um fsync tornou durável, medido pelo dublê — não pode deixar o vetor
// apontando para ids que o disco não tem.
func TestProcessCrashTailIsMadeDurableBeforeTheVectorReferencesIt(t *testing.T) {
	dir := t.TempDir()
	def := defByOLT("aut", false, 100)
	dictPath := filepath.Join(dir, dictFileName)
	var synced int64
	restore := wrapDictOpenForTest(func(f dictFile) dictFile {
		return &syncObserver{dictFile: f, path: dictPath, synced: &synced}
	})
	defer restore()
	olts := []string{"A", "B", "C"}
	transitionAll := func(c *Counter) {
		t.Helper()
		for r, olt := range olts {
			if err := c.Transition(types.RowID(r), payload(t, olt, 1, 1, false), false); err != nil {
				t.Fatal(err)
			}
		}
	}

	c1, err := Open(dir, def, 1024, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	transitionAll(c1)
	c1.abandonForTest()

	c2, err := Open(dir, def, 1024, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Fold(); err != nil {
		t.Fatal(err)
	}
	transitionAll(c2) // o replay do tail
	if err := c2.ApplyOverlay(nil); err != nil {
		t.Fatal(err)
	}
	if err := c2.Msync(); err != nil {
		t.Fatal(err)
	}
	c2.abandonForTest()

	if err := os.Truncate(dictPath, synced); err != nil {
		t.Fatal(err)
	}
	c3, err := Open(dir, def, 1024, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c3.Close() }()
	if err := c3.Fold(); err != nil {
		t.Fatal(err)
	}
	if n := c3.Stats().Orphans; n != 0 {
		t.Fatalf("%d entradas órfãs depois da queda de energia: o vetor apontava para ids que só existiam no page cache", n)
	}
	for _, olt := range olts {
		if _, refs := totalOf(t, c3, olt); refs != 1 {
			t.Fatalf("grupo %s com %d linhas depois da reabertura, quer 1", olt, refs)
		}
	}
}
