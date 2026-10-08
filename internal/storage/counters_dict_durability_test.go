package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Os testes deste arquivo provam o fsync do dicionário de grupos no
// checkpoint: o grupo novo é só um append, e o fsync de todos os appends
// pendentes é a primeira coisa do ApplyOverlay de cada contador.
//
// # Como o crash é simulado
//
// Abandonar o Store em processo não perde nada — o page cache e o mmap
// compartilhado sobrevivem à morte do processo, e a reabertura veria tudo.
// O que um crash com queda de energia perde é o que nenhum fsync tornou
// durável, e é isso que os testes fazem EXPLICITAMENTE: o seam do fsync do
// dicionário registra o tamanho do grupos.dict depois de cada fsync bem-
// sucedido (medido pelo os.Stat, e não pela contabilidade do Dict), e o teste
// trunca o arquivo nesse tamanho antes de reabrir.
//
// # O oráculo
//
// Com teto e aposentadoria, "igual à aplicação contínua" é falso: a
// aposentadoria é aplicada na abertura ANTES do replay, e um grupo que o teto
// recusou antes do crash pode caber no replay (e vice-versa). Os ids também
// podem mudar. O oráculo é por GROUPKEY: o contador reaberto é igual a uma
// reconstrução DO ZERO sobre o estado final, a auditoria não acha divergência
// nem nos grupos nem nos agregados, não há órfã, e as linhas sem grupo
// internado são exatamente as que o teste sabe que o teto recusou.

// dictSyncRecorder instala o seam do fsync do dicionário de um contador e
// mantém, em synced, o tamanho do grupos.dict depois do último fsync bem-
// sucedido. Começa no tamanho do arquivo na instalação: o Open termina num
// checkpoint, que deixa o dicionário sincronizado.
type dictSyncRecorder struct {
	path   string
	synced int64
}

func recordDictSyncs(t *testing.T, s *Store, dir, name string) *dictSyncRecorder {
	t.Helper()
	r := &dictSyncRecorder{path: filepath.Join(dir, "indexes", diretorioOfCounters, name, "grupos.dict")}
	st, err := os.Stat(r.path)
	if err != nil {
		t.Fatal(err)
	}
	r.synced = st.Size()
	s.SetCounterDictSyncHookForTest(func(counterName string, sync func() error) error {
		if err := sync(); err != nil {
			return err
		}
		if counterName == name {
			st, err := os.Stat(r.path)
			if err != nil {
				return err
			}
			r.synced = st.Size()
		}
		return nil
	})
	return r
}

// losePowerAfterClose trunca o grupos.dict no último tamanho sincronizado: é
// o que sobra no disco de uma queda de energia depois da morte do processo.
func (r *dictSyncRecorder) losePower(t *testing.T, synced int64) {
	t.Helper()
	if err := os.Truncate(r.path, synced); err != nil {
		t.Fatal(err)
	}
}

// groupsByKey é o retrato do contador por groupKey: (total, refs) de cada
// grupo vivo. Ids não entram — eles podem mudar no replay.
func groupsByKey(c *counter.Counter) map[string][2]int64 {
	out := map[string][2]int64{}
	for id := uint32(1); id < c.Limit(); id++ {
		_, text, total, refs, live := c.Group(id)
		if live {
			out[string(text)] = [2]int64{total, refs}
		}
	}
	return out
}

// requireCounterEqualsRebuildFromScratch é o oráculo descrito no topo do
// arquivo.
func requireCounterEqualsRebuildFromScratch(t *testing.T, s *Store, name string, notInterned uint64) {
	t.Helper()
	c := counterChamado(t, s, name)
	if n := c.Stats().Orphans; n != 0 {
		t.Fatalf("%s: %d entradas órfãs na dobra da reabertura — o vetor durável apontava para id que o dicionário em disco não tem", name, n)
	}
	reports, err := s.AuditaCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range reports {
		if r.Name != name {
			continue
		}
		if r.DivergentGroups != 0 || r.DivergentAggregates != 0 {
			t.Fatalf("%s: auditoria com %d grupos e %d agregados divergentes: %+v", name, r.DivergentGroups, r.DivergentAggregates, r.Sample)
		}
		if r.NotInterned != notInterned {
			t.Fatalf("%s: %d linhas sem grupo internado, quer %d (as que o teto recusou)", name, r.NotInterned, notInterned)
		}
	}
	reopened := groupsByKey(c)
	if _, err := s.RebuildCounters(context.Background()); err != nil {
		t.Fatal(err)
	}
	rebuilt := groupsByKey(c)
	if len(reopened) != len(rebuilt) {
		t.Fatalf("%s: reaberto com %v, reconstruído do zero com %v", name, reopened, rebuilt)
	}
	for k, v := range rebuilt {
		if reopened[k] != v {
			t.Fatalf("%s[%s]: reaberto %v, reconstruído do zero %v", name, k, reopened[k], v)
		}
	}
}

// TestCheckpointSyncsTheCounterDictBeforeTheVectorAndTheMeasures: no trace do
// checkpoint, o passo do fsync do dicionário vem antes do overlay do vetor, do
// msync dele, do snapshot das medidas e do manifest. O passo existe mesmo com
// o dicionário limpo — o trace descreve a posição no protocolo.
func TestCheckpointSyncsTheCounterDictBeforeTheVectorAndTheMeasures(t *testing.T) {
	s := openStore(t, measureOptions(t.TempDir(), false, time.Minute))
	at := time.Now()
	mustPut(t, s, "x", 1, payloadServico(t, s, "x", "A", true, at), nil)
	var steps []string
	s.SetCheckpointTraceForTest(func(step string) { steps = append(steps, step) })
	defer s.SetCheckpointTraceForTest(nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	order := []string{"segSync", "syncDictContadores", "applyOverlayContadores", "msyncContadores", "counterMeasures", "manifest"}
	pos := -1
	for _, step := range order {
		i := slices.Index(steps, step)
		if i < 0 {
			t.Fatalf("o checkpoint não passou por %q: %v", step, steps)
		}
		if i < pos {
			t.Fatalf("%q fora de ordem no checkpoint: %v (quer a ordem %v)", step, steps, order)
		}
		pos = i
	}
}

// TestCrashBeforeTheCheckpointLosesOnlyUnreferencedGroups é a garantia do
// crash entre o append e o checkpoint: o dicionário perde a cauda não
// sincronizada, e a reabertura sai sem erro, sem órfã e igual à reconstrução
// do zero.
//
// O cenário cruza os dois casos em que o replay muda os ids e o conjunto de
// grupos: uma aposentadoria (aplicada na abertura ANTES do replay) e o teto
// (maxGroups 2). Antes do crash, D foi recusado (A e B vivos) e E internado
// depois de B sair; no replay, B já saiu quando D chega, D cabe, e é E quem
// fica de fora.
func TestCrashBeforeTheCheckpointLosesOnlyUnreferencedGroups(t *testing.T) {
	dir := t.TempDir()
	def := byOLT("aut", false)
	def.ceiling = 2
	def.retention = time.Hour
	s, rel := storeWithCounters(t, dir, def)
	rec := recordDictSyncs(t, s, dir, "aut")
	put := func(i int, olt string) {
		t.Helper()
		k := keyOfServico(i)
		prev, _, ok, _ := s.GetByKey(s.presente(), []byte(k), nil)
		var p *types.Record
		if ok {
			p = &prev
		}
		mustPut(t, s, k, int64(i), payloadServico(t, s, k, olt, true, rel.agora()), p)
	}
	put(1, "A")
	put(2, "B")
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	put(2, "A") // B esvazia
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rel.advance(2 * time.Hour)
	put(3, "D") // recusado: A e B vivos
	if n, err := s.RetireEmptyGroups(); err != nil || n != 1 {
		t.Fatalf("aposentadoria: %d, %v; quer 1 grupo (B)", n, err)
	}
	put(4, "E") // internado no lugar de B, sem fsync
	if _, ok := counterChamado(t, s, "aut").ByText([]byte("E")); !ok {
		t.Fatal("antes do crash, E tinha de estar internado")
	}
	synced := rec.synced
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rec.losePower(t, synced)

	s2, _ := storeWithCounters(t, dir, def)
	if _, ok := counterChamado(t, s2, "aut").ByText([]byte("D")); !ok {
		t.Fatal("no replay, com B já aposentado, D cabe no teto")
	}
	// E é a única linha sem grupo: o teto (A e D vivos) a recusa no replay.
	requireCounterEqualsRebuildFromScratch(t, s2, "aut", 1)
}

// TestCrashBeforeTheCheckpointKeepsEveryCheckpointedGroup é a mesma garantia
// sem aposentadoria no meio — e é a metade que denuncia um checkpoint que não
// sincroniza o dicionário: no cenário acima, o fsync da aposentadoria levaria
// junto os grupos que o checkpoint deixou de sincronizar, e esconderia a
// falta. Aqui os grupos do primeiro checkpoint só ficam duráveis pelo fsync do
// próprio checkpoint, e os do segundo intervalo são a cauda que o crash leva.
func TestCrashBeforeTheCheckpointKeepsEveryCheckpointedGroup(t *testing.T) {
	dir := t.TempDir()
	def := byOLT("aut", true)
	s, rel := storeWithCounters(t, dir, def)
	rec := recordDictSyncs(t, s, dir, "aut")
	for i, olt := range []string{"A", "B", "C", "A"} {
		k := keyOfServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, olt, i%2 == 0, rel.agora()), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i, olt := range []string{"D", "E", "B"} {
		k := keyOfServico(10 + i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, olt, true, rel.agora()), nil)
	}
	synced := rec.synced
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rec.losePower(t, synced)

	s2, _ := storeWithCounters(t, dir, def)
	exigeTotal(t, s2, "aut", "A", 1, 2)
	exigeTotal(t, s2, "aut", "D", 1, 1)
	requireCounterEqualsRebuildFromScratch(t, s2, "aut", 0)
}

// TestCrashBetweenTheDictSyncAndTheManifestIsRecovered é a garantia do crash
// DEPOIS do fsync do dicionário e antes do manifest, em cada fronteira: logo
// depois do fsync (nenhum Set no vetor), depois do overlay no mmap e depois do
// msync do vetor — este último com o vetor durável à frente do manifest.
func TestCrashBetweenTheDictSyncAndTheManifestIsRecovered(t *testing.T) {
	for _, stage := range []string{"syncDictContadores", "applyOverlayContadores", "msyncContadores"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			def := byOLT("aut", true)
			s, rel := storeWithCounters(t, dir, def)
			rec := recordDictSyncs(t, s, dir, "aut")
			for i, olt := range []string{"A", "B", "C", "A", "B"} {
				k := keyOfServico(i)
				mustPut(t, s, k, 1, payloadServico(t, s, k, olt, i%2 == 0, rel.agora()), nil)
			}
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for i, olt := range []string{"D", "E", "A"} {
				k := keyOfServico(10 + i)
				mustPut(t, s, k, 1, payloadServico(t, s, k, olt, true, rel.agora()), nil)
			}
			s.SetCheckpointTraceForTest(func(step string) {
				if step == stage {
					panic(stage)
				}
			})
			func() {
				defer func() {
					if v := recover(); v != stage {
						t.Fatalf("interrupção = %v, quer %q", v, stage)
					}
				}()
				_ = s.Checkpoint()
			}()
			synced := rec.synced
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			rec.losePower(t, synced)

			s2, _ := storeWithCounters(t, dir, def)
			exigeTotal(t, s2, "aut", "D", 1, 1)
			exigeTotal(t, s2, "aut", "E", 1, 1)
			requireCounterEqualsRebuildFromScratch(t, s2, "aut", 0)
		})
	}
}

// TestColdBuildCrashBeforeItsCheckpointIsRebuiltOnReopen: a construção a frio
// na abertura interna cada grupo SEM fsync, e o contador construído só é
// adotado pelo checkpoint do fim do Open, que sincroniza o dicionário antes do
// ApplyOverlay. Um crash antes dele — com a cauda do dicionário perdida — ou
// depois do overlay e antes do manifest não pode deixar a reabertura adotar
// nada: ela reconstrói, sem órfã.
//
// Os arquivos do contador são apagados com o manifest ainda declarando-o, que
// é o ramo em que a construção acontece IN PLACE e nada além do marcador de
// construção separa o vetor parcial de um vetor adotável (no contador novo, o
// manifest sem ele já forçaria a reconstrução).
func TestColdBuildCrashBeforeItsCheckpointIsRebuiltOnReopen(t *testing.T) {
	for _, stage := range []string{"segSync", "applyOverlayContadores"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			def := byOLT("aut", true)
			s, rel := storeWithCounters(t, dir, def)
			for i, olt := range []string{"A", "B", "C", "A", "D"} {
				k := keyOfServico(i)
				mustPut(t, s, k, 1, payloadServico(t, s, k, olt, i%2 == 0, rel.agora()), nil)
			}
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"grupos.dict", "grupos.idx"} {
				if err := os.Remove(filepath.Join(dir, "indexes", diretorioOfCounters, "aut", name)); err != nil {
					t.Fatal(err)
				}
			}

			var rec *dictSyncRecorder
			opts := optionsWithCounters(dir, def)
			opts.traceRecovery = func(s *Store) {
				rec = recordDictSyncs(t, s, dir, "aut")
				s.SetCheckpointTraceForTest(func(step string) {
					if step == stage {
						panic(stage)
					}
				})
			}
			func() {
				defer func() {
					if v := recover(); v != stage {
						t.Fatalf("a abertura tinha de morrer em %q; recover = %v", stage, v)
					}
				}()
				if st, err := Open(opts); err == nil {
					_ = st.Close()
				}
			}()
			rec.losePower(t, rec.synced)

			s2, _ := storeWithCounters(t, dir, def)
			if n := s2.CountersBuiltForTest(); n != 1 {
				t.Fatalf("a reabertura reconstruiu %d contadores; a construção interrompida tem de ser refeita", n)
			}
			exigeTotal(t, s2, "aut", "A", 1, 2)
			exigeTotal(t, s2, "aut", "D", 1, 1)
			requireCounterEqualsRebuildFromScratch(t, s2, "aut", 0)
		})
	}
}

// TestFailedCounterDictSyncFailsTheWriter: o fsync do dicionário que falha no
// checkpoint deixa o writer falho (stickyErr) e não grava manifest. Tratá-lo
// como o erro retentável do Set deixaria o próximo checkpoint "passar" com
// grupos que o kernel já descartou (fsyncgate).
func TestFailedCounterDictSyncFailsTheWriter(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	mustPut(t, s, "k1", 1, payloadServico(t, s, "k1", "A", true, rel.agora()), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "checkpoint", "manifest.bin")
	before, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	s.SetCounterDictSyncHookForTest(func(string, func() error) error { return errors.New("EIO simulado") })
	mustPut(t, s, "k2", 1, payloadServico(t, s, "k2", "B", true, rel.agora()), nil)
	err = s.Checkpoint()
	if !errors.Is(err, ErrWriterFailed) || !errors.Is(err, counter.ErrDictSync) {
		t.Fatalf("checkpoint com o fsync do dicionário falho = %v; quer ErrWriterFailed com ErrDictSync na cadeia", err)
	}
	after, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("o checkpoint com o fsync do dicionário falho gravou manifest")
	}
	s.SetCounterDictSyncHookForTest(nil)
	if err := s.Checkpoint(); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("a retentativa do checkpoint = %v; o writer tinha de continuar falho", err)
	}
	if _, _, err := s.Put([]byte("k3"), 1, 0, payloadServico(t, s, "k3", "A", true, rel.agora()), nil); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("Put depois do fsync falho = %v; quer ErrWriterFailed", err)
	}
}

// TestFailedRetirementSyncFailsTheWriter: o fsync do registro de
// aposentadoria leva junto os grupos pendentes, e a falha dele é a mesma porta
// do fsyncgate. Só parar o laço — o comportamento antigo — deixaria o próximo
// checkpoint "passar" com os appends perdidos.
func TestFailedRetirementSyncFailsTheWriter(t *testing.T) {
	c := byOLT("aut", true)
	c.retention = time.Hour
	s, rel := storeWithCounters(t, t.TempDir(), c)
	k := keyOfServico(0)
	mustPut(t, s, k, 1, payloadServico(t, s, k, "X", true, rel.agora()), nil)
	prev, _, _, _ := s.GetByKey(s.presente(), []byte(k), nil)
	if _, _, err := s.Put([]byte(k), 2, types.FlagTombstone, payloadServico(t, s, k, "", false, rel.agora()), &prev); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rel.advance(2 * time.Hour)
	s.SetCounterDictSyncHookForTest(func(string, func() error) error { return errors.New("EIO simulado") })
	if _, err := s.RetireEmptyGroups(); !errors.Is(err, ErrWriterFailed) || !errors.Is(err, counter.ErrDictSync) {
		t.Fatalf("aposentadoria com o fsync falho = %v; quer ErrWriterFailed com ErrDictSync na cadeia", err)
	}
	s.SetCounterDictSyncHookForTest(nil)
	if err := s.Checkpoint(); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("checkpoint depois da aposentadoria falha = %v; o writer tinha de estar falho", err)
	}
}
