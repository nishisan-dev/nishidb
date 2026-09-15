package countermetrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

// Janela incompleta não é NULL: com uma amostra só, o agregado é o daquela
// amostra. NULL fica para o grupo sem amostra nenhuma na janela.
func TestSingleSamplePublishesItsOwnAggregate(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 7)
	s := h.open(t)
	m := mustMetric(t, s)
	wantNoValue(t, m, id)

	sampleOnce(t, s)
	wantAggregate(t, m, id, 1, 7, 7, 7)
}

// A janela são as 60 épocas terminando na corrente: a amostra da época E ainda
// conta na E+59 e sai na E+60, e leva junto o max que só ela tinha.
func TestSlidingWindowDropsTheSampleAfterSixtyBuckets(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 100)
	s := h.open(t)
	m := mustMetric(t, s)

	sampleOnce(t, s) // E
	h.clock.advance(time.Second)
	h.counter.setTotal(id, 10)
	sampleOnce(t, s) // E+1
	wantAggregate(t, m, id, 2, 55, 10, 100)

	h.clock.advance(58 * time.Second)
	sampleOnce(t, s) // E+59
	wantAggregate(t, m, id, 3, 40, 10, 100)

	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+60
	wantAggregate(t, m, id, 3, 10, 10, 10)
}

// A janela anda com a época mesmo quando o balde aberto está vazio: depois de
// uma restauração em E+59, a primeira passada em E+60 não tem balde aberto a
// fechar, e ainda assim a amostra de E tem de sair do cache.
func TestClosedCacheMovesWithTheEpochEvenWhenTheOpenBucketIsEmpty(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 100)
	s := h.open(t)
	sampleOnce(t, s) // E
	h.clock.advance(time.Second)
	h.counter.setTotal(id, 10)
	sampleOnce(t, s) // E+1
	closeSampler(t, s)

	h.clock.advance(58 * time.Second) // E+59
	reopened := h.open(t)
	m := mustMetric(t, reopened)
	wantAggregate(t, m, id, 2, 55, 10, 100)

	h.clock.advance(time.Second) // E+60
	h.counter.setTotal(id, 7)
	sampleOnce(t, reopened)
	wantAggregate(t, m, id, 2, 8.5, 7, 10)
}

func TestRetiredGroupHasNoValueAndReturningKeyStartsFromZero(t *testing.T) {
	h := newHarness(t)
	first := h.counter.add("OLT_A", 50)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	h.clock.advance(time.Second)
	sampleOnce(t, s)
	wantAggregate(t, m, first, 2, 50, 50, 50)

	position := m.state.positions.load(first)
	h.counter.setLive(first, false)
	h.clock.advance(time.Second)
	sampleOnce(t, s)
	wantNoValue(t, m, first)
	if got := m.file.Identity(position); got != (metricwindow.Identity{}) {
		t.Errorf("identidade em disco da posição do grupo aposentado = %+v, esperada vazia", got)
	}
	if got := m.state.positions.load(first); got != 0 {
		t.Errorf("o grupo aposentado continua na posição %d, esperado sem posição", got)
	}
	if groups, _, _, _ := m.Usage(); groups != 0 {
		t.Errorf("Usage.groups = %d depois de aposentar o único grupo, esperado 0", groups)
	}

	second := h.counter.add("OLT_A", 3)
	h.clock.advance(time.Second)
	sampleOnce(t, s)
	wantAggregate(t, m, second, 1, 3, 3, 3)
	wantNoValue(t, m, first)
}

// maxGroups limita os grupos VIVOS, e o dicionário nunca reaproveita o id de
// um grupo aposentado: com rotatividade, Limit() passa de maxGroups+1. A
// abertura e as passadas seguem, e publicam para os vivos.
func TestRetiredIdsBeyondMaxGroupsKeepTheSamplerRunning(t *testing.T) {
	h := newHarness(t)
	h.maxGroups = 2
	var live []uint32
	for i := range 9 {
		id := h.counter.add(fmt.Sprintf("OLT_%d", i), int64(i+1)*10)
		if i == 3 || i == 8 {
			live = append(live, id)
			continue
		}
		h.counter.setLive(id, false)
	}
	if got := h.counter.Limit(); got != 10 {
		t.Fatalf("Limit() = %d, esperado 10 para o cenário", got)
	}

	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	h.clock.advance(time.Second)
	sampleOnce(t, s)
	wantAggregate(t, m, live[0], 2, 40, 40, 40)
	wantAggregate(t, m, live[1], 2, 90, 90, 90)
	for id := uint32(1); id < 10; id++ {
		if id != live[0] && id != live[1] {
			wantNoValue(t, m, id)
		}
	}

	next := h.counter.add("OLT_new", 5)
	h.clock.advance(time.Second)
	sampleOnce(t, s)
	wantAggregate(t, m, next, 1, 5, 5, 5)
}

// Contador reconstruído: o mesmo id passa a ser de outra chave. Nem o heap nem
// o disco podem entregar ao dono novo os baldes do antigo.
func TestRebuiltCounterReusingAnIdDoesNotLeakTheOldBuckets(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("K1", 1000)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	h.clock.advance(time.Second)
	sampleOnce(t, s) // fecha e grava o balde de K1
	wantAggregate(t, m, id, 2, 1000, 1000, 1000)

	h.counter.setKey(id, "K2")
	h.counter.setTotal(id, 5)
	h.clock.advance(time.Second)
	sampleOnce(t, s)
	wantAggregate(t, m, id, 1, 5, 5, 5)

	closeSampler(t, s)
	h.clock.advance(time.Second)
	wantAggregate(t, mustMetric(t, h.open(t)), id, 1, 5, 5, 5)
}

// collidingKeys acha, por força bruta, duas chaves de hashes diferentes com a
// mesma marca de dono de 16 bits.
func collidingKeys(t *testing.T) (string, string) {
	t.Helper()
	seen := make(map[uint16]string)
	for i := range 1 << 20 {
		k := fmt.Sprintf("OLT_%d", i)
		h := keyHash([]byte(k))
		tag := metricwindow.OwnerTag(h)
		if other, ok := seen[tag]; ok && keyHash([]byte(other)) != h {
			return other, k
		}
		seen[tag] = k
	}
	t.Fatal("nenhuma colisão de marca de dono em 2^20 chaves")
	return "", ""
}

// Quando as marcas de 16 bits de duas chaves colidem, a marca não separa os
// baldes do dono antigo de um id reaproveitado: quem separa é validFromEpoch.
func TestClosedBucketsBeforeValidFromEpochAreExcludedEvenWithCollidingOwnerTags(t *testing.T) {
	k1, k2 := collidingKeys(t)
	h := newHarness(t)
	id := h.counter.add(k1, 1000)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s) // E
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+1: grava o balde E de k1
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+2: grava o balde E+1 de k1

	h.counter.setKey(id, k2)
	h.counter.setTotal(id, 5)
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+3: k2 adota o id, validFromEpoch = E+3
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+4: relê os slots, e os baldes E e E+1 trazem a marca de k2
	wantAggregate(t, m, id, 2, 5, 5, 5)

	closeSampler(t, s)
	h.clock.advance(time.Second)
	wantAggregate(t, mustMetric(t, h.open(t)), id, 2, 5, 5, 5)
}

// A troca de dono dentro da MESMA época é o único caso em que o balde aberto
// do dono antigo, gravado com a marca do novo, passaria pelas duas defesas do
// formato (época >= validFromEpoch e marca igual à da identidade). O balde é
// descartado, nunca gravado; um amostrador em modo leitura sobre as mesmas
// páginas é o que uma restauração depois de um crash veria.
func TestOwnerSwapWithinTheSameEpochDiscardsTheOldOpenBucket(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("K1", 1000)
	s := h.open(t)
	m := mustMetric(t, s)
	for range 3 {
		sampleOnce(t, s)
	}
	h.counter.setKey(id, "K2")
	h.counter.setTotal(id, 5)
	sampleOnce(t, s)
	wantAggregate(t, m, id, 1, 5, 5, 5)
	if got := m.columnWrites.Load(); got != 0 {
		t.Errorf("a troca de dono gravou %d balde(s); o balde aberto do dono antigo tem de ser descartado", got)
	}

	wantNoValue(t, mustMetric(t, h.openReadOnly(t)), id)
}

func TestRestartWithinTheWindowRestoresBeforeAnySample(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 10)
	s := h.open(t)
	sampleOnce(t, s)
	h.clock.advance(time.Second)
	h.counter.setTotal(id, 30)
	sampleOnce(t, s)
	closeSampler(t, s)

	h.clock.advance(10 * time.Second)
	reopened := h.open(t)
	m := mustMetric(t, reopened)
	wantAggregate(t, m, id, 2, 20, 10, 30)
	if got := reopened.RestoredGroups(); got != 1 {
		t.Errorf("RestoredGroups = %d, esperado 1", got)
	}
	if groups, _, _, sampledAt := m.Usage(); groups != 1 || sampledAt != 0 {
		t.Errorf("Usage depois da restauração: groups %d, sampledAt %d; esperado 1 e 0 (restaurar não é amostrar)", groups, sampledAt)
	}
}

func TestRestartBeyondTheWindowPublishesNoValue(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 10)
	s := h.open(t)
	sampleOnce(t, s) // E
	h.clock.advance(time.Second)
	sampleOnce(t, s) // E+1
	closeSampler(t, s)

	// E+61: o balde mais novo (E+1) está 60 épocas atrás, fora da janela.
	h.clock.advance(time.Duration(schema.MetricWindowBuckets) * time.Second)
	reopened := h.open(t)
	m := mustMetric(t, reopened)
	wantNoValue(t, m, id)
	if got := reopened.RestoredGroups(); got != 0 {
		t.Errorf("RestoredGroups = %d, esperado 0", got)
	}

	h.counter.setTotal(id, 4)
	sampleOnce(t, reopened)
	wantAggregate(t, m, id, 1, 4, 4, 4)
}

func TestRestartWithinTheSameEpochReloadsTheOpenBucket(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 10)
	s := h.open(t)
	sampleOnce(t, s)
	h.clock.advance(200 * time.Millisecond)
	h.counter.setTotal(id, 20)
	sampleOnce(t, s)
	closeSampler(t, s)

	h.clock.advance(300 * time.Millisecond)
	reopened := h.open(t)
	m := mustMetric(t, reopened)
	wantAggregate(t, m, id, 2, 15, 10, 20)

	h.counter.setTotal(id, 30)
	sampleOnce(t, reopened)
	wantAggregate(t, m, id, 3, 20, 10, 30)
}

// Identidade de K1 em disco com um balde gravado sob a marca de K2 (a página
// de baldes do dono novo chegou ao disco e a de identidade não): o balde é
// ignorado — como fechado, no cache, e como o balde aberto que a restauração
// recarregaria na época corrente. Os casos de controle, com a marca da própria
// chave, provam que o teste distingue: sem eles, uma restauração que ignorasse
// tudo passaria.
func TestRestoreIgnoresBucketsWrittenUnderAnotherOwnerTag(t *testing.T) {
	k1, k2 := keyHash([]byte("K1")), keyHash([]byte("K2"))
	if metricwindow.OwnerTag(k1) == metricwindow.OwnerTag(k2) {
		t.Fatalf("K1 e K2 têm a mesma marca de dono; escolha outras chaves para o teste")
	}
	cases := []struct {
		name string
		// bucketOffset é a época do balde relativa ao validFromEpoch. A
		// restauração acontece em +2: o balde +1 é fechado, e o +2 é o da época
		// corrente, que viraria o balde aberto.
		bucketOffset uint32
		bucketOwner  uint64
		wantSamples  uint64
	}{
		{name: "closed bucket with foreign owner tag", bucketOffset: 1, bucketOwner: k2, wantSamples: 0},
		{name: "closed bucket with own owner tag (control)", bucketOffset: 1, bucketOwner: k1, wantSamples: 4},
		{name: "open bucket with foreign owner tag", bucketOffset: 2, bucketOwner: k2, wantSamples: 0},
		{name: "open bucket with own owner tag (control)", bucketOffset: 2, bucketOwner: k1, wantSamples: 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			id := h.counter.add("K1", 0)
			epoch := h.clock.epoch()
			if err := os.MkdirAll(filepath.Dir(h.path()), 0o755); err != nil {
				t.Fatal(err)
			}
			// Source 1 é o código de disco da fonte total, escrito à mão: o
			// arquivo tem de ser adotado, e não recriado, pelo amostrador.
			f, _, err := metricwindow.Open(h.path(), metricwindow.Header{BucketCount: 60, BucketWidthMs: 1000, Source: 1}, 2, fileCapacityLimit)
			if err != nil {
				t.Fatal(err)
			}
			f.SetIdentity(id, metricwindow.Identity{KeyHash: k1, ValidFromEpoch: uint64(epoch)})
			bucketEpoch := epoch + c.bucketOffset
			f.WriteBucket(bucketEpoch%60, id, metricwindow.Bucket{
				Epoch: bucketEpoch, Owner: metricwindow.OwnerTag(c.bucketOwner), Count: 4, Sum: 40, Min: 10, Max: 10,
			})
			if err := f.Msync(); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			h.clock.advance(2 * time.Second)
			s := h.open(t)
			if resets := s.Stats()[0].ResetsTotal; resets != 0 {
				t.Fatalf("o amostrador recriou o arquivo (ResetsTotal %d); o cabeçalho escrito à mão diverge do dele", resets)
			}
			if samples, _, _, _ := mustMetric(t, s).Aggregate(id); samples != c.wantSamples {
				t.Errorf("restaurado com %d amostra(s), esperado %d", samples, c.wantSamples)
			}
		})
	}
}

// O layout colunar só vale se a passada dentro da mesma época não gravar
// balde nenhum: gravar a cada passada sujaria todas as colunas.
func TestPassesWithinTheSameEpochDoNotWriteBuckets(t *testing.T) {
	h := newHarness(t)
	for _, key := range []string{"OLT_A", "OLT_B", "OLT_C"} {
		h.counter.add(key, 1)
	}
	s := h.open(t)
	m := mustMetric(t, s)
	for range 5 {
		sampleOnce(t, s)
		h.clock.advance(100 * time.Millisecond)
	}
	if got := m.columnWrites.Load(); got != 0 {
		t.Fatalf("5 passadas na mesma época gravaram %d balde(s), esperado 0", got)
	}

	h.clock.advance(time.Second)
	sampleOnce(t, s)
	if got := m.columnWrites.Load(); got != 3 {
		t.Fatalf("cruzar a época gravou %d balde(s), esperado 1 por grupo (3)", got)
	}
	for range 3 {
		h.clock.advance(100 * time.Millisecond)
		sampleOnce(t, s)
	}
	if got := s.Stats()[0].ColumnWrites; got != 3 {
		t.Fatalf("ColumnWrites = %d depois de mais passadas na mesma época, esperado 3", got)
	}
}

// Stop repetido sem amostra nova no meio não regrava balde; balde restaurado
// do disco e não alterado também não volta ao disco.
func TestRepeatedStopDoesNotRewriteOpenBuckets(t *testing.T) {
	h := newHarness(t)
	for _, key := range []string{"OLT_A", "OLT_B", "OLT_C"} {
		h.counter.add(key, 1)
	}
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	s.Stop()
	if got := m.columnWrites.Load(); got != 3 {
		t.Fatalf("o primeiro Stop gravou %d balde(s), esperado 3", got)
	}
	s.Stop()
	if got := m.columnWrites.Load(); got != 3 {
		t.Fatalf("o segundo Stop, sem amostra nova, levou ColumnWrites a %d, esperado 3", got)
	}

	h.clock.advance(100 * time.Millisecond)
	sampleOnce(t, s)
	s.Stop()
	if got := m.columnWrites.Load(); got != 6 {
		t.Fatalf("Stop depois de amostra nova levou ColumnWrites a %d, esperado 6", got)
	}
	closeSampler(t, s)
	if got := m.columnWrites.Load(); got != 6 {
		t.Fatalf("Close depois do Stop levou ColumnWrites a %d, esperado 6", got)
	}

	reopened := h.open(t)
	closeSampler(t, reopened)
	if got := mustMetric(t, reopened).columnWrites.Load(); got != 0 {
		t.Errorf("fechar logo depois de restaurar gravou %d balde(s); o balde restaurado já está em disco", got)
	}
	wantAggregate(t, mustMetric(t, h.open(t)), 1, 2, 1, 1, 1)
}

func TestOpenRemovesOrphanFilesAndDirectories(t *testing.T) {
	h := newHarness(t)
	h.counter.add("OLT_A", 1)
	for _, rel := range []string{
		"by_olt/retired_metric.win",
		"by_olt/retired_metric.win.tmp",
		"by_olt/notes.txt",
		"gone_counter/auth_1m.win",
		"shared_counter/auth_1m.win",
		"shared_counter/README.md",
		"top_level.win",
	} {
		touch(t, filepath.Join(h.dir, rel))
	}
	h.open(t)

	for _, rel := range []string{"by_olt/retired_metric.win", "by_olt/retired_metric.win.tmp", "gone_counter", "shared_counter/auth_1m.win"} {
		if exists(t, filepath.Join(h.dir, rel)) {
			t.Errorf("%s sobreviveu à limpeza de órfãos", rel)
		}
	}
	for _, rel := range []string{"by_olt/notes.txt", "by_olt/auth_1m.win", "shared_counter/README.md", "top_level.win"} {
		if !exists(t, filepath.Join(h.dir, rel)) {
			t.Errorf("%s foi apagado; a limpeza só pode apagar arquivo .win abaixo de um contador e diretório de contador vazio", rel)
		}
	}

	events := h.logs.events(t, "counter_metric.orphans")
	if len(events) != 1 {
		t.Fatalf("%d WARN counter_metric.orphans, esperado 1", len(events))
	}
	wantRemoved := []string{"by_olt/retired_metric.win", "by_olt/retired_metric.win.tmp", "gone_counter/auth_1m.win", "gone_counter/", "shared_counter/auth_1m.win"}
	if got := stringList(t, events[0]["removed"]); !slices.Equal(got, wantRemoved) {
		t.Errorf("removed = %q, esperado %q", got, wantRemoved)
	}
	if got := stringList(t, events[0]["kept"]); !slices.Equal(got, []string{"shared_counter/"}) {
		t.Errorf("kept = %q, esperado [shared_counter/]", got)
	}
}

// A limpeza de órfãos nunca segue nem apaga link simbólico: nem o link com
// nome de arquivo de métrica apontando para fora de Dir, nem o link com nome
// de diretório de contador.
func TestOrphanCleanupNeverFollowsOrRemovesSymlinks(t *testing.T) {
	h := newHarness(t)
	h.counter.add("OLT_A", 1)
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim.win")
	touch(t, victim)
	victimInDir := filepath.Join(outside, "victim_counter", "auth_1m.win")
	touch(t, victimInDir)

	fileLink := filepath.Join(h.dir, testCounter, "evil_metric.win")
	if err := os.MkdirAll(filepath.Dir(fileLink), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, fileLink); err != nil {
		t.Fatal(err)
	}
	dirLink := filepath.Join(h.dir, "linked_counter")
	if err := os.Symlink(filepath.Dir(victimInDir), dirLink); err != nil {
		t.Fatal(err)
	}

	h.open(t)
	for _, path := range []string{fileLink, victim, dirLink, victimInDir} {
		if !exists(t, path) {
			t.Errorf("%s sumiu; link simbólico nunca é seguido nem apagado pela limpeza de órfãos", path)
		}
	}
}

func stringList(t *testing.T, v any) []string {
	t.Helper()
	items, ok := v.([]any)
	if !ok {
		t.Fatalf("atributo de log %v não é lista", v)
	}
	out := make([]string, len(items))
	for i, item := range items {
		s, ok := item.(string)
		if !ok {
			t.Fatalf("item %v da lista não é texto", item)
		}
		out[i] = s
	}
	return out
}

func TestOpenRecreatesFileWithDivergentHeaderAndWarns(t *testing.T) {
	h := newHarness(t)
	h.counter.add("OLT_A", 1)
	if err := os.MkdirAll(filepath.Dir(h.path()), 0o755); err != nil {
		t.Fatal(err)
	}
	f, _, err := metricwindow.Open(h.path(), metricwindow.Header{BucketCount: 60, BucketWidthMs: 2000, Source: 1}, 2, fileCapacityLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s := h.open(t)
	if got := s.Stats()[0].ResetsTotal; got != 1 {
		t.Errorf("ResetsTotal = %d com a largura de balde divergente, esperado 1", got)
	}
	events := h.logs.events(t, "counter_metric.reset")
	if len(events) != 1 {
		t.Fatalf("%d WARN counter_metric.reset, esperado 1", len(events))
	}
	if events[0]["dataset"] != testDataset || events[0]["counter"] != testCounter || events[0]["metric"] != testMetric {
		t.Errorf("WARN counter_metric.reset sem dataset/counter/metric certos: %v", events[0])
	}
	closeSampler(t, s)

	if got := h.open(t).Stats()[0].ResetsTotal; got != 0 {
		t.Errorf("a reabertura com o cabeçalho já certo contou %d reset(s), esperado 0", got)
	}
}

func TestReadOnlyPublishesFromDiskWithoutWriting(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 10)
	s := h.open(t)
	sampleOnce(t, s)
	h.clock.advance(time.Second)
	h.counter.setTotal(id, 30)
	sampleOnce(t, s)
	closeSampler(t, s)

	orphan := filepath.Join(h.dir, "gone_counter", "auth_1m.win")
	touch(t, orphan)
	before, err := os.ReadFile(h.path())
	if err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Stat(h.path())
	if err != nil {
		t.Fatal(err)
	}

	h.clock.advance(5 * time.Second)
	ro := h.openReadOnly(t)
	wantAggregate(t, mustMetric(t, ro), id, 2, 20, 10, 30)
	if got := ro.RestoredGroups(); got != 1 {
		t.Errorf("RestoredGroups em modo leitura = %d, esperado 1", got)
	}
	if err := ro.SampleOnce(); !errors.Is(err, ErrReadOnly) {
		t.Errorf("SampleOnce em modo leitura devolveu %v, esperado ErrReadOnly", err)
	}
	ro.Start(context.Background())
	if ro.Active() {
		t.Errorf("Start em modo leitura subiu o laço")
	}
	closeSampler(t, ro)

	after, err := os.ReadFile(h.path())
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(h.path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("o modo leitura alterou os bytes do arquivo")
	}
	if !beforeInfo.ModTime().Equal(afterInfo.ModTime()) {
		t.Errorf("o modo leitura alterou o mtime do arquivo: %v → %v", beforeInfo.ModTime(), afterInfo.ModTime())
	}
	if !exists(t, orphan) {
		t.Errorf("o modo leitura apagou um órfão; a limpeza é só do modo escrita")
	}
}

// O consumidor (tabela do contador) trata "Metric devolve false" como erro na
// subida, então TODA métrica declarada responde, nos dois modos — inclusive
// sem arquivo nenhum em disco.
func TestMetricAnswersEveryDeclaredMetricInBothModes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "counter_metrics")
	olts, pons := newFakeCounter(), newFakeCounter()
	olts.add("OLT_A", 1)
	pons.add("PON_1", 2)
	clock := newFakeClock()
	options := func(readOnly bool, logs *lockedBuffer) Options {
		return Options{
			Dataset: testDataset,
			Dir:     dir,
			Counters: []CounterInput{
				{Spec: schema.CounterSpec{Name: "by_olt", Metrics: []schema.CounterMetricSpec{metricSpec("auth_5m", 5*time.Minute), metricSpec("auth_24h", 24*time.Hour)}}, Reader: olts},
				{Spec: schema.CounterSpec{Name: "by_pon", Metrics: []schema.CounterMetricSpec{metricSpec("auth_5m", 5*time.Minute)}}, Reader: pons},
			},
			Log:      logging.New(logs, logging.FormatJSON, slog.LevelDebug),
			Now:      clock.Now,
			ReadOnly: readOnly,
		}
	}
	declared := [][2]string{{"by_olt", "auth_5m"}, {"by_olt", "auth_24h"}, {"by_pon", "auth_5m"}}
	undeclared := [][2]string{{"by_pon", "auth_24h"}, {"by_nothing", "auth_5m"}, {"by_olt", ""}}
	checkNames := func(t *testing.T, s *Sampler) {
		t.Helper()
		for _, d := range declared {
			if m, ok := s.Metric(d[0], d[1]); !ok || m == nil {
				t.Errorf("Metric(%q, %q) = (%v, %v), esperado leitura presente", d[0], d[1], m, ok)
			}
		}
		for _, u := range undeclared {
			if _, ok := s.Metric(u[0], u[1]); ok {
				t.Errorf("Metric(%q, %q) respondeu uma métrica não declarada", u[0], u[1])
			}
		}
	}

	t.Run("read-only without files", func(t *testing.T) {
		logs := &lockedBuffer{}
		s := openWith(t, options(true, logs))
		checkNames(t, s)
		for _, d := range declared {
			m, _ := s.Metric(d[0], d[1])
			wantNoValue(t, m, 1)
			if groups, heap, disk, sampledAt := m.Usage(); groups != 0 || heap != 0 || disk != 0 || sampledAt != 0 {
				t.Errorf("Usage de %s.%s sem arquivo = (%d, %d, %d, %d), esperado zeros", d[0], d[1], groups, heap, disk, sampledAt)
			}
		}
		if n := len(logs.events(t, "counter_metric.unreadable")); n != len(declared) {
			t.Errorf("%d WARN counter_metric.unreadable, esperado %d", n, len(declared))
		}
	})

	t.Run("write", func(t *testing.T) {
		s := openWith(t, options(false, &lockedBuffer{}))
		checkNames(t, s)
		sampleOnce(t, s)
		for _, d := range declared {
			m, _ := s.Metric(d[0], d[1])
			if samples, _, _, _ := m.Aggregate(1); samples != 1 {
				t.Errorf("%s.%s: %d amostra(s) depois de uma passada, esperado 1", d[0], d[1], samples)
			}
		}
	})
}

// Falha de infra para o laço de vez, com ERROR nomeado; o publicado congela e
// Metric continua respondendo — a tabela não pode perder a métrica porque o
// amostrador parou.
func TestLoopStopsAtTheFirstInfraFailureAndMetricKeepsAnswering(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 9)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	sampledAt := h.clock.ms.Load()

	// O arquivo fechado é o estado que um crescimento com falha na reabertura
	// deixa (metricwindow.File.EnsureCapacity). Na mesma época, o grupo já visto
	// não toca o arquivo; o grupo novo pede capacidade e recebe ErrClosed.
	if err := m.file.Close(); err != nil {
		t.Fatal(err)
	}
	h.counter.add("OLT_B", 1)
	if err := s.SampleOnce(); !errors.Is(err, metricwindow.ErrClosed) {
		t.Fatalf("SampleOnce com o arquivo fechado devolveu %v, esperado ErrClosed", err)
	}

	s.Start(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for s.Active() {
		if time.Now().After(deadline) {
			t.Fatal("o laço não parou depois da falha de infra")
		}
		time.Sleep(time.Millisecond)
	}
	stopped := h.logs.events(t, "counter_metric.stopped")
	if len(stopped) != 1 || stopped[0]["level"] != "ERROR" || stopped[0]["dataset"] != testDataset {
		t.Fatalf("esperado um ERROR counter_metric.stopped com o dataset, obtido %v", stopped)
	}
	if _, ok := s.Metric(testCounter, testMetric); !ok {
		t.Fatalf("Metric deixou de responder depois de o laço parar")
	}
	// A passada que falha já dobrou a amostra dos grupos anteriores ao que
	// falhou: uma no SampleOnce manual e outra na passada do laço, além da
	// primeira. Depois da parada, o publicado congela aí.
	wantAggregate(t, m, id, 3, 9, 9, 9)
	if _, _, _, got := m.Usage(); got != sampledAt {
		t.Errorf("sampledAt = %d depois da falha, esperado o da última passada completa (%d)", got, sampledAt)
	}
	closeSampler(t, s)
	if n := len(h.logs.events(t, "counter_metric.state_not_saved")); n != 1 {
		t.Errorf("%d WARN counter_metric.state_not_saved no Close com o arquivo fechado, esperado 1: o msync devolve ErrClosed e o encerramento tem de dizer que o balde aberto não foi gravado", n)
	}
}

func TestStartDoesNotSpawnTheLoopWithoutMetrics(t *testing.T) {
	s := openWith(t, Options{Dataset: testDataset, Dir: filepath.Join(t.TempDir(), "counter_metrics")})
	s.Start(context.Background())
	if s.Active() {
		t.Errorf("Start subiu o laço sem nenhuma métrica declarada")
	}
}

func TestStopAndCloseAreIdempotent(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 6)
	s := h.open(t)
	m := mustMetric(t, s)
	sampleOnce(t, s)
	s.Start(context.Background())
	s.Stop()
	s.Stop()
	closeSampler(t, s)
	closeSampler(t, s)
	if err := s.SampleOnce(); !errors.Is(err, ErrClosed) {
		t.Errorf("SampleOnce depois de Close devolveu %v, esperado ErrClosed", err)
	}
	s.Start(context.Background())
	if s.Active() {
		t.Errorf("Start depois de Close subiu o laço")
	}
	if samples, _, _, _ := m.Aggregate(id); samples == 0 {
		t.Errorf("Close apagou o publicado; Metric responde o último valor depois dele")
	}
}

func TestOpenRefusesOptionsThatWouldShareOrLoseFiles(t *testing.T) {
	reader := newFakeCounter()
	valid := func() CounterInput {
		return CounterInput{
			Spec:   schema.CounterSpec{Name: testCounter, Metrics: []schema.CounterMetricSpec{metricSpec(testMetric, time.Minute)}},
			Reader: reader,
		}
	}
	cases := map[string]func(o *Options){
		"no dataset":              func(o *Options) { o.Dataset = "" },
		"no directory":            func(o *Options) { o.Dir = "" },
		"counter without metrics": func(o *Options) { o.Counters[0].Spec.Metrics = nil },
		"counter twice":           func(o *Options) { o.Counters = append(o.Counters, valid()) },
		"nil reader":              func(o *Options) { o.Counters[0].Reader = nil },
		"counter name escapes":    func(o *Options) { o.Counters[0].Spec.Name = "../escape" },
		"metric name escapes":     func(o *Options) { o.Counters[0].Spec.Metrics[0].Name = "a/b" },
		"metric twice": func(o *Options) {
			o.Counters[0].Spec.Metrics = append(o.Counters[0].Spec.Metrics, metricSpec(testMetric, 2*time.Minute))
		},
		"window without whole bucket": func(o *Options) { o.Counters[0].Spec.Metrics[0].Window = 1001 * time.Millisecond },
		"unknown source":              func(o *Options) { o.Counters[0].Spec.Metrics[0].Source = "refs" },
		"unknown type":                func(o *Options) { o.Counters[0].Spec.Metrics[0].Type = "rate" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := Options{Dataset: testDataset, Dir: filepath.Join(t.TempDir(), "counter_metrics"), Counters: []CounterInput{valid()}}
			mutate(&o)
			if s, err := Open(o); err == nil {
				_ = s.Close()
				t.Fatalf("Open aceitou opções inválidas (%s)", name)
			}
		})
	}
}

func TestPublishedAvgIsFiniteForExtremeTotals(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 0)
	s := h.open(t)
	m := mustMetric(t, s)
	for _, total := range []int64{math.MaxInt64, math.MaxInt64, math.MinInt64, 0, math.MaxInt64, math.MinInt64} {
		h.counter.setTotal(id, total)
		sampleOnce(t, s)
		samples, avg, _, _ := m.Aggregate(id)
		if samples == 0 || math.IsNaN(avg) || math.IsInf(avg, 0) {
			t.Fatalf("total %d: publicado (amostras %d, avg %v); com amostra, o avg tem de ser finito", total, samples, avg)
		}
		h.clock.advance(time.Second)
	}
}

// A contagem do balde vai para 16 bits em disco. Este é o gate que impede
// baixar o piso de amostragem ou subir a janela máxima sem refazer a conta: o
// máximo sai das constantes, com uma amostra a mais para a que cai exatamente
// nas duas bordas do balde.
func TestOpenBucketCountFitsInSixteenBitsAtTheSamplingFloor(t *testing.T) {
	widest := schema.MaxMetricWindow / schema.MetricWindowBuckets
	maxSamples := int64(widest/minSampleInterval) + 1
	if maxSamples > math.MaxUint16 {
		t.Fatalf("o maior balde (%s) amostrado a cada %s recebe até %d amostras, acima de %d que cabem na contagem de 16 bits",
			widest, minSampleInterval, maxSamples, math.MaxUint16)
	}
}

// Acima do teto, contagem e soma param juntas (o avg é a média exata das
// amostras contadas) e min/max seguem vendo toda amostra.
func TestOpenBucketCountSaturatesAtTheSixteenBitCeiling(t *testing.T) {
	h := newHarness(t)
	id := h.counter.add("OLT_A", 1)
	s := h.open(t)
	m := mustMetric(t, s)
	for i := range math.MaxUint16 + 5_000 {
		total := int64(1)
		if i%2 == 1 {
			total = 3
		}
		h.counter.setTotal(id, total)
		sampleOnce(t, s)
	}
	// As 65.535 primeiras amostras: 32.768 de valor 1 e 32.767 de valor 3.
	wantAvg := float64(32_768+3*32_767) / math.MaxUint16
	wantAggregate(t, m, id, math.MaxUint16, wantAvg, 1, 3)

	closeSampler(t, s)
	wantAggregate(t, mustMetric(t, h.open(t)), id, math.MaxUint16, wantAvg, 1, 3)
}

// O hash da chave vai para a identidade e para a marca de dono em disco. Os
// esperados são os vetores publicados do FNV-1a de 64 bits, e hash/fnv confere
// os vetores: um golden copiado da própria implementação não provaria nada.
func TestKeyHashMatchesTheFNV1aGolden(t *testing.T) {
	cases := []struct {
		key  string
		want uint64
	}{
		{key: "", want: 0xcbf29ce484222325},
		{key: "a", want: 0xaf63dc4c8601ec8c},
		{key: "foobar", want: 0x85944171f73967e8},
	}
	for _, c := range cases {
		ref := fnv.New64a()
		_, _ = ref.Write([]byte(c.key))
		if ref.Sum64() != c.want {
			t.Fatalf("hash/fnv(%q) = %#x diverge do vetor %#x; o golden está errado", c.key, ref.Sum64(), c.want)
		}
		if got := keyHash([]byte(c.key)); got != c.want {
			t.Errorf("keyHash(%q) = %#x, esperado %#x", c.key, got, c.want)
		}
	}
}

// O teto de capacidade é o espaço de ids do dicionário, e tem de ser uma
// capacidade que o formato aceite: potência de 2.
func TestFileCapacityLimitCoversEveryDictionaryIDAsAPowerOfTwo(t *testing.T) {
	if uint64(fileCapacityLimit) != uint64(maxGroupID)+1 {
		t.Errorf("fileCapacityLimit = %d, esperado maxGroupID+1 = %d", fileCapacityLimit, uint64(maxGroupID)+1)
	}
	if fileCapacityLimit&(fileCapacityLimit-1) != 0 {
		t.Errorf("fileCapacityLimit = %d não é potência de 2", fileCapacityLimit)
	}
	if maxGroupID != math.MaxInt32 {
		t.Errorf("maxGroupID = %d, e o dicionário recusa a partir de math.MaxInt32 chaves", maxGroupID)
	}
}

func TestSamplingIntervalIsClampedBetweenFloorAndCeiling(t *testing.T) {
	cases := []struct {
		windows []time.Duration
		want    time.Duration
	}{
		{windows: nil, want: 0},
		{windows: []time.Duration{time.Minute}, want: time.Second},
		{windows: []time.Duration{5 * time.Minute}, want: 5 * time.Second},
		{windows: []time.Duration{24 * time.Hour}, want: 10 * time.Second},
		{windows: []time.Duration{24 * time.Hour, 2 * time.Minute}, want: 2 * time.Second},
		{windows: []time.Duration{schema.MaxMetricWindow}, want: 10 * time.Second},
	}
	for _, c := range cases {
		metrics := make([]*Metric, len(c.windows))
		for i, w := range c.windows {
			metrics[i] = &Metric{window: w}
		}
		if got := sampleInterval(metrics); got != c.want {
			t.Errorf("sampleInterval(%v) = %s, esperado %s", c.windows, got, c.want)
		}
	}
}

func TestStatsReportTheHeapEstimateAndTheFileSize(t *testing.T) {
	h := newHarness(t)
	h.counter.add("OLT_A", 1)
	s := h.open(t)
	sampleOnce(t, s)
	st := s.Stats()
	if len(st) != 1 {
		t.Fatalf("%d MetricStats, esperado 1", len(st))
	}
	got := st[0]
	if got.Dataset != testDataset || got.Counter != testCounter || got.Name != testMetric {
		t.Errorf("MetricStats sem os nomes certos: %+v", got)
	}
	if want := int64(chunkSize) * entryBytes; got.HeapBytes != want {
		t.Errorf("HeapBytes = %d, esperado um bloco (%d)", got.HeapBytes, want)
	}
	info, err := os.Stat(h.path())
	if err != nil {
		t.Fatal(err)
	}
	if got.DiskBytes != info.Size() {
		t.Errorf("DiskBytes = %d, arquivo tem %d", got.DiskBytes, info.Size())
	}
	if got.Groups != 1 || got.NewGroups != 1 || got.SampledAtUnixMs != baseMs {
		t.Errorf("Groups %d, NewGroups %d, SampledAtUnixMs %d; esperado 1, 1, %d", got.Groups, got.NewGroups, got.SampledAtUnixMs, baseMs)
	}
}
