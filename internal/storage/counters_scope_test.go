package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// splScope é o texto de scope dos testes; o avaliador é `eligible = true`.
const splScope = "eligible = true"

func byOLTWithScope(name, scope string) counterOfTest {
	return counterOfTest{name: name, groupBy: []string{"olt"}, where: true, scope: scope}
}

// payloadScoped é payloadServico com o campo do scope.
func payloadScoped(t *testing.T, s *Store, key, olt string, auth, eligible bool, write time.Time) []byte {
	t.Helper()
	fields := map[string]types.Value{
		"id":            {Type: types.TypeString, Bytes: []byte(key)},
		"authenticated": {Type: types.TypeBool},
		"eligible":      {Type: types.TypeBool},
		"olt":           {Type: types.TypeString, Bytes: []byte(olt)},
	}
	if auth {
		fields["authenticated"] = types.Value{Type: types.TypeBool, I64: 1}
	}
	if eligible {
		fields["eligible"] = types.Value{Type: types.TypeBool, I64: 1}
	}
	if !write.IsZero() {
		fields["_writtenAt"] = types.Value{Type: types.TypeTimestamp, I64: write.UnixMilli()}
	}
	return payloadOrdenado(t, s, fields)
}

// putScoped grava (ou regrava) a linha da key com o estado dado.
func putScoped(t *testing.T, s *Store, seq int64, key, olt string, auth, eligible bool, write time.Time) types.RowID {
	t.Helper()
	prev, _, found, err := s.GetByKey(s.presente(), []byte(key), nil)
	if err != nil {
		t.Fatal(err)
	}
	p := payloadScoped(t, s, key, olt, auth, eligible, write)
	if !found {
		id, _ := mustPut(t, s, key, seq, p, nil)
		return id
	}
	id, _ := mustPut(t, s, key, seq, p, &prev)
	return id
}

// TestScopeLeavesTheGroupOnPatchAndOnTTLExpiry é a garantia 2 pelo Store
// inteiro: o patch que torna `eligible = false` e a lápide da expiração
// descontam rows e total do grupo na MESMA aplicação do applyRecord.
func TestScopeLeavesTheGroupOnPatchAndOnTTLExpiry(t *testing.T) {
	s, rel := storeWithCounters(t, t.TempDir(), byOLTWithScope("spl", splScope))
	a := putScoped(t, s, 1, "a", "OLT", true, true, rel.agora())
	putScoped(t, s, 1, "b", "OLT", true, true, rel.agora())
	putScoped(t, s, 1, "c", "OLT", false, true, rel.agora())
	putScoped(t, s, 1, "fora", "OLT", true, false, rel.agora())
	exigeTotal(t, s, "spl", "OLT", 2, 3)

	putScoped(t, s, 2, "b", "OLT", true, false, rel.agora())
	exigeTotal(t, s, "spl", "OLT", 1, 2)

	rel.advance(2 * time.Hour)
	if expired, err := s.Expira(a); err != nil || !expired {
		t.Fatalf("Expira: %v, %v", expired, err)
	}
	exigeTotal(t, s, "spl", "OLT", 0, 1)
	if st := counterChamado(t, s, "spl").Stats(); st.Rows != 1 || st.Total != 0 {
		t.Fatalf("agregados = rows %d total %d, quero 1 e 0", st.Rows, st.Total)
	}
}

// TestScopedCounterRecoversToTheContinuousStateAndAuditsClean é a garantia 4:
// crash sem checkpoint no meio de transições de escopo, e a reabertura (dobra +
// replay pelo applyRecord) chega aos mesmos números de uma instância que nunca
// caiu; a auditoria, cujo Deriver carrega o MESMO scope, não acusa nada.
func TestScopedCounterRecoversToTheContinuousStateAndAuditsClean(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLTWithScope("spl", splScope))
	for i := 0; i < 60; i++ {
		putScoped(t, s, 1, keyOfServico(i), "A", i%2 == 0, i%3 != 0, rel.agora())
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// A ponta: entra e sai do escopo, troca de OLT, perde a condição.
	for i := 0; i < 60; i += 2 {
		olt := "A"
		if i%4 == 0 {
			olt = "B"
		}
		putScoped(t, s, 2, keyOfServico(i), olt, i%4 != 0, i%3 == 0, rel.agora())
	}
	totA, refsA := totalOfCounter(t, s, "spl", "A")
	totB, refsB := totalOfCounter(t, s, "spl", "B")
	if refsA+refsB == 0 || totA+totB == 0 {
		t.Fatalf("a bancada não exercitou nada: A=%d/%d B=%d/%d", totA, refsA, totB, refsB)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, optionsWithCounters(dir, byOLTWithScope("spl", splScope)))
	if s2.CountersBuiltForTest() != 0 {
		t.Fatal("a recovery tem de chegar lá por dobra + replay, sem reconstrução")
	}
	exigeTotal(t, s2, "spl", "A", totA, refsA)
	exigeTotal(t, s2, "spl", "B", totB, refsB)
	rs, err := s2.AuditaCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := relatorioOf(t, rs, "spl")
	if r.DivergentGroups != 0 || r.NotInterned != 0 || r.DivergentAggregates != 0 {
		t.Fatalf("auditoria acusou divergência num contador com scope: %+v", r)
	}
	if r.RowsRecomputed != refsA+refsB || r.Recomputed != totA+totB {
		t.Fatalf("auditoria recalculou rows %d total %d, quero %d e %d", r.RowsRecomputed, r.Recomputed, refsA+refsB, totA+totB)
	}
}

// reopenCounted fecha s, reabre com a definição dada e devolve quantos
// contadores a abertura reconstruiu.
func reopenCounted(t *testing.T, s *Store, dir string, c counterOfTest) (*Store, int) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, optionsWithCounters(dir, c))
	return s2, s2.CountersBuiltForTest()
}

// TestScopeIdentityDecidesAdoptOrRebuild cobre as garantias 5 e 6: manter o
// scope adota; trocá-lo reconstrói; e a definição SEM scope e com o mesmo where
// — o binário antigo num rollback — também reconstrói, em vez de adotar um
// vetor cujo conteúdo o scope decidiu.
func TestScopeIdentityDecidesAdoptOrRebuild(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLTWithScope("spl", splScope))
	for i := 0; i < 10; i++ {
		putScoped(t, s, 1, keyOfServico(i), "A", i%2 == 0, i < 6, rel.agora())
	}
	exigeTotal(t, s, "spl", "A", 3, 6)

	s, n := reopenCounted(t, s, dir, byOLTWithScope("spl", splScope))
	if n != 0 {
		t.Fatalf("o mesmo scope reconstruiu (%d): tem de adotar", n)
	}
	exigeTotal(t, s, "spl", "A", 3, 6)

	s, n = reopenCounted(t, s, dir, byOLTWithScope("spl", "eligible = true AND eligible IS NOT NULL"))
	if n != 1 {
		t.Fatalf("trocar o scope reconstruiu %d contadores, quero 1", n)
	}

	s, n = reopenCounted(t, s, dir, byOLTWithScope("spl", splScope))
	if n != 1 {
		t.Fatalf("voltar ao scope anterior reconstruiu %d contadores, quero 1", n)
	}
	// Rollback: mesmo nome, mesmo where, sem scope.
	s, n = reopenCounted(t, s, dir, byOLT("spl", true))
	if n != 1 {
		t.Fatalf("a definição sem scope ADOTOU o vetor construído com scope (reconstruiu %d)", n)
	}
	exigeTotal(t, s, "spl", "A", 5, 10)
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Counters) != 1 || m.Counters[0].Where != "authenticated = true" {
		t.Fatalf("sem scope o manifest grava o where byte a byte: %+v", m.Counters)
	}
}

// byOLTDefinition é a Definition mínima para exercitar identityOf.
func byOLTDefinition(t *testing.T, where, scope string) counter.Definition {
	t.Helper()
	return counter.Definition{Name: "spl", Where: where, Scope: scope}
}

// TestIdentityOfKeepsTheWhereAloneAndSplitsBack: sem scope a identidade é o
// where byte a byte; com scope e sem where ela nunca é vazia; e splitIdentity
// devolve as duas metades para o log de reconstrução.
func TestIdentityOfKeepsTheWhereAloneAndSplitsBack(t *testing.T) {
	where, scope := splitIdentity(identityOf(byOLTDefinition(t, "authenticated = true", splScope)))
	if where != "authenticated = true" || scope != splScope {
		t.Fatalf("splitIdentity(identityOf) = %q, %q", where, scope)
	}
	if got := identityOf(byOLTDefinition(t, "", splScope)); got != "\n"+splScope {
		t.Fatalf("identidade sem where com scope = %q, quero %q — nunca vazia", got, "\n"+splScope)
	}
	if got := identityOf(byOLTDefinition(t, "authenticated = true", "")); got != "authenticated = true" {
		t.Fatalf("identidade sem scope = %q, quero o where byte a byte", got)
	}
}

// TestCountersManifestSectionBytesAreLiteral é o golden da seção 0x0001 do
// manifest, com os bytes escritos À MÃO a partir do formato documentado em
// countersSectionSize — nunca gerados pelo encoder nem por identityOf. Dois
// contadores: `aut` sem scope (os bytes que o binário sem scope gravaria, e é
// isso que prova "um dataset pré-existente abre sem reconstruir") e `spl` com
// scope.
//
//	n u16 | por contador: lenNome u16 | nome | nCols u16 | nCols×u16 | lenWhere u32 | where
func TestCountersManifestSectionBytesAreLiteral(t *testing.T) {
	dir := t.TempDir()
	s, _ := storeWithCounters(t, dir, byOLT("aut", true), byOLTWithScope("spl", splScope))
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	golden := strings.Join([]string{
		"0100", "54000000", // tag 0x0001, payload de 84 bytes
		"0200",           // dois contadores
		"0300", "617574", // "aut"
		"0100", "0300", // groupBy: olt (FieldID 3)
		"14000000", // where de 20 bytes
		"61757468656e74696361746564203d2074727565", // "authenticated = true"
		"0300", "73706c", // "spl"
		"0100", "0300", // groupBy: olt
		"24000000", // identidade de 36 bytes
		"61757468656e74696361746564203d2074727565", // "authenticated = true"
		"0a",                             // o separador
		"656c696769626c65203d2074727565", // "eligible = true"
	}, "")
	want, err := hex.DecodeString(golden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, want) {
		m, _ := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
		t.Fatalf("a seção dos contadores no manifest não é a do golden\nquero %x\nmanifest %x\nlido %+v", want, raw, m.Counters)
	}
}
