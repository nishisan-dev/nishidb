package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Os testes deste arquivo provam o marcador de construção em curso
// (counter/building.go): um vetor zerado ou parcial, deixado por um processo
// que morreu entre o começo de uma construção e o checkpoint que a adota, não
// pode ser adotado pela reabertura.
//
// O "processo que morre" é simulado em processo, e o que o torna fiel é o
// mmap COMPARTILHADO do vetor: o Close do Store não faz msync, mas as páginas
// que o Clear zerou já estão no page cache do arquivo, e é delas que a
// reabertura lê — exatamente o que o kernel poderia ter levado ao disco antes
// de um crash.

// buildingMarkerPath é o caminho do marcador de um contador do dataset.
func buildingMarkerPath(dir, name string) string {
	return filepath.Join(dir, "indexes", diretorioOfCounters, name, "building")
}

// TestOnlineRebuildInterruptedBeforeTheCheckpointIsRebuiltOnReopen: o
// RebuildCounters zera o vetor in place e o refaz no overlay; sem checkpoint
// depois, o vetor em disco é o ZERADO, com os mesmos parâmetros no manifest, e
// a reabertura o adotava — contagem zero, sem órfã e sem log.
func TestOnlineRebuildInterruptedBeforeTheCheckpointIsRebuiltOnReopen(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	for i := 0; i < 10; i++ {
		k := "c-" + string(rune('a'+i))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RebuildCounters(context.Background()); err != nil {
		t.Fatal(err)
	}
	exigeTotal(t, s, "aut", "A", 5, 10)
	if _, err := os.Stat(buildingMarkerPath(dir, "aut")); err != nil {
		t.Fatalf("a reconstrução em operação tem de deixar o marcador até o checkpoint que a adota: %v", err)
	}
	// O processo morre aqui: nenhum checkpoint depois da reconstrução.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openStore(t, optionsWithCounters(dir, byOLT("aut", true)))
	if n := s2.CountersBuiltForTest(); n != 1 {
		t.Errorf("a reabertura reconstruiu %d contadores; com a construção interrompida ela tem de reconstruir, não adotar", n)
	}
	exigeTotal(t, s2, "aut", "A", 5, 10)
	if _, err := os.Stat(buildingMarkerPath(dir, "aut")); !os.IsNotExist(err) {
		t.Fatalf("o checkpoint do fim do Open adota a construção e tem de remover o marcador: %v", err)
	}
}

// TestOnlineRebuildFollowedByACheckpointIsAdoptedOnReopen é a outra ponta:
// depois do checkpoint que leva o vetor reconstruído ao disco, o marcador sai
// e a reabertura ADOTA. Sem ela, um marcador que nunca saísse passaria no
// teste acima custando uma reconstrução a cada abertura.
func TestOnlineRebuildFollowedByACheckpointIsAdoptedOnReopen(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	for i := 0; i < 10; i++ {
		k := "c-" + string(rune('a'+i))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	if _, err := s.RebuildCounters(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(buildingMarkerPath(dir, "aut")); !os.IsNotExist(err) {
		t.Fatalf("o checkpoint depois da reconstrução tem de remover o marcador: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, optionsWithCounters(dir, byOLT("aut", true)))
	if n := s2.CountersBuiltForTest(); n != 0 {
		t.Errorf("a reabertura reconstruiu %d contadores; a construção adotada por checkpoint tem de ser adotada", n)
	}
	exigeTotal(t, s2, "aut", "A", 5, 10)
}

// ctxFailsAfter é o contexto cujo Err devolve nil nas primeiras n chamadas e
// Canceled depois — o jeito determinístico de cancelar uma varredura DEPOIS do
// Clear, que é quando o vetor já está zerado.
type ctxFailsAfter struct {
	context.Context
	n atomic.Int32
}

func (c *ctxFailsAfter) Err() error {
	if c.n.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

func (c *ctxFailsAfter) Done() <-chan struct{}       { return nil }
func (c *ctxFailsAfter) Deadline() (time.Time, bool) { return time.Time{}, false }

// TestFailedRebuildAfterASuccessfulOneKeepsTheBuildingMarker: a primeira
// reconstrução termina e põe o contador na fila de adoção; a segunda, sem
// checkpoint no meio, é cancelada na varredura, com o vetor já zerado. O
// checkpoint seguinte não pode remover o marcador — a construção em disco é a
// incompleta —, e a reabertura tem de reconstruir em vez de adotar o vetor
// zerado. (Achado da refutação: a fila guardava o contador da primeira.)
func TestFailedRebuildAfterASuccessfulOneKeepsTheBuildingMarker(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	for i := 0; i < 10; i++ {
		k := "c-" + string(rune('a'+i))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RebuildCounters(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := &ctxFailsAfter{Context: context.Background()}
	ctx.n.Store(1) // passa na entrada do RebuildCounters, falha na varredura
	if _, err := s.RebuildCounters(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("a segunda reconstrução = %v; quer cancelada na varredura", err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(buildingMarkerPath(dir, "aut")); err != nil {
		t.Fatalf("o checkpoint depois de uma reconstrução interrompida removeu o marcador: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, optionsWithCounters(dir, byOLT("aut", true)))
	if n := s2.CountersBuiltForTest(); n != 1 {
		t.Errorf("a reabertura reconstruiu %d contadores; com a última construção interrompida ela tem de reconstruir", n)
	}
	exigeTotal(t, s2, "aut", "A", 5, 10)
}

// TestOpenTimeBuildInterruptedBeforeItsCheckpointIsRebuiltOnReopen: a
// construção na abertura do ramo "os arquivos do contador não descrevem as
// linhas" acontece IN PLACE, com os mesmos parâmetros no manifest. O vetor
// recriado nasce com capacidade para 512 linhas — cobre o dataset do teste
// desde a criação —, e um processo que morra antes do checkpoint do fim do
// Open deixa esse vetor (zerado, ou parcial se o ApplyOverlay tiver começado)
// para a reabertura seguinte, que o via com Born falso e Covers verdadeiro.
//
// O ponto de crash é o primeiro passo do checkpoint do Open, antes de qualquer
// Set do ApplyOverlay. O Store não tem como interromper o ApplyOverlay depois
// de k Sets sem um seam de produção no contador, e não precisa: com o vetor
// cobrindo as linhas desde a criação, a decisão de adotar é a mesma para
// qualquer k — e o marcador não depende de k.
func TestOpenTimeBuildInterruptedBeforeItsCheckpointIsRebuiltOnReopen(t *testing.T) {
	dir := t.TempDir()
	s, rel := storeWithCounters(t, dir, byOLT("aut", true))
	for i := 0; i < 10; i++ {
		k := "c-" + string(rune('a'+i))
		mustPut(t, s, k, 1, payloadServico(t, s, k, "A", i%2 == 0, rel.agora()), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Sem o vetor, a abertura cai no ramo que zera e constrói in place.
	if err := os.Remove(filepath.Join(dir, "indexes", diretorioOfCounters, "aut", "grupos.idx")); err != nil {
		t.Fatal(err)
	}

	opts := optionsWithCounters(dir, byOLT("aut", true))
	opts.traceRecovery = func(s *Store) {
		s.SetCheckpointTraceForTest(func(step string) {
			if step == "segSync" {
				panic("crash")
			}
		})
	}
	func() {
		defer func() {
			if v := recover(); v != "crash" {
				t.Fatalf("a abertura tinha de morrer no checkpoint do fim do Open; recover = %v", v)
			}
		}()
		st, err := Open(opts)
		if err == nil {
			_ = st.Close()
		}
		t.Fatalf("a abertura não passou pelo checkpoint que devia interromper: err = %v", err)
	}()

	s2 := openStore(t, optionsWithCounters(dir, byOLT("aut", true)))
	if n := s2.CountersBuiltForTest(); n != 1 {
		t.Errorf("a reabertura reconstruiu %d contadores; a construção que não chegou ao checkpoint tem de ser refeita", n)
	}
	exigeTotal(t, s2, "aut", "A", 5, 10)
}
