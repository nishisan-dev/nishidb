package countermetrics

import (
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// TestRemoveOrphansWithoutSamplerRemovesWhatIsNoLongerDeclared: a limpeza de
// quem não abre amostrador apaga o que não está declarado com as MESMAS
// garantias do Open — só .win/.win.tmp, e diretório com conteúdo alheio fica.
func TestRemoveOrphansWithoutSamplerRemovesWhatIsNoLongerDeclared(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "counter_metrics")
	for _, rel := range []string{
		"by_olt/auth_1m.win",
		"by_olt/auth_1m.win.tmp",
		"by_olt/notes.txt",
		"kept_counter/rows_5m.win",
		"other/rows_5m.win",
	} {
		touch(t, filepath.Join(dir, rel))
	}
	logs := &lockedBuffer{}
	err := RemoveOrphans(testDataset, dir, map[string][]string{"kept_counter": {"rows_5m"}},
		logging.New(logs, logging.FormatJSON, slog.LevelDebug))
	if err != nil {
		t.Fatalf("RemoveOrphans: %v", err)
	}
	for _, rel := range []string{"by_olt/auth_1m.win", "by_olt/auth_1m.win.tmp", "other"} {
		if exists(t, filepath.Join(dir, rel)) {
			t.Errorf("%s sobreviveu à limpeza de órfãos", rel)
		}
	}
	for _, rel := range []string{"by_olt/notes.txt", "kept_counter/rows_5m.win"} {
		if !exists(t, filepath.Join(dir, rel)) {
			t.Errorf("%s foi apagado; só .win não declarado e diretório de contador vazio podem sair", rel)
		}
	}
	events := logs.events(t, "counter_metric.orphans")
	if len(events) != 1 {
		t.Fatalf("%d WARN counter_metric.orphans, esperado 1", len(events))
	}
	wantRemoved := []string{"by_olt/auth_1m.win", "by_olt/auth_1m.win.tmp", "other/rows_5m.win", "other/"}
	if got := stringList(t, events[0]["removed"]); !slices.Equal(got, wantRemoved) {
		t.Errorf("removed = %q, esperado %q", got, wantRemoved)
	}
	if got := stringList(t, events[0]["kept"]); !slices.Equal(got, []string{"by_olt/"}) {
		t.Errorf("kept = %q, esperado [by_olt/]", got)
	}
}

// TestRemoveOrphansOnAMissingDirectoryCreatesNothing: o dataset que nunca
// declarou métrica não ganha diretório nem WARN.
func TestRemoveOrphansOnAMissingDirectoryCreatesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "counter_metrics")
	logs := &lockedBuffer{}
	if err := RemoveOrphans(testDataset, dir, nil, logging.New(logs, logging.FormatJSON, slog.LevelDebug)); err != nil {
		t.Fatalf("RemoveOrphans num diretório inexistente: %v", err)
	}
	if exists(t, dir) {
		t.Fatalf("RemoveOrphans criou %s", dir)
	}
	if events := logs.events(t, "counter_metric.orphans"); len(events) != 0 {
		t.Fatalf("%d WARN counter_metric.orphans sem nada para limpar", len(events))
	}
}

// TestRemoveOrphansNeverFollowsASymlinkedDirectory: o diretório base que é link
// simbólico não é seguido — o que está do outro lado não é nosso.
func TestRemoveOrphansNeverFollowsASymlinkedDirectory(t *testing.T) {
	outside := t.TempDir()
	victim := filepath.Join(outside, "by_olt", "auth_1m.win")
	touch(t, victim)
	link := filepath.Join(t.TempDir(), "counter_metrics")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if err := RemoveOrphans(testDataset, link, nil, nil); err != nil {
		t.Fatalf("RemoveOrphans: %v", err)
	}
	if !exists(t, victim) {
		t.Fatalf("RemoveOrphans seguiu o link e apagou %s", victim)
	}
}
