package buildinfo

import (
	"runtime/debug"
	"strings"
	"testing"
)

// restoreVars devolve Version, Commit e Date ao estado anterior ao teste — as
// três são globais de propósito (é o único jeito de -ldflags -X escrever
// nelas), e um teste que as muda sem devolver contamina o próximo.
func restoreVars(t *testing.T) {
	t.Helper()
	v, c, d := Version, Commit, Date
	t.Cleanup(func() { Version, Commit, Date = v, c, d })
}

// TestVersionRespondeComSHAEData cobre o caminho feliz: os três ldflags
// vieram (equivalente ao binário que `make build` produz), e String() os
// junta no MESMO texto que toda superfície reaproveita.
func TestVersionRespondeComSHAEData(t *testing.T) {
	restoreVars(t)
	Version, Commit, Date = "v9.9.9", "cafefeed1234", "2026-01-02T03:04:05Z"

	got := String()
	for _, want := range []string{"v9.9.9", "cafefeed1234", "2026-01-02T03:04:05Z"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, quer conter %q", got, want)
		}
	}
}

// TestFallbackUsaOCarimboDoVCSQuandoOsLdflagsNaoVieram cobre `go install`
// direto: sem -ldflags as três vars ficam vazias, mas o toolchain do Go já
// carimbou vcs.revision/vcs.time no binário de graça — responder "dev sem
// versão" jogaria essa informação fora.
func TestFallbackUsaOCarimboDoVCSQuandoOsLdflagsNaoVieram(t *testing.T) {
	restoreVars(t)
	Version, Commit, Date = "", "", ""

	prev := readBuildInfo
	defer func() { readBuildInfo = prev }()
	readBuildInfo = func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "abcdef0123456789"},
			{Key: "vcs.time", Value: "2026-03-04T05:06:07Z"},
		}}, true
	}

	got := String()
	if !strings.Contains(got, "abcdef012345") {
		t.Errorf("String() = %q, quer o commit do VCS stamp", got)
	}
	if !strings.Contains(got, "2026-03-04T05:06:07Z") {
		t.Errorf("String() = %q, quer a data do VCS stamp", got)
	}
}

// TestSemLdflagsESemCarimboRespondeDev é o "sem informação nenhuma": um
// binário de teste pode não carregar vcs stamp, e a resposta tem de ser
// definida — "dev" — em vez de string vazia ou pânico.
func TestSemLdflagsESemCarimboRespondeDev(t *testing.T) {
	restoreVars(t)
	Version, Commit, Date = "", "", ""

	prev := readBuildInfo
	defer func() { readBuildInfo = prev }()
	readBuildInfo = func() (*debug.BuildInfo, bool) { return nil, false }

	if got := String(); got != "dev" {
		t.Errorf("String() = %q, quer %q", got, "dev")
	}
}
