//go:build linux

package memprof

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	kiB = 1024
	miB = 1024 * kiB
)

func fixture(name string) string { return filepath.Join("testdata", name) }

// O caminho comum: o kernel moderno já separa RssAnon de RssFile no status, e
// é de lá que os quatro números saem — sem tocar no smaps_rollup.
func TestReadFromStatusModerno(t *testing.T) {
	s, err := readFrom(fixture("status_normal.txt"), fixture("smaps_rollup_normal.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// As unidades do /proc são KiB; o Sample é em BYTES. Confundir os dois
	// erra o orçamento de 1 GiB por um fator de 1024 — na direção que faz
	// tudo parecer folgado.
	if want := uint64(983040) * kiB; s.VmRSSBytes != want {
		t.Errorf("VmRSSBytes = %d, want %d", s.VmRSSBytes, want)
	}
	if want := uint64(1048576) * kiB; s.VmHWMBytes != want {
		t.Errorf("VmHWMBytes = %d, want %d", s.VmHWMBytes, want)
	}
	if want := uint64(262144) * kiB; s.AnonRSSBytes != want {
		t.Errorf("AnonRSSBytes = %d, want %d", s.AnonRSSBytes, want)
	}
	if want := uint64(720896) * kiB; s.FileRSSBytes != want {
		t.Errorf("FileRSSBytes = %d, want %d", s.FileRSSBytes, want)
	}
	// A separação é o ponto da spec §79: 703 MiB residentes de mmap (índices)
	// contra 256 MiB de heap Go — confundir os dois é o erro que a meta de
	// memória do M1 não perdoa.
	if s.AnonRSSBytes+s.FileRSSBytes != s.VmRSSBytes {
		t.Errorf("anon (%d) + file (%d) != VmRSS (%d)", s.AnonRSSBytes, s.FileRSSBytes, s.VmRSSBytes)
	}
	if s.HeapAlloc == 0 || s.HeapObjects == 0 || s.Mallocs == 0 {
		t.Errorf("MemStats não preenchidas: %+v", s)
	}
	if s.Timestamp.IsZero() || time.Since(s.Timestamp) > time.Minute {
		t.Errorf("Timestamp = %v", s.Timestamp)
	}
}

// Kernel sem RssAnon/RssFile no status (< 4.5): a separação vem do
// smaps_rollup, com file = Rss − Anonymous.
func TestReadCaiParaSmapsRollup(t *testing.T) {
	s, err := readFrom(fixture("status_kernel_antigo.txt"), fixture("smaps_rollup_normal.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(983040) * kiB; s.VmRSSBytes != want {
		t.Errorf("VmRSSBytes = %d, want %d", s.VmRSSBytes, want)
	}
	if want := uint64(262144) * kiB; s.AnonRSSBytes != want {
		t.Errorf("AnonRSSBytes = %d, want %d", s.AnonRSSBytes, want)
	}
	if want := uint64(983040-262144) * kiB; s.FileRSSBytes != want {
		t.Errorf("FileRSSBytes = %d, want %d", s.FileRSSBytes, want)
	}
}

// Sem nenhuma das duas fontes de separação, o RSS total continua válido: é o
// número do critério de aceite (§109). Perder a decomposição degrada o
// relatório, não a medição principal.
func TestSemSeparacaoAindaDevolveRSS(t *testing.T) {
	s, err := readFrom(fixture("status_kernel_antigo.txt"), fixture("nao_existe.txt"))
	if err != nil {
		t.Fatalf("a ausência do smaps_rollup não pode invalidar a amostra: %v", err)
	}
	if want := uint64(983040) * kiB; s.VmRSSBytes != want {
		t.Errorf("VmRSSBytes = %d, want %d", s.VmRSSBytes, want)
	}
	if s.AnonRSSBytes != 0 || s.FileRSSBytes != 0 {
		t.Errorf("sem fonte de separação, anon e file deveriam ficar zerados: %+v", s)
	}
}

// smaps_rollup existe mas não traz Anonymous: mesma degradação, sem erro.
func TestSmapsRollupSemAnonymous(t *testing.T) {
	s, err := readFrom(fixture("status_kernel_antigo.txt"), fixture("smaps_rollup_sem_anonymous.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if s.AnonRSSBytes != 0 || s.FileRSSBytes != 0 {
		t.Errorf("anon/file = %d/%d, want 0/0", s.AnonRSSBytes, s.FileRSSBytes)
	}
}

func TestErrosDeLeitura(t *testing.T) {
	cases := []struct {
		name   string
		status string
		msg    string
	}{
		{"status ausente", "nao_existe.txt", "nao_existe.txt"},
		{"sem VmRSS", "status_sem_vmrss.txt", "VmRSS"},
		{"valor não numérico", "status_valor_nao_numerico.txt", "VmRSS"},
		{"unidade desconhecida", "status_unidade_desconhecida.txt", "MB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readFrom(fixture(tc.status), fixture("smaps_rollup_normal.txt"))
			if err == nil {
				t.Fatal("erro esperado")
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
			if !errors.Is(err, ErrProcRead) {
				t.Errorf("erro = %v, want ErrProcRead", err)
			}
		})
	}
}

// Smoke real: o processo de teste tem RSS de verdade, e as invariantes do
// kernel valem. É o que separa "o parser funciona nas fixtures" de "a leitura
// funciona nesta máquina".
func TestReadProcessoReal(t *testing.T) {
	s, err := Read()
	if err != nil {
		t.Fatal(err)
	}
	if s.VmRSSBytes == 0 {
		t.Fatal("VmRSSBytes = 0 num processo vivo")
	}
	if s.VmHWMBytes < s.VmRSSBytes {
		t.Errorf("VmHWM (%d) < VmRSS (%d): o pico não pode ser menor que o atual", s.VmHWMBytes, s.VmRSSBytes)
	}
	// Anon + file podem ficar abaixo do total (o resto é shmem), nunca acima.
	if sum := s.AnonRSSBytes + s.FileRSSBytes; sum > s.VmRSSBytes {
		t.Errorf("anon+file (%d) > VmRSS (%d)", sum, s.VmRSSBytes)
	}
	if s.HeapAlloc == 0 || s.NumGC > 1<<40 {
		t.Errorf("MemStats implausíveis: %+v", s)
	}
	if got := time.Since(s.Timestamp); got < 0 || got > time.Minute {
		t.Errorf("Timestamp fora da janela: %v", got)
	}
}

// Duas leituras seguidas de um processo que alocou de verdade: os contadores
// acumulados do Go só andam para a frente. É a propriedade de que o harness
// depende para derivar allocs/evento por diferença.
func TestContadoresSaoMonotonicos(t *testing.T) {
	before, err := Read()
	if err != nil {
		t.Fatal(err)
	}
	junk := make([][]byte, 0, 1024)
	for i := 0; i < 1024; i++ {
		junk = append(junk, make([]byte, 1024))
	}
	after, err := Read()
	if err != nil {
		t.Fatal(err)
	}
	if after.Mallocs <= before.Mallocs {
		t.Errorf("Mallocs não avançou: %d → %d", before.Mallocs, after.Mallocs)
	}
	if after.NumGC < before.NumGC {
		t.Errorf("NumGC retrocedeu: %d → %d", before.NumGC, after.NumGC)
	}
	if !after.Timestamp.After(before.Timestamp) && after.Timestamp != before.Timestamp {
		t.Errorf("Timestamp retrocedeu: %v → %v", before.Timestamp, after.Timestamp)
	}
	runtimeKeepAlive(junk)
}

// runtimeKeepAlive impede o compilador de descartar as alocações do teste
// acima antes da segunda leitura.
func runtimeKeepAlive(v [][]byte) {
	if len(v) == 0 {
		panic("impossível")
	}
}

func TestParseKiBValue(t *testing.T) {
	cases := []struct {
		line    string
		want    uint64
		wantErr bool
	}{
		{"VmRSS:\t  983040 kB", 983040 * kiB, false},
		{"VmRSS:\t0 kB", 0, false},
		{"Rss:               2092 kB", 2092 * kiB, false},
		{"VmRSS:\t  983040 KB", 983040 * kiB, false}, // maiúscula ocorre em kernels antigos
		{"VmRSS:\t  983040", 0, true},                // sem unidade: não dá para assumir
		{"VmRSS:\t  983040 MB", 0, true},
		{"VmRSS:\tmuitos kB", 0, true},
		{"VmRSS:", 0, true},
	}
	for _, tc := range cases {
		_, value, ok := strings.Cut(tc.line, ":")
		if !ok {
			value = ""
		}
		got, err := parseKiBValue("VmRSS", value)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseKiBValue(%q) = %d, want erro", tc.line, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseKiBValue(%q): %v", tc.line, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseKiBValue(%q) = %d, want %d", tc.line, got, tc.want)
		}
	}
}
