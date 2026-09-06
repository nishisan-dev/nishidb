//go:build linux

// Package memprof lê o consumo de memória do PRÓPRIO processo (spec §79 e
// §108-111).
//
// Nasceu servindo só aos harnesses de benchmark e por isso morava em
// internal/testutil. Saiu de lá no M2 porque o §79 é obrigação do MOTOR, não
// da bancada de medição: "a engine deve coletar e expor process RSS, Go heap,
// Go heap objects, mapped bytes". O /metrics do §93 lê daqui.
//
// A distinção que justifica o pacote: `runtime.MemStats` mede o heap Go, e o
// NishiDB guarda a maior parte do estado FORA dele — em índices mmap, que são
// páginas file-backed. Um relatório que só olhasse HeapAlloc mostraria
// centenas de MiB a menos que a realidade, e o critério de aceite do M1 é RSS
// do processo, não heap.
//
// Por isso o Sample junta as duas contabilidades e, principalmente, SEPARA o
// RSS anônimo (heap Go, stacks) do RSS file-backed (mmap dos índices). A spec
// §79 é explícita: tamanho do arquivo mapeado não é RSS residente — 4 GiB de
// índice mapeado podem ter 200 MiB residentes, e é o residente que conta.
//
// Linux-only por construção (lê /proc). Nos demais sistemas o pacote compila
// e Read devolve erro — o harness precisa falhar com mensagem, não deixar de
// compilar.
package memprof

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// ErrProcRead categoriza qualquer falha de leitura ou parse do /proc.
var ErrProcRead = errors.New("memprof: leitura do /proc")

const (
	procStatus = "/proc/self/status"
	procRollup = "/proc/self/smaps_rollup"
)

// Sample é uma medição instantânea. Todos os tamanhos em BYTES — o /proc
// reporta em KiB, e a conversão acontece na leitura para que nenhum consumidor
// precise lembrar da unidade.
type Sample struct {
	// VmRSSBytes é o RSS atual do processo: o número do critério de aceite
	// (§109), porque é o que o sistema operacional cobra.
	VmRSSBytes uint64
	// VmHWMBytes é o pico histórico de RSS. Uma rodada que termina dentro do
	// orçamento mas com HWM acima dele passou por um momento em que o
	// processo ocupou mais do que pode.
	VmHWMBytes uint64
	// FileRSSBytes é a parte residente de mapeamentos de arquivo — os índices
	// mmap. Pressão de memória a devolve ao sistema sem OOM.
	FileRSSBytes uint64
	// AnonRSSBytes é a parte anônima: heap Go, stacks, arenas. É o que NÃO
	// pode ser devolvido sob pressão, e por isso o número que precisa ficar
	// desacoplado do tamanho do dataset.
	AnonRSSBytes uint64

	HeapAlloc   uint64 // runtime: bytes vivos no heap Go
	HeapObjects uint64 // runtime: objetos vivos
	Mallocs     uint64 // runtime: alocações ACUMULADAS (o harness deriva allocs/evento por diferença)
	NumGC       uint64 // runtime: ciclos de GC acumulados

	Timestamp time.Time
}

// Read tira uma amostra do processo atual.
//
// Aloca (lê dois arquivos do /proc e chama ReadMemStats), e essas alocações
// entram no Mallocs do próprio processo medido: amostre FORA da janela cujo
// allocs/evento você quer derivar, senão o instrumento entra na conta.
//
// ReadMemStats faz stop-the-world; a uma amostra a cada N mil eventos o custo
// é irrelevante, a cada evento não seria.
func Read() (Sample, error) { return readFrom(procStatus, procRollup) }

// readFrom é o Read parametrizado pelos caminhos, para os testes exercitarem
// fixtures de /proc de kernels diferentes.
func readFrom(statusPath, rollupPath string) (Sample, error) {
	raw, err := os.ReadFile(statusPath)
	if err != nil {
		return Sample{}, fmt.Errorf("%w: %s: %w", ErrProcRead, statusPath, err)
	}
	st, err := parseStatus(string(raw))
	if err != nil {
		return Sample{}, fmt.Errorf("%w: %s: %w", ErrProcRead, statusPath, err)
	}

	anon, file := st.rssAnon, st.rssFile
	if !st.hasSeparation {
		// Kernel anterior ao 4.5 não expõe RssAnon/RssFile no status. O
		// smaps_rollup dá o mesmo por subtração — é mais caro (percorre as
		// VMAs), então só entra quando a fonte barata não serve.
		anon, file = rollupSeparation(rollupPath)
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return Sample{
		VmRSSBytes:   st.vmRSS,
		VmHWMBytes:   st.vmHWM,
		FileRSSBytes: file,
		AnonRSSBytes: anon,
		HeapAlloc:    ms.HeapAlloc,
		HeapObjects:  ms.HeapObjects,
		Mallocs:      ms.Mallocs,
		NumGC:        uint64(ms.NumGC),
		Timestamp:    time.Now(),
	}, nil
}

// rollupSeparation extrai (anônimo, file-backed) do smaps_rollup. Falha ali
// NÃO invalida a amostra: perder a decomposição degrada o relatório, enquanto
// o RSS total — que é o critério de aceite — continua válido. Devolve (0,0)
// quando não dá para separar, em vez de inventar um número.
func rollupSeparation(path string) (anon, file uint64) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0
	}
	rss, hasRSS, err := findKiB(string(raw), "Rss")
	if err != nil || !hasRSS {
		return 0, 0
	}
	anonRSS, hasAnon, err := findKiB(string(raw), "Anonymous")
	if err != nil || !hasAnon || anonRSS > rss {
		return 0, 0
	}
	return anonRSS, rss - anonRSS
}

// statusInfo é o subconjunto de /proc/self/status que interessa.
type statusInfo struct {
	vmRSS, vmHWM     uint64
	rssAnon, rssFile uint64
	hasSeparation    bool
}

func parseStatus(content string) (statusInfo, error) {
	var st statusInfo
	vmRSS, hasRSS, err := findKiB(content, "VmRSS")
	if err != nil {
		return st, err
	}
	if !hasRSS {
		return st, fmt.Errorf("campo VmRSS ausente")
	}
	st.vmRSS = vmRSS

	// VmHWM pode faltar em ambientes exóticos (alguns containers): o pico é
	// informativo, o atual é que é critério — degradar é melhor que falhar.
	if hwm, ok, err := findKiB(content, "VmHWM"); err == nil && ok {
		st.vmHWM = hwm
	} else {
		st.vmHWM = vmRSS
	}

	anon, hasAnon, errA := findKiB(content, "RssAnon")
	file, hasFile, errF := findKiB(content, "RssFile")
	if errA == nil && errF == nil && hasAnon && hasFile {
		st.rssAnon, st.rssFile, st.hasSeparation = anon, file, true
	}
	return st, nil
}

// findKiB procura a linha "<campo>:\t<n> kB" e devolve o valor em bytes.
// ok=false quando o campo não existe (situação legítima); erro quando existe
// mas está malformado — o que indica formato de /proc diferente do esperado, e
// aí adivinhar seria pior que parar.
func findKiB(content, field string) (uint64, bool, error) {
	for line := range strings.Lines(content) {
		name, value, ok := strings.Cut(line, ":")
		if !ok || name != field {
			continue
		}
		v, err := parseKiBValue(field, value)
		if err != nil {
			return 0, true, err
		}
		return v, true, nil
	}
	return 0, false, nil
}

// parseKiBValue converte "  983040 kB" em bytes.
//
// A unidade é conferida em vez de descartada: o /proc sempre diz kB (que é
// KiB), e um dia em que dissesse outra coisa, ignorar o sufixo produziria um
// número calmamente errado por um fator de 1024 — exatamente o tipo de erro
// que faz um orçamento de 1 GiB parecer cumprido.
func parseKiBValue(field, value string) (uint64, error) {
	parts := strings.Fields(value)
	if len(parts) != 2 {
		return 0, fmt.Errorf("campo %s com formato inesperado: %q", field, strings.TrimSpace(value))
	}
	n, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("campo %s com valor não numérico %q", field, parts[0])
	}
	if u := strings.ToLower(parts[1]); u != "kb" {
		return 0, fmt.Errorf("campo %s em unidade inesperada %q (esperado kB)", field, parts[1])
	}
	return n * 1024, nil
}
