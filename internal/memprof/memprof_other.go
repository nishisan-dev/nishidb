//go:build !linux

// Fora do Linux não há /proc/self/status: o pacote compila para que o harness
// e seus imports continuem construindo em qualquer máquina de desenvolvimento,
// mas Read falha com mensagem clara em vez de devolver zeros que passariam por
// medição.
package memprof

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

// ErrProcRead categoriza qualquer falha de leitura ou parse do /proc.
var ErrProcRead = errors.New("memprof: leitura do /proc")

// Sample é uma medição instantânea. Todos os tamanhos em BYTES.
//
// Os campos são os mesmos da build Linux: um consumidor que compile nos dois
// não muda de código, só descobre em runtime que não há como medir aqui.
type Sample struct {
	VmRSSBytes   uint64
	VmHWMBytes   uint64
	FileRSSBytes uint64
	AnonRSSBytes uint64

	HeapAlloc   uint64
	HeapObjects uint64
	Mallocs     uint64
	NumGC       uint64

	Timestamp time.Time
}

// Read devolve erro: o RSS do processo, que é o critério de aceite do M1, não
// tem fonte portável. Devolver só as MemStats seria pior — o número pareceria
// uma medição e estaria faltando tudo o que mora fora do heap.
func Read() (Sample, error) {
	return Sample{}, fmt.Errorf("%w: /proc só existe no Linux (GOOS=%s)", ErrProcRead, runtime.GOOS)
}
