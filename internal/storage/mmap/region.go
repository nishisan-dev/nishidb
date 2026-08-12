// Package mmap encapsula todo o uso de memory mapping e unsafe do storage.
// Nenhum outro pacote deve tocar mmap/unsafe diretamente.
package mmap

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Region é um arquivo mapeado em memória (leitura/escrita, MAP_SHARED),
// com acesso atômico a words de 64 bits alinhadas.
//
// Concorrência: leitores e o writer único podem usar os métodos Atomic*
// simultaneamente (RLock interno); Grow remapeia sob Lock exclusivo, então
// nenhum acesso em voo observa o remap. Acesso a bytes crus é sempre
// escopado via WithBytes — nenhuma slice do mapeamento escapa do RLock.
type Region struct {
	mu   sync.RWMutex
	f    *os.File
	data []byte
	size int64
}

// ErrFallocateUnsupported indica filesystem sem suporte a fallocate real.
// O NishiDB exige alocação real de blocos: página esparsa + disco cheio no
// writeback mata o processo com SIGBUS (fatal error incapturável). Não há
// fallback silencioso para Truncate — mova o dataset para ext4/xfs/btrfs.
var ErrFallocateUnsupported = errors.New(
	"mmap: filesystem não suporta fallocate; NishiDB exige alocação real de blocos")

// fallocateUnsupported classifica errnos de "sem suporte". EINVAL entra
// porque alguns filesystems o devolvem no lugar de EOPNOTSUPP para o modo 0
// — e nossos argumentos (off≥0, len>0, modo 0) nunca são inválidos de fato.
func fallocateUnsupported(err error) bool {
	return errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL)
}

// allocate garante alocação real de blocos (não sparse) em [off, off+len).
func allocate(f *os.File, off, length int64) error {
	if length <= 0 {
		return nil
	}
	err := unix.Fallocate(int(f.Fd()), 0, off, length)
	if err == nil {
		return nil
	}
	if fallocateUnsupported(err) {
		return fmt.Errorf("%w (%s: %v)", ErrFallocateUnsupported, f.Name(), err)
	}
	return fmt.Errorf("fallocate %s: %w", f.Name(), err)
}

// Open cria (se necessário) e mapeia o arquivo com pelo menos minSize bytes.
// Arquivo existente maior é mapeado por inteiro; nunca encolhe.
func Open(path string, minSize int64) (*Region, error) {
	if minSize <= 0 {
		return nil, fmt.Errorf("mmap: minSize inválido %d", minSize)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("mmap: abrir %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap: stat %s: %w", path, err)
	}
	size := max(st.Size(), minSize)
	if err := allocate(f, 0, size); err != nil {
		f.Close()
		return nil, err
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap: mapear %s (%d bytes): %w", path, size, err)
	}
	return &Region{f: f, data: data, size: size}, nil
}

// Size devolve o tamanho mapeado atual.
func (r *Region) Size() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.size
}

// Grow estende o arquivo (alocação real) e remapeia. Nunca encolhe.
func (r *Region) Grow(newSize int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if newSize <= r.size {
		return fmt.Errorf("mmap: Grow(%d) não excede tamanho atual %d", newSize, r.size)
	}
	if err := allocate(r.f, r.size, newSize-r.size); err != nil {
		return err
	}
	data, err := unix.Mmap(int(r.f.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: remapear %s (%d bytes): %w", r.f.Name(), newSize, err)
	}
	old := r.data
	r.data = data
	r.size = newSize
	if err := unix.Munmap(old); err != nil {
		return fmt.Errorf("mmap: munmap antigo de %s: %w", r.f.Name(), err)
	}
	return nil
}

// WithBytes executa fn sobre uma view mutável de n bytes a partir de off,
// segurando o RLock durante TODA a execução: a slice nunca escapa do lock,
// então é estruturalmente impossível retê-la através de um Grow/Close
// (reter uma view através do Munmap seria SIGSEGV incapturável).
//
// fn NÃO pode chamar outros métodos desta Region (RWMutex não é reentrante:
// com um Grow enfileirado, o RLock aninhado deadlocka) nem reter a slice
// após retornar.
func (r *Region) WithBytes(off, n int, fn func(b []byte)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if off < 0 || n < 0 || int64(off)+int64(n) > r.size {
		panic(fmt.Sprintf("mmap: WithBytes(%d, %d) fora dos limites (size=%d)", off, n, r.size))
	}
	fn(r.data[off : off+n : off+n])
}

// word devolve o ponteiro para a word alinhada em off. Chamar com RLock.
func (r *Region) word(off int64) *uint64 {
	if off%8 != 0 {
		panic(fmt.Sprintf("mmap: offset %d desalinhado (exige múltiplo de 8)", off))
	}
	if off < 0 || off+8 > r.size {
		panic(fmt.Sprintf("mmap: offset %d fora dos limites (size=%d)", off, r.size))
	}
	return (*uint64)(unsafe.Pointer(&r.data[off]))
}

// AtomicLoad64 lê uma word de 64 bits com semântica acquire.
func (r *Region) AtomicLoad64(off int64) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return atomic.LoadUint64(r.word(off))
}

// AtomicStore64 grava uma word de 64 bits com semântica release.
func (r *Region) AtomicStore64(off int64, v uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	atomic.StoreUint64(r.word(off), v)
}

// AtomicOr64 aplica OR bit a bit atômico na word em off.
func (r *Region) AtomicOr64(off int64, v uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	atomic.OrUint64(r.word(off), v)
}

// AtomicAnd64 aplica AND bit a bit atômico na word em off.
func (r *Region) AtomicAnd64(off int64, v uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	atomic.AndUint64(r.word(off), v)
}

// Msync força a escrita das páginas sujas para o arquivo (MS_SYNC).
func (r *Region) Msync() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := unix.Msync(r.data, unix.MS_SYNC); err != nil {
		return fmt.Errorf("mmap: msync %s: %w", r.f.Name(), err)
	}
	return nil
}

// Close desfaz o mapeamento e fecha o arquivo.
func (r *Region) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	if r.data != nil {
		if err := unix.Munmap(r.data); err != nil {
			errs = append(errs, fmt.Errorf("mmap: munmap %s: %w", r.f.Name(), err))
		}
		r.data = nil
	}
	if err := r.f.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
