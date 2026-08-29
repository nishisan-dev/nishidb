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

// Region é um arquivo mapeado em memória (MAP_SHARED), com acesso atômico a
// words de 64 bits alinhadas. Vem em duas variedades, e a diferença é de
// CAPACIDADE, não de conveniência:
//
//   - crescível (Open): PROT_READ|PROT_WRITE, mantém o descritor aberto porque
//     Grow, Msync e LiberaBlocos precisam dele;
//   - somente-leitura (AbreSomenteLeitura): PROT_READ, FECHA o descritor logo
//     após o mmap e deixa f == nil.
//
// Concorrência: leitores e o writer único podem usar os métodos Atomic*
// simultaneamente (RLock interno); Grow remapeia sob Lock exclusivo, então
// nenhum acesso em voo observa o remap. Acesso a bytes crus é sempre
// escopado via WithBytes — nenhuma slice do mapeamento escapa do RLock. A
// exceção é Bytes(), que só existe na variedade somente-leitura justamente
// porque lá não há remap de que escapar.
type Region struct {
	mu sync.RWMutex
	// f é nil na região somente-leitura: o descritor é fechado logo após o
	// mmap. Nenhum caminho pode derreferenciá-lo sem antes olhar
	// somenteLeitura.
	f *os.File
	// nome guarda o caminho para as mensagens de erro. Ele existe porque
	// f.Name() morreria com nil pointer dereference na região somente-leitura
	// — e o lugar onde isso apareceria seria dentro do fmt.Errorf de um
	// caminho de falha, ou seja, um pânico obscuro escondido atrás do erro
	// que ele deveria estar reportando.
	nome           string
	data           []byte
	size           int64
	somenteLeitura bool
}

// ErrFallocateUnsupported indica filesystem sem suporte a fallocate real.
// O NishiDB exige alocação real de blocos: página esparsa + disco cheio no
// writeback mata o processo com SIGBUS (fatal error incapturável). Não há
// fallback silencioso para Truncate — mova o dataset para ext4/xfs/btrfs.
var ErrFallocateUnsupported = errors.New(
	"mmap: filesystem não suporta fallocate; NishiDB exige alocação real de blocos")

// ErrSomenteLeitura marca a tentativa de mutar uma região aberta por
// AbreSomenteLeitura. É erro, e não degradação: o mapeamento é PROT_READ e o
// descritor já foi fechado, então não existe caminho pelo qual a escrita
// pudesse dar certo mais adiante.
var ErrSomenteLeitura = errors.New("mmap: região somente-leitura não aceita mutação")

// ErrArquivoVazio marca o mapeamento de um arquivo de zero bytes.
//
// unix.Mmap com length 0 falha com EINVAL, e propagar esse errno cru diria ao
// operador "argumento inválido" sobre um argumento que ele não escolheu. Um
// arquivo de índice com zero bytes só existe como escrita truncada — o
// escritor selado se recusa a emitir índice vazio —, e o chamador trata a
// recusa nomeada como "sem índice", que é um estado que ele já sabe resolver
// por enumeração e reconstrução.
var ErrArquivoVazio = errors.New("mmap: arquivo vazio não pode ser mapeado")

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
	return &Region{f: f, nome: path, data: data, size: size}, nil
}

// AbreSomenteLeitura mapeia um arquivo EXISTENTE em PROT_READ e FECHA o
// descritor logo depois, devolvendo uma região que não cresce, não sincroniza
// e não libera blocos.
//
// Fechar o descritor é o ponto da função, não uma economia. Um dataset de
// 250 GiB tem ~500 segmentos selados, e reter um descritor por segmento
// estouraria o `ulimit -n` típico de 1024 (spec §1.4). No Linux o mapeamento
// sobrevive ao close, e um arquivo selado nunca precisa do fd de volta — as
// três únicas operações que exigiriam o descritor são justamente as que a
// imutabilidade elimina.
//
// O mapeamento é preguiçoso: um segmento nunca consultado não custa nada, e a
// residência é decidida pelo kernel página a página. São 500 VMAs contra um
// teto padrão de 65.530.
func AbreSomenteLeitura(path string) (*Region, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mmap: abrir %s somente-leitura: %w", path, err)
	}
	// Nos caminhos de recusa o erro do Close é descartado EXPLICITAMENTE: já
	// existe um erro melhor a devolver, e o descritor está sendo abandonado de
	// qualquer jeito. O `_ =` diz isso em vez de deixar a decisão implícita.
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("mmap: stat %s: %w", path, err)
	}
	size := st.Size()
	if size <= 0 {
		_ = f.Close()
		return nil, fmt.Errorf("%w (%s)", ErrArquivoVazio, path)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("mmap: mapear %s somente-leitura (%d bytes): %w", path, size, err)
	}
	// O erro do Close entra no resultado em vez de ir para um defer mudo, e a
	// região é desmontada junto: devolver uma Region viva com um descritor que
	// não fechou reintroduziria — silenciosamente e só sob pressão de fd — o
	// custo que esta função existe para eliminar.
	if err := f.Close(); err != nil {
		_ = unix.Munmap(data)
		return nil, fmt.Errorf("mmap: fechar descritor de %s após o mapeamento: %w", path, err)
	}
	return &Region{nome: path, data: data, size: size, somenteLeitura: true}, nil
}

// Bytes devolve o mapeamento cru, sem cópia. Existe SÓ na região
// somente-leitura, e entra em pânico na crescível.
//
// A restrição é a razão de o método existir. Numa região crescível o Grow
// remapeia e faz munmap do endereço antigo, então uma slice que tivesse
// escapado do RLock apontaria para memória liberada — e o acesso seguinte
// seria SIGSEGV incapturável, longe da linha que reteve a slice. É por isso
// que o resto da região só expõe bytes por WithBytes, escopado no callback.
// Na região somente-leitura não há Grow, o endereço nunca se move, e a única
// desmontagem é o Close, que o dono chama por último — então entregar a slice
// é seguro, e é o que permite ao leitor selado ler o mmap com
// binary.LittleEndian direto, sem lock e sem cópia (débito 102: o RWMutex sai
// do caminho quente).
//
// O RLock aqui NÃO protege a slice devolvida — ela escapa do lock, e é para
// isso que serve. O que ele protege é a leitura do cabeçalho de slice contra o
// Close concorrente que o zera; um Close logo depois do retorno desmonta
// memória que o chamador ainda tem em mãos, e nenhum lock deste método pode
// impedir isso. Quem garante que não acontece é o dono, que faz quiesce antes
// de fechar — a mesma disciplina do resto do storage. O chamador guarda o
// resultado UMA vez, na abertura, e não volta a este método por leitura.
func (r *Region) Bytes() []byte {
	if !r.somenteLeitura {
		panic("mmap: Bytes() só existe em região somente-leitura; " +
			"numa região crescível o Grow remapearia e a slice viraria memória liberada — use WithBytes")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.data
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
	if r.somenteLeitura {
		return fmt.Errorf("%w: Grow(%d) em %s", ErrSomenteLeitura, newSize, r.nome)
	}
	if newSize <= r.size {
		return fmt.Errorf("mmap: Grow(%d) não excede tamanho atual %d", newSize, r.size)
	}
	if err := allocate(r.f, r.size, newSize-r.size); err != nil {
		return err
	}
	data, err := unix.Mmap(int(r.f.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: remapear %s (%d bytes): %w", r.nome, newSize, err)
	}
	old := r.data
	r.data = data
	r.size = newSize
	if err := unix.Munmap(old); err != nil {
		return fmt.Errorf("mmap: munmap antigo de %s: %w", r.nome, err)
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
	// A view é MUTÁVEL, e o mapeamento somente-leitura é PROT_READ: escrever
	// nela seria SIGSEGV incapturável dentro do callback do chamador. O pânico
	// nomeado troca isso por uma falha de teste que aponta para o método
	// certo — quem só quer ler uma região selada usa Bytes().
	if r.somenteLeitura {
		panic(fmt.Sprintf("mmap: WithBytes em região somente-leitura (%s) entrega view mutável sobre PROT_READ — use Bytes()", r.nome))
	}
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

// exigeMutavel entra em pânico se a região for somente-leitura. Chamar com
// RLock, ao lado do bounds check de word: os mutadores atômicos não têm por
// onde devolver erro, e escrever num mapeamento PROT_READ é SIGSEGV — um sinal
// que o processo não captura e que não nomeia nem o offset nem o arquivo. O
// pânico é a única forma de a violação chegar ao autor do chamador.
func (r *Region) exigeMutavel(op string, off int64) {
	if r.somenteLeitura {
		panic(fmt.Sprintf("mmap: %s(%d) em região somente-leitura (%s)", op, off, r.nome))
	}
}

// AtomicStore64 grava uma word de 64 bits com semântica release.
func (r *Region) AtomicStore64(off int64, v uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.exigeMutavel("AtomicStore64", off)
	atomic.StoreUint64(r.word(off), v)
}

// AtomicOr64 aplica OR bit a bit atômico na word em off.
func (r *Region) AtomicOr64(off int64, v uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.exigeMutavel("AtomicOr64", off)
	atomic.OrUint64(r.word(off), v)
}

// AtomicAnd64 aplica AND bit a bit atômico na word em off.
func (r *Region) AtomicAnd64(off int64, v uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.exigeMutavel("AtomicAnd64", off)
	atomic.AndUint64(r.word(off), v)
}

// Msync força a escrita das páginas sujas para o arquivo (MS_SYNC).
func (r *Region) Msync() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Não há página suja a sincronizar num mapeamento PROT_READ, e o descritor
	// já foi fechado. Recusar em vez de virar no-op mantém a chamada visível:
	// um caminho que acredita estar durabilizando algo tem de descobrir que
	// não está.
	if r.somenteLeitura {
		return fmt.Errorf("%w: Msync em %s", ErrSomenteLeitura, r.nome)
	}
	if err := unix.Msync(r.data, unix.MS_SYNC); err != nil {
		return fmt.Errorf("mmap: msync %s: %w", r.nome, err)
	}
	return nil
}

// Close desfaz o mapeamento e fecha o arquivo.
//
// ZERA o tamanho junto com o mapeamento, e isso é parte da correção: sem
// zerar, o bounds check de word/WithBytes continuava aprovando offsets contra
// o tamanho ANTIGO e a falha virava um índice em slice nil — panic obscuro,
// longe da causa. Com o tamanho zerado, qualquer acesso após o Close falha na
// checagem de limites, que nomeia o problema.
//
// A garantia de que NENHUM acesso chega aqui depois do Close é do Store, que
// faz quiesce antes de fechar os seus arquivos (design §14.21); isto é a rede
// do nível de baixo, para o dia em que um caminho novo escapar.
func (r *Region) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	if r.data != nil {
		if err := unix.Munmap(r.data); err != nil {
			errs = append(errs, fmt.Errorf("mmap: munmap %s: %w", r.nome, err))
		}
		r.data = nil
		r.size = 0
	}
	// A região somente-leitura já fechou o descritor no AbreSomenteLeitura, e
	// f é nil: aqui só resta o munmap.
	if r.f != nil {
		if err := r.f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// LiberaBlocos devolve ao filesystem os blocos de [off, off+length),
// mantendo o TAMANHO do arquivo e o mapeamento válidos. Leitura da faixa
// passa a devolver zeros sem alocar nada.
//
// É a exceção deliberada à regra do allocate, e a exceção precisa do
// argumento por escrito. O NishiDB exige alocação real porque página esparsa
// mais disco cheio no writeback mata o processo com SIGBUS — mas isso vale
// para página que ainda vai ser ESCRITA. Aqui a faixa liberada é a de rowIds
// abaixo da marca-d'água de retenção, e rowId nunca regride nem é reutilizado
// (bijeção rowId↔record, e a recovery reconcilia por max): nenhuma escrita
// futura pode cair ali. O que sobra é leitura, e ler um buraco não aloca.
//
// Filesystem sem suporte devolve ErrFallocateUnsupported, e o chamador trata
// como degradação de ESPAÇO: os segmentos voltam ao disco de qualquer jeito, e
// o que fica para trás é o prefixo morto de um índice denso.
func (r *Region) LiberaBlocos(off, length int64) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Punch-hole exige o descritor, e a região somente-leitura não tem um. É a
	// terceira das três operações que a imutabilidade elimina, e a razão de
	// AbreSomenteLeitura poder fechar o fd (spec §1.4).
	//
	// A guarda vem ANTES do curto-circuito de length <= 0 de propósito: recusar
	// é a resposta, e deixar o comprimento zero escapar como no-op faria a
	// mesma chamada errada devolver nil ou erro conforme o argumento. Um
	// chamador que acredita estar liberando blocos numa região que não pode
	// tem de descobrir isso sempre, não só quando pede um punch não vazio.
	if r.somenteLeitura {
		return fmt.Errorf("%w: LiberaBlocos em %s", ErrSomenteLeitura, r.nome)
	}
	if length <= 0 {
		return nil
	}
	if off < 0 || off+length > r.size {
		return fmt.Errorf("mmap: LiberaBlocos [%d,%d) fora do mapeamento de %d bytes", off, off+length, r.size)
	}
	const punch = unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE
	if err := unix.Fallocate(int(r.f.Fd()), punch, off, length); err != nil {
		if fallocateUnsupported(err) {
			return fmt.Errorf("%w (%s: %v)", ErrFallocateUnsupported, r.nome, err)
		}
		return fmt.Errorf("fallocate punch %s: %w", r.nome, err)
	}
	return nil
}
