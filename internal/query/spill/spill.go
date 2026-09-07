// Package spill dá a um operador de query um lugar em DISCO para o que não
// cabe no orçamento de memória.
//
// É primitiva geral e não peça do ORDER BY: o sort é apenas o primeiro
// operador que acumula. Qualquer outro que venha a acumular — agregação,
// distinct — usa a mesma coisa.
//
// # O que ela NÃO é
//
// Não é armazenamento durável. Não há fsync, e um crash perde o que estiver
// aqui — que é o desfecho CERTO: o conteúdo é resultado intermediário de uma
// query que também morreu no crash. Gastar fsync para preservar o rascunho de
// uma resposta que ninguém vai receber seria custo sem consumidor.
//
// # Onde os arquivos vivem
//
// Em `<dir do dataset>/spill/`, ao lado de `data/`, `indexes/` e
// `checkpoint/`. Dentro do dataset, e não em os.TempDir, por três razões: fica
// sob o mesmo flock exclusivo, então nenhum outro processo pode confundir os
// arquivos; herda a política de filesystem do dataset; e o operador que
// dimensionou o disco não precisa dimensionar um segundo lugar.
//
// A limpeza de resíduo de crash é feita na SUBIDA do dataset, e é segura por
// construção: o flock já garantiu que nenhum outro processo o tem, então todo
// arquivo ali é de um processo morto.
package spill

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrBudgetExceeded é o teto de bytes em disco de UMA query estourado.
//
// Sentinela, e não um erro qualquer, porque o consumidor precisa distinguir
// "não coube" de "o disco falhou": o primeiro é resposta ao usuário, o segundo
// é problema do dataset.
var ErrBudgetExceeded = errors.New("spill: orçamento de disco excedido")

// DirName é o subdiretório do dataset onde os arquivos vivem.
const DirName = "spill"

// Cleanup remove todo resíduo de spill do diretório.
//
// Chamada na subida do dataset, DEPOIS do flock. Não é "melhor esforço": um
// erro aqui sobe, porque não poder limpar significa não poder confiar no que
// será escrito em seguida.
func Cleanup(datasetDir string) error {
	dir := filepath.Join(datasetDir, DirName)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("spill: listar %s: %w", dir, err)
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("spill: remover resíduo %s: %w", e.Name(), err)
		}
	}
	return nil
}

// Area é o espaço de spill de UMA query.
//
// Não é seguro para uso concorrente: uma query tem um operador acumulador, e o
// operador é de uma goroutine só. Duas queries têm duas Areas.
type Area struct {
	dir          string
	ceiling      int64
	bytesWritten int64
	seq          int
	openRuns     []*Run
}

// NewArea prepara o espaço. ceiling <= 0 significa SEM TETO de disco.
func NewArea(datasetDir string, ceiling int64) (*Area, error) {
	dir := filepath.Join(datasetDir, DirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("spill: criar %s: %w", dir, err)
	}
	return &Area{dir: dir, ceiling: ceiling}, nil
}

// Bytes é quanto esta query já escreveu em disco.
func (a *Area) Bytes() int64 { return a.bytesWritten }

// NewRun abre um run.
//
// O pid entra no nome para que o resíduo de um processo morto seja
// DIAGNOSTICÁVEL — a limpeza não depende dele, porque o flock já decidiu a
// propriedade, mas quem for olhar o diretório precisa saber de onde veio.
func (a *Area) NewRun() (*Run, error) {
	a.seq++
	name := fmt.Sprintf("spill-%d-%03d.tmp", os.Getpid(), a.seq)
	path := filepath.Join(a.dir, name)
	// 0600 e não 0644: é resultado de consulta de um usuário autenticado, não
	// estado do dataset.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("spill: criar run %s: %w", name, err)
	}
	run := &Run{area: a, f: f, path: path}
	a.openRuns = append(a.openRuns, run)
	return run, nil
}

// Close remove TUDO o que a query deixou. Idempotente.
//
// É o único ponto de limpeza do caminho feliz, e por isso o consumidor tem de
// chamá-lo inclusive nos caminhos de erro e de cancelamento — um defer no
// Close do iterador.
func (a *Area) Close() error {
	var firstErr error
	for _, run := range a.openRuns {
		if err := run.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	a.openRuns = nil
	return firstErr
}

// Run é UM run: escreve em ordem, sela, lê em ordem.
type Run struct {
	area   *Area
	f      *os.File
	path   string
	sealed bool
	closed bool
	buf    []byte
}

// Write acrescenta um registro ao run.
//
// O ENQUADRAMENTO É DAQUI, e não do consumidor: dois formatos de moldura
// divergem no primeiro registro de tamanho patológico, e a divergência aparece
// como resultado errado. Reusa o record do storage, que já tem CRC32C e aplica
// os tetos de tamanho ANTES de qualquer alocação do leitor.
func (r *Run) Write(rowID types.RowID, payload []byte) error {
	if r.sealed {
		return errors.New("spill: Write depois de Seal")
	}
	var err error
	// key vazia: o spill endereça por rowID, e a key do record não tem
	// consumidor aqui. O campo continua no formato porque o formato é
	// compartilhado com o segmento.
	if r.buf, err = encoding.BuildRecord(r.buf[:0], rowID, int64(rowID), 0, nil, payload); err != nil {
		return fmt.Errorf("spill: montar registro da row %d: %w", rowID, err)
	}
	if a := r.area; a.ceiling > 0 && a.bytesWritten+int64(len(r.buf)) > a.ceiling {
		return fmt.Errorf("%w: %d bytes acima do teto de %d",
			ErrBudgetExceeded, a.bytesWritten+int64(len(r.buf)), a.ceiling)
	}
	n, err := r.f.Write(r.buf)
	r.area.bytesWritten += int64(n)
	if err != nil {
		return fmt.Errorf("spill: escrever em %s: %w", r.path, err)
	}
	return nil
}

// Seal fecha a escrita e rebobina para leitura. SEM fsync — ver o godoc do
// pacote.
func (r *Run) Seal() error {
	if r.sealed {
		return nil
	}
	r.sealed = true
	if _, err := r.f.Seek(0, 0); err != nil {
		return fmt.Errorf("spill: rebobinar %s: %w", r.path, err)
	}
	return nil
}

// NewReader devolve o leitor sequencial. Só depois de Seal.
func (r *Run) NewReader() (*Reader, error) {
	if !r.sealed {
		return nil, errors.New("spill: NewReader antes de Seal")
	}
	return &Reader{f: r.f, path: r.path}, nil
}

// Close fecha e REMOVE o arquivo. Idempotente.
func (r *Run) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	closeErr := r.f.Close()
	removeErr := os.Remove(r.path)
	if closeErr != nil {
		return fmt.Errorf("spill: fechar %s: %w", r.path, closeErr)
	}
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("spill: remover %s: %w", r.path, removeErr)
	}
	return nil
}

// Reader percorre um run selado, em ordem de escrita.
type Reader struct {
	f    *os.File
	path string
	buf  []byte
}

// Next devolve o registro seguinte.
//
// O payload é ZERO-COPY sobre o buffer do leitor e vale só até a chamada
// SEGUINTE — mesma disciplina do ColView.Val.Bytes do executor, e pela mesma
// razão: alocar por registro num merge de N runs seria alocação por LINHA do
// resultado.
func (l *Reader) Next() (types.RowID, []byte, bool, error) {
	if cap(l.buf) < encoding.RecordHeaderSize {
		l.buf = make([]byte, encoding.RecordHeaderSize, 4096)
	}
	header := l.buf[:encoding.RecordHeaderSize]
	if _, err := readFull(l.f, header); err != nil {
		if errors.Is(err, errEnd) {
			return 0, nil, false, nil // fim do run, não erro
		}
		return 0, nil, false, fmt.Errorf("spill: ler header de %s: %w", l.path, err)
	}
	h, err := encoding.ParseHeader(header)
	if err != nil {
		return 0, nil, false, fmt.Errorf("spill: header de %s: %w", l.path, err)
	}
	total := h.TotalSize()
	if cap(l.buf) < total {
		newBuf := make([]byte, total)
		copy(newBuf, header)
		l.buf = newBuf
	}
	l.buf = l.buf[:total]
	if _, err := readFull(l.f, l.buf[encoding.RecordHeaderSize:total]); err != nil {
		return 0, nil, false, fmt.Errorf("spill: ler corpo de %s: %w", l.path, err)
	}
	rec, err := encoding.ParseRecord(l.buf)
	if err != nil {
		return 0, nil, false, fmt.Errorf("spill: registro de %s: %w", l.path, err)
	}
	return rec.RowID, rec.Payload, true, nil
}

// Close não fecha o arquivo: quem o possui é o Run, e fechá-lo aqui
// tornaria dois donos responsáveis pelo mesmo descritor.
func (l *Reader) Close() error { return nil }

var errEnd = errors.New("fim")

// readFull preenche p por inteiro. EOF no PRIMEIRO byte é fim de arquivo
// legítimo; EOF no meio é truncamento, e vira erro.
func readFull(f *os.File, p []byte) (int, error) {
	nRead := 0
	for nRead < len(p) {
		n, err := f.Read(p[nRead:])
		nRead += n
		if err != nil {
			if nRead == 0 {
				return 0, errEnd
			}
			return nRead, fmt.Errorf("registro truncado: %d de %d bytes", nRead, len(p))
		}
	}
	return nRead, nil
}
