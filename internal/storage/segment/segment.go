// Package segment implementa os arquivos append-only de dados do NishiDB
// (spec §30): data/segment-%08d.dat com header fixo e records back-to-back.
package segment

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// HeaderSize é o tamanho do header de segmento em bytes.
//
// Layout (little-endian):
//
//	0   4  magic "NDBS"
//	4   1  version = 1
//	5   3  reserved
//	8   8  segmentID (u64)
//	16  8  createdAtUnixMs (i64)
//	24  8  segmentSizeHint (u64)
//	32  32 reserved
const HeaderSize = 64

const segmentFormatVersion = 1

var segmentMagic = [4]byte{'N', 'D', 'B', 'S'}

// speculativeRead é o tamanho do primeiro pread: cobre header de record e a
// imensa maioria dos records (média esperada 700-1000 bytes) em 1 syscall.
const speculativeRead = 4096

func segmentPath(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%08d.dat", id))
}

func writeHeader(f *os.File, id uint32, sizeHint int64) error {
	var h [HeaderSize]byte
	copy(h[0:4], segmentMagic[:])
	h[4] = segmentFormatVersion
	binary.LittleEndian.PutUint64(h[8:16], uint64(id))
	binary.LittleEndian.PutUint64(h[16:24], uint64(time.Now().UnixMilli()))
	binary.LittleEndian.PutUint64(h[24:32], uint64(sizeHint))
	if _, err := f.WriteAt(h[:], 0); err != nil {
		return fmt.Errorf("segment: escrever header de %s: %w", f.Name(), err)
	}
	return nil
}

func checkHeader(f *os.File, wantID uint32) error {
	var h [HeaderSize]byte
	if _, err := f.ReadAt(h[:], 0); err != nil {
		return fmt.Errorf("segment: ler header de %s: %w", f.Name(), err)
	}
	if [4]byte(h[0:4]) != segmentMagic {
		return fmt.Errorf("segment: magic inválida em %s: %q", f.Name(), h[0:4])
	}
	if h[4] != segmentFormatVersion {
		return fmt.Errorf("segment: version desconhecida %d em %s", h[4], f.Name())
	}
	if id := binary.LittleEndian.Uint64(h[8:16]); id != uint64(wantID) {
		return fmt.Errorf("segment: header declara segmentID %d, esperado %d", id, wantID)
	}
	return nil
}

// Writer gerencia o segmento corrente e a rotação. Uso exclusivo do writer
// único — NÃO é thread-safe.
type Writer struct {
	dir         string
	segmentSize int64
	f           *os.File
	segID       uint32
	offset      int64
}

// MaxSegmentSize é o teto de tamanho de segmento: o offset dentro da
// Location empacotada tem 30 bits (design §3/§14.7), então offsets válidos
// vão até 2³⁰-1. Acima disso, Pack panicaria com o record já durável no
// segmento — crash loop no replay.
const MaxSegmentSize = 1 << 30 // 1 GiB

// OpenWriter abre o diretório de segmentos para escrita. segID=0 cria o
// segmento 1; segID>0 reabre o segmento existente posicionado em offset,
// truncando qualquer lixo além dele (recovery).
func OpenWriter(dir string, segmentSize int64, segID uint32, offset int64) (*Writer, error) {
	if segmentSize <= HeaderSize {
		return nil, fmt.Errorf("segment: segmentSize %d inválido", segmentSize)
	}
	if segmentSize > MaxSegmentSize {
		return nil, fmt.Errorf(
			"segment: segmentSize %d excede o máximo de 1 GiB (offset de location tem 30 bits — design §3/§14.7)",
			segmentSize)
	}
	w := &Writer{dir: dir, segmentSize: segmentSize}
	if segID == 0 {
		if err := w.openNew(1); err != nil {
			return nil, err
		}
		return w, nil
	}
	if offset < HeaderSize {
		return nil, fmt.Errorf("segment: offset de reopen %d menor que o header (%d)", offset, HeaderSize)
	}
	f, err := os.OpenFile(segmentPath(dir, segID), os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("segment: reabrir segmento %d: %w", segID, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("segment: stat do segmento %d: %w", segID, err)
	}
	if st.Size() < HeaderSize {
		// Rotação interrompida entre o O_CREATE|O_EXCL e o writeHeader do
		// openNew: o arquivo existe sem header. Completar a criação é a
		// única saída — recusar trava o dataset para sempre, e o arquivo
		// existente já bloquearia o O_EXCL de qualquer rotação futura. O
		// caller é quem decide que este segmento não continha records (só o
		// último pode estar assim, e o manifest não pode declará-lo com
		// dados).
		if offset != HeaderSize {
			f.Close()
			return nil, fmt.Errorf("segment: segmento %d tem %d bytes (sem header) mas o offset pedido é %d",
				segID, st.Size(), offset)
		}
		if err := writeHeader(f, segID, segmentSize); err != nil {
			f.Close()
			return nil, err
		}
		if err := syncDir(dir); err != nil {
			f.Close()
			return nil, err
		}
	} else if err := checkHeader(f, segID); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(offset); err != nil {
		f.Close()
		return nil, fmt.Errorf("segment: truncar %s em %d: %w", f.Name(), offset, err)
	}
	w.f, w.segID, w.offset = f, segID, offset
	return w, nil
}

func (w *Writer) openNew(id uint32) error {
	f, err := os.OpenFile(segmentPath(w.dir, id), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("segment: criar segmento %d: %w", id, err)
	}
	if err := writeHeader(f, id, w.segmentSize); err != nil {
		f.Close()
		return err
	}
	// Política de fsync de diretório (design §14): a ENTRADA do segmento
	// novo precisa ser durável — sem isso, um power loss pós-rotação pode
	// deixar records fsyncados num arquivo sem dirent.
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.f, w.segID, w.offset = f, id, HeaderSize
	return nil
}

// syncDir durabiliza as entradas de um diretório.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("segment: fsync do diretório %s: %w", dir, err)
	}
	return nil
}

// Append grava um record completo e devolve sua localização física.
// Rotaciona para um novo segmento quando o record não cabe inteiro.
func (w *Writer) Append(rec []byte) (uint32, uint32, error) {
	if int64(len(rec)) > w.segmentSize-HeaderSize {
		return 0, 0, fmt.Errorf("segment: record de %d bytes não cabe em segmento de %d", len(rec), w.segmentSize)
	}
	if w.offset+int64(len(rec)) > w.segmentSize {
		// Sela o corrente e rotaciona. O fdatasync garante que o segmento
		// selado esteja durável antes de qualquer escrita no próximo.
		if err := w.Sync(); err != nil {
			return 0, 0, err
		}
		if err := w.f.Close(); err != nil {
			return 0, 0, fmt.Errorf("segment: fechar segmento %d: %w", w.segID, err)
		}
		if err := w.openNew(w.segID + 1); err != nil {
			return 0, 0, err
		}
	}
	off := w.offset
	if _, err := w.f.WriteAt(rec, off); err != nil {
		return 0, 0, fmt.Errorf("segment: append no segmento %d offset %d: %w", w.segID, off, err)
	}
	w.offset += int64(len(rec))
	return w.segID, uint32(off), nil
}

// Sync força fdatasync do segmento corrente.
func (w *Writer) Sync() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("segment: fsync do segmento %d: %w", w.segID, err)
	}
	return nil
}

// Current devolve o segmento corrente e o offset do próximo append.
func (w *Writer) Current() (uint32, int64) { return w.segID, w.offset }

// Close fecha o segmento corrente.
func (w *Writer) Close() error { return w.f.Close() }

// SegmentRef descreve um segmento presente no diretório de dados.
type SegmentRef struct {
	ID        uint32
	SizeBytes int64
}

// ListSegments enumera os segmentos do diretório em ordem crescente de ID
// (usado pelo checkpoint e pelo dir-listing da recovery, design §10.3).
func ListSegments(dir string) ([]SegmentRef, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("segment: listar %s: %w", dir, err)
	}
	var refs []SegmentRef
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id uint32
		if _, err := fmt.Sscanf(e.Name(), "segment-%08d.dat", &id); err != nil || id == 0 {
			continue
		}
		// Só o nome canônico exato: Sscanf aceitaria sufixos
		// (segment-N.dat.tmp/.new) e padding divergente, produzindo IDs
		// duplicados no manifest e no dir-listing da recovery.
		if e.Name() != fmt.Sprintf("segment-%08d.dat", id) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // removido entre ReadDir e Info
			}
			return nil, err
		}
		refs = append(refs, SegmentRef{ID: id, SizeBytes: info.Size()})
	}
	slices.SortFunc(refs, func(a, b SegmentRef) int { return cmp.Compare(a.ID, b.ID) })
	return refs, nil
}

// Reader lê records por localização física. Seguro para uso concorrente:
// ReadAt não usa o cursor do arquivo e o cache de handles é protegido.
type Reader struct {
	dir string
	mu  sync.RWMutex
	fds map[uint32]*os.File
}

// OpenReader abre o diretório de segmentos para leitura.
func OpenReader(dir string) (*Reader, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("segment: diretório %s: %w", dir, err)
	}
	return &Reader{dir: dir, fds: make(map[uint32]*os.File)}, nil
}

func (r *Reader) file(seg uint32) (*os.File, error) {
	r.mu.RLock()
	f, ok := r.fds[seg]
	r.mu.RUnlock()
	if ok {
		return f, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.fds[seg]; ok {
		return f, nil
	}
	f, err := os.Open(segmentPath(r.dir, seg))
	if err != nil {
		return nil, fmt.Errorf("segment: abrir segmento %d para leitura: %w", seg, err)
	}
	if err := checkHeader(f, seg); err != nil {
		f.Close()
		return nil, err
	}
	r.fds[seg] = f
	return f, nil
}

// ReadRecord lê e valida (CRC) o record em (seg, off), usando buf como
// buffer reutilizável. Devolve a visão zero-copy sobre o buffer retornado.
func (r *Reader) ReadRecord(seg, off uint32, buf []byte) (types.Record, []byte, error) {
	f, err := r.file(seg)
	if err != nil {
		return types.Record{}, buf, err
	}
	return readRecordAt(f, seg, off, buf)
}

// readRecordAt implementa a leitura em (até) dois preads sobre qualquer
// io.ReaderAt (separado de ReadRecord para ser testável com fakes).
func readRecordAt(f io.ReaderAt, seg, off uint32, buf []byte) (types.Record, []byte, error) {
	if cap(buf) < speculativeRead {
		buf = make([]byte, speculativeRead)
	}
	buf = buf[:cap(buf)]

	// A janela do 1º pread é sempre ≤ speculativeRead, mesmo com um buffer
	// reutilizado que já cresceu para um record grande — senão um único
	// record de 4 MiB faria toda leitura seguinte emitir preads de 4 MiB
	// (o buffer nunca encolhe). O min(…, resto) do §3 sai de graça: o
	// readFullAt para no EOF.
	first := min(len(buf), speculativeRead)
	n, err := readFullAt(f, buf[:first], int64(off))
	if ioErr := genuineIOError(err); ioErr != nil {
		return types.Record{}, buf, fmt.Errorf("segment: ler segmento %d offset %d: %w", seg, off, ioErr)
	}
	if n < encoding.RecordHeaderSize {
		return types.Record{}, buf, fmt.Errorf("%w: leitura de %d bytes no segmento %d offset %d (err=%v)",
			encoding.ErrCorrupt, n, seg, off, err)
	}
	h, err := encoding.ParseHeader(buf[:n])
	if err != nil {
		return types.Record{}, buf, err
	}
	total := h.TotalSize()
	if total > n {
		// Record maior que a leitura especulativa: segundo pread do resto.
		if cap(buf) < total {
			grown := make([]byte, total)
			copy(grown, buf[:n])
			buf = grown
		} else {
			buf = buf[:total]
		}
		m, err := readFullAt(f, buf[n:total], int64(off)+int64(n))
		if ioErr := genuineIOError(err); ioErr != nil {
			return types.Record{}, buf, fmt.Errorf("segment: ler segmento %d offset %d: %w", seg, off, ioErr)
		}
		if m < total-n {
			return types.Record{}, buf, fmt.Errorf("%w: record truncado no segmento %d offset %d (err=%v)",
				encoding.ErrCorrupt, seg, off, err)
		}
	}
	rec, err := encoding.ParseRecord(buf[:total])
	if err != nil {
		return types.Record{}, buf, err
	}
	return rec, buf, nil
}

// genuineIOError separa "acabou o arquivo" (EOF: fim do prefixo válido, o
// caller decide) de uma falha real do dispositivo, que nunca pode ser
// interpretada como fim do log.
func genuineIOError(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// readFullAt lê até len(b) bytes em off; EOF antes disso não é erro fatal
// aqui — o caller decide com base no total necessário.
func readFullAt(f io.ReaderAt, b []byte, off int64) (int, error) {
	n := 0
	for n < len(b) {
		m, err := f.ReadAt(b[n:], off+int64(n))
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// ScanFrom percorre os records válidos do segmento a partir de from,
// devolvendo o offset do fim válido (após o último record íntegro). Para no
// primeiro record inválido/incompleto SEM erro (spec §52 — o caller decide
// truncar naquele offset); erro do callback e erro de I/O genuíno abortam o
// scan e são propagados.
func (r *Reader) ScanFrom(seg uint32, from int64, fn func(off uint32, rec types.Record) error) (int64, error) {
	f, err := r.file(seg)
	if err != nil {
		return 0, err
	}
	return scanFrom(f, seg, from, fn)
}

// scanFrom implementa o scan sobre qualquer io.ReaderAt (separado de
// ScanFrom para ser testável com fakes de I/O).
func scanFrom(f io.ReaderAt, seg uint32, from int64, fn func(off uint32, rec types.Record) error) (int64, error) {
	off := from
	var buf []byte
	for {
		rec, b, err := readRecordAt(f, seg, uint32(off), buf)
		buf = b
		if err != nil {
			// Só corrupção/fim de arquivo marca o fim do prefixo válido. Um
			// EIO NÃO pode virar "fim do log": o caller trunca no offset
			// devolvido, e descartar records íntegros por uma falha de
			// leitura seria perda de dados silenciosa.
			if errors.Is(err, encoding.ErrCorrupt) {
				return off, nil
			}
			return off, err
		}
		if err := fn(uint32(off), rec); err != nil {
			return off, err
		}
		off += int64(encoding.RecordHeaderSize + len(rec.Key) + len(rec.Payload))
	}
}

// FechaSegmento fecha e descarta o descritor cacheado de UM segmento.
//
// Existe para a aposentadoria da compaction, e é o que devolve os blocos ao
// filesystem: o unlink remove o NOME, e enquanto qualquer descritor mantiver o
// inode aberto os blocos seguem alocados. Sem esta chamada, um processo de vida
// longa acumula um inode preso por segmento aposentado, e a compaction devolve
// disco ao listing do diretório sem devolver um byte ao dispositivo.
//
// Segmento não cacheado não é erro: quem nunca foi lido não tem descritor a
// fechar, e cobrar isso do chamador só o obrigaria a saber do cache.
//
// O CHAMADOR PRECISA GARANTIR que nenhuma leitura esteja em voo sobre este
// segmento — no Store isso é a barreira aposMu. Fechar sob um pread em
// andamento faz a leitura falhar com os.ErrClosed, que aborta a query.
func (r *Reader) FechaSegmento(seg uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.fds[seg]
	if !ok {
		return nil
	}
	delete(r.fds, seg)
	if err := f.Close(); err != nil {
		return fmt.Errorf("segment: fechar o segmento %d: %w", seg, err)
	}
	return nil
}

// Close fecha todos os segmentos abertos pelo reader.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for id, f := range r.fds {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(r.fds, id)
	}
	return firstErr
}
