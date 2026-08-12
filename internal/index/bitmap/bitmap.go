// Package bitmap implementa índices bitmap file-backed por valor distinto
// (spec §41, §116): um arquivo de bits denso indexado por rowId para cada
// valor de um campo de baixa cardinalidade.
//
// Consistência: bits podem estar momentaneamente defasados para leitores —
// o executor sempre revalida o predicate contra o estado corrente (spec
// §44), e o replay do tail reconverge os bitmaps após crash.
package bitmap

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Layout dos arquivos:
//
//	values.dict (header 8B): magic "NDBV" | version u8 | reserved[3]
//	    entry: type u8 | valLen u16 LE | valBytes | crc32c u32
//	    (valueID implícito, 1-based; I64-kinds serializados como 8B LE)
//
//	%08d.bm (header 64B): magic "NDBB" | version u8 | reserved u8 |
//	    fieldID u16 LE | valueID u32 LE | reserved até 64
//	    words u64 a partir de 64; bit do rowId = word[rowId/64] >> (rowId%64)
const (
	dictHeaderSize = 8
	bmHeaderSize   = 64

	bmVersion = 1

	// initialWords dimensiona um bitmap novo para 64Ki rows (8 KiB).
	initialWords = 1024
)

var (
	dictMagic  = [4]byte{'N', 'D', 'B', 'V'}
	bmMagic    = [4]byte{'N', 'D', 'B', 'B'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// canonicalValue serializa a Value em bytes canônicos [type][payload] para
// chavear o dicionário de valores.
func canonicalValue(dst []byte, v types.Value) ([]byte, error) {
	switch v.Type {
	case types.TypeInt64, types.TypeDouble, types.TypeBool, types.TypeTimestamp:
		dst = append(dst, byte(v.Type))
		return binary.LittleEndian.AppendUint64(dst, uint64(v.I64)), nil
	case types.TypeString, types.TypeBytes, types.TypeJSON,
		types.TypeIPv4, types.TypeIPv6, types.TypeIPv4Prefix, types.TypeIPv6Prefix:
		dst = append(dst, byte(v.Type))
		return append(dst, v.Bytes...), nil
	default:
		return nil, fmt.Errorf("bitmap: tipo %#x não indexável", v.Type)
	}
}

// Index é o índice bitmap de um campo. Set/Clear são exclusivos do writer;
// Lookup e iteradores são seguros para leitores concorrentes. Os mapas em
// heap são bounded pela cardinalidade de VALORES (baixa por definição).
type Index struct {
	dir      string
	fieldID  types.FieldID
	dictF    *os.File
	dictSize int64

	mu      sync.RWMutex
	byKey   map[string]uint32
	keys    []string
	regions map[uint32]*mmap.Region

	scratch []byte // buffer do writer (Set/Clear); Lookup aloca o próprio
}

func bmPath(dir string, valueID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%08d.bm", valueID))
}

// Open cria/reabre o diretório do índice de um campo, validando o prefixo
// do values.dict por CRC e truncando tail rasgado.
func Open(dir string, fieldID types.FieldID) (*Index, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("bitmap: criar %s: %w", dir, err)
	}
	path := filepath.Join(dir, "values.dict")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("bitmap: abrir %s: %w", path, err)
	}
	ix := &Index{
		dir:     dir,
		fieldID: fieldID,
		dictF:   f,
		byKey:   make(map[string]uint32),
		regions: make(map[uint32]*mmap.Region),
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	if len(raw) == 0 {
		var h [dictHeaderSize]byte
		copy(h[0:4], dictMagic[:])
		h[4] = bmVersion
		if _, err := f.WriteAt(h[:], 0); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		ix.dictSize = dictHeaderSize
		return ix, nil
	}
	if len(raw) < dictHeaderSize || [4]byte(raw[0:4]) != dictMagic || raw[4] != bmVersion {
		f.Close()
		return nil, fmt.Errorf("bitmap: header inválido em %s", path)
	}
	pos := dictHeaderSize
	for pos < len(raw) {
		rest := raw[pos:]
		if len(rest) < 3+4 {
			break
		}
		valLen := int(binary.LittleEndian.Uint16(rest[1:3]))
		if len(rest) < 3+valLen+4 {
			break
		}
		body := rest[:3+valLen]
		crc := binary.LittleEndian.Uint32(rest[3+valLen : 3+valLen+4])
		if crc32.Checksum(body, castagnoli) != crc {
			break
		}
		key := string(body[0:1]) + string(body[3:3+valLen])
		ix.keys = append(ix.keys, key)
		ix.byKey[key] = uint32(len(ix.keys))
		pos += 3 + valLen + 4
	}
	if pos != len(raw) {
		if err := f.Truncate(int64(pos)); err != nil {
			f.Close()
			return nil, fmt.Errorf("bitmap: truncar tail rasgado: %w", err)
		}
	}
	ix.dictSize = int64(pos)
	return ix, nil
}

// lookupID resolve o valueID de uma key canônica sem criar.
func (ix *Index) lookupID(key []byte) (uint32, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	id, ok := ix.byKey[string(key)]
	return id, ok
}

// createID registra um valor novo no dict (com fsync antes de devolver o
// id — mesma regra do fields.dict). Exclusivo do writer.
func (ix *Index) createID(key []byte) (uint32, error) {
	valBytes := key[1:]
	if len(valBytes) > math.MaxUint16 {
		return 0, fmt.Errorf("bitmap: valor com %d bytes excede u16", len(valBytes))
	}
	entry := make([]byte, 0, 3+len(valBytes)+4)
	entry = append(entry, key[0])
	entry = binary.LittleEndian.AppendUint16(entry, uint16(len(valBytes)))
	entry = append(entry, valBytes...)
	entry = binary.LittleEndian.AppendUint32(entry, crc32.Checksum(entry, castagnoli))

	if _, err := ix.dictF.WriteAt(entry, ix.dictSize); err != nil {
		return 0, fmt.Errorf("bitmap: append no values.dict: %w", err)
	}
	if err := ix.dictF.Sync(); err != nil {
		return 0, fmt.Errorf("bitmap: fsync do values.dict: %w", err)
	}
	ix.mu.Lock()
	ix.dictSize += int64(len(entry))
	ix.keys = append(ix.keys, string(key))
	id := uint32(len(ix.keys))
	ix.byKey[string(key)] = id
	ix.mu.Unlock()
	return id, nil
}

// region devolve o mapeamento do bitmap do valueID; com create=false e
// arquivo ausente devolve (nil, nil) — bitmap vazio.
func (ix *Index) region(valueID uint32, create bool) (*mmap.Region, error) {
	ix.mu.RLock()
	r, ok := ix.regions[valueID]
	ix.mu.RUnlock()
	if ok {
		return r, nil
	}
	path := bmPath(ix.dir, valueID)
	st, err := os.Stat(path)
	exists := err == nil && st.Size() > 0
	if !exists && !create {
		return nil, nil
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()
	if r, ok := ix.regions[valueID]; ok {
		return r, nil
	}
	r, err = mmap.Open(path, bmHeaderSize+initialWords*8)
	if err != nil {
		return nil, err
	}
	h := r.Bytes(0, bmHeaderSize)
	if !exists {
		copy(h[0:4], bmMagic[:])
		h[4] = bmVersion
		binary.LittleEndian.PutUint16(h[6:8], uint16(ix.fieldID))
		binary.LittleEndian.PutUint32(h[8:12], valueID)
	} else {
		if [4]byte(h[0:4]) != bmMagic || h[4] != bmVersion {
			r.Close()
			return nil, fmt.Errorf("bitmap: header inválido em %s", path)
		}
		if got := types.FieldID(binary.LittleEndian.Uint16(h[6:8])); got != ix.fieldID {
			r.Close()
			return nil, fmt.Errorf("bitmap: %s pertence ao campo %d, esperado %d", path, got, ix.fieldID)
		}
		if got := binary.LittleEndian.Uint32(h[8:12]); got != valueID {
			r.Close()
			return nil, fmt.Errorf("bitmap: %s declara valueID %d, esperado %d", path, got, valueID)
		}
	}
	ix.regions[valueID] = r
	return r, nil
}

func wordOff(rowID types.RowID) int64 { return bmHeaderSize + int64(rowID/64)*8 }

// Set liga o bit do rowId no bitmap do valor, alocando valueID/arquivo na
// primeira ocorrência do valor. Exclusivo do writer.
func (ix *Index) Set(v types.Value, rowID types.RowID) error {
	key, err := canonicalValue(ix.scratch[:0], v)
	if err != nil {
		return err
	}
	ix.scratch = key
	id, ok := ix.lookupID(key)
	if !ok {
		if id, err = ix.createID(key); err != nil {
			return err
		}
	}
	r, err := ix.region(id, true)
	if err != nil {
		return err
	}
	off := wordOff(rowID)
	if off+8 > r.Size() {
		newSize := max(r.Size()*2, off+8)
		newSize = (newSize + 4095) &^ 4095
		if err := r.Grow(newSize); err != nil {
			return err
		}
	}
	r.AtomicOr64(off, 1<<(rowID%64))
	return nil
}

// Clear desliga o bit do rowId no bitmap do valor. Valor desconhecido,
// bitmap ausente ou rowId além do arquivo são no-ops (nada a limpar).
// Exclusivo do writer.
func (ix *Index) Clear(v types.Value, rowID types.RowID) error {
	key, err := canonicalValue(ix.scratch[:0], v)
	if err != nil {
		return err
	}
	ix.scratch = key
	id, ok := ix.lookupID(key)
	if !ok {
		return nil
	}
	r, err := ix.region(id, false)
	if err != nil || r == nil {
		return err
	}
	off := wordOff(rowID)
	if off+8 > r.Size() {
		return nil
	}
	r.AtomicAnd64(off, ^uint64(1<<(rowID%64)))
	return nil
}

// Lookup devolve um iterador ordenado de rowIds com o bit ligado.
// ok=false indica valor nunca visto pelo índice.
func (ix *Index) Lookup(v types.Value) (*Iterator, bool, error) {
	key, err := canonicalValue(nil, v)
	if err != nil {
		return nil, false, err
	}
	id, ok := ix.lookupID(key)
	if !ok {
		return nil, false, nil
	}
	r, err := ix.region(id, false)
	if err != nil {
		return nil, false, err
	}
	if r == nil {
		return &Iterator{}, true, nil
	}
	return &Iterator{region: r, nWords: (r.Size() - bmHeaderSize) / 8}, true, nil
}

// Values devolve o número de valores distintos registrados.
func (ix *Index) Values() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.keys)
}

// Msync força as páginas sujas de todos os bitmaps para disco.
func (ix *Index) Msync() error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for id, r := range ix.regions {
		if err := r.Msync(); err != nil {
			return fmt.Errorf("bitmap: msync do valueID %d: %w", id, err)
		}
	}
	return nil
}

// Close libera todos os mapeamentos e o dict.
func (ix *Index) Close() error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var firstErr error
	for id, r := range ix.regions {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(ix.regions, id)
	}
	if err := ix.dictF.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Iterator percorre rowIds em ordem crescente. O snapshot de tamanho é
// tirado no Lookup; bits alterados durante a iteração podem ou não aparecer
// (aceitável: o predicate é revalidado contra o estado corrente).
type Iterator struct {
	region  *mmap.Region
	nWords  int64
	nextIdx int64
	cur     uint64
	row     types.RowID
}

// Next avança para o próximo rowId; false no fim.
func (it *Iterator) Next() bool {
	for {
		if it.cur != 0 {
			tz := bits.TrailingZeros64(it.cur)
			it.row = types.RowID(uint64(it.nextIdx-1)*64 + uint64(tz))
			it.cur &= it.cur - 1
			return true
		}
		if it.region == nil || it.nextIdx >= it.nWords {
			return false
		}
		it.cur = it.region.AtomicLoad64(bmHeaderSize + it.nextIdx*8)
		it.nextIdx++
	}
}

// RowID devolve o rowId corrente.
func (it *Iterator) RowID() types.RowID { return it.row }

// Err devolve o erro do iterador (sempre nil nesta implementação; mantido
// pelo contrato de iteradores).
func (it *Iterator) Err() error { return nil }

// Close libera o iterador.
func (it *Iterator) Close() error { return nil }
