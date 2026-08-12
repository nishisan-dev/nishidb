package primary

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ValidateFunc confirma se o rowId candidato pertence de fato à key
// procurada — no Store real, lê o record via locations e compara a key.
// O hash nunca é prova de igualdade (spec §36).
type ValidateFunc func(rowID types.RowID) (bool, error)

// Layout do arquivo primary.idx (little-endian):
//
//	0   4  magic "NDBP"
//	4   1  version = 1
//	5   1  hashKind = 1 (fnv1a64+fmix64)
//	6   2  reserved
//	8   8  slotCount (u64, potência de 2)
//	16  8  seed (u64, 0 na v1)
//	24  40 reserved
//	64  .. slots de 16 bytes: [hash u64][rowId u64]
//
// Estados de slot pelo campo hash: 0=EMPTY, 1=DELETED (reservado, sem
// remoção na v1); hashes reais são sempre >= 2 (remapReserved).
const (
	headerSize = 64
	slotSize   = 16

	idxVersion  = 1
	hashKindFnv = 1

	slotEmpty   = 0
	slotDeleted = 1

	// loadFactorNum/Den: rehash ao exceder 70% de ocupação.
	loadFactorNum = 7
	loadFactorDen = 10

	minSlotCount = 1024
)

var idxMagic = [4]byte{'N', 'D', 'B', 'P'}

// Index é a hash table open-addressing disk-backed (mmap) da primary.
// Find é seguro para leitores concorrentes; Insert é exclusivo do writer.
type Index struct {
	mu        sync.RWMutex // protege region/slotCount contra swap do rehash
	region    *mmap.Region
	path      string
	slotCount uint64
	used      atomic.Uint64
	hashFn    func([]byte) uint64
}

func nextPow2(v uint64) uint64 {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len64(v-1)
}

func fileSizeFor(slots uint64) int64 { return headerSize + int64(slots)*slotSize }

func writeIdxHeader(r *mmap.Region, slots uint64) {
	h := r.Bytes(0, headerSize)
	copy(h[0:4], idxMagic[:])
	h[4] = idxVersion
	h[5] = hashKindFnv
	binary.LittleEndian.PutUint64(h[8:16], slots)
	binary.LittleEndian.PutUint64(h[16:24], 0) // seed
}

func readIdxHeader(r *mmap.Region) (uint64, error) {
	h := r.Bytes(0, headerSize)
	if [4]byte(h[0:4]) != idxMagic {
		return 0, fmt.Errorf("primary: magic inválida %q", h[0:4])
	}
	if h[4] != idxVersion {
		return 0, fmt.Errorf("primary: version desconhecida %d", h[4])
	}
	if h[5] != hashKindFnv {
		return 0, fmt.Errorf("primary: hashKind desconhecido %d (rebuild necessário)", h[5])
	}
	slots := binary.LittleEndian.Uint64(h[8:16])
	if slots < minSlotCount || slots&(slots-1) != 0 {
		return 0, fmt.Errorf("primary: slotCount inválido %d", slots)
	}
	if r.Size() < fileSizeFor(slots) {
		return 0, fmt.Errorf("primary: arquivo com %d bytes menor que o necessário para %d slots", r.Size(), slots)
	}
	return slots, nil
}

// Open cria (ou reabre) o índice com pelo menos minSlots slots
// (arredondado para potência de 2). Rehash interrompido por crash deixa um
// ".new" órfão, que é descartado aqui.
func Open(path string, minSlots uint64) (*Index, error) {
	if err := os.Remove(path + ".new"); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("primary: remover rehash órfão: %w", err)
	}
	st, err := os.Stat(path)
	isNew := os.IsNotExist(err)
	if err != nil && !isNew {
		return nil, fmt.Errorf("primary: stat %s: %w", path, err)
	}
	if !isNew && st.Size() == 0 {
		isNew = true
	}

	slots := nextPow2(max(minSlots, minSlotCount))
	ix := &Index{path: path, hashFn: HashKey}
	if isNew {
		region, err := mmap.Open(path, fileSizeFor(slots))
		if err != nil {
			return nil, err
		}
		writeIdxHeader(region, slots)
		ix.region, ix.slotCount = region, slots
		return ix, nil
	}

	region, err := mmap.Open(path, st.Size())
	if err != nil {
		return nil, err
	}
	slots, err = readIdxHeader(region)
	if err != nil {
		region.Close()
		return nil, err
	}
	ix.region, ix.slotCount = region, slots
	// Recontagem de ocupação: o valor não é confiável em header mmap após
	// crash; a varredura sequencial no open é a fonte de verdade.
	var used uint64
	for i := uint64(0); i < slots; i++ {
		if region.AtomicLoad64(headerSize+int64(i)*slotSize) >= 2 {
			used++
		}
	}
	ix.used.Store(used)
	return ix, nil
}

// SlotCount devolve o número de slots da tabela.
func (ix *Index) SlotCount() uint64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.slotCount
}

// Used devolve o número de slots ocupados.
func (ix *Index) Used() uint64 { return ix.used.Load() }

func slotOff(i uint64) int64 { return headerSize + int64(i)*slotSize }

// Find procura a key e devolve o rowId validado.
func (ix *Index) Find(key []byte, validate ValidateFunc) (types.RowID, bool, error) {
	h := remapReserved(ix.hashFn(key))
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	mask := ix.slotCount - 1
	for probe := uint64(0); probe < ix.slotCount; probe++ {
		off := slotOff((h + probe) & mask)
		switch sh := ix.region.AtomicLoad64(off); sh {
		case slotEmpty:
			return 0, false, nil
		case slotDeleted:
			continue
		case h:
			// load-acquire do hash garante visibilidade do rowId (gravado
			// antes do hash no Insert).
			rid := types.RowID(ix.region.AtomicLoad64(off + 8))
			ok, err := validate(rid)
			if err != nil {
				return 0, false, err
			}
			if ok {
				return rid, true, nil
			}
		}
	}
	return 0, false, nil
}

// Insert é insert-if-absent: se a key já existe (validada), devolve o rowId
// existente com inserted=false. Cresce a tabela automaticamente ao exceder
// o load factor. Exclusivo do writer único.
func (ix *Index) Insert(key []byte, rowID types.RowID, validate ValidateFunc) (types.RowID, bool, error) {
	if (ix.used.Load()+1)*loadFactorDen > ix.SlotCount()*loadFactorNum {
		if err := ix.grow(); err != nil {
			return 0, false, err
		}
	}
	h := remapReserved(ix.hashFn(key))
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	mask := ix.slotCount - 1
	for probe := uint64(0); probe < ix.slotCount; probe++ {
		off := slotOff((h + probe) & mask)
		switch sh := ix.region.AtomicLoad64(off); sh {
		case slotEmpty:
			// Ordem de publicação: rowId primeiro, hash (marcador de
			// ocupação) por último com release — leitor que vê o hash
			// necessariamente vê o rowId.
			ix.region.AtomicStore64(off+8, uint64(rowID))
			ix.region.AtomicStore64(off, h)
			ix.used.Add(1)
			return rowID, true, nil
		case slotDeleted:
			continue
		case h:
			rid := types.RowID(ix.region.AtomicLoad64(off + 8))
			ok, err := validate(rid)
			if err != nil {
				return 0, false, err
			}
			if ok {
				return rid, false, nil
			}
		}
	}
	return 0, false, fmt.Errorf("primary: tabela cheia com %d slots (bug de load factor)", ix.slotCount)
}

// grow reconstrói a tabela com o dobro de slots em um arquivo novo e troca
// atomicamente (rename). Chamado apenas pelo writer.
func (ix *Index) grow() error {
	oldSlots := ix.SlotCount()
	newSlots := oldSlots * 2
	newPath := ix.path + ".new"

	if err := os.Remove(newPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("primary: limpar rehash anterior: %w", err)
	}
	newRegion, err := mmap.Open(newPath, fileSizeFor(newSlots))
	if err != nil {
		return err
	}
	writeIdxHeader(newRegion, newSlots)

	// Re-insere por hash armazenado — sem re-hash das keys, sem validação
	// (os pares hash/rowId já são válidos).
	newMask := newSlots - 1
	for i := uint64(0); i < oldSlots; i++ {
		sh := ix.region.AtomicLoad64(slotOff(i))
		if sh < 2 {
			continue
		}
		rid := ix.region.AtomicLoad64(slotOff(i) + 8)
		for probe := uint64(0); ; probe++ {
			off := slotOff((sh + probe) & newMask)
			if newRegion.AtomicLoad64(off) == slotEmpty {
				newRegion.AtomicStore64(off+8, rid)
				newRegion.AtomicStore64(off, sh)
				break
			}
		}
	}
	if err := newRegion.Msync(); err != nil {
		newRegion.Close()
		return err
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()
	if err := os.Rename(newPath, ix.path); err != nil {
		newRegion.Close()
		return fmt.Errorf("primary: rename do rehash: %w", err)
	}
	old := ix.region
	ix.region = newRegion
	ix.slotCount = newSlots
	if err := old.Close(); err != nil {
		return fmt.Errorf("primary: fechar tabela antiga: %w", err)
	}
	return nil
}

// Msync força as páginas sujas do índice para disco.
func (ix *Index) Msync() error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.region.Msync()
}

// Close desfaz o mapeamento e fecha o arquivo.
func (ix *Index) Close() error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.region.Close()
}
