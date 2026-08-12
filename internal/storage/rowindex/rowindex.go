// Package rowindex implementa os arrays densos indexados por rowId do
// NishiDB: locations.idx (rowId→localização física empacotada, spec §37) e
// sequences.idx (rowId→última sequence, spec §38).
package rowindex

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const (
	headerSize = 64
	entrySize  = 8

	fileVersion = 1

	// Limites do empacotamento de Location.
	offsetBits  = 30
	segmentBits = 32
	maxOffset   = (1 << offsetBits) - 1
	maxFlags    = 3
)

// Pack empacota uma localização física em um u64 publicável atomicamente:
// [63..62] flags | [61..30] segmentID | [29..0] offset.
// Nenhum valor válido é 0 (segmentID começa em 1): packed==0 = sem location.
// Offset limitado a 30 bits ⇒ segmentos de até 1 GiB (validado na config).
func Pack(segmentID, offset uint32, flags uint8) uint64 {
	if segmentID == 0 {
		panic("rowindex: segmentID 0 é reservado para 'sem location'")
	}
	if offset > maxOffset {
		panic(fmt.Sprintf("rowindex: offset %d excede %d bits", offset, offsetBits))
	}
	if flags > maxFlags {
		panic(fmt.Sprintf("rowindex: flags %#x excede 2 bits", flags))
	}
	return uint64(flags)<<(segmentBits+offsetBits) | uint64(segmentID)<<offsetBits | uint64(offset)
}

// Unpack desfaz o empacotamento de Pack.
func Unpack(p uint64) (segmentID, offset uint32, flags uint8) {
	return uint32(p >> offsetBits), uint32(p & maxOffset), uint8(p >> (segmentBits + offsetBits))
}

// denseFile é um array denso de u64 indexado por rowId sobre mmap, com
// header de 64 bytes. Base comum de Locations e Sequences.
type denseFile struct {
	region *mmap.Region
}

func openDense(path string, magic [4]byte, minRows uint64) (*denseFile, error) {
	minRows = max(minRows, 512)
	r, err := mmap.Open(path, headerSize+int64(minRows)*entrySize)
	if err != nil {
		return nil, err
	}
	var hdrErr error
	r.WithBytes(0, headerSize, func(h []byte) {
		if [4]byte(h[0:4]) == [4]byte{0, 0, 0, 0} && h[4] == 0 {
			// Arquivo novo: grava o header.
			copy(h[0:4], magic[:])
			h[4] = fileVersion
			h[5] = entrySize
			binary.LittleEndian.PutUint64(h[8:16], 0) // seed/reserved
			return
		}
		if [4]byte(h[0:4]) != magic {
			hdrErr = fmt.Errorf("rowindex: magic inválida em %s: %q (esperado %q)", path, h[0:4], magic[:])
			return
		}
		if h[4] != fileVersion || h[5] != entrySize {
			hdrErr = fmt.Errorf("rowindex: version/entrySize incompatível em %s: %d/%d", path, h[4], h[5])
		}
	})
	if hdrErr != nil {
		r.Close()
		return nil, hdrErr
	}
	return &denseFile{region: r}, nil
}

func entryOff(rowID types.RowID) int64 { return headerSize + int64(rowID)*entrySize }

// load devolve (0, false) para rowId além do arquivo — nunca cresce na leitura.
func (d *denseFile) load(rowID types.RowID) (uint64, bool) {
	off := entryOff(rowID)
	if off+entrySize > d.region.Size() {
		return 0, false
	}
	return d.region.AtomicLoad64(off), true
}

// store grava a entry, crescendo o arquivo (doubling) se necessário.
// Exclusivo do writer.
func (d *denseFile) store(rowID types.RowID, v uint64) error {
	off := entryOff(rowID)
	if needed := off + entrySize; needed > d.region.Size() {
		newSize := max(d.region.Size()*2, needed)
		newSize = (newSize + 4095) &^ 4095
		if err := d.region.Grow(newSize); err != nil {
			return err
		}
	}
	d.region.AtomicStore64(off, v)
	return nil
}

// Locations é o índice rowId→packed location com overlay em heap.
//
// Publicações via Publish ficam no overlay (bounded pelo intervalo de
// checkpoint) até ApplyOverlay gravá-las no mmap. Invariante de
// durabilidade: as páginas do arquivo só referenciam records já fsyncados,
// porque ApplyOverlay roda no checkpoint APÓS o fdatasync dos segmentos —
// writeback antecipado de página nunca cria dangling pointer.
type Locations struct {
	dense *denseFile
	mu    sync.RWMutex // protege over/applying
	over  map[types.RowID]uint64
	// applying é o lote em aplicação pelo checkpoint: não-nil somente
	// durante ApplyOverlay, somente-leitura enquanto exposto.
	applying map[types.RowID]uint64

	applyHook func() // seam de teste; nil em produção
}

// overlayEntry é um par (rowId, packed) materializado do overlay para
// iteração fora do lock.
type overlayEntry struct {
	row    types.RowID
	packed uint64
}

var locationsMagic = [4]byte{'N', 'D', 'B', 'L'}

// OpenLocations cria/reabre o arquivo com capacidade para minRows.
func OpenLocations(path string, minRows uint64) (*Locations, error) {
	d, err := openDense(path, locationsMagic, minRows)
	if err != nil {
		return nil, err
	}
	return &Locations{dense: d, over: make(map[types.RowID]uint64)}, nil
}

// Get devolve a location do rowId: overlay corrente, depois o lote em
// aplicação pelo checkpoint, depois o mmap.
func (l *Locations) Get(rowID types.RowID) (uint64, bool) {
	l.mu.RLock()
	p, ok := l.over[rowID]
	if !ok && l.applying != nil {
		p, ok = l.applying[rowID]
	}
	l.mu.RUnlock()
	if ok {
		return p, true
	}
	p, ok = l.dense.load(rowID)
	if !ok || p == 0 {
		return 0, false
	}
	return p, true
}

// Publish grava a location no overlay (commit point de visibilidade).
// Exclusivo do writer.
func (l *Locations) Publish(rowID types.RowID, packed uint64) {
	l.mu.Lock()
	l.over[rowID] = packed
	l.mu.Unlock()
}

// ApplyOverlay grava todas as entradas do overlay no mmap e o esvazia.
// Chamado no checkpoint, após o fdatasync dos segmentos, SEMPRE no goroutine
// do writer (invariante do §10.2 — Publish e ApplyOverlay nunca concorrem).
// O swap do map sob Lock codifica essa invariante: nenhuma iteração de map
// vivo acontece, então nem uma violação acidental aborta o processo.
// Leitores concorrentes (Get) veem cada entrada no overlay até ela já estar
// no mmap — nunca há janela sem a location.
func (l *Locations) ApplyOverlay() error {
	l.mu.Lock()
	pending := l.over
	l.over = make(map[types.RowID]uint64)
	// pending permanece visível aos leitores via l.applying até TODAS as
	// entradas estarem no mmap — uma row criada desde o último checkpoint
	// nunca fica invisível durante o apply. A partir daqui pending é
	// somente-leitura; a iteração acontece sobre a slice.
	l.applying = pending
	entries := make([]overlayEntry, 0, len(pending))
	for r, p := range pending {
		entries = append(entries, overlayEntry{row: r, packed: p})
	}
	l.mu.Unlock()

	for i, e := range entries {
		if err := l.dense.store(e.row, e.packed); err != nil {
			// Falha parcial (ex.: Grow sem espaço em disco): devolve as
			// entradas não aplicadas ao overlay para a retentativa do
			// próximo checkpoint; publicações posteriores ao swap têm
			// precedência (são mais novas).
			l.mu.Lock()
			for _, rest := range entries[i:] {
				if _, newer := l.over[rest.row]; !newer {
					l.over[rest.row] = rest.packed
				}
			}
			l.applying = nil
			l.mu.Unlock()
			return err
		}
		if l.applyHook != nil {
			l.applyHook()
		}
	}
	l.mu.Lock()
	l.applying = nil
	l.mu.Unlock()
	return nil
}

// SetDirect grava direto no mmap, sem overlay (recovery, sem leitores).
func (l *Locations) SetDirect(rowID types.RowID, packed uint64) error {
	return l.dense.store(rowID, packed)
}

// OverlayLen devolve o número de entradas pendentes no overlay.
func (l *Locations) OverlayLen() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.over)
}

// Msync força as páginas sujas para disco.
func (l *Locations) Msync() error { return l.dense.region.Msync() }

// Close libera o mapeamento.
func (l *Locations) Close() error { return l.dense.region.Close() }

// Sequences é o índice rowId→última sequence aplicada. A sequence só tem
// significado para rowIds com location publicada; 0 = nunca gravada.
type Sequences struct {
	dense *denseFile
}

var sequencesMagic = [4]byte{'N', 'D', 'B', 'Q'}

// OpenSequences cria/reabre o arquivo com capacidade para minRows.
func OpenSequences(path string, minRows uint64) (*Sequences, error) {
	d, err := openDense(path, sequencesMagic, minRows)
	if err != nil {
		return nil, err
	}
	return &Sequences{dense: d}, nil
}

// Get devolve a sequence do rowId (0 se nunca gravada).
func (s *Sequences) Get(rowID types.RowID) int64 {
	v, _ := s.dense.load(rowID)
	return int64(v)
}

// Set grava a sequence do rowId, crescendo o arquivo se necessário.
// Exclusivo do writer.
func (s *Sequences) Set(rowID types.RowID, seq int64) error {
	return s.dense.store(rowID, uint64(seq))
}

// Msync força as páginas sujas para disco.
func (s *Sequences) Msync() error { return s.dense.region.Msync() }

// Close libera o mapeamento.
func (s *Sequences) Close() error { return s.dense.region.Close() }
