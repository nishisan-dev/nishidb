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
	// entrySize é a entrada dos vetores de 64 bits — todos os deste arquivo e
	// a agenda. entrySize32 é a do Dense32 (dense32.go). O byte 5 do header
	// grava qual das duas o arquivo usa, e a abertura recusa a outra.
	entrySize   = 8
	entrySize32 = 4

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

// denseFile é um array denso indexado por rowId sobre mmap, com header de 64
// bytes. Base comum de Locations, Sequences, LastWrites, TTLs, da agenda (entrada
// de 8 bytes) e do Dense32 (entrada de 4).
type denseFile struct {
	region *mmap.Region
	// entry é o tamanho da entrada, gravado no byte 5 do header. É campo, e
	// não a constante de sempre, desde o Dense32: os métodos de 64 e de 32
	// endereçam pelo mesmo off(), e é este campo que faz o off de um vetor de 4
	// bytes cair na entrada certa.
	entry int64
	// born registra que ESTA abertura criou o arquivo, e não que ela o
	// reencontrou. É o único sinal que distingue "índice ainda não existia"
	// de "índice existe e está vazio" — os dois têm o mesmo conteúdo, e para
	// um índice derivado a diferença decide entre construir e adotar.
	born bool
}

// openDense abre um vetor de entradas de 64 bits — a forma de todos os
// vetores anteriores ao Dense32, mantida como está para que nenhum chamador
// mude.
func openDense(path string, magic [4]byte, minRows uint64) (*denseFile, error) {
	return openDenseWithEntrySize(path, magic, minRows, entrySize)
}

// openDenseWithEntrySize abre um vetor com o tamanho de entrada dado. O
// tamanho vai para o byte 5 do header na criação e é CONFERIDO na reabertura:
// um vetor gravado com um tamanho e lido com outro endereçaria a entrada
// errada em toda linha sem que nada falhasse.
func openDenseWithEntrySize(path string, magic [4]byte, minRows uint64, entry int64) (*denseFile, error) {
	minRows = max(minRows, 512)
	r, err := mmap.Open(path, headerSize+int64(minRows)*entry)
	if err != nil {
		return nil, err
	}
	var hdrErr error
	var born bool
	r.WithBytes(0, headerSize, func(h []byte) {
		if [4]byte(h[0:4]) == [4]byte{0, 0, 0, 0} && h[4] == 0 {
			// Arquivo novo: grava o header.
			born = true
			copy(h[0:4], magic[:])
			h[4] = fileVersion
			h[5] = byte(entry)
			binary.LittleEndian.PutUint64(h[8:16], 0) // seed/reserved
			return
		}
		if [4]byte(h[0:4]) != magic {
			hdrErr = fmt.Errorf("rowindex: magic inválida em %s: %q (esperado %q)", path, h[0:4], magic[:])
			return
		}
		if h[4] != fileVersion || int64(h[5]) != entry {
			hdrErr = fmt.Errorf("rowindex: version/entrySize incompatível em %s: %d/%d (esperado %d/%d)",
				path, h[4], h[5], fileVersion, entry)
		}
	})
	if hdrErr != nil {
		r.Close()
		return nil, hdrErr
	}
	return &denseFile{region: r, entry: entry, born: born}, nil
}

// entryOff é o offset de uma entrada de 64 bits. Continua função de pacote —
// e não método — porque a agenda e o ReleaseBelow o usam sobre vetores que são
// sempre de 64.
func entryOff(rowID types.RowID) int64 { return headerSize + int64(rowID)*entrySize }

// off é o offset da entrada deste arquivo, no tamanho que ele gravou.
func (d *denseFile) off(rowID types.RowID) int64 { return headerSize + int64(rowID)*d.entry }

// grow estende o arquivo (doubling, arredondado a página) até caber needed.
func (d *denseFile) grow(needed int64) error {
	if needed <= d.region.Size() {
		return nil
	}
	newSize := max(d.region.Size()*2, needed)
	newSize = (newSize + 4095) &^ 4095
	return d.region.Grow(newSize)
}

// load devolve (0, false) para rowId além do arquivo — nunca cresce na leitura.
func (d *denseFile) load(rowID types.RowID) (uint64, bool) {
	off := d.off(rowID)
	if off+entrySize > d.region.Size() {
		return 0, false
	}
	return d.region.AtomicLoad64(off), true
}

// store grava a entry, crescendo o arquivo (doubling) se necessário.
// Exclusivo do writer.
func (d *denseFile) store(rowID types.RowID, v uint64) error {
	off := d.off(rowID)
	if err := d.grow(off + entrySize); err != nil {
		return err
	}
	d.region.AtomicStore64(off, v)
	return nil
}

// load32 e store32 são os irmãos de 4 bytes de load e store, para o Dense32.
func (d *denseFile) load32(rowID types.RowID) (uint32, bool) {
	off := d.off(rowID)
	if off+entrySize32 > d.region.Size() {
		return 0, false
	}
	return d.region.AtomicLoad32(off), true
}

func (d *denseFile) store32(rowID types.RowID, v uint32) error {
	off := d.off(rowID)
	if err := d.grow(off + entrySize32); err != nil {
		return err
	}
	d.region.AtomicStore32(off, v)
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

	applyHook    func()                  // seam de teste; nil em produção
	storeErrHook func(types.RowID) error // seam de teste; nil em produção
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
// NÃO é reentrante nem thread-safe consigo mesmo: uma chamada por vez.
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
		err := l.storeOne(e)
		if err != nil {
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

// storeOne grava uma entrada do lote no mmap, com o seam de injeção de
// erro dos testes de falha parcial.
func (l *Locations) storeOne(e overlayEntry) error {
	if l.storeErrHook != nil {
		if err := l.storeErrHook(e.row); err != nil {
			return err
		}
	}
	return l.dense.store(e.row, e.packed)
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

// LastWrites é o índice rowId→instante da última escrita, em epoch millis.
//
// É DERIVADO, como o Sequences: a fonte da verdade é o campo de sistema no
// payload do record, e o replay do tail o reconstrói pelo mesmo applyRecord
// que o Put usa. Isso não é detalhe de implementação, é o que torna a
// expiração segura contra crash — um índice sem fonte no record deixaria a
// row nascida no tail com entrada zerada, e não existe interpretação segura
// de zero: "sem prazo" vaza a row para sempre, "vencido" apaga uma row viva.
//
// Por isso zero significa DESCONHECIDO, nunca vencido. Row desconhecida ganha
// um prazo cheio de carência na primeira varredura, e é o que permite ligar a
// expiração num dataset já materializado sem reprocessar nada.
type LastWrites struct {
	dense *denseFile
}

var lastWritesMagic = [4]byte{'N', 'D', 'B', 'E'}

// OpenLastWrites cria/reabre o arquivo com capacidade para minRows.
func OpenLastWrites(path string, minRows uint64) (*LastWrites, error) {
	d, err := openDense(path, lastWritesMagic, minRows)
	if err != nil {
		return nil, err
	}
	return &LastWrites{dense: d}, nil
}

// Get devolve o instante da última escrita do rowId em epoch millis, e
// known=false quando a entrada nunca foi gravada.
func (e *LastWrites) Get(rowID types.RowID) (millis int64, known bool) {
	v, ok := e.dense.load(rowID)
	if !ok || v == 0 {
		return 0, false
	}
	return int64(v), true
}

// Set grava o instante da última escrita. Exclusivo do writer.
func (e *LastWrites) Set(rowID types.RowID, millis int64) error {
	return e.dense.store(rowID, uint64(millis))
}

// Msync força as páginas sujas para disco.
func (e *LastWrites) Msync() error { return e.dense.region.Msync() }

// Close libera o mapeamento.
func (e *LastWrites) Close() error { return e.dense.region.Close() }

// ImmortalTTL é o valor gravado para a entidade cuja regra decidiu que ela
// NÃO expira.
//
// Existe porque denseFile.load devolve zero para a entrada nunca gravada,
// então "gravei zero" e "nunca gravei" seriam o mesmo byte — e aqui os dois
// não podem significar a mesma coisa. Desconhecido cai no prazo de fundo;
// imortal não pode cair em lugar nenhum. Sem a distinção, a entidade que casa
// com uma regra `never` num dataset que TAMBÉM declara regra de fundo herdaria
// o prazo de fundo, e o alarme que o operador mandou nunca expirar sairia
// sozinho.
//
// A duração declarável mínima é um minuto, mas o prazo gravado NÃO é sempre a
// declarada: numa regra com `anchor` ele é o que resta até o vencimento, e
// pode ser de poucos millis — ou nenhum. Por isso o materializer nunca grava
// menos que MinMortalTTL: 1 aqui seria lido como imortal, e uma linha cuja
// âncora já venceu viveria para sempre.
const ImmortalTTL uint64 = 1

// MinMortalTTL é o menor prazo gravável que SIGNIFICA prazo: o sentinela de
// imortal mais um. É o valor que o materializer grava para a linha que nasce
// vencida — invisível a partir de `_writtenAt + 2 ms` e recolhida pelo coletor
// como qualquer outra.
const MinMortalTTL = ImmortalTTL + 1

// TTLs é o índice rowId→prazo contado a partir da escrita, em millis.
//
// É DERIVADO, como o LastWrites: a fonte da verdade é o campo de sistema no
// payload, e o replay do tail o reconstrói pelo mesmo applyRecord que o Put
// usa. Zero significa DESCONHECIDO — entidade anterior à adoção das regras —,
// nunca imortal; para imortal existe o ImmortalTTL.
type TTLs struct {
	dense *denseFile
}

var ttlsMagic = [4]byte{'N', 'D', 'B', 'P'}

// OpenTTLs cria/reabre o arquivo com capacidade para minRows.
func OpenTTLs(path string, minRows uint64) (*TTLs, error) {
	d, err := openDense(path, ttlsMagic, minRows)
	if err != nil {
		return nil, err
	}
	return &TTLs{dense: d}, nil
}

// Get devolve a duração em millis, e known=false quando a entrada nunca foi
// gravada. Duração zero COM known=true é a entidade imortal.
func (p *TTLs) Get(rowID types.RowID) (millis int64, known bool) {
	v, ok := p.dense.load(rowID)
	if !ok || v == 0 {
		return 0, false
	}
	if v == ImmortalTTL {
		return 0, true
	}
	return int64(v), true
}

// Set grava a duração. Exclusivo do writer.
//
// Duração não positiva vira o sentinela: é a entidade que uma regra `never`
// declarou imortal, e ela precisa ser distinguível da que nunca foi gravada.
func (p *TTLs) Set(rowID types.RowID, millis int64) error {
	v := uint64(millis)
	if millis <= 0 {
		v = ImmortalTTL
	}
	return p.dense.store(rowID, v)
}

// Msync força as páginas sujas para disco.
func (p *TTLs) Msync() error { return p.dense.region.Msync() }

// Close libera o mapeamento.
func (p *TTLs) Close() error { return p.dense.region.Close() }

// ReleaseBelow devolve ao filesystem os blocos das entradas ABAIXO de rowID.
//
// A purga do histórico é de segmento e move uma marca-d'água, não limpa
// entrada por entrada: zerar milhões de u64 sob o lock do writer a cada
// passada disputaria a escrita por um trabalho proporcional ao que foi
// apagado, quando o que mudou é um número só. O custo dessa escolha é o
// prefixo morto do arquivo, que continua alocado — e é ele que sai aqui.
//
// O tamanho APARENTE do arquivo não muda: um índice denso é endereçado por
// offset, e encolhê-lo pela frente mudaria o endereço de toda entrada viva.
// Quem quiser medir o que o histórico ocupa de verdade precisa ler blocos
// (st_blocks), não Size().
//
// Erro de filesystem sem suporte sobe para o chamador decidir: é degradação
// de espaço, nunca de correção.
func (l *Locations) ReleaseBelow(rowID types.RowID) error {
	end := entryOff(rowID)
	if end <= headerSize {
		return nil
	}
	return l.dense.region.ReleaseBlocks(headerSize, end-headerSize)
}
