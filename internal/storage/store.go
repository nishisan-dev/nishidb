// Package storage é a fachada do subsistema de storage do NishiDB: orquestra
// segments, índices e dicionários atrás de uma API única (spec §15A do
// design). Checkpoint/manifest são a S11; recovery é a S12.
package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/index/bitmap"
	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrInvalidArgument indica violação dos argumentos de Put (ex.: key vazia).
var ErrInvalidArgument = errors.New("storage: argumento inválido")

// Options configura a abertura de um Store.
type Options struct {
	Dir           string
	SegmentSize   int64           // default 512 MiB; teto validado pelo segment
	EstimatedRows uint64          // pre-sizing da primary (piso 1<<20 slots)
	BitmapFields  []types.FieldID // campos com índice bitmap
	SyncEveryPut  bool            // durability=sync (testes/config)
}

// Stats é o snapshot mínimo de estado do Store.
type Stats struct {
	RowCount          uint64
	SegmentsTotal     uint32
	CurrentSegmentID  uint32
	PrimaryLoadFactor float64
	OverlayLen        int
}

// RowIDIterator itera rowIds em ordem crescente.
type RowIDIterator interface {
	Next() bool
	RowID() types.RowID
	Err() error
	Close() error
}

const defaultSegmentSize = 512 << 20

// Store é a fachada do storage.
//
// Concorrência: Put e Sync são exclusivos do writer único (não são
// thread-safe entre si); GetByKey/GetByRowID/ScanLive/LookupBitmap/Sequence/
// Stats são seguros para leitores concorrentes com o writer.
type Store struct {
	opts Options

	dict    *fielddict.Dict
	segW    *segment.Writer
	segR    *segment.Reader
	prim    *primary.Index
	locs    *rowindex.Locations
	seqs    *rowindex.Sequences
	bitmaps map[types.FieldID]*bitmap.Index

	nextRowID atomic.Uint64
	rowCount  atomic.Uint64
	epoch     uint64 // último epoch de manifest gravado (writer-only)

	// Buffers exclusivos do writer (Put).
	buildBuf []byte // record em construção
	prevBuf  []byte // fallback de leitura do estado anterior
	valBuf   []byte // validação de colisão no Insert
}

// Open abre (ou cria) o dataset no diretório. Nesta fase (pré-S12) o
// diretório de dados precisa estar sem segmentos: dataset existente exige
// recovery, que chega na S12.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("%w: Dir vazio", ErrInvalidArgument)
	}
	if opts.SegmentSize == 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	metaDir := filepath.Join(opts.Dir, "metadata")
	dataDir := filepath.Join(opts.Dir, "data")
	idxDir := filepath.Join(opts.Dir, "indexes")
	cpDir := filepath.Join(opts.Dir, "checkpoint")
	for _, d := range []string{metaDir, dataDir, idxDir, cpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("storage: criar %s: %w", d, err)
		}
	}
	if entries, err := os.ReadDir(dataDir); err != nil {
		return nil, err
	} else if len(entries) > 0 {
		return nil, fmt.Errorf("storage: %s contém segmentos; dataset existente exige recovery (S12)", dataDir)
	}

	s := &Store{opts: opts, bitmaps: make(map[types.FieldID]*bitmap.Index)}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()

	var err error
	if s.dict, err = fielddict.Open(filepath.Join(metaDir, "fields.dict")); err != nil {
		return nil, err
	}
	if s.segW, err = segment.OpenWriter(dataDir, opts.SegmentSize, 0, 0); err != nil {
		return nil, fmt.Errorf("storage: abrir segmentos (SegmentSize=%d): %w", opts.SegmentSize, err)
	}
	if s.segR, err = segment.OpenReader(dataDir); err != nil {
		return nil, err
	}
	// Piso de 1<<20 slots do design §5 aplica-se aqui, não no pacote primary.
	minSlots := max(uint64(1<<20), opts.EstimatedRows*10/7)
	if s.prim, err = primary.Open(filepath.Join(idxDir, "primary.idx"), minSlots); err != nil {
		return nil, err
	}
	minRows := max(uint64(512), opts.EstimatedRows)
	if s.locs, err = rowindex.OpenLocations(filepath.Join(idxDir, "locations.idx"), minRows); err != nil {
		return nil, err
	}
	if s.seqs, err = rowindex.OpenSequences(filepath.Join(idxDir, "sequences.idx"), minRows); err != nil {
		return nil, err
	}
	for _, field := range opts.BitmapFields {
		if field == 0 {
			return nil, fmt.Errorf("%w: fieldID 0 em BitmapFields", ErrInvalidArgument)
		}
		dir := filepath.Join(idxDir, "bitmap", fmt.Sprintf("%05d", field))
		ix, err := bitmap.Open(dir, field)
		if err != nil {
			return nil, err
		}
		s.bitmaps[field] = ix
	}
	ok = true
	return s, nil
}

// FieldDict expõe o dicionário de campos do dataset.
func (s *Store) FieldDict() *fielddict.Dict { return s.dict }

// lookup resolve a key pela primary, lendo o record corrente em buf.
// exists = key presente; live = sem flag de tombstone na location.
//
// O callback de validação roda sob o RLock do índice primary: ele NÃO pode
// reentrar no índice (deadlock com um rehash enfileirado) — mantém-se
// mínimo: locations.Get + pread + comparação de key (spec §36).
func (s *Store) lookup(key, buf []byte) (rec types.Record, out []byte, exists, live bool, err error) {
	out = buf
	validate := func(rid types.RowID) (bool, error) {
		packed, ok := s.locs.Get(rid)
		if !ok {
			// Slot da primary sem location (janela de crash): não é a row.
			return false, nil
		}
		seg, off, locFlags := rowindex.Unpack(packed)
		var r types.Record
		var rerr error
		r, out, rerr = s.segR.ReadRecord(seg, off, out)
		if rerr != nil {
			return false, rerr
		}
		if !bytes.Equal(r.Key, key) {
			return false, nil // colisão de hash: continua o probe
		}
		rec = r
		live = locFlags&uint8(types.FlagTombstone) == 0
		return true, nil
	}
	_, exists, err = s.prim.Find(key, validate)
	if err != nil {
		return types.Record{}, out, false, false, err
	}
	return rec, out, exists, live, nil
}

// GetByKey resolve a key pela primary (validação de colisão inclusa) e
// devolve o record corrente sobre buf (possivelmente crescido).
// ok=false para key ausente ou tombstoned.
func (s *Store) GetByKey(key, buf []byte) (types.Record, []byte, bool, error) {
	rec, out, exists, live, err := s.lookup(key, buf)
	if err != nil || !exists || !live {
		return types.Record{}, out, false, err
	}
	return rec, out, true, nil
}

// GetByRowID devolve o record corrente da row. ok=false para row sem
// location ou tombstoned (checagem de liveness dos caminhos indexados).
func (s *Store) GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	packed, ok := s.locs.Get(id)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	seg, off, locFlags := rowindex.Unpack(packed)
	if locFlags&uint8(types.FlagTombstone) != 0 {
		return types.Record{}, buf, false, nil
	}
	rec, out, err := s.segR.ReadRecord(seg, off, buf)
	if err != nil {
		return types.Record{}, out, false, err
	}
	if rec.RowID != id {
		return types.Record{}, out, false, fmt.Errorf("%w: location da row %d aponta para record da row %d",
			encoding.ErrCorrupt, id, rec.RowID)
	}
	return rec, out, true, nil
}

// Put aplica um estado materializado na ordem do design §10.1 e publica a
// location atomicamente (commit point, spec §45/§46). prev, quando
// fornecido, é confiado (rowId e estado anterior) — evita refazer o Find.
// Exclusivo do writer único.
func (s *Store) Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error) {
	if len(key) == 0 {
		return 0, false, fmt.Errorf("%w: key vazia", ErrInvalidArgument)
	}
	var (
		rowID       types.RowID
		created     bool
		prevPayload []byte
	)
	if prev != nil {
		rowID = prev.RowID
		prevPayload = prev.Payload
	} else {
		found, out, exists, _, err := s.lookup(key, s.prevBuf[:0])
		s.prevBuf = out
		if err != nil {
			return 0, false, err
		}
		if exists {
			rowID = found.RowID
			prevPayload = found.Payload
		} else {
			rowID = types.RowID(s.nextRowID.Load())
			created = true
		}
	}

	// 1. encode (BuildRecord embute ValidatePayload e os tetos do formato).
	rec, err := encoding.BuildRecord(s.buildBuf[:0], rowID, seq, flags, key, payload)
	s.buildBuf = rec
	if err != nil {
		return 0, false, err
	}
	// 2. append (rotação interna ao segment).
	seg, off, err := s.segW.Append(rec)
	if err != nil {
		return 0, false, err
	}
	// 3. offset accounting: nada além do packing — os bounds do formato já
	// foram validados na construção, e o segment garante off < segmentSize.
	packed := rowindex.Pack(seg, off, flags&uint8(types.FlagTombstone))

	// 4. primary insert-if-absent para key nova.
	if created {
		_, inserted, err := s.prim.Insert(key, rowID, s.insertValidator(key))
		if err != nil {
			return 0, false, err
		}
		if !inserted {
			return 0, false, fmt.Errorf("storage: key %q já presente na primary com prev==nil e lookup ausente", key)
		}
	}
	// 5. diff de bitmaps (two-pointer sobre payloads ordenados por fieldID).
	if len(s.bitmaps) > 0 {
		if err := s.applyBitmapDiff(rowID, prevPayload, payload); err != nil {
			return 0, false, err
		}
	}
	// 6. sequence.
	if err := s.seqs.Set(rowID, seq); err != nil {
		return 0, false, err
	}
	// 7. COMMIT POINT: publica a location (overlay; visível a leitores).
	s.locs.Publish(rowID, packed)
	if created {
		s.nextRowID.Add(1)
		s.rowCount.Add(1)
	}
	if s.opts.SyncEveryPut {
		if err := s.Sync(); err != nil {
			return 0, false, err
		}
	}
	return rowID, created, nil
}

// insertValidator devolve o ValidateFunc de colisão do Insert. Roda sob o
// RLock da primary — não reentra no índice (ver lookup). Usa um buffer
// próprio para não invalidar o prevPayload em uso pelo Put.
func (s *Store) insertValidator(key []byte) primary.ValidateFunc {
	return func(rid types.RowID) (bool, error) {
		packed, ok := s.locs.Get(rid)
		if !ok {
			return false, nil
		}
		seg, off, _ := rowindex.Unpack(packed)
		var r types.Record
		var err error
		r, s.valBuf, err = s.segR.ReadRecord(seg, off, s.valBuf)
		if err != nil {
			return false, err
		}
		return bytes.Equal(r.Key, key), nil
	}
}

// valueEqual compara duas Values de mesmo campo.
func valueEqual(a, b types.Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case types.TypeInt64, types.TypeDouble, types.TypeBool, types.TypeTimestamp:
		return a.I64 == b.I64
	default:
		return bytes.Equal(a.Bytes, b.Bytes)
	}
}

// applyBitmapDiff aplica Clear/Set nos campos indexados que mudaram entre o
// estado anterior e o novo (merge linear de duas listas ordenadas por
// fieldID). Campo com o mesmo valor não toca o bitmap.
func (s *Store) applyBitmapDiff(rowID types.RowID, prevPayload, newPayload []byte) error {
	var pr, nr encoding.FieldReader
	pr.Reset(prevPayload)
	nr.Reset(newPayload)
	pid, pv, pok := pr.Next()
	nid, nv, nok := nr.Next()
	for pok || nok {
		switch {
		case pok && (!nok || pid < nid):
			if ix := s.bitmaps[pid]; ix != nil {
				if err := ix.Clear(pv, rowID); err != nil {
					return err
				}
			}
			pid, pv, pok = pr.Next()
		case nok && (!pok || nid < pid):
			if ix := s.bitmaps[nid]; ix != nil {
				if err := ix.Set(nv, rowID); err != nil {
					return err
				}
			}
			nid, nv, nok = nr.Next()
		default: // pid == nid
			if ix := s.bitmaps[pid]; ix != nil && !valueEqual(pv, nv) {
				if err := ix.Clear(pv, rowID); err != nil {
					return err
				}
				if err := ix.Set(nv, rowID); err != nil {
					return err
				}
			}
			pid, pv, pok = pr.Next()
			nid, nv, nok = nr.Next()
		}
	}
	if err := pr.Err(); err != nil {
		return err
	}
	return nr.Err()
}

// liveIterator percorre lazy as rows vivas até o limite fixado na criação.
type liveIterator struct {
	s     *Store
	next  types.RowID
	limit types.RowID
	cur   types.RowID
}

func (it *liveIterator) Next() bool {
	for it.next < it.limit {
		id := it.next
		it.next++
		packed, ok := it.s.locs.Get(id)
		if !ok {
			continue
		}
		if _, _, locFlags := rowindex.Unpack(packed); locFlags&uint8(types.FlagTombstone) != 0 {
			continue
		}
		it.cur = id
		return true
	}
	return false
}

func (it *liveIterator) RowID() types.RowID { return it.cur }
func (it *liveIterator) Err() error         { return nil }
func (it *liveIterator) Close() error       { return nil }

// emptyIterator é um RowIDIterator sem elementos.
type emptyIterator struct{}

func (emptyIterator) Next() bool         { return false }
func (emptyIterator) RowID() types.RowID { return 0 }
func (emptyIterator) Err() error         { return nil }
func (emptyIterator) Close() error       { return nil }

// ScanLive devolve um iterador lazy das rows vivas (rowIds crescentes,
// location válida — overlay incluso — e sem tombstone). Rows criadas após a
// chamada não aparecem (snapshot do limite superior).
func (s *Store) ScanLive() RowIDIterator {
	return &liveIterator{s: s, limit: types.RowID(s.nextRowID.Load())}
}

// LookupBitmap consulta o índice bitmap do campo; valor nunca visto devolve
// iterador vazio; campo não indexado é erro.
func (s *Store) LookupBitmap(field types.FieldID, v types.Value) (RowIDIterator, error) {
	ix, ok := s.bitmaps[field]
	if !ok {
		return nil, fmt.Errorf("%w: campo %d não possui índice bitmap", ErrInvalidArgument, field)
	}
	it, found, err := ix.Lookup(v)
	if err != nil {
		return nil, err
	}
	if !found {
		return emptyIterator{}, nil
	}
	return it, nil
}

// Sequence devolve a última sequence aplicada à row (ok=false para row sem
// location publicada — a sequence só tem significado com estado visível).
func (s *Store) Sequence(id types.RowID) (int64, bool) {
	if _, ok := s.locs.Get(id); !ok {
		return 0, false
	}
	return s.seqs.Get(id), true
}

// Sync força fdatasync do segmento corrente (loop de durability batch).
// Exclusivo do writer.
func (s *Store) Sync() error { return s.segW.Sync() }

// Stats devolve o snapshot de estado do Store.
func (s *Store) Stats() Stats {
	segID, _ := s.segW.Current()
	var lf float64
	if slots := s.prim.SlotCount(); slots > 0 {
		lf = float64(s.prim.Used()) / float64(slots)
	}
	return Stats{
		RowCount:          s.rowCount.Load(),
		SegmentsTotal:     segID, // IDs monotônicos desde 1, sem remoção no M1
		CurrentSegmentID:  segID,
		PrimaryLoadFactor: lf,
		OverlayLen:        s.locs.OverlayLen(),
	}
}

// Close fecha o dataset (sem checkpoint — S11).
func (s *Store) Close() error {
	var errs []error
	for _, ix := range s.bitmaps {
		if ix != nil {
			errs = append(errs, ix.Close())
		}
	}
	if s.seqs != nil {
		errs = append(errs, s.seqs.Close())
	}
	if s.locs != nil {
		errs = append(errs, s.locs.Close())
	}
	if s.prim != nil {
		errs = append(errs, s.prim.Close())
	}
	if s.segR != nil {
		errs = append(errs, s.segR.Close())
	}
	if s.segW != nil {
		errs = append(errs, s.segW.Close())
	}
	if s.dict != nil {
		errs = append(errs, s.dict.Close())
	}
	return errors.Join(errs...)
}
