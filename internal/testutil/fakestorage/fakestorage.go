// Package fakestorage é o dublê de teste do Store do storage engine para o
// ramo materializer/query. Implementa o contrato acordado (§15A do plano de
// storage / §2.2 e §3.7 do plano de materializer+query): GetByKey/Put para o
// writer e GetByRowID/ScanLive/LookupBitmap/LookupHash para o executor,
// espelhando as invariantes do Store real — validação de payload no Put,
// diff prev↔novo dos campos indexados, cópia para o buffer do caller nas
// leituras e iteradores de rowIds em ordem crescente.
//
// Escala pequena por design (maps): é infraestrutura de teste, não engine.
package fakestorage

import (
	"fmt"
	"sort"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Indexes declara quais campos têm índice secundário no fake (o Store real
// deriva isso do schema do dataset).
type Indexes struct {
	Bitmap []types.FieldID
	Hash   []types.FieldID
}

type row struct {
	rowID    types.RowID
	seq      int64
	flags    uint8
	key      []byte // cópia própria
	payload  []byte // cópia própria
	tombsted bool
}

// PutCall registra uma chamada de Put para asserções de instrumentação.
type PutCall struct {
	Key     string
	Seq     int64
	Flags   uint8
	HadPrev bool
	Created bool
}

// Store é o fake. Não é thread-safe (testes são single-goroutine por caso).
type Store struct {
	byKey    map[string]types.RowID
	rows     map[types.RowID]*row
	nextRow  types.RowID
	indexed  map[types.FieldID]bool // união de bitmap+hash (diff é igual)
	postings map[postingKey]map[types.RowID]struct{}

	// PutCalls instrumenta as chamadas na ordem em que ocorreram.
	PutCalls []PutCall
}

type postingKey struct {
	field types.FieldID
	val   string // encoding textual do Value (tipo+bytes+i64)
}

// New cria um Store vazio com os índices declarados.
func New(idx Indexes) *Store {
	s := &Store{
		byKey:    map[string]types.RowID{},
		rows:     map[types.RowID]*row{},
		nextRow:  1,
		indexed:  map[types.FieldID]bool{},
		postings: map[postingKey]map[types.RowID]struct{}{},
	}
	for _, f := range idx.Bitmap {
		s.indexed[f] = true
	}
	for _, f := range idx.Hash {
		s.indexed[f] = true
	}
	return s
}

func valKey(v types.Value) string {
	return fmt.Sprintf("%d|%d|%x", v.Type, v.I64, v.Bytes)
}

// Put espelha o write path do Store real: valida o payload (ordem crescente
// de FieldID, tag 0x00 proibida), cria a row se a key é nova, diffa os
// campos indexados prev↔novo e publica. prev==nil com key existente é
// resolvido internamente (fallback do contrato). Sequence NÃO é checada
// aqui — stale check é responsabilidade do Apply.
func (s *Store) Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (uint64, bool, error) {
	if _, err := encoding.ValidatePayload(payload); err != nil {
		return 0, false, err
	}
	rowID, exists := s.byKey[string(key)]
	created := !exists
	if !exists {
		rowID = s.nextRow
		s.nextRow++
		s.byKey[string(key)] = rowID
	}
	var prevPayload []byte
	if prev != nil {
		prevPayload = prev.Payload
	} else if exists {
		prevPayload = s.rows[rowID].payload
	}
	s.diffPostings(rowID, prevPayload, payload)
	s.rows[rowID] = &row{
		rowID:   rowID,
		seq:     seq,
		flags:   flags,
		key:     append([]byte(nil), key...),
		payload: append([]byte(nil), payload...),
	}
	s.PutCalls = append(s.PutCalls, PutCall{
		Key: string(key), Seq: seq, Flags: flags, HadPrev: prev != nil, Created: created,
	})
	return uint64(rowID), created, nil
}

// diffPostings percorre os dois payloads ordenados (merge linear, como o
// Store real) removendo postings antigos e inserindo novos dos campos
// indexados.
func (s *Store) diffPostings(rowID types.RowID, prevPayload, newPayload []byte) {
	var pr, nr encoding.FieldReader
	pr.Reset(prevPayload)
	nr.Reset(newPayload)
	pid, pv, pok := pr.Next()
	nid, nv, nok := nr.Next()
	for pok || nok {
		switch {
		case pok && (!nok || pid < nid):
			s.removePosting(pid, pv, rowID)
			pid, pv, pok = pr.Next()
		case nok && (!pok || nid < pid):
			s.addPosting(nid, nv, rowID)
			nid, nv, nok = nr.Next()
		default: // mesmo id
			if valKey(pv) != valKey(nv) {
				s.removePosting(pid, pv, rowID)
				s.addPosting(nid, nv, rowID)
			}
			pid, pv, pok = pr.Next()
			nid, nv, nok = nr.Next()
		}
	}
}

func (s *Store) addPosting(field types.FieldID, v types.Value, rowID types.RowID) {
	if !s.indexed[field] {
		return
	}
	k := postingKey{field, valKey(v)}
	set, ok := s.postings[k]
	if !ok {
		set = map[types.RowID]struct{}{}
		s.postings[k] = set
	}
	set[rowID] = struct{}{}
}

func (s *Store) removePosting(field types.FieldID, v types.Value, rowID types.RowID) {
	if !s.indexed[field] {
		return
	}
	delete(s.postings[postingKey{field, valKey(v)}], rowID)
}

// GetByKey devolve o record corrente da key, copiado para buf (Record.Key e
// Record.Payload são janelas zero-copy sobre o buf devolvido — mesmo
// contrato de lifetime do Store real).
func (s *Store) GetByKey(key, buf []byte) (types.Record, []byte, bool, error) {
	rowID, ok := s.byKey[string(key)]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return s.readRow(s.rows[rowID], buf)
}

// GetByRowID idem, por rowId; ok=false para row inexistente ou tombstoned
// (é a checagem de liveness dos caminhos indexados).
func (s *Store) GetByRowID(rowID uint64, buf []byte) (types.Record, []byte, bool, error) {
	r, ok := s.rows[types.RowID(rowID)]
	if !ok || r.tombsted {
		return types.Record{}, buf, false, nil
	}
	return s.readRow(r, buf)
}

func (s *Store) readRow(r *row, buf []byte) (types.Record, []byte, bool, error) {
	buf = buf[:0]
	keyStart := len(buf)
	buf = append(buf, r.key...)
	payStart := len(buf)
	buf = append(buf, r.payload...)
	rec := types.Record{
		RowID:    r.rowID,
		Sequence: r.seq,
		Flags:    r.flags,
		Key:      buf[keyStart:payStart:payStart],
		Payload:  buf[payStart:len(buf):len(buf)],
	}
	return rec, buf, true, nil
}

// SetTombstone marca a row como morta (helper de teste; no Store real isso
// virá do TTL/tombstones na Fase 9).
func (s *Store) SetTombstone(rowID uint64) {
	if r, ok := s.rows[types.RowID(rowID)]; ok {
		r.tombsted = true
	}
}

// ScanLive devolve um iterador de rowIds vivos em ordem CRESCENTE
// (pré-condição das interseções/uniões do executor).
func (s *Store) ScanLive() *RowIDIter {
	ids := make([]uint64, 0, len(s.rows))
	for id, r := range s.rows {
		if !r.tombsted {
			ids = append(ids, uint64(id))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return &RowIDIter{ids: ids}
}

// LookupBitmap devolve os rowIds com field==v, crescentes. Valor sem
// postings dá iterador vazio (não erro).
func (s *Store) LookupBitmap(field types.FieldID, v types.Value) (*RowIDIter, error) {
	return s.lookup(field, v)
}

// LookupHash é a capability da Fase 8 — no fake, idêntica ao bitmap.
func (s *Store) LookupHash(field types.FieldID, v types.Value) (*RowIDIter, error) {
	return s.lookup(field, v)
}

func (s *Store) lookup(field types.FieldID, v types.Value) (*RowIDIter, error) {
	set := s.postings[postingKey{field, valKey(v)}]
	ids := make([]uint64, 0, len(set))
	for id := range set {
		if r, ok := s.rows[id]; ok && !r.tombsted {
			ids = append(ids, uint64(id))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return &RowIDIter{ids: ids}, nil
}

// RowIDIter itera rowIds em ordem crescente (mesmo shape da interface que o
// executor declara).
type RowIDIter struct {
	ids []uint64
	pos int
}

func (it *RowIDIter) Next() bool {
	if it.pos >= len(it.ids) {
		return false
	}
	it.pos++
	return true
}

func (it *RowIDIter) Value() uint64 { return it.ids[it.pos-1] }
func (it *RowIDIter) Err() error    { return nil }
func (it *RowIDIter) Close() error  { return nil }

// Dict é o fake do FieldDictionary (IDs sequenciais a partir de 1).
type Dict struct {
	ids   map[string]types.FieldID
	names map[types.FieldID]string
	next  types.FieldID
}

func NewDict() *Dict {
	return &Dict{ids: map[string]types.FieldID{}, names: map[types.FieldID]string{}, next: 1}
}

func (d *Dict) LookupOrCreate(name []byte) (types.FieldID, error) {
	if id, ok := d.ids[string(name)]; ok {
		return id, nil
	}
	id := d.next
	d.next++
	d.ids[string(name)] = id
	d.names[id] = string(name)
	return id, nil
}

func (d *Dict) Name(id types.FieldID) (string, bool) {
	n, ok := d.names[id]
	return n, ok
}
