// Package fakestorage é o dublê de teste do Store do storage engine para o
// ramo materializer/query. Implementa a MESMA superfície do *storage.Store
// (§15A item 4 do plano de storage): GetByKey/Put para o writer e
// GetByRowID/ScanLive/LookupBitmap/Sequence para o executor, com assinaturas
// idênticas às do real para que qualquer interface declarada no consumidor
// aceite os dois.
//
// As invariantes observáveis também são espelhadas, inclusive as
// desconfortáveis: payload validado com a rotina real, rowId monotônico
// nunca reutilizado, liveness derivada das flags, lookup indexado devolvendo
// SUPERSET de candidatos (§44) e campo sem índice devolvendo erro. Um dublê
// mais gentil que o real esconde justamente os bugs que ele deveria expor.
//
// Escala pequena por design (maps): é infraestrutura de teste, não engine.
package fakestorage

import (
	"bytes"
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
	rowID   types.RowID
	seq     int64
	flags   uint8
	key     []byte // cópia própria
	payload []byte // cópia própria
}

// live deriva a liveness das flags — ÚNICA fonte de verdade, como no real
// (o bit vai empacotado na location, §15A item 7).
func (r *row) live() bool { return r.flags&types.FlagTombstone == 0 }

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
	bitmap   map[types.FieldID]bool
	hash     map[types.FieldID]bool
	indexed  map[types.FieldID]bool // união bitmap+hash (o diff é igual)
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
		byKey: map[string]types.RowID{},
		rows:  map[types.RowID]*row{},
		// rowIds são 0-based, como no real (nextRowID.Add(1)-1 sobre um
		// contador zerado): os índices densos indexam por rowId, e começar
		// em 1 desperdiçaria o slot 0 em todos eles.
		nextRow:  0,
		bitmap:   map[types.FieldID]bool{},
		hash:     map[types.FieldID]bool{},
		indexed:  map[types.FieldID]bool{},
		postings: map[postingKey]map[types.RowID]struct{}{},
	}
	for _, f := range idx.Bitmap {
		s.bitmap[f] = true
		s.indexed[f] = true
	}
	for _, f := range idx.Hash {
		s.hash[f] = true
		s.indexed[f] = true
	}
	return s
}

func valKey(v types.Value) string {
	return fmt.Sprintf("%d|%d|%x", v.Type, v.I64, v.Bytes)
}

// Put espelha o write path do Store real: valida os argumentos, resolve o
// rowId (novo, do prev ou da key existente), valida o payload (ordem
// crescente de FieldID, tag 0x00 proibida), diffa os campos indexados
// prev↔novo e publica. Sequence NÃO é checada aqui — stale check é
// responsabilidade do Apply.
//
// Contrato do prev (idêntico ao real): quando fornecido, prev DEVE ser o
// record corrente DESTA key; prev de outra key é ErrInvalidArgument, porque
// a violação silenciosa gravaria o estado na row errada. prev==nil com key
// existente é resolvido internamente.
//
// flags é a única fonte de liveness: Put com types.FlagTombstone mata a row.
//
// TODA validação (key, prev, payload) acontece ANTES da reserva do rowId: um
// evento rejeitado não pode queimar um id, senão os índices densos por rowId
// crescem com buracos que nenhum estado ocupa.
func (s *Store) Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error) {
	if len(key) == 0 {
		return 0, false, fmt.Errorf("%w: key vazia", types.ErrInvalidArgument)
	}
	if prev != nil && !bytes.Equal(prev.Key, key) {
		return 0, false, fmt.Errorf("%w: prev pertence à key %q, não a %q", types.ErrInvalidArgument, prev.Key, key)
	}
	if _, err := encoding.ValidatePayload(payload); err != nil {
		return 0, false, err
	}

	var (
		rowID       types.RowID
		created     bool
		prevPayload []byte
	)
	switch existing, exists := s.byKey[string(key)]; {
	case prev != nil:
		rowID = prev.RowID
		prevPayload = prev.Payload
	case exists:
		rowID = existing
		prevPayload = s.rows[existing].payload
	default:
		rowID = s.nextRow
		s.nextRow++
		created = true
	}

	s.byKey[string(key)] = rowID
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
	return rowID, created, nil
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
// contrato de lifetime do Store real). ok=false para key ausente OU
// tombstoned.
func (s *Store) GetByKey(key, buf []byte) (types.Record, []byte, bool, error) {
	rowID, ok := s.byKey[string(key)]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	r := s.rows[rowID]
	if !r.live() {
		return types.Record{}, buf, false, nil
	}
	return s.readRow(r, buf)
}

// GetByRowID idem, por rowId; ok=false para row inexistente ou tombstoned
// (é a checagem de liveness dos caminhos indexados).
func (s *Store) GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	r, ok := s.rows[id]
	if !ok || !r.live() {
		return types.Record{}, buf, false, nil
	}
	return s.readRow(r, buf)
}

// Sequence devolve a última sequence aplicada à row. ok=false apenas para
// row sem estado publicado: row tombstoned MANTÉM a sequence visível (no
// real a location existe, só carrega o bit de tombstone).
func (s *Store) Sequence(id types.RowID) (int64, bool) {
	r, ok := s.rows[id]
	if !ok {
		return 0, false
	}
	return r.seq, true
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

// SetTombstone liga o bit de tombstone nas flags da row (helper de teste; no
// Store real isso virá do TTL/tombstones na Fase 9, também via flags). Note
// que as postings NÃO são limpas — é justamente o que torna o lookup um
// superset.
func (s *Store) SetTombstone(id types.RowID) {
	if r, ok := s.rows[id]; ok {
		r.flags |= types.FlagTombstone
	}
}

// ScanLive devolve um iterador de rowIds vivos em ordem CRESCENTE
// (pré-condição das interseções/uniões do executor).
func (s *Store) ScanLive() types.RowIDIterator {
	ids := make([]types.RowID, 0, len(s.rows))
	for id, r := range s.rows {
		if r.live() {
			ids = append(ids, id)
		}
	}
	sortRowIDs(ids)
	return &rowIDIter{ids: ids}
}

// HasBitmap informa se o campo tem índice bitmap — o capability check do
// planner. Sem ele o plano escolhe o caminho indexado e a query falha em
// runtime, em vez de degradar para full scan como o design promete.
func (s *Store) HasBitmap(field types.FieldID) bool { return s.bitmap[field] }

// BitmapFields devolve, em ordem crescente, os campos com índice bitmap.
func (s *Store) BitmapFields() []types.FieldID {
	out := make([]types.FieldID, 0, len(s.bitmap))
	for field := range s.bitmap {
		out = append(out, field)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// LookupBitmap consulta o índice bitmap do campo; valor nunca visto devolve
// iterador vazio; campo sem índice bitmap é ErrInvalidArgument. O resultado
// é um SUPERSET de candidatos: rows tombstoned podem aparecer — valide
// liveness e predicate via GetByRowID (spec §44).
func (s *Store) LookupBitmap(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	if !s.bitmap[field] {
		return nil, fmt.Errorf("%w: campo %d não possui índice bitmap", types.ErrInvalidArgument, field)
	}
	return s.lookup(field, v), nil
}

// LookupHash é a capability da Fase 8 — no fake, idêntica ao bitmap sobre os
// campos declarados com índice hash.
func (s *Store) LookupHash(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	if !s.hash[field] {
		return nil, fmt.Errorf("%w: campo %d não possui índice hash", types.ErrInvalidArgument, field)
	}
	return s.lookup(field, v), nil
}

func (s *Store) lookup(field types.FieldID, v types.Value) types.RowIDIterator {
	set := s.postings[postingKey{field, valKey(v)}]
	ids := make([]types.RowID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sortRowIDs(ids)
	return &rowIDIter{ids: ids}
}

func sortRowIDs(ids []types.RowID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}

// rowIDIter é a implementação materializada (o real é lazy; para o consumidor
// só importa a interface).
type rowIDIter struct {
	ids []types.RowID
	pos int
}

func (it *rowIDIter) Next() bool {
	if it.pos >= len(it.ids) {
		return false
	}
	it.pos++
	return true
}

func (it *rowIDIter) RowID() types.RowID { return it.ids[it.pos-1] }
func (it *rowIDIter) Err() error         { return nil }
func (it *rowIDIter) Close() error       { return nil }

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
