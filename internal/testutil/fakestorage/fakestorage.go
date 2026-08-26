// Package fakestorage é o dublê de teste do Store do storage engine para o
// ramo materializer/query. Implementa a MESMA superfície do *storage.Store
// (§15A item 4 do plano de storage): GetByKey/Put para o writer e
// GetByRowID/ScanLive/LookupBitmap/Sequence para o executor, com assinaturas
// idênticas às do real para que qualquer interface declarada no consumidor
// aceite os dois.
//
// As invariantes observáveis também são espelhadas, inclusive as
// desconfortáveis: payload validado com a rotina real, rowId monotônico
// nunca reutilizado nem queimado por evento rejeitado, liveness derivada das
// flags, lookup indexado devolvendo SUPERSET de candidatos (§44), campo sem
// índice devolvendo erro e teto do values.dict aplicado a campo indexado. Um
// dublê mais gentil que o real esconde justamente os bugs que ele deveria
// expor.
//
// O que o fake NÃO modela, e por quê:
//
//   - lock de dataset (storage.ErrDatasetLocked): não há diretório nem Open
//     aqui, então não existe exclusão mútua a violar;
//   - Options.BitmapFieldNames e AllowBitmapDrop: o análogo é o struct
//     Indexes, fixado na criação — não há backfill nem drop autorizado a
//     testar, e o conjunto de índices nunca muda em vida;
//   - modo falho sticky (types.ErrWriterFailed): o fake não tem append
//     durável, então nada o mata. O Apply (Q10) precisa exercitar esse
//     caminho e vai instalar um seam de injeção quando o teste existir;
//   - queima de rowId APÓS o append: no real, falha pós-append não tem
//     rollback e o id fica queimado. Aqui não há ponto de não-retorno, então
//     a validação é a única fronteira — a que importa para o consumidor;
//   - validação de fieldID contra o DICIONÁRIO: o Store real recusa payload
//     que referencie um fieldID inexistente (o dicionário é dele), e o dublê
//     não tem dicionário — o `fakestorage.Dict` é um objeto separado, do
//     teste. Consequência prática: um payload com fieldID inventado passa
//     aqui e é recusado lá. Não é corrigível sem dar um dicionário ao dublê,
//     e a rota real (materializer sobre schema.Compile) nunca produz um ID
//     que não venha do dicionário. Registrado na I1; a paridade de
//     comportamento do que o executor observa está coberta por
//     comportamento_test.go.
//
// Escala pequena por design (maps): é infraestrutura de teste, não engine.
package fakestorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
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

// valKey serializa a Value na MESMA forma canônica do índice bitmap real
// ([type][payload], valuekey.Append): tipo + os 8 bytes do I64 para os
// tipos de largura fixa, tipo + os bytes crus para os variáveis.
//
// Já foi um fmt.Sprintf. Era chamado por campo no diff, o que fazia um Apply
// de evento RADIUS custar ~120 alocações — todas do dublê, escondendo a
// medição do código sob teste nos benchmarks que o usam.
func valKey(v types.Value) string {
	var scratch [40]byte // cobre ipv6_prefix (17B) e a maioria das strings
	b := append(scratch[:0], byte(v.Type))
	// CarregaI64 e não um switch local: um tipo de largura fixa esquecido no
	// ramo de bytes faria TODOS os seus valores colapsarem na chave [tag] no
	// dublê — teste verde aqui e explosão na integração, que é exatamente a
	// divergência que este dublê existe para expor.
	if v.Type.CarregaI64() {
		b = binary.LittleEndian.AppendUint64(b, uint64(v.I64))
	} else {
		b = append(b, v.Bytes...)
	}
	return string(b)
}

// sameValue compara duas Values sem passar por valKey — o diff só precisa
// saber se o valor MUDOU, e comparar campo a campo não constrói string
// nenhuma.
func sameValue(a, b types.Value) bool {
	return a.Type == b.Type && a.I64 == b.I64 && bytes.Equal(a.Bytes, b.Bytes)
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
	// ValidateRecord, e não ValidatePayload: é o que o Store REAL chama, e ele
	// aplica também os tetos de formato (MaxKeySize, MaxRecordSize). Com a
	// validação mais fraca, o dublê ACEITAVA record que o real recusa — a
	// divergência mais perigosa que um dublê pode ter, porque o teste passa no
	// desenvolvimento e a ingestão falha em produção.
	if _, err := encoding.ValidateRecord(key, payload); err != nil {
		return 0, false, err
	}
	if err := s.validateIndexable(payload); err != nil {
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

// validateIndexable recusa valor grande demais para o dicionário de valores
// do índice bitmap, como o real faz ANTES de tornar o record durável.
//
// Vale só para campo COM índice bitmap: o teto é do values.dict, não do
// payload — campo sem índice aceita o mesmo valor sem reclamar, e o teste
// prova os dois lados. Espelhar isto importa porque a violação, no real, é
// fatal se escapar: o record fica durável, o replay do tail tenta reindexá-lo
// a cada abertura e o dataset inteiro deixa de abrir. Um dublê que aceitasse
// deixaria o teste passar e a integração explodir.
func (s *Store) validateIndexable(payload []byte) error {
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			return r.Err()
		}
		if s.bitmap[id] && len(v.Bytes) > types.MaxIndexedValueBytes {
			return fmt.Errorf("%w: campo %d tem índice bitmap e valor de %d bytes, acima do teto de %d",
				types.ErrInvalidArgument, id, len(v.Bytes), types.MaxIndexedValueBytes)
		}
	}
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
			// O check de índice vem ANTES da comparação: campo sem índice não
			// tem posting a atualizar, e é a maioria esmagadora dos campos de
			// um evento.
			if s.indexed[pid] && !sameValue(pv, nv) {
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

// Estado espelha o Store real: três estados, e o record de uma entidade
// expirada volta com RowID e Sequence VÁLIDOS — é deles que o stale check
// depende para não deixar um evento antigo ressuscitar a row.
func (s *Store) Estado(key, buf []byte) (types.Record, []byte, types.Presenca, error) {
	rowID, ok := s.byKey[string(key)]
	if !ok {
		return types.Record{}, buf, types.PresencaAusente, nil
	}
	r := s.rows[rowID]
	rec, out, lido, err := s.readRow(r, buf)
	if err != nil || !lido {
		return types.Record{}, out, types.PresencaAusente, err
	}
	if !r.live() {
		return rec, out, types.PresencaExpirada, nil
	}
	return rec, out, types.PresencaViva, nil
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
func (s *Store) Sequence(id types.RowID) (int64, types.Presenca) {
	r, ok := s.rows[id]
	if !ok {
		return 0, types.PresencaAusente
	}
	// A sequence de uma row expirada volta, como no Store real: é o
	// lastSequence do §58, e escondê-la reabriria o débito 25 no dublê.
	if !r.live() {
		return r.seq, types.PresencaExpirada
	}
	return r.seq, types.PresencaViva
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

// LimpaTombstone desliga o bit, para o teste observar a entidade reaberta.
func (s *Store) LimpaTombstone(id types.RowID) {
	if r, ok := s.rows[id]; ok {
		r.flags &^= types.FlagTombstone
	}
}

// RowIDDaKey resolve a key sem passar pela liveness — o teste precisa do id
// da row que acabou de marcar como lápide.
func (s *Store) RowIDDaKey(key string) (types.RowID, bool) {
	id, ok := s.byKey[key]
	return id, ok
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

// HasHash informa se o campo tem índice hash — o capability check POR CAMPO
// do planner (Fase 8), análogo do HasBitmap. Sem ele, um schema que declara
// `index: hash` num campo que o store não indexa faria o planner escolher o
// plano de hash e o executor falhar em runtime, que é exatamente o que a
// degradação para full scan existe para evitar.
func (s *Store) HasHash(field types.FieldID) bool { return s.hash[field] }

// InjectPosting insere um posting no índice do campo SEM alterar o estado da
// row. Existe para reproduzir o falso positivo que o Store real tolera por
// construção (§14.16: erro de Clear é contabilizado e deixa posting órfão) — é
// o único jeito de provar que o executor revalida o predicate (§44) em vez de
// confiar no índice.
//
// Campo sem índice declarado, ou valor acima do teto do values.dict, é
// ErrInvalidArgument: são estados que o Store real não alcança, e um dublê que
// os criasse deixaria passar aqui um teste que falha na integração. RowId
// inexistente é ACEITO — posting apontando para row que não está mais lá é
// justamente um dos estados órfãos do real.
func (s *Store) InjectPosting(field types.FieldID, v types.Value, id types.RowID) error {
	if !s.indexed[field] {
		return fmt.Errorf("%w: campo %d não possui índice", types.ErrInvalidArgument, field)
	}
	if s.bitmap[field] && len(v.Bytes) > types.MaxIndexedValueBytes {
		return fmt.Errorf("%w: campo %d tem índice bitmap e valor de %d bytes, acima do teto de %d",
			types.ErrInvalidArgument, field, len(v.Bytes), types.MaxIndexedValueBytes)
	}
	s.addPosting(field, v, id)
	return nil
}

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

// Dict é o fake do FieldDictionary (IDs sequenciais a partir de 1), com o
// mesmo contrato de tipo fixado do real.
type Dict struct {
	ids   map[string]types.FieldID
	names map[types.FieldID]string
	hints map[types.FieldID]types.FieldType
	next  types.FieldID
}

func NewDict() *Dict {
	return &Dict{
		ids:   map[string]types.FieldID{},
		names: map[types.FieldID]string{},
		hints: map[types.FieldID]types.FieldType{},
		next:  1,
	}
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

// Len devolve quantos campos o dicionário registrou. Junto com Name é o que
// permite ao schema.Compile enumerar os campos que uma execução anterior
// materializou.
func (d *Dict) Len() int { return len(d.names) }

// TypeHint devolve o tipo fixado do campo; ok=false para campo inexistente ou
// sem tipo fixado — mesma semântica do real.
func (d *Dict) TypeHint(id types.FieldID) (types.FieldType, bool) {
	t, ok := d.hints[id]
	return t, ok && t != types.TypeNull
}

// SetTypeHint espelha a regra R11 do dicionário real: idempotente para o
// mesmo tipo, conflito para tipo diferente.
//
// Usa o MESMO sentinela do real (fielddict.ErrTypeHintConflict). Um dublê com
// erro próprio faria o teste do consumidor passar com o fake e falhar com o
// real — que é exatamente a divergência que um dublê existe para expor.
func (d *Dict) SetTypeHint(id types.FieldID, t types.FieldType) error {
	if t == types.TypeNull {
		return fmt.Errorf("fakestorage: tipo nulo não pode ser fixado no campo %d", id)
	}
	if _, existe := d.names[id]; !existe {
		return fmt.Errorf("fakestorage: campo %d inexistente", id)
	}
	switch atual := d.hints[id]; atual {
	case t:
		return nil
	case types.TypeNull:
		d.hints[id] = t
		return nil
	default:
		return fmt.Errorf("%w: campo %q fixado como %s, tentativa de %s",
			fielddict.ErrTypeHintConflict, d.names[id], atual, t)
	}
}
