// Package metausers serve a tabela virtual `_meta.users`: a tabela de usuários
// do servidor (issue #40) como um ADMIN pode vê-la — nome, papel e quem mexeu
// quando, NUNCA o hash.
//
// Mora fora de `internal/users` e de `internal/query` porque precisa dos dois,
// e nenhum dos dois pode passar a importar o outro — o mesmo argumento do
// `internal/alarmtable`.
//
// A tabela é registrada como SÓ-ADMIN no motor: a lista de contas e de quem é
// ADMIN é informação que um usuário READ não recebe, e a barreira mora no
// motor, e não no servidor, para que nenhum caminho de consulta a esqueça.
package metausers

import (
	"fmt"
	"slices"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/nishisan-dev/nishidb/internal/users"
)

// TableMetaUsers é o nome da tabela sob o `_meta`.
const TableMetaUsers = "users"

// specOfTable é a spec da tabela, no MESMO YAML de um dataset de verdade, como
// as outras tabelas do `_meta`: reusar o compilador dá WHERE, ORDER BY, LIMIT e
// projeção sem gramática nova.
//
// Não há coluna de hash, nem de sal, nem de custo. A ausência é a propriedade:
// o que não está no schema não sai por projeção nenhuma, nem por `SELECT *`.
const specOfTable = `version: 1
name: ` + TableMetaUsers + `
schemaName: ` + schema.SyntheticSchemaName + `
materialization:
  key:
    field: username
schema:
  include: [username, role, bootstrap, createdAt, createdBy, updatedAt, updatedBy, passwordChangedAt]
  fields:
    username:
      type: string
    role:
      type: string
    bootstrap:
      type: boolean
    createdAt:
      type: timestamp
    createdBy:
      type: string
    updatedAt:
      type: timestamp
    updatedBy:
      type: string
    passwordChangedAt:
      type: timestamp
`

type column int

const (
	cUsername column = iota
	cRole
	cBootstrap
	cCreatedAt
	cCreatedBy
	cUpdatedAt
	cUpdatedBy
	cPasswordChangedAt
)

var columnNames = []string{
	"username", "role", "bootstrap", "createdAt", "createdBy", "updatedAt", "updatedBy", "passwordChangedAt",
}

// Source é o que a tabela precisa, resolvido NA HORA de cada consulta.
//
// Users é um closure, e não a fatia: a tabela muda com o servidor no ar, e uma
// fatia capturada no registro descreveria para sempre a tabela da subida. nil
// vale "nenhum usuário" — a tabela vazia é resposta, não erro.
type Source struct {
	Users func() []users.UserView
}

// Table monta o schema sintético e o Store virtual de `_meta.users`.
func Table(f Source) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(specOfTable), schema.BuiltinDefaults)
	if err != nil {
		return nil, nil, fmt.Errorf("metausers: invalid spec for %s.%s: %w", meta.Namespace, TableMetaUsers, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, nil, fmt.Errorf("metausers: compiling %s.%s: %w", meta.Namespace, TableMetaUsers, err)
	}
	src := &source{f: f, open: map[uint64][]users.UserView{}}
	src.columns = make([]columnWithID, 0, len(columnNames))
	for k, name := range columnNames {
		field, ok := sc.FieldByName([]byte(name))
		if !ok {
			return nil, nil, fmt.Errorf("metausers: column %q has no FieldID in the synthetic schema", name)
		}
		src.columns = append(src.columns, columnWithID{id: field.ID, k: column(k)})
	}
	// O payload exige FieldID CRESCENTE, e a ordem do `include` não garante
	// isso. Resolvida uma vez aqui; sem este passo, o leitor para no primeiro
	// campo fora de ordem e as colunas seguintes chegam como NULL, sem erro.
	slices.SortFunc(src.columns, func(a, b columnWithID) int { return int(a.id) - int(b.id) })
	return sc, src, nil
}

// Register declara `_meta.users` sob o dataset já registrado no motor, como
// tabela só-ADMIN.
func Register(eng *query.Engine, dataset string, f Source) error {
	sc, store, err := Table(f)
	if err != nil {
		return err
	}
	return eng.RegisterAdminVirtual(dataset, meta.Namespace, TableMetaUsers, sc, store)
}

type columnWithID struct {
	id types.FieldID
	k  column
}

// source é o Store virtual, compartilhado por todas as consultas concorrentes.
type source struct {
	f       Source
	columns []columnWithID

	mu        sync.Mutex
	nextEpoch uint64
	open      map[uint64][]users.UserView
}

// OpenSnapshot congela a tabela desta consulta: uma mutação no meio dela não
// troca o universo pela metade. Users() já devolve uma fatia nova por chamada.
func (s *source) OpenSnapshot() types.Snapshot {
	var rows []users.UserView
	if s.f.Users != nil {
		rows = s.f.Users()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextEpoch++
	epoch := s.nextEpoch
	s.open[epoch] = rows
	return types.Snapshot{Epoch: epoch, LimitRowID: types.RowID(len(rows)) + 1}
}

func (s *source) CloseSnapshot(snap types.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.open, snap.Epoch)
}

func (s *source) rowsOf(snap types.Snapshot) ([]users.UserView, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, ok := s.open[snap.Epoch]
	return rows, ok
}

func (s *source) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &iterator{limit: snap.LimitRowID}
}

func (s *source) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	rows, ok := s.rowsOf(snap)
	if !ok || id < 1 || int(id) > len(rows) {
		return types.Record{}, buf, false, nil
	}
	rec, buf, err := s.record(id, rows[id-1], buf)
	return rec, buf, err == nil, err
}

func (s *source) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	rows, ok := s.rowsOf(snap)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	for i, u := range rows {
		if u.Username == string(key) {
			rec, buf, err := s.record(types.RowID(i)+1, u, buf)
			return rec, buf, err == nil, err
		}
	}
	return types.Record{}, buf, false, nil
}

// Indexed é sempre false: são poucas linhas, e a varredura já é trivial.
func (s *source) Indexed(types.FieldID) bool { return false }

func (s *source) Lookup(types.FieldID, types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("metausers: %w", types.ErrInvalidArgument)
}

func (s *source) record(row types.RowID, u users.UserView, buf []byte) (types.Record, []byte, error) {
	var bootstrap int64
	if u.Bootstrap {
		bootstrap = 1
	}
	values := [...]types.Value{
		cUsername:          {Type: types.TypeString, Bytes: []byte(u.Username)},
		cRole:              {Type: types.TypeString, Bytes: []byte(u.Role)},
		cBootstrap:         {Type: types.TypeBool, I64: bootstrap},
		cCreatedAt:         {Type: types.TypeTimestamp, I64: u.CreatedAt.UnixMilli()},
		cCreatedBy:         {Type: types.TypeString, Bytes: []byte(u.CreatedBy)},
		cUpdatedAt:         {Type: types.TypeTimestamp, I64: u.UpdatedAt.UnixMilli()},
		cUpdatedBy:         {Type: types.TypeString, Bytes: []byte(u.UpdatedBy)},
		cPasswordChangedAt: {Type: types.TypeTimestamp, I64: u.PasswordChangedAt.UnixMilli()},
	}
	buf = buf[:0]
	var err error
	for _, c := range s.columns {
		if buf, err = encoding.AppendField(buf, c.id, values[c.k]); err != nil {
			return types.Record{}, buf, fmt.Errorf("metausers: encoding row %q: %w", u.Username, err)
		}
	}
	key := []byte(u.Username)
	return types.Record{RowID: row, Sequence: int64(row), Key: key, Payload: buf}, buf, nil
}

type iterator struct {
	next  types.RowID
	limit types.RowID
}

func (it *iterator) Next() bool {
	it.next++
	return it.next < it.limit
}
func (it *iterator) RowID() types.RowID { return it.next }
func (it *iterator) Err() error         { return nil }
func (it *iterator) Close() error       { return nil }
