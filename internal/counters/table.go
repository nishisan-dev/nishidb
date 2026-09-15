package counters

import (
	"fmt"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// maxGroupingColumns é o teto de colunas do groupBy que a tabela virtual
// monta sem alocar: a linha é composta num array de pilha desse tamanho mais
// as duas colunas fixas.
const maxGroupingColumns = 16

// Reader é o que a tabela virtual precisa do contador. Interface, e não o
// *counter.Counter, para que o teste da tabela e o do contador não se
// conheçam além do contrato — *counter.Counter a satisfaz.
type Reader interface {
	// Limit é o maior id de grupo emitido mais um.
	Limit() uint32
	// Group devolve a chave composta, o texto (groupKey), o total e os refs do
	// grupo, e se ele está vivo. Chave e texto são estáveis.
	Group(id uint32) (chave, texto []byte, total, refs int64, vivo bool)
	// ByText resolve o groupKey de um grupo vivo para o id.
	ByText(texto []byte) (uint32, bool)
}

// Table monta o schema sintético e o Store virtual de `_counters.<nome>`.
//
// O schema é compilado do MESMO YAML de um dataset de verdade, como o `_meta`
// faz: reusar o compilador é o que garante que a tabela obedeça às mesmas
// regras — tipos, chave, ordem determinística de FieldID — que qualquer outro
// dataset, e o que dá WHERE, ORDER BY, LIMIT e projeção sem gramática nova.
//
// A tabela tem a coluna `groupKey` como key (o texto canônico do grupo — a
// key da v1 é uma coluna string única, e um groupBy composto não tem coluna
// que identifique a linha), as colunas do groupBy abertas e tipadas, e
// `total`. Um grupo que chegou a zero permanece com total 0 (é o equipamento
// que apagou por inteiro, e é o caso que interessa); um grupo aposentado não
// aparece.
//
// A tabela lê o PRESENTE: os totais não são versionados pelo journal do
// snapshot. O que o snapshot congela é o CONJUNTO de grupos — um grupo que
// nasce no meio da varredura não entra no universo dela.
func Table(def counter.Definition, l Reader) (*schema.Compiled, query.Store, error) {
	sc, err := TableSchema(def)
	if err != nil {
		return nil, nil, err
	}
	f := &source{l: l, columns: make([]types.FieldID, len(def.Names))}
	if f.idKey, err = idOfColumn(sc, schema.CounterKeyColumn); err != nil {
		return nil, nil, err
	}
	if f.idTotal, err = idOfColumn(sc, schema.CounterTotalColumn); err != nil {
		return nil, nil, err
	}
	for i, n := range def.Names {
		if f.columns[i], err = idOfColumn(sc, n); err != nil {
			return nil, nil, err
		}
	}
	return sc, f, nil
}

// TableSchema monta e compila o schema sintético da tabela de um contador, sem
// o Store virtual.
//
// Existe separada da Table porque quem compila uma condição sobre as colunas do
// contador — o `when` de um alarme — precisa do schema e não tem Reader para
// dar. Exigir um obrigaria o declarante a inventar um leitor vazio só para
// compilar texto, e um leitor inventado é a porta para a condição acabar
// compilada contra um schema que não é o da tabela de verdade.
func TableSchema(def counter.Definition) (*schema.Compiled, error) {
	if len(def.Columns) == 0 || len(def.Columns) > maxGroupingColumns {
		return nil, fmt.Errorf("contadores: %q tem %d colunas de agrupamento; a tabela aceita de 1 a %d", def.Name, len(def.Columns), maxGroupingColumns)
	}
	if len(def.Names) != len(def.Columns) || len(def.Types) != len(def.Columns) {
		return nil, fmt.Errorf("contadores: %q com Names, Types e Columns de tamanhos diferentes", def.Name)
	}
	// A colisão sai da MESMA lista de colunas que a validação do YAML usa
	// (schema.CounterColumns), e não de uma relação própria de colunas fixas.
	// Aqui ela é defensiva: toda Definition compilada de um YAML já foi
	// recusada na subida se colidisse. Fica porque a Definition é montável sem
	// YAML, e sem ela o schema sintético declararia a mesma coluna duas vezes.
	// A tabela ainda não gera colunas de métrica, e por isso a lista é montada
	// sem métricas.
	if first, second, found := schema.FindCounterColumnCollision(def.Names, nil); found {
		return nil, fmt.Errorf("counters: %q: %s collides with %s", def.Name, second, first)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "version: 1\nname: %s\nschemaName: %s\nmaterialization:\n  key:\n    field: %s\nschema:\n  include: [%s",
		def.Name, schema.SyntheticSchemaName, schema.CounterKeyColumn, schema.CounterKeyColumn)
	for _, n := range def.Names {
		fmt.Fprintf(&sb, ", %s", n)
	}
	fmt.Fprintf(&sb, ", %s]\n  fields:\n    %s:\n      type: string\n", schema.CounterTotalColumn, schema.CounterKeyColumn)
	for i, n := range def.Names {
		kind, err := typeName(def.Types[i])
		if err != nil {
			return nil, fmt.Errorf("contadores: %q, coluna %q: %w", def.Name, n, err)
		}
		fmt.Fprintf(&sb, "    %s:\n      type: %s\n", n, kind)
	}
	fmt.Fprintf(&sb, "    %s:\n      type: int64\n", schema.CounterTotalColumn)

	spec, err := schema.Parse([]byte(sb.String()), schema.BuiltinDefaults)
	if err != nil {
		return nil, fmt.Errorf("contadores: spec da tabela %s.%s inválida: %w", schema.CountersNamespace, def.Name, err)
	}
	sc, err := spec.Compile(schema.NewInMemoryDict())
	if err != nil {
		return nil, fmt.Errorf("contadores: compilando a tabela %s.%s: %w", schema.CountersNamespace, def.Name, err)
	}
	return sc, nil
}

func typeName(t types.FieldType) (string, error) {
	switch t {
	case types.TypeString:
		return "string", nil
	case types.TypeInt64:
		return "int64", nil
	case types.TypeBool:
		return "boolean", nil
	default:
		return "", fmt.Errorf("tipo %#x não tem coluna na tabela do contador (groupBy aceita string, int64 e boolean)", t)
	}
}

func idOfColumn(sc *schema.Compiled, name string) (types.FieldID, error) {
	f, ok := sc.FieldByName([]byte(name))
	if !ok {
		return 0, fmt.Errorf("contadores: coluna %q sem FieldID no schema sintético", name)
	}
	return f.ID, nil
}

// Register declara o namespace `_counters` sob o dataset, registra a tabela
// de cada contador aberto no Store e a `_meta.counters` que os lista — esta
// SEMPRE, mesmo sem contador nenhum: "quais existem" tem resposta quando é
// nenhum. Chamado por todo caminho que registra um dataset no motor —
// servidor e CLI — para que `FROM _counters.<nome>` e `FROM _meta.counters`
// funcionem da mesma forma nos dois.
func Register(eng *query.Engine, dataset string, st *storage.Store) error {
	if err := eng.DeclaraNamespace(dataset, schema.CountersNamespace); err != nil {
		return err
	}
	cs := st.Counters()
	contados := make([]Contado, 0, len(cs))
	for _, c := range cs {
		sc, source, err := Table(c.Def(), c)
		if err != nil {
			return err
		}
		if err := eng.RegisterVirtual(dataset, schema.CountersNamespace, c.Def().Name, sc, source); err != nil {
			return err
		}
		contados = append(contados, c)
	}
	msc, mfonte, err := TableMeta(contados)
	if err != nil {
		return err
	}
	return eng.RegisterVirtual(dataset, meta.Namespace, TableMetaCounters, msc, mfonte)
}

// source é o Store virtual: cada rowID é um id de grupo, e o record é
// codificado sob demanda a partir do que o contador tem em memória.
type source struct {
	l       Reader
	idKey   types.FieldID
	idTotal types.FieldID
	columns []types.FieldID // FieldID de cada coluna do groupBy, na ordem declarada
}

// OpenSnapshot congela o conjunto de grupos: ids a partir do limite nasceram
// depois da consulta. Época e instante ficam zerados e são ignorados — os
// totais são lidos no presente.
func (f *source) OpenSnapshot() types.Snapshot {
	return types.Snapshot{LimitRowID: types.RowID(f.l.Limit())}
}

// CloseSnapshot é no-op: a fonte não retém nada.
func (f *source) CloseSnapshot(types.Snapshot) {}

func (f *source) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &iterador{l: f.l, limit: uint32(snap.LimitRowID)}
}

func (f *source) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if id == 0 || id >= snap.LimitRowID {
		return types.Record{}, buf, false, nil
	}
	key, text, total, _, vivo := f.l.Group(uint32(id))
	if !vivo {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, key, text, total, buf)
}

func (f *source) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	id, ok := f.l.ByText(key)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.GetByRowID(snap, types.RowID(id), buf)
}

// Indexed responde SEMPRE false: a tabela não declara índice, e o planner
// degrada para a varredura dos grupos — que sobre milhares de linhas é o
// plano certo. A key (groupKey) dá o point lookup por conta própria.
func (f *source) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: sem índice declarado o planner não escolhe plano
// indexado. Rede de segurança, como no _meta.
func (f *source) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("contadores: a tabela virtual não tem índice (campo %d)", field)
}

type pair struct {
	id types.FieldID
	v  types.Value
}

// record codifica UM grupo como uma linha. O payload exige FieldID crescente;
// os pares são ordenados num array de pilha, sem alocar — a linha é montada
// por grupo a cada consulta do alarme.
func (f *source) record(row types.RowID, key, text []byte, total int64, buf []byte) (types.Record, []byte, bool, error) {
	var pares [maxGroupingColumns + 2]pair
	n := 0
	pares[n] = pair{f.idKey, types.Value{Type: types.TypeString, Bytes: text}}
	n++
	pares[n] = pair{f.idTotal, types.Value{Type: types.TypeInt64, I64: total}}
	n++
	err := valuekey.WalkComposite(key, func(i int, v types.Value) error {
		if i >= len(f.columns) {
			return fmt.Errorf("contadores: a chave do grupo %d tem mais componentes que o groupBy", row)
		}
		pares[n] = pair{f.columns[i], v}
		n++
		return nil
	})
	if err != nil {
		return types.Record{}, buf, false, err
	}
	if n != len(f.columns)+2 {
		return types.Record{}, buf, false, fmt.Errorf("contadores: a chave do grupo %d tem %d componentes, o groupBy tem %d", row, n-2, len(f.columns))
	}
	for i := 1; i < n; i++ {
		for j := i; j > 0 && pares[j-1].id > pares[j].id; j-- {
			pares[j-1], pares[j] = pares[j], pares[j-1]
		}
	}
	buf = buf[:0]
	for i := 0; i < n; i++ {
		if buf, err = encoding.AppendField(buf, pares[i].id, pares[i].v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("contadores: codificando a coluna %d do grupo %d: %w", pares[i].id, row, err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: text, Payload: buf}, buf, true, nil
}

// iterador percorre os ids de grupo vivos abaixo do limite congelado.
type iterador struct {
	l     Reader
	limit uint32
	id    uint32
}

func (it *iterador) Next() bool {
	for it.id+1 < it.limit {
		it.id++
		if _, _, _, _, vivo := it.l.Group(it.id); vivo {
			return true
		}
	}
	it.id = it.limit
	return false
}

func (it *iterador) RowID() types.RowID { return types.RowID(it.id) }
func (it *iterador) Err() error         { return nil }
func (it *iterador) Close() error       { return nil }
