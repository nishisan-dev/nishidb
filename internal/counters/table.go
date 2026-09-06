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

// maxColunasDeAgrupamento é o teto de colunas do groupBy que a tabela virtual
// monta sem alocar: a linha é composta num array de pilha desse tamanho mais
// as duas colunas fixas.
const maxColunasDeAgrupamento = 16

// Leitor é o que a tabela virtual precisa do contador. Interface, e não o
// *contador.Contador, para que o teste da tabela e o do contador não se
// conheçam além do contrato — *contador.Contador a satisfaz.
type Leitor interface {
	// Limite é o maior id de grupo emitido mais um.
	Limite() uint32
	// Grupo devolve a chave composta, o texto (groupKey), o total e os refs do
	// grupo, e se ele está vivo. Chave e texto são estáveis.
	Grupo(id uint32) (chave, texto []byte, total, refs int64, vivo bool)
	// PorTexto resolve o groupKey de um grupo vivo para o id.
	PorTexto(texto []byte) (uint32, bool)
}

// Tabela monta o schema sintético e o Store virtual de `_counters.<nome>`.
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
func Tabela(def counter.Definicao, l Leitor) (*schema.Compiled, query.Store, error) {
	if len(def.Colunas) == 0 || len(def.Colunas) > maxColunasDeAgrupamento {
		return nil, nil, fmt.Errorf("contadores: %q tem %d colunas de agrupamento; a tabela aceita de 1 a %d", def.Nome, len(def.Colunas), maxColunasDeAgrupamento)
	}
	if len(def.Nomes) != len(def.Colunas) || len(def.Tipos) != len(def.Colunas) {
		return nil, nil, fmt.Errorf("contadores: %q com Nomes, Tipos e Colunas de tamanhos diferentes", def.Nome)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "version: 1\nname: %s\nmaterialization:\n  key:\n    field: %s\nschema:\n  include: [%s",
		def.Nome, schema.ColunaChaveDoContador, schema.ColunaChaveDoContador)
	for _, n := range def.Nomes {
		if n == schema.ColunaChaveDoContador || n == schema.ColunaTotalDoContador {
			return nil, nil, fmt.Errorf("contadores: %q agrupa por %q, que colide com a coluna fixa da tabela", def.Nome, n)
		}
		fmt.Fprintf(&sb, ", %s", n)
	}
	fmt.Fprintf(&sb, ", %s]\n  fields:\n    %s:\n      type: string\n", schema.ColunaTotalDoContador, schema.ColunaChaveDoContador)
	for i, n := range def.Nomes {
		tipo, err := nomeDoTipo(def.Tipos[i])
		if err != nil {
			return nil, nil, fmt.Errorf("contadores: %q, coluna %q: %w", def.Nome, n, err)
		}
		fmt.Fprintf(&sb, "    %s:\n      type: %s\n", n, tipo)
	}
	fmt.Fprintf(&sb, "    %s:\n      type: int64\n", schema.ColunaTotalDoContador)

	spec, err := schema.Parse([]byte(sb.String()), schema.DefaultsPadrao)
	if err != nil {
		return nil, nil, fmt.Errorf("contadores: spec da tabela %s.%s inválida: %w", schema.NamespaceContadores, def.Nome, err)
	}
	sc, err := spec.Compile(schema.NovoDicionarioEmMemoria())
	if err != nil {
		return nil, nil, fmt.Errorf("contadores: compilando a tabela %s.%s: %w", schema.NamespaceContadores, def.Nome, err)
	}
	f := &fonte{l: l, colunas: make([]types.FieldID, len(def.Nomes))}
	if f.idChave, err = idDaColuna(sc, schema.ColunaChaveDoContador); err != nil {
		return nil, nil, err
	}
	if f.idTotal, err = idDaColuna(sc, schema.ColunaTotalDoContador); err != nil {
		return nil, nil, err
	}
	for i, n := range def.Nomes {
		if f.colunas[i], err = idDaColuna(sc, n); err != nil {
			return nil, nil, err
		}
	}
	return sc, f, nil
}

func nomeDoTipo(t types.FieldType) (string, error) {
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

func idDaColuna(sc *schema.Compiled, nome string) (types.FieldID, error) {
	f, ok := sc.FieldByName([]byte(nome))
	if !ok {
		return 0, fmt.Errorf("contadores: coluna %q sem FieldID no schema sintético", nome)
	}
	return f.ID, nil
}

// Registra declara o namespace `_counters` sob o dataset, registra a tabela
// de cada contador aberto no Store e a `_meta.counters` que os lista — esta
// SEMPRE, mesmo sem contador nenhum: "quais existem" tem resposta quando é
// nenhum. Chamado por todo caminho que registra um dataset no motor —
// servidor e CLI — para que `FROM _counters.<nome>` e `FROM _meta.counters`
// funcionem da mesma forma nos dois.
func Registra(eng *query.Engine, dataset string, st *storage.Store) error {
	if err := eng.DeclaraNamespace(dataset, schema.NamespaceContadores); err != nil {
		return err
	}
	cs := st.Contadores()
	contados := make([]Contado, 0, len(cs))
	for _, c := range cs {
		sc, fonte, err := Tabela(c.Def(), c)
		if err != nil {
			return err
		}
		if err := eng.RegistraVirtual(dataset, schema.NamespaceContadores, c.Def().Nome, sc, fonte); err != nil {
			return err
		}
		contados = append(contados, c)
	}
	msc, mfonte, err := TabelaMeta(contados)
	if err != nil {
		return err
	}
	return eng.RegistraVirtual(dataset, meta.Namespace, TabelaMetaCounters, msc, mfonte)
}

// fonte é o Store virtual: cada rowID é um id de grupo, e o record é
// codificado sob demanda a partir do que o contador tem em memória.
type fonte struct {
	l       Leitor
	idChave types.FieldID
	idTotal types.FieldID
	colunas []types.FieldID // FieldID de cada coluna do groupBy, na ordem declarada
}

// AbreSnapshot congela o conjunto de grupos: ids a partir do limite nasceram
// depois da consulta. Época e instante ficam zerados e são ignorados — os
// totais são lidos no presente.
func (f *fonte) AbreSnapshot() types.Snapshot {
	return types.Snapshot{LimitRowID: types.RowID(f.l.Limite())}
}

// FechaSnapshot é no-op: a fonte não retém nada.
func (f *fonte) FechaSnapshot(types.Snapshot) {}

func (f *fonte) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &iterador{l: f.l, limite: uint32(snap.LimitRowID)}
}

func (f *fonte) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if id == 0 || id >= snap.LimitRowID {
		return types.Record{}, buf, false, nil
	}
	chave, texto, total, _, vivo := f.l.Grupo(uint32(id))
	if !vivo {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, chave, texto, total, buf)
}

func (f *fonte) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	id, ok := f.l.PorTexto(key)
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.GetByRowID(snap, types.RowID(id), buf)
}

// Indexado responde SEMPRE false: a tabela não declara índice, e o planner
// degrada para a varredura dos grupos — que sobre milhares de linhas é o
// plano certo. A key (groupKey) dá o point lookup por conta própria.
func (f *fonte) Indexado(types.FieldID) bool { return false }

// Lookup nunca é chamado: sem índice declarado o planner não escolhe plano
// indexado. Rede de segurança, como no _meta.
func (f *fonte) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("contadores: a tabela virtual não tem índice (campo %d)", field)
}

type par struct {
	id types.FieldID
	v  types.Value
}

// record codifica UM grupo como uma linha. O payload exige FieldID crescente;
// os pares são ordenados num array de pilha, sem alocar — a linha é montada
// por grupo a cada consulta do alarme.
func (f *fonte) record(row types.RowID, chave, texto []byte, total int64, buf []byte) (types.Record, []byte, bool, error) {
	var pares [maxColunasDeAgrupamento + 2]par
	n := 0
	pares[n] = par{f.idChave, types.Value{Type: types.TypeString, Bytes: texto}}
	n++
	pares[n] = par{f.idTotal, types.Value{Type: types.TypeInt64, I64: total}}
	n++
	err := valuekey.PercorreComposta(chave, func(i int, v types.Value) error {
		if i >= len(f.colunas) {
			return fmt.Errorf("contadores: a chave do grupo %d tem mais componentes que o groupBy", row)
		}
		pares[n] = par{f.colunas[i], v}
		n++
		return nil
	})
	if err != nil {
		return types.Record{}, buf, false, err
	}
	if n != len(f.colunas)+2 {
		return types.Record{}, buf, false, fmt.Errorf("contadores: a chave do grupo %d tem %d componentes, o groupBy tem %d", row, n-2, len(f.colunas))
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
	return types.Record{RowID: row, Sequence: int64(row), Key: texto, Payload: buf}, buf, true, nil
}

// iterador percorre os ids de grupo vivos abaixo do limite congelado.
type iterador struct {
	l      Leitor
	limite uint32
	id     uint32
}

func (it *iterador) Next() bool {
	for it.id+1 < it.limite {
		it.id++
		if _, _, _, _, vivo := it.l.Grupo(it.id); vivo {
			return true
		}
	}
	it.id = it.limite
	return false
}

func (it *iterador) RowID() types.RowID { return types.RowID(it.id) }
func (it *iterador) Err() error         { return nil }
func (it *iterador) Close() error       { return nil }
