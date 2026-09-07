package counters

import (
	"fmt"
	"sort"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TabelaMetaCounters é a tabela `_meta.counters`: uma linha por contador do
// dataset consultado. Responde "quais contadores existem, e como estão" por
// SQL — antes disso a lista só saía pelo `nishidb counters` e pelo
// `/v1/stats`, e por SQL só por engano, na mensagem de tabela desconhecida.
//
// Mora aqui, e não em `internal/query/meta`, pela mesma razão que o
// `_counters.<nome>`: o `meta` descreve o dataset pela definição estática e
// não conhece o storage; os números desta tabela são o retrato vivo de
// `EstatisticasDeContadores`, e o pacote que já cola schema, motor e storage
// é este.
const TabelaMetaCounters = "counters"

// Contado é o que a tabela precisa de cada contador: a definição (nome,
// groupBy, condição, teto) e o retrato corrente. Interface, e não o
// *contador.Contador, pelo mesmo motivo do Leitor — *contador.Contador a
// satisfaz.
type Contado interface {
	Def() counter.Definicao
	Stats() counter.Estatisticas
}

// especDaTabelaMeta é a spec da tabela, no MESMO YAML de um dataset de
// verdade, como o `_meta.fields` e o `_counters.<nome>`.
//
// `rows` e `total` são os agregados do contador — quantas linhas apontam para
// algum grupo e quantas satisfazem a condição, no dataset inteiro. Ao
// contrário das duas da auditoria, NUNCA são NULL: zero linhas é um fato
// medido, e o contador sabe dizê-lo desde a abertura. `total` repete de
// propósito o nome da coluna do `_counters.<nome>`: é a soma dela. E `rows`
// menor que o rows do dataset é diagnóstico direto — são as linhas sem alguma
// coluna do groupBy, que o contador não conta por desenho.
//
// `condition`, e não `where`: WHERE é palavra reservada do SQL, e uma coluna
// com esse nome seria impossível de projetar. `groupBy` sai com o mesmo
// separador do `groupKey` (`olt|slot|port`), para que quem lê a lista saiba
// como a chave da tabela do contador é escrita. `auditDivergent` e `auditAt`
// são NULL até a primeira auditoria conclusiva desta abertura — zero
// divergências e "nunca auditado" são afirmações diferentes, e um zero
// sozinho as confundiria.
const especDaTabelaMeta = `version: 1
name: ` + TabelaMetaCounters + `
materialization:
  key:
    field: name
schema:
  include: [name, groupBy, condition, rows, total, groups, maxGroups, retired, refused, orphans, bytes, auditDivergent, auditAt]
  fields:
    name:
      type: string
    groupBy:
      type: string
    condition:
      type: string
    rows:
      type: int64
    total:
      type: int64
    groups:
      type: int64
    maxGroups:
      type: int64
    retired:
      type: int64
    refused:
      type: int64
    orphans:
      type: int64
    bytes:
      type: int64
    auditDivergent:
      type: int64
    auditAt:
      type: timestamp
`

// colunaMeta identifica cada coluna da tabela, para a linha ser montada por
// um switch em vez de por nome.
type colunaMeta uint8

const (
	cmNome colunaMeta = iota
	cmGroupBy
	cmCondicao
	cmLinhas
	cmTotal
	cmGrupos
	cmTeto
	cmAposentados
	cmRecusados
	cmOrfas
	cmBytes
	cmAuditoriaDivergentes
	cmAuditoriaEm
)

// nomesDasColunasMeta casa, posição a posição, com as constantes acima.
var nomesDasColunasMeta = [...]string{
	"name", "groupBy", "condition", "rows", "total", "groups", "maxGroups",
	"retired", "refused", "orphans", "bytes", "auditDivergent", "auditAt",
}

// SeparadorDoGroupBy é o que separa as colunas no `groupBy` da tabela — o
// mesmo `|` do groupKey.
const SeparadorDoGroupBy = "|"

// TabelaMeta monta o schema sintético e o Store virtual de `_meta.counters`
// sobre os contadores dados. A lista pode ser vazia: a tabela existe em todo
// dataset, e "nenhum" é uma resposta.
//
// O conjunto de linhas é fixo pela vida do processo — contador só nasce na
// abertura do dataset —, então o rowID é a posição na lista mais um, e a key
// (o nome) resolve por mapa. O que muda entre duas consultas são os números,
// lidos no presente a cada linha, como o `_counters` lê os totais.
func TabelaMeta(cs []Contado) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(especDaTabelaMeta), schema.DefaultsPadrao)
	if err != nil {
		return nil, nil, fmt.Errorf("contadores: spec da tabela %s.%s inválida: %w", meta.Namespace, TabelaMetaCounters, err)
	}
	sc, err := spec.Compile(schema.NovoDicionarioEmMemoria())
	if err != nil {
		return nil, nil, fmt.Errorf("contadores: compilando a tabela %s.%s: %w", meta.Namespace, TabelaMetaCounters, err)
	}
	f := &fonteMeta{
		linhas:  make([]linhaMeta, 0, len(cs)),
		porNome: make(map[string]int, len(cs)),
		colunas: make([]colunaComID, 0, len(nomesDasColunasMeta)),
	}
	for k, nome := range nomesDasColunasMeta {
		id, err := idDaColuna(sc, nome)
		if err != nil {
			return nil, nil, err
		}
		f.colunas = append(f.colunas, colunaComID{id: id, k: colunaMeta(k)})
	}
	// O payload exige FieldID crescente; a ordem é resolvida UMA vez aqui,
	// e a linha é montada sem ordenar.
	sort.Slice(f.colunas, func(i, j int) bool { return f.colunas[i].id < f.colunas[j].id })
	for _, c := range cs {
		def := c.Def()
		if _, existe := f.porNome[def.Nome]; existe {
			return nil, nil, fmt.Errorf("contadores: dois contadores chamados %q", def.Nome)
		}
		f.porNome[def.Nome] = len(f.linhas)
		f.linhas = append(f.linhas, linhaMeta{
			c:       c,
			nome:    []byte(def.Nome),
			groupBy: []byte(strings.Join(def.Nomes, SeparadorDoGroupBy)),
			cond:    []byte(def.Where),
		})
	}
	return sc, f, nil
}

type colunaComID struct {
	id types.FieldID
	k  colunaMeta
}

// linhaMeta é um contador como linha: os textos são calculados uma vez, na
// montagem, para que a linha não aloque.
type linhaMeta struct {
	c       Contado
	nome    []byte
	groupBy []byte
	cond    []byte // vazio = sem condição (o contador conta toda linha com grupo)
}

// fonteMeta é o Store virtual de `_meta.counters`.
type fonteMeta struct {
	linhas  []linhaMeta
	porNome map[string]int
	colunas []colunaComID // em ordem crescente de FieldID
}

// OpenSnapshot devolve o limite de linhas; o conjunto é fixo, então não há o
// que congelar além dele. Época e instante ficam zerados e são ignorados.
func (f *fonteMeta) OpenSnapshot() types.Snapshot {
	return types.Snapshot{LimitRowID: types.RowID(len(f.linhas) + 1)}
}

// CloseSnapshot é no-op: a fonte não retém nada.
func (f *fonteMeta) CloseSnapshot(types.Snapshot) {}

func (f *fonteMeta) ScanLive(snap types.Snapshot) types.RowIDIterator {
	return &iteradorMeta{limite: snap.LimitRowID}
}

func (f *fonteMeta) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	if id == 0 || id >= snap.LimitRowID || int(id) > len(f.linhas) {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, buf)
}

func (f *fonteMeta) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	i, ok := f.porNome[string(key)]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.GetByRowID(snap, types.RowID(i+1), buf)
}

// Indexed responde SEMPRE false, como nas outras tabelas virtuais: sem
// índice declarado o planner varre as linhas — uma por contador — e a key
// (o nome) dá o point lookup por conta própria.
func (f *fonteMeta) Indexed(types.FieldID) bool { return false }

// Lookup nunca é chamado: rede de segurança, como no _meta e no _counters.
func (f *fonteMeta) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("contadores: a tabela %s.%s não tem índice (campo %d)", meta.Namespace, TabelaMetaCounters, field)
}

// record codifica UM contador como uma linha, lendo as estatísticas no
// presente. Coluna ausente é NULL: a condição quando o contador não tem
// `where`, e as duas da auditoria enquanto nenhuma auditoria concluiu.
func (f *fonteMeta) record(row types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	l := f.linhas[row-1]
	est := l.c.Stats()
	buf = buf[:0]
	var err error
	for _, col := range f.colunas {
		var v types.Value
		switch col.k {
		case cmNome:
			v = types.Value{Type: types.TypeString, Bytes: l.nome}
		case cmGroupBy:
			v = types.Value{Type: types.TypeString, Bytes: l.groupBy}
		case cmCondicao:
			if len(l.cond) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: l.cond}
		case cmLinhas:
			v = types.Value{Type: types.TypeInt64, I64: est.Linhas}
		case cmTotal:
			v = types.Value{Type: types.TypeInt64, I64: est.Total}
		case cmGrupos:
			v = types.Value{Type: types.TypeInt64, I64: int64(est.Grupos)}
		case cmTeto:
			v = types.Value{Type: types.TypeInt64, I64: int64(est.Teto)}
		case cmAposentados:
			v = types.Value{Type: types.TypeInt64, I64: int64(est.Aposentados)}
		case cmRecusados:
			v = types.Value{Type: types.TypeInt64, I64: int64(est.Recusados)} //nolint:gosec // contagem, nunca perto do teto do int64
		case cmOrfas:
			v = types.Value{Type: types.TypeInt64, I64: int64(est.Orfas)} //nolint:gosec // idem
		case cmBytes:
			v = types.Value{Type: types.TypeInt64, I64: est.Bytes}
		case cmAuditoriaDivergentes:
			if est.AuditoriaUltimaUnix == 0 {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: est.AuditoriaDivergentes}
		case cmAuditoriaEm:
			if est.AuditoriaUltimaUnix == 0 {
				continue
			}
			v = types.Value{Type: types.TypeTimestamp, I64: est.AuditoriaUltimaUnix * 1000}
		}
		if buf, err = encoding.AppendField(buf, col.id, v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("contadores: codificando a coluna %s do contador %q: %w", nomesDasColunasMeta[col.k], l.nome, err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: l.nome, Payload: buf}, buf, true, nil
}

// iteradorMeta percorre 1..limite-1: toda linha é viva.
type iteradorMeta struct {
	limite types.RowID
	id     types.RowID
}

func (it *iteradorMeta) Next() bool {
	if it.id+1 < it.limite {
		it.id++
		return true
	}
	it.id = it.limite
	return false
}

func (it *iteradorMeta) RowID() types.RowID { return it.id }
func (it *iteradorMeta) Err() error         { return nil }
func (it *iteradorMeta) Close() error       { return nil }
