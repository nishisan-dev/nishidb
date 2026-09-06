package metakafka

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ChaveDoResumo é o `id` da linha de resumo — a que responde quando não há
// partição nenhuma. Nunca colide com o id de uma partição, que é o número em
// decimal e portanto só tem dígitos.
const ChaveDoResumo = "source"

// especDaTabela é a spec da tabela, no MESMO YAML de um dataset de verdade,
// como o `_meta.fields` e o `_meta.counters`. Reusar o compilador é o que
// garante que ela obedeça às mesmas regras — tipos, chave, ordem
// determinística de FieldID — que qualquer outro dataset.
//
// AS DUAS CLASSES DE COLUNA, e é isso que faz a linha de resumo e as linhas
// por partição serem a mesma tabela:
//
//   - de DATASET (topic…enabled, state, detail, a fila, o desfecho, as
//     partições, o watchdog): repetem em toda linha. Uma linha por partição
//     que não trouxesse o estado do dataset obrigaria uma segunda consulta
//     para responder "e a fila, está cheia?".
//   - de PARTIÇÃO (partition, appliedOffset, committedOffset, highWatermark,
//     lag, markAt): são NULL na linha de resumo. markAt é de partição, e não
//     de dataset, porque carimba o HWM e o lag, que só existem por partição.
//
// A key é `id`, e não `partition`: a key tem de ser string (a spec só suporta
// isso) e tem de existir em TODA linha, e `partition` é NULL no resumo.
const especDaTabela = `version: 1
name: ` + TabelaMetaKafka + `
materialization:
  key:
    field: id
schema:
  include: [id, topic, groupId, bootstrap, partition, enabled, state, detail,
            appliedOffset, committedOffset, highWatermark, lag, markAt,
            lastBatchAt, queueRecords, queueBytes, events, applied, stale,
            discarded, failed, partitionsAssigned, partitionsTotal,
            watchdogTrips, watchdogGaveUp]
  fields:
    id:
      type: string
    topic:
      type: string
    groupId:
      type: string
    bootstrap:
      type: string
    partition:
      type: int64
    enabled:
      type: boolean
    state:
      type: string
    detail:
      type: string
    appliedOffset:
      type: int64
    committedOffset:
      type: int64
    highWatermark:
      type: int64
    lag:
      type: int64
    markAt:
      type: timestamp
    lastBatchAt:
      type: timestamp
    queueRecords:
      type: int64
    queueBytes:
      type: int64
    events:
      type: int64
    applied:
      type: int64
    stale:
      type: int64
    discarded:
      type: int64
    failed:
      type: int64
    partitionsAssigned:
      type: int64
    partitionsTotal:
      type: int64
    watchdogTrips:
      type: int64
    watchdogGaveUp:
      type: boolean
`

// coluna identifica cada coluna, para a linha ser montada por um switch em vez
// de por nome.
type coluna uint8

const (
	colID coluna = iota
	colTopic
	colGroupID
	colBootstrap
	colParticao
	colEnabled
	colState
	colDetail
	colOffsetAplicado
	colOffsetDuravel
	colFimDoLog
	colLag
	colMarcaEm
	colUltimoLote
	colFilaRegistros
	colFilaBytes
	colEventos
	colAplicados
	colStale
	colDescartados
	colFalhas
	colParticoesAtribuidas
	colParticoesDoTopico
	colWatchdogTrips
	colWatchdogDesistiu
)

// nomesDasColunas casa, posição a posição, com as constantes acima.
var nomesDasColunas = [...]string{
	"id", "topic", "groupId", "bootstrap", "partition", "enabled", "state",
	"detail", "appliedOffset", "committedOffset", "highWatermark", "lag",
	"markAt", "lastBatchAt", "queueRecords", "queueBytes", "events", "applied",
	"stale", "discarded", "failed", "partitionsAssigned", "partitionsTotal",
	"watchdogTrips", "watchdogGaveUp",
}

type colunaComID struct {
	id types.FieldID
	k  coluna
}

// Tabela monta o schema sintético e o Store virtual de `_meta.kafka`.
func Tabela(f Fonte) (*schema.Compiled, query.Store, error) {
	spec, err := schema.Parse([]byte(especDaTabela), schema.DefaultsPadrao)
	if err != nil {
		return nil, nil, fmt.Errorf("metakafka: spec da tabela %s.%s inválida: %w", meta.Namespace, TabelaMetaKafka, err)
	}
	sc, err := spec.Compile(schema.NovoDicionarioEmMemoria())
	if err != nil {
		return nil, nil, fmt.Errorf("metakafka: compilando a tabela %s.%s: %w", meta.Namespace, TabelaMetaKafka, err)
	}
	fo := &fonte{
		f:         f,
		topico:    []byte(f.Declarada.Topic),
		grupo:     []byte(f.Declarada.GroupID),
		bootstrap: []byte(f.Declarada.Bootstrap),
		abertos:   map[uint64]*retrato{},
		colunas:   make([]colunaComID, 0, len(nomesDasColunas)),
	}
	for k, nome := range nomesDasColunas {
		campo, ok := sc.FieldByName([]byte(nome))
		if !ok {
			return nil, nil, erroDeColuna(nome)
		}
		fo.colunas = append(fo.colunas, colunaComID{id: campo.ID, k: coluna(k)})
	}
	// O payload exige FieldID crescente; a ordem é resolvida UMA vez aqui, e a
	// linha é montada sem ordenar.
	sort.Slice(fo.colunas, func(i, j int) bool { return fo.colunas[i].id < fo.colunas[j].id })
	return sc, fo, nil
}

// comum são as colunas de dataset — iguais em toda linha do resultado.
type comum struct {
	estado, detalhe   string
	temConsumidor     bool
	filaRegistros     int
	filaBytes         int64
	ultimoLote        time.Time
	desfecho          kafka.Desfecho
	particoesAtrib    int64
	particoesDoTopico int64
	trips             uint64
	desistiu          bool
	temVigia          bool
}

// linha é uma linha do retrato: a de resumo (Resumo=true) ou a de uma partição.
type linha struct {
	Resumo   bool
	Posicao  kafka.PosicaoDaParticao
	Duravel  int64
	TemDurav bool
	MarcaEm  time.Time
}

// retrato é o que UMA consulta enxerga: colhido no AbreSnapshot, IMUTÁVEL, e
// devolvido no FechaSnapshot.
type retrato struct {
	comum  comum
	linhas map[types.RowID]linha
	limite types.RowID
}

// fonte é o Store virtual. Registrada UMA vez no motor, serve TODAS as
// consultas concorrentes — então nada do que UMA consulta enxerga mora em
// campo mutável compartilhado.
type fonte struct {
	f                        Fonte
	colunas                  []colunaComID // em ordem crescente de FieldID
	topico, grupo, bootstrap []byte

	mu        sync.Mutex
	proxEpoca uint64
	abertos   map[uint64]*retrato

	// medindo é o voo único da marca d'água: uma consulta por vez fala com o
	// broker. Ver AbreSnapshot.
	medindo atomic.Bool
}

// AbreSnapshot congela o universo DESTA consulta: o conjunto de linhas E os
// números.
//
// Aqui a tabela diverge do `_counters`, que congela só o conjunto e lê os
// totais no presente — e a divergência é o motivo de ela existir. As colunas
// desta são uma MEDIÇÃO COM CARIMBO: markAt diz quando o broker respondeu, e
// `lag == highWatermark - appliedOffset` é um invariante que o coletor do
// /metrics tornou testável dentro de uma raspagem. Lendo no presente linha a
// linha, um resultado de três partições poderia carregar três markAt
// diferentes e um lag que não fecha com o HWM da mesma linha — exatamente o
// painel que discorda de si mesmo.
//
// O retrato é guardado por ÉPOCA, mintada aqui. O executor fecha o par
// AbreSnapshot/FechaSnapshot em todos os caminhos de saída, inclusive no Open
// que falha depois de abrir.
func (f *fonte) AbreSnapshot() types.Snapshot {
	var inst kafka.Instantaneo
	c := f.consumidor()
	if c != nil {
		inst = c.Instantaneo(f.contextoDaMarca())
	}

	r := &retrato{
		comum:  f.colheComum(c != nil, inst),
		linhas: map[types.RowID]linha{},
	}
	duraveis, temDuraveis := f.colheDuraveis()

	// Os rowIDs valem DENTRO deste retrato e são atribuídos na ordem em que
	// as partições vêm, que já é crescente. Um registro estável entre
	// consultas não teria o que proteger: o retrato é imutável e privado, e o
	// rowID não é coluna — nenhuma consulta enxerga o número.
	//
	// A linha de resumo existe SÓ quando não há partição nenhuma — com
	// partições, ela seria uma linha a mais no resultado —, e por isso as
	// duas formas nunca coexistem: a numeração a partir de 2 não evita
	// colisão (não há uma a evitar), só deixa o resumo sempre no rowID 1.
	for i, p := range inst.Particoes {
		l := linha{Posicao: p, MarcaEm: inst.MarcaEm}
		if temDuraveis {
			if off, ok := duraveis.Offsets[p.Particao]; ok {
				l.Duravel, l.TemDurav = off, true
			}
		}
		r.linhas[types.RowID(i)+2] = l
	}
	if len(r.linhas) == 0 {
		r.linhas[1] = linha{Resumo: true}
	}
	r.limite = 2 + types.RowID(len(inst.Particoes))

	f.mu.Lock()
	defer f.mu.Unlock()
	f.proxEpoca++
	epoca := f.proxEpoca
	f.abertos[epoca] = r
	return types.Snapshot{Epoca: epoca, LimiteRowID: r.limite}
}

// contextoDaMarca dá à ida ao broker um prazo PRÓPRIO, derivado do
// context.Background(): a interface do query.Store não recebe context, e este
// é o único jeito de a chamada ter teto. O preço está registrado: o
// cancelamento da consulta não alcança essa ida, e o pior caso é segurar a
// goroutine por kafka.PrazoDaMarcaDagua depois de o cliente desistir — o mesmo
// teto que o scrape do /metrics já aceita.
//
// O VOO ÚNICO limita o dano: uma consulta por vez fala com o broker. Quem não
// pega o lugar recebe um contexto já cancelado, o que faz a atualização sair
// na hora e a consulta reusar a última marca — ou publicar NULL, se estiver
// vencida, que é a resposta honesta. Sem isto, com o broker fora do ar, N
// consultas simultâneas gastariam o prazo inteiro cada uma.
func (f *fonte) contextoDaMarca() context.Context {
	if !f.medindo.CompareAndSwap(false, true) {
		ctx, cancela := context.WithCancel(context.Background())
		cancela()
		return ctx
	}
	ctx, cancela := context.WithTimeout(context.Background(), kafka.PrazoDaMarcaDagua)
	go func() {
		<-ctx.Done()
		cancela()
		f.medindo.Store(false)
	}()
	return ctx
}

func (f *fonte) consumidor() Consumido {
	if f.f.Consumidor == nil {
		return nil
	}
	return f.f.Consumidor()
}

func (f *fonte) colheDuraveis() (checkpoint.SourceOffsets, bool) {
	if f.f.Duraveis == nil {
		return checkpoint.SourceOffsets{}, false
	}
	o, ok, err := f.f.Duraveis()
	// Arquivo ilegível é ausência de offset, não offset zero: as duas
	// respostas do tri-estado que não são um número viram NULL.
	if err != nil || !ok {
		return checkpoint.SourceOffsets{}, false
	}
	return o, true
}

func (f *fonte) colheComum(temConsumidor bool, inst kafka.Instantaneo) comum {
	c := comum{temConsumidor: temConsumidor}
	if f.f.Estado != nil {
		c.estado, c.detalhe = f.f.Estado()
	} else {
		c.estado, c.detalhe = EstadoDeclarado(f.f.Declarada)
	}
	if temConsumidor {
		c.filaRegistros, c.filaBytes = inst.FilaRegistros, inst.FilaBytes
		c.ultimoLote = inst.UltimoLote
		c.desfecho = inst.Desfecho
		c.particoesAtrib, c.particoesDoTopico = inst.ParticoesAtribuidas, inst.ParticoesDoTopico
		if inst.Pausa != nil && c.detalhe == "" {
			c.detalhe = inst.Pausa.Error()
		}
	}
	if f.f.Vigia != nil {
		c.trips, c.desistiu, c.temVigia = f.f.Vigia()
	}
	return c
}

// FechaSnapshot devolve o retrato desta consulta. O registro de partições NÃO
// é podado: ele é a identidade dos rowIDs.
func (f *fonte) FechaSnapshot(snap types.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.abertos, snap.Epoca)
}

func (f *fonte) retratoDe(snap types.Snapshot) (*retrato, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.abertos[snap.Epoca]
	return r, ok
}

func (f *fonte) ScanLive(snap types.Snapshot) types.RowIDIterator {
	r, ok := f.retratoDe(snap)
	if !ok {
		return &iterador{}
	}
	ids := make([]types.RowID, 0, len(r.linhas))
	for id := range r.linhas {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return &iterador{ids: ids}
}

func (f *fonte) GetByRowID(snap types.Snapshot, id types.RowID, buf []byte) (types.Record, []byte, bool, error) {
	r, ok := f.retratoDe(snap)
	if !ok {
		// Época desconhecida é DEFEITO de pareamento, não linha ausente:
		// devolver ok=false faria a linha sumir do resultado sem nada acusar.
		return types.Record{}, buf, false, fmt.Errorf("metakafka: snapshot %d não está aberto", snap.Epoca)
	}
	l, ok := r.linhas[id]
	if !ok {
		return types.Record{}, buf, false, nil
	}
	return f.record(id, r, l, buf)
}

func (f *fonte) GetByKey(snap types.Snapshot, key, buf []byte) (types.Record, []byte, bool, error) {
	r, ok := f.retratoDe(snap)
	if !ok {
		return types.Record{}, buf, false, fmt.Errorf("metakafka: snapshot %d não está aberto", snap.Epoca)
	}
	// A busca é sobre as linhas DO RETRATO, e não sobre o registro de
	// partições: o point lookup tem de enxergar exatamente o que o scan
	// enxerga, senão devolveria falso negativo numa linha que o scan traria.
	for id, l := range r.linhas {
		if igualAChave(key, l) {
			return f.record(id, r, l, buf)
		}
	}
	return types.Record{}, buf, false, nil
}

// igualAChave compara a key pedida com o `id` da linha SEM alocar: o resumo é
// a literal fixa, e a partição é o número em decimal.
func igualAChave(key []byte, l linha) bool {
	if l.Resumo {
		return string(key) == ChaveDoResumo
	}
	n, ok := decimal(key)
	return ok && n == int64(l.Posicao.Particao)
}

// decimal lê um inteiro não negativo de bytes ASCII. Devolve false para vazio,
// sinal, não dígito ou estouro — nenhum desses é um número de partição.
func decimal(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 10 {
		return 0, false
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

// Indexado responde SEMPRE false, como nas outras tabelas virtuais: sem índice
// declarado o planner varre as linhas — uma por partição — e a key dá o point
// lookup por conta própria.
func (f *fonte) Indexado(types.FieldID) bool { return false }

// Lookup nunca é chamado: rede de segurança, como no _meta e no _counters.
func (f *fonte) Lookup(field types.FieldID, _ types.Value) (types.RowIDIterator, error) {
	return nil, fmt.Errorf("metakafka: a tabela %s.%s não tem índice (campo %d)", meta.Namespace, TabelaMetaKafka, field)
}

type iterador struct {
	ids []types.RowID
	i   int
}

func (it *iterador) Next() bool {
	if it.i < len(it.ids) {
		it.i++
		return true
	}
	return false
}

func (it *iterador) RowID() types.RowID {
	if it.i == 0 || it.i > len(it.ids) {
		return 0
	}
	return it.ids[it.i-1]
}

func (it *iterador) Err() error   { return nil }
func (it *iterador) Close() error { return nil }

// record codifica UMA linha.
//
// A regra que governa o método inteiro: COLUNA AUSENTE É NULL, e ausência
// nunca vira zero. Sem consumidor, as colunas vivas — fila, desfecho,
// partições — são NULL, porque "não há o que medir" e "medi e deu zero" são
// afirmações diferentes e o operador precisa das duas. Na linha de resumo, as
// colunas de partição saem NULL pela mesma razão. E o fim do log e o lag saem
// NULL quando a marca d'água venceu ou o broker não respondeu por aquela
// partição — um zero ali diria "está em dia" a quem perdeu o broker.
func (f *fonte) record(row types.RowID, r *retrato, l linha, buf []byte) (types.Record, []byte, bool, error) {
	buf = buf[:0]
	chave := chaveDaLinha(l)
	var err error
	for _, col := range f.colunas {
		var v types.Value
		switch col.k {
		case colID:
			v = types.Value{Type: types.TypeString, Bytes: chave}
		case colTopic:
			if len(f.topico) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: f.topico}
		case colGroupID:
			if len(f.grupo) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: f.grupo}
		case colBootstrap:
			if len(f.bootstrap) == 0 {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: f.bootstrap}
		case colEnabled:
			// Nunca NULL: um dataset sem bloco Kafka não consome, e isso é um
			// fato, não uma ausência de medida.
			v = types.Value{Type: types.TypeBool, I64: booleano(f.f.Declarada.Declarado && f.f.Declarada.Habilitado)}
		case colState:
			if r.comum.estado == "" {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: []byte(r.comum.estado)}
		case colDetail:
			if r.comum.detalhe == "" {
				continue
			}
			v = types.Value{Type: types.TypeString, Bytes: []byte(r.comum.detalhe)}
		case colParticao:
			if l.Resumo {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: int64(l.Posicao.Particao)}
		case colOffsetAplicado:
			if l.Resumo {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Posicao.Aplicado}
		case colOffsetDuravel:
			if l.Resumo || !l.TemDurav {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Duravel}
		case colFimDoLog:
			if l.Resumo || !l.Posicao.TemFim {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Posicao.FimDoLog}
		case colLag:
			if l.Resumo || !l.Posicao.TemFim {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: l.Posicao.Atraso}
		case colMarcaEm:
			if l.Resumo || !l.Posicao.TemFim || l.MarcaEm.IsZero() {
				continue
			}
			v = types.Value{Type: types.TypeTimestamp, I64: l.MarcaEm.UnixMilli()}
		case colUltimoLote:
			if !r.comum.temConsumidor || r.comum.ultimoLote.IsZero() {
				continue
			}
			v = types.Value{Type: types.TypeTimestamp, I64: r.comum.ultimoLote.UnixMilli()}
		case colFilaRegistros:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: int64(r.comum.filaRegistros)}
		case colFilaBytes:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.filaBytes}
		case colEventos:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Eventos}
		case colAplicados:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Aplicados}
		case colStale:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Stale}
		case colDescartados:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Descartados}
		case colFalhas:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.desfecho.Falhas}
		case colParticoesAtribuidas:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.particoesAtrib}
		case colParticoesDoTopico:
			if !r.comum.temConsumidor {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: r.comum.particoesDoTopico}
		case colWatchdogTrips:
			// NULL sem goroutine de ingestão: "não há watchdog" e "nunca
			// recriou" são afirmações diferentes, e um zero as confundiria.
			if !r.comum.temVigia {
				continue
			}
			v = types.Value{Type: types.TypeInt64, I64: int64(r.comum.trips)} //nolint:gosec // contagem de recriações, nunca perto do teto do int64
		case colWatchdogDesistiu:
			if !r.comum.temVigia {
				continue
			}
			v = types.Value{Type: types.TypeBool, I64: booleano(r.comum.desistiu)}
		}
		if buf, err = encoding.AppendField(buf, col.id, v); err != nil {
			return types.Record{}, buf, false, fmt.Errorf("metakafka: codificando a coluna %s: %w", nomesDasColunas[col.k], err)
		}
	}
	return types.Record{RowID: row, Sequence: int64(row), Key: chave, Payload: buf}, buf, true, nil
}

// chaveDaLinha devolve o `id`: a literal do resumo, ou a partição em decimal.
func chaveDaLinha(l linha) []byte {
	if l.Resumo {
		return []byte(ChaveDoResumo)
	}
	return []byte(strconv.Itoa(int(l.Posicao.Particao)))
}

// booleano converte para a representação de bool do payload (0 ou 1).
func booleano(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
