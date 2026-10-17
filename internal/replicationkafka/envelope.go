// Package replicationkafka publica o feed de replicação de um dataset num
// tópico Kafka, um registro Kafka por Entry, em JSON.
//
// Os arquivos envelope*.go são o ENVELOPE: a forma de cada registro. O laço
// do publicador (snapshot, tail, acks) mora noutro arquivo e só consome o que
// está aqui.
//
// # Contrato do envelope
//
// Registro de dado — as chaves saem SEMPRE nesta ordem:
//
//	{"epoch":"<hex>","sequence":N,"dataset":"<base>","table":"<tabela>","op":"insert|update|delete|snapshot","key":"<key>","at":"<instante>","record":{...}}
//
// Marcadores de controle, sem `record`:
//
//	{"epoch":"<hex>","dataset":"<base>","op":"reset","reason":"startup|gap|snapshot_failed|partitions_changed|crash|rewind|retention|config_changed|requested|periodic","previousEpoch":"<hex>","cursor":N,"at":"<instante>"}
//	{"epoch":"<hex>","dataset":"<base>","op":"snapshot_end","cursor":N,"count":N,"at":"<instante>"}
//	{"epoch":"<hex>","dataset":"<base>","op":"resume","cursor":N,"at":"<instante>"}
//	{"epoch":"<hex>","sequence":N,"dataset":"<base>","table":"<tabela>","op":"retain","floor":N,"at":"<instante>"}
//
// `dataset` é sempre o nome do dataset BASE, inclusive na linha da tabela
// histórica, que se distingue pelo `table`. `at` é o instante UTC com
// milissegundos, na MESMA grafia do timestamp das consultas
// ("2026-09-27T12:34:56.789Z") — é o executor quem o escreve, e não uma
// segunda implementação que pudesse divergir dele.
//
// A chave `previousEpoch` do `reset` é OPCIONAL, vem logo depois de `reason`
// e carrega a epoch da geração que o reset substitui; vazia, é omitida (o
// primeiro reset de um destino não tem geração anterior). Os seis motivos
// `crash`, `rewind`, `retention`, `config_changed`, `requested` e `periodic`,
// a chave `previousEpoch`, o op `resume` e o header `nishidb-reason` são
// ADITIVOS ao contrato publicado na v0.14: um consumidor que só conhece os
// quatro motivos antigos continua lendo o `reset` pelo `op`.
//
// O `resume` marca a retomada, na MESMA epoch, de um publicador que reabriu
// o feed de um fechamento limpo: nenhum snapshot se segue, e `cursor` é o
// último ponto confirmado antes do reinício, de onde o tail continua
// (`sequence` acima dele). Não tem `sequence` nem `record`. Sai em TODAS as
// partições, logo na subida e sem barreira de ack, e não exige ação nenhuma
// do consumidor: é só observabilidade.
//
// A escolha entre `resume` e `reset` na subida (e o motivo do `reset`) é do
// laço do publicador (publisher_resume.go, decide); o envelope só escreve o
// que ele decide. O `previousEpoch` é a última epoch publicada NESTE destino,
// lida do checkpoint dele, e sai só quando difere da epoch do `reset`.
//
// O `reset` sai com DOIS headers, `nishidb-op: reset` e
// `nishidb-reason: <motivo>` (a mesma grafia do `reason` do JSON), para o
// consumidor decidir sem abrir o corpo; os demais ops saem só com o
// `nishidb-op`.
//
// `op: snapshot` é a linha de um Insert vindo do snapshot, e sai com
// `sequence` igual ao cursor do reset corrente (nunca o Record.Sequence, nem
// zero): é o que permite ao consumidor podar, no `snapshot_end`, toda chave
// cujo último `(epoch, sequence)` aplicado ficou abaixo do reset. O `retain`
// emitido dentro do snapshot segue a mesma regra.
//
// # O `record`
//
// É a linha exatamente como o `SELECT *` da tabela a devolve em NDJSON — o
// rótulo é o alias quando existe, os valores saem pelo mesmo
// executor.AppendValue, e as colunas de sistema (IsSystem) e as derivadas
// (Parent != nil) ficam de fora pela mesma regra do FieldsByID —, com DUAS
// diferenças deliberadas. A primeira: a coluna ausente é OMITIDA, e não
// emitida como `null`. No NishiDB NULL é ausência nas três camadas, e o
// `null` explícito do NDJSON existe só porque a consulta tem colunas fixas;
// um registro de CDC não tem, e o `null` ali só custaria bytes e sugeriria um
// valor que não existe.
//
// O `record` do `delete` é o ÚLTIMO estado vivo da linha, que é o que o feed
// publica no delete. Dois deletes saem com `record:{}`, porque o que o feed
// publica neles é uma lápide, que só tem colunas de sistema: o delete de uma
// key que nunca existiu (o feed publica a própria lápide) e o delete de uma
// key JÁ apagada (o estado anterior é a lápide do delete anterior).
//
// A SEGUNDA diferença deliberada é o valor json que não é JSON. O `SELECT *`
// emite a coluna json verbatim; aqui o valor só sai inline se for JSON
// sintaticamente válido (RFC 8259) E UTF-8 válido, e senão sai como STRING
// JSON, com o byte de UTF-8 inválido trocado por \ufffd. O caso real é o
// `_raw` da tabela histórica para o evento que não parseou: ele guarda os
// bytes crus do evento, sem validação nenhuma, e inline faria do envelope
// inteiro um JSON inválido — uma mensagem venenosa para todo consumidor do
// tópico. O consumidor distingue os dois casos pelo tipo do valor: objeto ou
// array é o documento; string é o texto cru de um documento inválido.
//
// A ordem das colunas é a do payload, que é FieldID crescente
// (internal/storage/encoding) — a mesma ordem do FieldsByID, e portanto do
// `SELECT *`. O renderizador NUNCA chama FieldsByID por registro (aloca e
// ordena): ele percorre o payload com o FieldReader e resolve cada campo pelo
// FieldByID do Compiled da tabela.
//
// # A key
//
// A chave do REGISTRO KAFKA é autoritativa: na tabela base, os bytes crus da
// key da entidade; na histórica, o rowID em decimal ASCII; no controle, a
// constante ControlKey. O `key` do JSON é conveniência de leitura e passa
// pelo AppendJSONString, que troca byte de UTF-8 inválido por U+FFFD — duas
// keys binárias diferentes podem sair com o MESMO `key` no JSON, e só a key
// do registro Kafka as distingue.
//
// # Erros
//
// O feed publica dado ruim num caso só: o `_raw` do evento ruim na tabela
// histórica, que é o evento como chegou. Ele NÃO é erro de renderização — sai
// como string, pela regra acima —, porque recusá-lo pararia o publicador por
// causa de um evento que a ingestão já contou e seguiu (§101).
//
// Todo erro deste pacote é BUG: operação sem envelope (Schema, Reset do
// feed), tabela estranha ao dataset, campo que o Compiled da tabela não
// conhece e payload corrompido. O publicador tem de parar — pular o registro
// faria o tópico divergir do dataset em silêncio.
//
// Os arquivos do envelope NÃO importam fmt nem encoding/json (há teste de
// AST que cobra, em todo envelope*.go que não é de teste): os dois trazem
// reflexão e formatação genérica, e o gate de zero alocação por registro
// depende de não haver nenhum dos dois. O laço do publicador (publisher*.go)
// não está nesse caminho e pode usar fmt.
package replicationkafka

import (
	"errors"
	"strconv"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrUnsupportedOperation é a operação do feed que não tem envelope: Schema e
// Reset (que o feed só emite para projeções de consulta), ou tag
// desconhecida. É bug, nunca registro a pular.
var ErrUnsupportedOperation = errors.New("replicationkafka: feed operation has no envelope")

// ErrUnknownField é o campo do payload que o Compiled da tabela não conhece.
// Descartá-lo publicaria um `record` que o `SELECT *` não devolve.
var ErrUnknownField = errors.New("replicationkafka: payload field is unknown to the table schema")

// ErrUnknownTable é a Entry de uma tabela que não é nem o dataset base nem a
// sua tabela histórica.
var ErrUnknownTable = errors.New("replicationkafka: entry table does not belong to the dataset")

// ErrUnknownResetReason é o motivo de reset fora da lista fechada do contrato.
var ErrUnknownResetReason = errors.New("replicationkafka: unknown reset reason")

// ErrInvalidRenderer é a configuração recusada pelo NewRenderer.
var ErrInvalidRenderer = errors.New("replicationkafka: invalid renderer configuration")

// ControlKey é a chave Kafka dos marcadores de controle (reset, snapshot_end,
// resume, retain). O slice é compartilhado: quem o recebe não pode escrever nele.
var ControlKey = []byte("__nishidb_control__")

// HeaderOp é o nome do header Kafka que carrega o `op` do registro, para o
// consumidor rotear sem abrir o JSON.
const HeaderOp = "nishidb-op"

// HeaderReason é o nome do header Kafka que o `reset` carrega ALÉM do
// HeaderOp, com o motivo na mesma grafia do `reason` do JSON.
const HeaderReason = "nishidb-reason"

// Op é o `op` do envelope. Não é a replication.Operation: o envelope tem ops
// que o feed não tem (snapshot, reset, snapshot_end) e o feed tem operações
// que o envelope recusa (Schema, Reset).
type Op uint8

const (
	// OpInsert é a primeira versão materializada de uma key (ou uma linha da
	// histórica, no tail).
	OpInsert Op = iota + 1
	// OpUpdate é a versão nova de uma key existente — inclusive a
	// ressurreição de uma key deletada.
	OpUpdate
	// OpDelete é a remoção de uma key, com o último estado vivo no `record`.
	OpDelete
	// OpSnapshot é uma linha viva publicada pelo snapshot.
	OpSnapshot
	// OpRetain é o avanço do piso de retenção da histórica.
	OpRetain
	// OpReset é o marcador de início de reconstrução.
	OpReset
	// OpSnapshotEnd é o marcador de fim do snapshot.
	OpSnapshotEnd
	// OpResume é o marcador de retomada, na mesma epoch, depois de um
	// fechamento limpo.
	OpResume
)

// opNames é a grafia de cada Op no fio, indexada pelo próprio Op. É ao mesmo
// tempo o valor do header e o conteúdo do `op` do JSON (que não precisa de
// escape: é ASCII sem aspas).
var opNames = [...][]byte{
	OpInsert:      []byte("insert"),
	OpUpdate:      []byte("update"),
	OpDelete:      []byte("delete"),
	OpSnapshot:    []byte("snapshot"),
	OpRetain:      []byte("retain"),
	OpReset:       []byte("reset"),
	OpSnapshotEnd: []byte("snapshot_end"),
	OpResume:      []byte("resume"),
}

// String devolve a grafia do op no fio, ou "" para um Op inválido.
func (o Op) String() string { return string(OpHeader(o)) }

// OpHeader devolve o valor do header HeaderOp para o op, sem alocar, ou nil
// para um Op inválido. O slice é compartilhado entre todos os registros:
// quem o recebe não pode escrever nele.
func OpHeader(o Op) []byte {
	if int(o) >= len(opNames) {
		return nil
	}
	return opNames[o]
}

// EntryOp mapeia a operação de uma Entry do TAIL para o op do envelope.
// Schema, Reset e tag desconhecida devolvem ErrUnsupportedOperation.
func EntryOp(o replication.Operation) (Op, error) {
	switch o {
	case replication.Insert:
		return OpInsert, nil
	case replication.Update:
		return OpUpdate, nil
	case replication.Delete:
		return OpDelete, nil
	case replication.Retain:
		return OpRetain, nil
	default:
		return 0, unsupported(o)
	}
}

// SnapshotEntryOp mapeia a operação de uma Entry do SNAPSHOT: o Insert vira
// OpSnapshot e o Retain continua OpRetain. Qualquer outra é
// ErrUnsupportedOperation — o snapshot só exporta linhas vivas e o piso da
// histórica.
func SnapshotEntryOp(o replication.Operation) (Op, error) {
	switch o {
	case replication.Insert:
		return OpSnapshot, nil
	case replication.Retain:
		return OpRetain, nil
	default:
		return 0, unsupported(o)
	}
}

// ResetReason é o motivo de um reset, numa lista fechada.
type ResetReason uint8

const (
	// ResetStartup é o primeiro snapshot depois da subida do publicador.
	ResetStartup ResetReason = iota + 1
	// ResetGap é a lacuna no feed (ErrResyncRequired): a epoch se mantém e o
	// cursor salta.
	ResetGap
	// ResetSnapshotFailed é o snapshot anterior que falhou no meio.
	ResetSnapshotFailed
	// ResetPartitionsChanged é a mudança do número de partições do tópico,
	// que muda o destino das keys.
	ResetPartitionsChanged
	// ResetCrash é a subida depois de uma queda: o feed em disco não foi
	// fechado de forma limpa, então a geração é nova.
	ResetCrash
	// ResetRewind é o feed que voltou no tempo em relação ao que o destino
	// já recebeu: o .ckpt está à frente do feed, OU o topo do tópico tem uma
	// posição acima da retomada, OU o topo traz uma epoch que o .ckpt não
	// conhece, OU o topo não pode ser conferido (partição sem registro
	// legível).
	ResetRewind
	// ResetRetention é o ponto confirmado que a retenção do feed já
	// descartou.
	ResetRetention
	// ResetConfigChanged é a mudança da impressão da renderização declarada
	// (RenderHash: tabelas publicadas, aliases e publishHistory), que
	// invalida o que o tópico já tem. Tópico ou cluster diferentes NÃO são
	// este motivo: são destino novo e saem como ResetStartup.
	ResetConfigChanged
	// ResetRequested é o snapshot pedido pelo operador.
	ResetRequested
	// ResetPeriodic é o snapshot agendado pela configuração.
	ResetPeriodic
)

var resetReasonJSON = [...][]byte{
	ResetStartup:           []byte(`"startup"`),
	ResetGap:               []byte(`"gap"`),
	ResetSnapshotFailed:    []byte(`"snapshot_failed"`),
	ResetPartitionsChanged: []byte(`"partitions_changed"`),
	ResetCrash:             []byte(`"crash"`),
	ResetRewind:            []byte(`"rewind"`),
	ResetRetention:         []byte(`"retention"`),
	ResetConfigChanged:     []byte(`"config_changed"`),
	ResetRequested:         []byte(`"requested"`),
	ResetPeriodic:          []byte(`"periodic"`),
}

// ReasonHeader devolve o valor do header HeaderReason para o motivo, sem
// alocar, ou nil para um motivo inválido. É o `reason` do JSON sem as aspas.
// O slice é compartilhado: quem o recebe não pode escrever nele. A capacidade
// é limitada ao comprimento, então um append sobre ele copia em vez de
// escrever na tabela.
func ReasonHeader(r ResetReason) []byte {
	if r == 0 || int(r) >= len(resetReasonJSON) {
		return nil
	}
	b := resetReasonJSON[r]
	return b[1 : len(b)-1 : len(b)-1]
}

// String devolve a grafia do motivo no fio, ou "" para um motivo inválido.
func (r ResetReason) String() string { return string(ReasonHeader(r)) }

// FieldSource é o que o renderizador precisa do Compiled de uma tabela. O
// *schema.Compiled a satisfaz.
type FieldSource interface {
	FieldByID(id types.FieldID) (*schema.CompiledField, bool)
}

// Renderer escreve os envelopes de UM dataset: a tabela base e, quando há,
// a sua tabela histórica.
//
// NÃO é seguro para uso concorrente: ele guarda o cursor do payload e o
// cache de rótulos, e pertence à goroutine do publicador. O buffer de saída é
// do chamador, que o reaproveita entre registros (dst[:0]).
type Renderer struct {
	// datasetJSON é `,"dataset":"<base>"`, pré-renderizado.
	datasetJSON []byte
	base        tableRenderer
	history     *tableRenderer

	// epoch e epochJSON são o cache da abertura `{"epoch":"<hex>"`: a epoch
	// só muda com o reinício do master, então re-escapá-la por registro seria
	// trabalho jogado fora.
	epoch     string
	epochJSON []byte

	// previousEpoch e previousEpochJSON são o cache do fragmento
	// `,"previousEpoch":"<hex>"` do reset, pelo mesmo motivo da epoch.
	previousEpoch     string
	previousEpochJSON []byte

	fr encoding.FieldReader
}

// tableRenderer é o estado de UMA tabela.
type tableRenderer struct {
	name string
	// tableJSON é `,"table":"<nome>"`, pré-renderizado.
	tableJSON []byte
	fields    FieldSource
	// isHistory troca a regra da key: rowID em decimal em vez da key da
	// entidade, que a histórica não tem.
	isHistory bool
	// labels é o cache de rótulo por FieldID. Ver label para o porquê de o
	// cache ser seguro.
	labels []fieldLabel
}

// fieldLabel é o rótulo resolvido de um FieldID. known distingue a entrada
// preenchida do zero-value; hidden marca a coluna que o `SELECT *` não
// projeta.
type fieldLabel struct {
	known  bool
	hidden bool
	// json é `"<rótulo>":`, escapado.
	json []byte
}

// NewRenderer monta o renderizador do dataset. historyTable e history vêm os
// dois ou nenhum: o dataset sem tabela histórica passa "" e nil.
//
// base é o Compiled com que o motor de consulta responde o `SELECT *` do
// dataset; history é o da tabela irmã — o `specIrma.Compile` que o servidor
// registra no motor sob o nome da histórica (internal/server/history.go), e
// NÃO o do base: é ele que conhece o `_raw` e que deixa as congeladas de
// fora, e a histórica é consultada por ele.
func NewRenderer(dataset, historyTable string, base, history FieldSource) (*Renderer, error) {
	switch {
	case dataset == "":
		return nil, invalid("dataset name is empty")
	case base == nil:
		return nil, invalid("base table schema is nil")
	case (historyTable == "") != (history == nil):
		return nil, invalid("history table name and schema must be given together")
	case historyTable != "" && historyTable == dataset:
		return nil, invalid("history table name equals the dataset name")
	}
	r := &Renderer{
		datasetJSON: appendMember(nil, `,"dataset":`, dataset),
		base:        newTableRenderer(dataset, base, false),
	}
	if history != nil {
		h := newTableRenderer(historyTable, history, true)
		r.history = &h
	}
	return r, nil
}

func newTableRenderer(name string, fields FieldSource, isHistory bool) tableRenderer {
	return tableRenderer{
		name:      name,
		tableJSON: appendMember(nil, `,"table":`, name),
		fields:    fields,
		isHistory: isHistory,
	}
}

// AppendEntry escreve o envelope de uma Entry do TAIL (insert, update,
// delete ou retain) ao final de dst e devolve dst crescido.
//
// Em erro, devolve dst no comprimento de entrada — o chamador nunca fica com
// um envelope pela metade no buffer.
func (r *Renderer) AppendEntry(dst []byte, epoch string, e replication.Entry) ([]byte, error) {
	op, err := EntryOp(e.Operation)
	if err != nil {
		return dst, err
	}
	return r.appendEntry(dst, epoch, op, e.Sequence, e.At, e)
}

// AppendSnapshot escreve o envelope de uma Entry do SNAPSHOT: o Insert sai
// como `op: snapshot` e o Retain como `op: retain`, os dois com `sequence`
// igual a cursor (o cursor do reset corrente) e `at` igual ao instante do
// snapshot. Em erro, devolve dst no comprimento de entrada.
func (r *Renderer) AppendSnapshot(dst []byte, epoch string, cursor uint64, at time.Time, e replication.Entry) ([]byte, error) {
	op, err := SnapshotEntryOp(e.Operation)
	if err != nil {
		return dst, err
	}
	return r.appendEntry(dst, epoch, op, cursor, at, e)
}

func (r *Renderer) appendEntry(dst []byte, epoch string, op Op, sequence uint64, at time.Time, e replication.Entry) ([]byte, error) {
	t, err := r.table(e.Table)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst = r.appendEpoch(dst, epoch)
	dst = append(dst, `,"sequence":`...)
	dst = strconv.AppendUint(dst, sequence, 10)
	dst = append(dst, r.datasetJSON...)
	dst = append(dst, t.tableJSON...)
	dst = appendOp(dst, op)
	if op == OpRetain {
		// O piso viaja no RowID do Record (replication.Retain).
		dst = append(dst, `,"floor":`...)
		dst = strconv.AppendUint(dst, uint64(e.Record.RowID), 10)
		dst = appendAt(dst, at)
		return append(dst, '}'), nil
	}
	dst = append(dst, `,"key":`...)
	if t.isHistory {
		dst = append(dst, '"')
		dst = strconv.AppendUint(dst, uint64(e.Record.RowID), 10)
		dst = append(dst, '"')
	} else {
		dst = executor.AppendJSONString(dst, e.Record.Key)
	}
	dst = appendAt(dst, at)
	dst = append(dst, `,"record":`...)
	dst, err = r.appendRecord(dst, t, e.Record)
	if err != nil {
		return dst[:start], err
	}
	return append(dst, '}'), nil
}

// AppendReset escreve o marcador de reset ao final de dst. cursor é de onde o
// tail recomeça; o snapshot que segue sai com `sequence` igual a ele.
//
// previousEpoch é a epoch da geração que este reset substitui e sai na chave
// `previousEpoch`, logo depois de `reason`; vazia, a chave é omitida. Em erro
// (motivo fora da lista fechada), devolve dst intacto.
func (r *Renderer) AppendReset(dst []byte, epoch, previousEpoch string, reason ResetReason, cursor uint64, at time.Time) ([]byte, error) {
	if reason == 0 || int(reason) >= len(resetReasonJSON) {
		return dst, &renderError{
			msg: "replicationkafka: unknown reset reason " + strconv.FormatUint(uint64(reason), 10),
			err: ErrUnknownResetReason,
		}
	}
	dst = r.appendEpoch(dst, epoch)
	dst = append(dst, r.datasetJSON...)
	dst = appendOp(dst, OpReset)
	dst = append(dst, `,"reason":`...)
	dst = append(dst, resetReasonJSON[reason]...)
	if previousEpoch != "" {
		dst = r.appendPreviousEpoch(dst, previousEpoch)
	}
	dst = append(dst, `,"cursor":`...)
	dst = strconv.AppendUint(dst, cursor, 10)
	dst = appendAt(dst, at)
	return append(dst, '}'), nil
}

// AppendResume escreve o marcador de retomada ao final de dst. cursor é o
// ponto de onde o tail continua, na mesma epoch.
func (r *Renderer) AppendResume(dst []byte, epoch string, cursor uint64, at time.Time) []byte {
	dst = r.appendEpoch(dst, epoch)
	dst = append(dst, r.datasetJSON...)
	dst = appendOp(dst, OpResume)
	dst = append(dst, `,"cursor":`...)
	dst = strconv.AppendUint(dst, cursor, 10)
	dst = appendAt(dst, at)
	return append(dst, '}')
}

// AppendSnapshotEnd escreve o marcador de fim do snapshot ao final de dst.
// count é quantos registros de snapshot foram para a partição que recebe
// ESTE marcador — o chamador o conta por partição.
func (r *Renderer) AppendSnapshotEnd(dst []byte, epoch string, cursor, count uint64, at time.Time) []byte {
	dst = r.appendEpoch(dst, epoch)
	dst = append(dst, r.datasetJSON...)
	dst = appendOp(dst, OpSnapshotEnd)
	dst = append(dst, `,"cursor":`...)
	dst = strconv.AppendUint(dst, cursor, 10)
	dst = append(dst, `,"count":`...)
	dst = strconv.AppendUint(dst, count, 10)
	dst = appendAt(dst, at)
	return append(dst, '}')
}

// KafkaKey escreve a chave do registro Kafka da Entry ao final de dst: os
// bytes crus da key na tabela base, o rowID em decimal ASCII na histórica, e
// ControlKey no retain. Serve ao tail e ao snapshot (o Insert do snapshot tem
// a mesma chave do Insert do tail).
func (r *Renderer) KafkaKey(dst []byte, e replication.Entry) ([]byte, error) {
	switch e.Operation {
	case replication.Insert, replication.Update, replication.Delete:
	case replication.Retain:
		if _, err := r.table(e.Table); err != nil {
			return dst, err
		}
		return append(dst, ControlKey...), nil
	default:
		return dst, unsupported(e.Operation)
	}
	t, err := r.table(e.Table)
	if err != nil {
		return dst, err
	}
	if t.isHistory {
		return strconv.AppendUint(dst, uint64(e.Record.RowID), 10), nil
	}
	return append(dst, e.Record.Key...), nil
}

// table resolve a tabela da Entry. A comparação de string não aloca.
func (r *Renderer) table(name string) (*tableRenderer, error) {
	if name == r.base.name {
		return &r.base, nil
	}
	if r.history != nil && name == r.history.name {
		return r.history, nil
	}
	return nil, &renderError{
		msg: "replicationkafka: table " + strconv.Quote(name) + " is neither dataset " +
			strconv.Quote(r.base.name) + " nor its history table",
		err: ErrUnknownTable,
	}
}

// appendEpoch abre o objeto com `{"epoch":"<hex>"`.
func (r *Renderer) appendEpoch(dst []byte, epoch string) []byte {
	if epoch != r.epoch || r.epochJSON == nil {
		r.epoch = epoch
		r.epochJSON = appendMember(r.epochJSON[:0], `{"epoch":`, epoch)
	}
	return append(dst, r.epochJSON...)
}

// appendPreviousEpoch escreve `,"previousEpoch":"<hex>"`. Como a epoch, o
// fragmento é cacheado: a conversão do appendMember aloca, e o reset de um
// destino repete a mesma epoch anterior em todas as retentativas do snapshot.
func (r *Renderer) appendPreviousEpoch(dst []byte, previousEpoch string) []byte {
	if previousEpoch != r.previousEpoch || r.previousEpochJSON == nil {
		r.previousEpoch = previousEpoch
		r.previousEpochJSON = appendMember(r.previousEpochJSON[:0], `,"previousEpoch":`, previousEpoch)
	}
	return append(dst, r.previousEpochJSON...)
}

// appendRecord escreve o `record`: o objeto com as colunas presentes do
// payload, na ordem do payload, com o rótulo do `SELECT *`.
func (r *Renderer) appendRecord(dst []byte, t *tableRenderer, rec types.Record) ([]byte, error) {
	dst = append(dst, '{')
	first := true
	r.fr.Reset(rec.Payload)
	for {
		id, v, ok := r.fr.Next()
		if !ok {
			break
		}
		l, err := t.label(id, rec)
		if err != nil {
			return dst, err
		}
		if l.hidden {
			continue
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = append(dst, l.json...)
		dst = appendFieldValue(dst, v)
	}
	if err := r.fr.Err(); err != nil {
		return dst, &renderError{
			msg: "replicationkafka: table " + strconv.Quote(t.name) + " row " +
				strconv.FormatUint(uint64(rec.RowID), 10) + ": " + err.Error(),
			err: err,
		}
	}
	return append(dst, '}'), nil
}

// label resolve o rótulo do FieldID, pelo cache ou pelo FieldByID.
//
// O cache é seguro porque a entrada de um FieldID no Compiled nunca é
// substituída depois de criada: o dicionário liga nome e id um a um, os três
// caminhos de runtime que registram coluna (createUnknown, createNested e
// adoptDiscovered, em internal/schema) só escrevem quando o nome — e portanto
// o id — ainda não existe, e o CompiledField é imutável fora do tipo fixado
// do campo auto, que não entra no rótulo. Sem o cache, cada campo de cada
// registro tomaria o RLock do Compiled, e o piloto tem payloads com centenas
// de colunas.
//
// O MISS não é cacheado: um FieldID desconhecido é erro, e a próxima
// chamada volta a perguntar ao Compiled.
func (t *tableRenderer) label(id types.FieldID, rec types.Record) (*fieldLabel, error) {
	if int(id) < len(t.labels) && t.labels[id].known {
		return &t.labels[id], nil
	}
	f, ok := t.fields.FieldByID(id)
	if !ok || f == nil {
		return nil, &renderError{
			msg: "replicationkafka: table " + strconv.Quote(t.name) + " row " +
				strconv.FormatUint(uint64(rec.RowID), 10) + ": field id " +
				strconv.FormatUint(uint64(id), 10) + " is unknown to the table schema",
			err: ErrUnknownField,
		}
	}
	if int(id) >= len(t.labels) {
		t.labels = append(t.labels, make([]fieldLabel, int(id)+1-len(t.labels))...)
	}
	l := &t.labels[id]
	l.known = true
	// A mesma regra do FieldsByID, que é o que o `SELECT *` expande: fora as
	// colunas de sistema e as derivadas.
	l.hidden = f.IsSystem || f.Parent != nil
	if !l.hidden {
		// No `*` o rótulo é o alias quando existe (binder.projection).
		name := f.Name
		if f.Alias != "" {
			name = f.Alias
		}
		l.json = executor.AppendJSONString(make([]byte, 0, len(name)+3), []byte(name))
		l.json = append(l.json, ':')
	}
	return l, nil
}

// appendFieldValue escreve o valor de uma coluna do `record`.
//
// É o executor.AppendValue do `SELECT *` em tudo, menos no json que não é
// JSON: esse sai como STRING JSON (AppendJSONString troca byte de UTF-8
// inválido por � e escapa os controles), e nunca inline. Ver validJSON
// para o porquê, e o godoc do pacote para a divergência com a consulta.
func appendFieldValue(dst []byte, v types.Value) []byte {
	if v.Type == types.TypeJSON && !validJSON(v.Bytes) {
		return executor.AppendJSONString(dst, v.Bytes)
	}
	return executor.AppendValue(dst, v)
}

// appendOp escreve `,"op":"<op>"`.
func appendOp(dst []byte, op Op) []byte {
	dst = append(dst, `,"op":"`...)
	dst = append(dst, opNames[op]...)
	return append(dst, '"')
}

// appendAt escreve `,"at":"<instante>"` pelo mesmo formatador do timestamp
// das consultas. O instante é truncado ao milissegundo, que é a resolução do
// tipo timestamp.
func appendAt(dst []byte, at time.Time) []byte {
	dst = append(dst, `,"at":`...)
	return executor.AppendValue(dst, types.Value{Type: types.TypeTimestamp, I64: at.UnixMilli()})
}

// appendMember escreve prefix seguido de s como string JSON. Só roda fora do
// caminho quente (construção e troca de epoch): a conversão para []byte
// aloca.
func appendMember(dst []byte, prefix, s string) []byte {
	dst = append(dst, prefix...)
	return executor.AppendJSONString(dst, []byte(s))
}

// renderError carrega a mensagem com o contexto (tabela, linha, campo) e
// preserva o sentinela para errors.Is.
type renderError struct {
	msg string
	err error
}

func (e *renderError) Error() string { return e.msg }
func (e *renderError) Unwrap() error { return e.err }

func unsupported(o replication.Operation) error {
	return &renderError{
		msg: "replicationkafka: feed operation " + strconv.FormatUint(uint64(o), 10) + " has no envelope",
		err: ErrUnsupportedOperation,
	}
}

func invalid(msg string) error {
	return &renderError{msg: "replicationkafka: " + msg, err: ErrInvalidRenderer}
}
