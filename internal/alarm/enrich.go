package alarm

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// maxEnrichmentsPerSweep é quantas consultas de enriquecimento uma passada
// executa.
//
// Não é chave de configuração, pela mesma razão do intervalo entre passadas da
// compaction: o valor protege o processo de um cenário (avalanche), e não
// expressa uma política que o operador queira ajustar. Milhares de grupos
// colapsando de uma vez — um cluster inteiro caindo — produziriam milhares de
// consultas numa única varredura, e o remédio é dizer no evento que o
// enriquecimento foi pulado, nunca deixar de alarmar.
// @TODO: Deixar configurável, mas com teto de 1000. O operador pode querer mais, mas o teto protege o processo de um cenário de avalanche.
const maxEnrichmentsPerSweep = 16

// enrichTimeout é o prazo de UMA consulta de enriquecimento.
//
// Curto de propósito: o enriquecimento é auxiliar, e uma consulta que não
// responde em segundos não pode segurar a varredura dos outros alarmes. O
// desfecho de estourar é o bloco vazio com o motivo, que é informação.
// @TODO: Deixar configurável.
const enrichTimeout = 2 * time.Second

// Executor é o que o enriquecimento precisa do motor de consulta.
//
// Interface, e não o *query.Engine, porque este pacote só usa uma operação dele
// — e porque a assinatura declarada aqui é o que documenta que o valor do grupo
// viaja em `params`, NUNCA concatenado no `sql`.
type Executor interface {
	Execute(ctx context.Context, dataset, sql string, params []types.Value) (executor.RowIterator, error)
}

// Enrichment é o resultado de UMA consulta que acompanha um evento.
type Enrichment struct {
	// Name é o nome declarado da consulta, e é por ele que o consumidor
	// identifica o bloco.
	Name string
	// Rows são as linhas como um array JSON já montado, pelo MESMO
	// serializador das respostas de consulta. Vem pronto em vez de estruturado
	// para não haver uma segunda renderização de valor no produto — a do
	// NDJSON é a canônica, e duas divergiriam nos casos de ponta (double,
	// instante, bytes).
	Rows json.RawMessage
	// Note diz por que não há linhas: a consulta falhou, estourou o prazo, ou
	// foi pulada pelo teto da passada. Vazio quando as linhas vieram.
	Note string
}

// enrich executa as consultas de um alarme que acabou de disparar.
//
// Roda FORA de qualquer lock do storage, e depois de o alarme já estar
// decidido: o resultado nunca muda o veredito. A consequência é que a consulta
// vê um estado alguns instantes mais novo que o da decisão, e é por isso que o
// evento carrega os dois instantes — o `since` da sustentação e o `at` do
// evento.
func (r *Runner) enrich(ra *runnerAlarm, key, text []byte, counts groupCounts) []Enrichment {
	if len(ra.compiled.enrich) == 0 {
		return nil
	}
	out := make([]Enrichment, 0, len(ra.compiled.enrich))
	for i := range ra.compiled.enrich {
		q := &ra.compiled.enrich[i]
		e := Enrichment{Name: q.name}
		switch {
		case r.engine == nil:
			ra.stats.enrichFailed.Add(1)
			// Sem motor declarado não há como consultar. É configuração do
			// chamador, não falha de runtime, e o evento o diz em vez de
			// esconder.
			e.Note = "sem motor de consulta"
		case r.enrichBudget <= 0:
			e.Note = "pulado: teto de enriquecimentos da passada"
			ra.stats.enrichSkipped.Add(1)
		default:
			r.enrichBudget--
			rows, err := r.runQuery(q, key, text, counts)
			if err != nil {
				e.Note = "consulta falhou: " + err.Error()
				ra.stats.enrichFailed.Add(1)
			} else {
				e.Rows = rows
			}
		}
		out = append(out, e)
	}
	return out
}

// runQuery executa uma consulta e devolve as linhas como array JSON.
func (r *Runner) runQuery(q *enrichQuery, key, text []byte, counts groupCounts) (json.RawMessage, error) {
	params, err := r.paramsOf(q, key, text, counts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), enrichTimeout)
	defer cancel()
	it, err := r.engine.Execute(ctx, r.dataset, q.sql, params)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	buf := append(r.enrichBuf[:0], '[')
	rows := 0
	for it.Next() {
		if rows > 0 {
			buf = append(buf, ',')
		}
		buf = appendEnrichRow(buf, it.Row())
		rows++
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	buf = append(buf, ']')
	r.enrichBuf = buf
	// A cópia existe porque o buffer é reusado pela consulta seguinte, e o
	// evento sobrevive à passada.
	return json.RawMessage(append([]byte(nil), buf...)), nil
}

// appendEnrichRow serializa uma linha do resultado como objeto JSON, com o
// MESMO serializador das respostas de consulta.
//
// A coluna cujo NOME cai na supressão de log sai redigida aqui, na montagem. É
// obrigatório fazê-lo aqui: o handler de log percorre atributos e grupos, mas
// não entra num valor já serializado, e o nome da coluna vem do SQL que o
// operador escreveu — nada impede um `SELECT token FROM ...`.
func appendEnrichRow(dst []byte, row executor.Row) []byte {
	dst = append(dst, '{')
	for i, c := range row.Cols() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = executor.AppendJSONString(dst, []byte(c.Name))
		dst = append(dst, ':')
		switch {
		case logging.IsSensitiveKey(c.Name):
			dst = executor.AppendJSONString(dst, []byte(logging.Redacted))
		case c.Null:
			dst = append(dst, "null"...)
		default:
			dst = executor.AppendValue(dst, c.Val)
		}
	}
	return append(dst, '}')
}

// paramsOf monta os valores ligados aos `?` da consulta, na ordem do bind.
//
// Os valores saem da linha do contador: a chave canônica aberta nas colunas do
// groupBy, o texto do grupo, o total ou os rows — os dois últimos do MESMO
// payload que decidiu o disparo (countsOf). O binder COPIA o que recebe, então
// apontar para o scratch aqui é seguro.
func (r *Runner) paramsOf(q *enrichQuery, key, text []byte, counts groupCounts) ([]types.Value, error) {
	if len(q.binds) == 0 {
		return nil, nil
	}
	vals, err := valuesOf(key, r.vals)
	r.vals = vals
	if err != nil {
		return nil, err
	}
	params := r.params[:0]
	for _, b := range q.binds {
		if b.null {
			params = append(params, types.Value{})
			continue
		}
		switch b.kind {
		case bindGroupKey:
			params = append(params, types.Value{Type: types.TypeString, Bytes: text})
		case bindTotal:
			params = append(params, types.Value{Type: types.TypeInt64, I64: counts.total})
		case bindRows:
			params = append(params, types.Value{Type: types.TypeInt64, I64: counts.rows})
		default:
			if b.groupIndex >= len(vals) {
				return nil, errors.New("alarm: chave do grupo com menos colunas que o groupBy")
			}
			params = append(params, vals[b.groupIndex])
		}
	}
	r.params = params
	return params, nil
}
