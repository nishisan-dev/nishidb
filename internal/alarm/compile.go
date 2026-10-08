package alarm

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// MaxEnrichRows é o teto do LIMIT de uma consulta de enriquecimento.
//
// O enriquecimento vai INTEIRO para dentro de um evento, e um evento é uma
// linha de log. O teto existe para que um `LIMIT 100000` declarado por engano
// não transforme um disparo em despejo — e o LIMIT é exigido, em vez de imposto
// em silêncio, porque quem escreve a consulta tem de saber quantas linhas ela
// traz para o evento.
const MaxEnrichRows = 100

// Definition são os alarmes compilados de UM contador de um dataset.
type Definition struct {
	// Dataset é onde as consultas de enriquecimento correm. Nome qualificado
	// no FROM alcança outro dataset registrado no mesmo motor.
	Dataset string
	// Counter é o nome do contador cujas linhas os alarmes observam.
	Counter string
	// def é a definição do contador, guardada porque o laço precisa dela para
	// montar a tabela virtual, abrir a chave do grupo nas colunas e renderizar
	// o texto de um grupo que a varredura não viu mais. Ela JÁ é o insumo desta
	// compilação: recuperá-la depois do leitor por type assertion acoplaria o
	// laço ao leitor concreto e falharia em silêncio com qualquer outro.
	def counter.Definition
	// metrics são as métricas de janela declaradas no contador. Guardadas pela
	// mesma razão de def: a tabela própria do laço tem de ter o MESMO schema
	// contra o qual as condições foram compiladas, colunas das métricas
	// incluídas.
	metrics []schema.CounterMetricSpec
	alarms  []compiledAlarm
}

// compiledAlarm é um alarme pronto para o laço.
type compiledAlarm struct {
	name string
	when string
	// qualifyWhen é a condição de qualificação normalizada; vazio quando o
	// alarme não a declara, e aí todo grupo é elegível.
	qualifyWhen  string
	sustainedFor time.Duration
	// metricsFingerprint é o texto canônico das métricas citadas pelas duas
	// condições (ver metricsFingerprintOf). Entra na assinatura da regra
	// gravada no estado durável.
	metricsFingerprint string
	// newEvaluator é FÁBRICA, e não um avaliador: o avaliador do predicado tem
	// scratch próprio e é de um dono por vez (internal/predicate). Dois donos
	// no mesmo scratch é data race.
	newEvaluator func() counter.Evaluator
	// newQualifier é a fábrica do avaliador do qualifyWhen, pela mesma razão;
	// nil quando o alarme não o declara.
	newQualifier func() counter.Evaluator
	enrich       []enrichQuery
	// valueColumn é a coluna de `_counters.<contador>` cujo valor, lido na
	// linha que decidiu o disparo, vira o `firingValue` do evento —
	// `spec.Value` compilado, ou schema.CounterTotalColumn quando a chave
	// `value:` não veio.
	valueColumn string
	// valueFieldID é o FieldID de valueColumn NESTE schema — o MESMO que o
	// avaliador do `when`/`qualifyWhen` usa, resolvido uma vez aqui e não a
	// cada disparo.
	valueFieldID types.FieldID
	// valueType é types.TypeDouble para o agregado `avg` e types.TypeInt64
	// para `total`, `min` e `max` (counters.TableSchema é quem decide isso).
	// É o que diz como os 8 bytes do campo viram o float64 do firingValue —
	// ver floatOfFieldValue em runner.go.
	valueType types.FieldType
}

// enrichQuery é uma consulta de enriquecimento com os binds resolvidos.
type enrichQuery struct {
	// name identifica o bloco de resultado dentro do evento.
	name  string
	sql   string
	binds []bind
}

// bindKind diz de qual das três fontes da linha do contador sai o valor.
type bindKind uint8

const (
	// bindGroupColumn: uma coluna do groupBy, pela posição declarada.
	bindGroupColumn bindKind = iota
	// bindGroupKey: o texto canônico do grupo.
	bindGroupKey
	// bindTotal: a contagem do grupo.
	bindTotal
	// bindRows: os membros do grupo.
	bindRows
)

// bind liga um `?` da consulta a um valor da linha do contador.
//
// O valor é LIGADO como parâmetro, nunca interpolado no texto do SQL. É a
// diferença entre uma consulta e uma injeção: o nome de um equipamento é dado
// vindo do cliente, e um apóstrofo dentro dele reescreveria a consulta.
type bind struct {
	null bool
	// column é o nome declarado, para a mensagem de erro.
	column string
	kind   bindKind
	// groupIndex é a posição no groupBy; válido só com bindGroupColumn.
	groupIndex int
	// fieldType é o tipo do valor, para montar o types.Value no disparo.
	fieldType types.FieldType
}

// Compile compila os alarmes declarados de um dataset contra o schema das
// tabelas dos contadores.
//
// A condição passa pelo avaliador COMPARTILHADO (internal/predicate), o mesmo
// do WHERE de uma consulta e do `when:` de uma regra de TTL, sobre o schema que
// a tabela `_counters.<nome>` de fato publica. Não há gramática nova, e
// `total < 100` num alarme significa exatamente o que significaria num SELECT.
//
// Tudo o que pode ser recusado é recusado AQUI, na subida: nome de coluna
// errado, SQL malformado, contagem de parâmetro divergente. O alarme é o pior
// lugar do produto para descobrir um erro de digitação, porque ele só é
// exercitado quando algo já está errado — de madrugada, quando ninguém quer
// descobrir que a consulta de enriquecimento nunca teria funcionado.
//
// Contador sem alarme não entra no resultado: é o que permite ao chamador
// decidir pela lista vazia se sobe goroutine ou não.
func Compile(dataset string, specs []schema.CounterSpec, defs []counter.Definition) ([]Definition, error) {
	byName := make(map[string]counter.Definition, len(defs))
	for _, d := range defs {
		byName[d.Name] = d
	}
	out := make([]Definition, 0, len(specs))
	for i, cs := range specs {
		if len(cs.Alarms) == 0 {
			continue
		}
		label := fmt.Sprintf("counters[%d]", i)
		def, ok := byName[cs.Name]
		if !ok {
			// Acontece quando o contador foi declarado e o Store não o abriu —
			// modo history, ou reconstrução pendente. Seguir em frente deixaria
			// o alarme calado para sempre sem ninguém saber.
			return nil, fmt.Errorf("%s.alarms: o contador %q não está aberto neste dataset", label, cs.Name)
		}
		sc, err := counters.TableSchema(def, cs.Metrics)
		if err != nil {
			return nil, fmt.Errorf("%s.alarms: %w", label, err)
		}
		d := Definition{Dataset: dataset, Counter: cs.Name, def: def, metrics: cs.Metrics, alarms: make([]compiledAlarm, 0, len(cs.Alarms))}
		for j, as := range cs.Alarms {
			a, err := compileAlarm(fmt.Sprintf("%s.alarms[%d]", label, j), as, def, cs.Metrics, sc)
			if err != nil {
				return nil, err
			}
			d.alarms = append(d.alarms, a)
		}
		out = append(out, d)
	}
	return out, nil
}

// checkAlarmStateText recusa um texto da assinatura da regra que o
// alarms.state não comporta.
//
// A recusa é AQUI, e não só na escrita do estado: aceito na subida, o texto
// compilaria e falharia na primeira gravação — que acontece na primeira
// transição, de madrugada, e para o laço com `alarm.stopped`. O teto é o do
// próprio arquivo (checkpoint.MaxAlarmStateText), para que as duas recusas nunca
// divirjam.
func checkAlarmStateText(path, text string) error {
	if len(text) > checkpoint.MaxAlarmStateText {
		return fmt.Errorf("%s: %d bytes, above the %d bytes the alarm state can hold",
			path, len(text), checkpoint.MaxAlarmStateText)
	}
	return nil
}

// compileAlarm compila um alarme contra o schema da tabela do contador.
//
// O `qualifyWhen` passa pelo MESMO CompileAlarmCondition e contra o MESMO
// schema do `when`: `auth_24h_max > 10` na qualificação significa exatamente o
// que significaria num WHERE sobre `_counters.<nome>`, NULL comparando falso
// incluído. As duas aceitam, além do WHERE, a comparação linear entre duas
// colunas int64 da linha (`total * 10 <= rows`), em inteiro exato — a única
// forma que o alarme tem e o SQL não.
func compileAlarm(label string, as schema.AlarmSpec, def counter.Definition, metrics []schema.CounterMetricSpec,
	sc *schema.Compiled) (compiledAlarm, error) {
	a := compiledAlarm{name: as.Name, when: as.When, qualifyWhen: as.QualifyWhen, sustainedFor: as.SustainedFor}
	if err := checkAlarmStateText(label+".when", as.When); err != nil {
		return a, err
	}
	if err := checkAlarmStateText(label+".qualifyWhen", as.QualifyWhen); err != nil {
		return a, err
	}
	prog, slots, err := predicate.CompileAlarmCondition(label+".when", as.When, sc)
	if err != nil {
		return a, err
	}
	ids := slots.IDs
	a.newEvaluator = func() counter.Evaluator { return predicate.NewAvaliadorOfPayload(prog, ids) }
	cited := ids
	if as.QualifyWhen != "" {
		qualifyProg, qualifySlots, err := predicate.CompileAlarmCondition(label+".qualifyWhen", as.QualifyWhen, sc)
		if err != nil {
			return a, err
		}
		qualifyIDs := qualifySlots.IDs
		a.newQualifier = func() counter.Evaluator { return predicate.NewAvaliadorOfPayload(qualifyProg, qualifyIDs) }
		cited = slices.Concat(ids, qualifyIDs)
	}
	a.metricsFingerprint = metricsFingerprintOf(sc, metrics, cited, def.Measures...)
	// O contexto do contador entra na assinatura quando a regra depende dele: a
	// taxa citada é contada sobre a população que groupBy, where e scope
	// decidem, e o scope decide a população de TODA coluna — `rows`, `total` e
	// as métricas. Trocar o scope reconstrói o contador e zera as measures;
	// sem o scope aqui, o cronômetro restaurado mediria uma sustentação sobre
	// outra população. Com scope vazio o campo é omitido, e a assinatura de um
	// alarme sem scope fica byte a byte a de antes — nada é descartado na troca
	// de binário.
	if def.Scope != "" || citesMeasure(sc, def, metrics, cited) {
		context, err := json.Marshal(struct {
			GroupBy []string
			Where   string
			Scope   string `json:",omitempty"`
		}{def.Names, def.Where, def.Scope})
		if err != nil {
			return a, err
		}
		a.metricsFingerprint += ";context:" + string(context)
	}
	if err := checkAlarmStateText(label+" metrics fingerprint", a.metricsFingerprint); err != nil {
		return a, err
	}
	col, fieldID, fieldType, err := resolveValueColumn(label, as, metrics, sc, def.Measures...)
	if err != nil {
		return a, err
	}
	a.valueColumn, a.valueFieldID, a.valueType = col, fieldID, fieldType
	if len(as.Enrich) == 0 {
		return a, nil
	}
	a.enrich = make([]enrichQuery, 0, len(as.Enrich))
	for k, es := range as.Enrich {
		q, err := compileEnrich(fmt.Sprintf("%s.enrich[%d]", label, k), es, def)
		if err != nil {
			return a, err
		}
		a.enrich = append(a.enrich, q)
	}
	return a, nil
}

// metricsFingerprintOf monta o fingerprint das métricas de janela citadas por
// um alarme: para cada métrica cuja coluna gerada aparece nos campos lidos pelas
// condições (ids, os Slots.IDs do `when` e do `qualifyWhen`), o texto
// `name:type:source:windowMs`, sem repetição, ordenado e unido por `;`. Nenhuma
// métrica citada dá o texto vazio.
//
// # Por que ele existe
//
// É o que faz uma janela alterada no YAML invalidar o cronômetro salvo, como
// `when` e `sustainedFor` já invalidam: `auth_24h_max > 10` sobre uma janela de
// 24h e sobre uma de 1h têm o MESMO texto e são regras diferentes. Sem ele, um
// prazo medido contra a janela antiga assombraria a condição nova depois do
// reinício, e o alarme dispararia por uma sustentação que ninguém mediu.
//
// Só as métricas CITADAS entram: mudar a janela de uma métrica que o alarme não
// lê não muda a regra dele, e descartar o cronômetro nesse caso seria perder
// estado sem motivo. Os agregados declarados também ficam de fora — acrescentar
// `min` a uma métrica não altera o valor de `auth_24h_max`.
//
// A ordem sai dos nomes, e não dos FieldIDs nem da declaração, para que o texto
// dependa só do que as métricas SÃO.
func metricsFingerprintOf(sc *schema.Compiled, metrics []schema.CounterMetricSpec, ids []types.FieldID, measures ...counter.MeasureDefinition) string {
	if (len(metrics) == 0 && len(measures) == 0) || len(ids) == 0 {
		return ""
	}
	byColumn := make(map[string]int, len(metrics)*3)
	for i, m := range metrics {
		for _, col := range m.Columns() {
			byColumn[col] = i
		}
	}
	cited := make([]bool, len(metrics))
	parts := make([]string, 0, len(metrics))
	for _, id := range ids {
		f, ok := sc.FieldByID(id)
		if !ok {
			continue
		}
		for _, m := range measures {
			if f.Name == m.Name {
				parts = append(parts, fmt.Sprintf("%s:rate:%d:%d", m.Name, m.Window, m.Unit))
			}
		}
		i, ok := byColumn[f.Name]
		if !ok || cited[i] {
			continue
		}
		cited[i] = true
		m := metrics[i]
		part := fmt.Sprintf("%s:%s:%s:%d", m.Name, m.Type, m.Source, m.Window.Milliseconds())
		if m.Rate != nil {
			part += fmt.Sprintf(":%d:%s", m.Rate.Window, m.Rate.Unit)
		}
		parts = append(parts, part)
	}
	slices.Sort(parts)
	return strings.Join(slices.Compact(parts), ";")
}

func compileEnrich(label string, es schema.EnrichSpec, def counter.Definition) (enrichQuery, error) {
	q := enrichQuery{name: es.Name, sql: es.SQL}
	stmt, err := parser.Parse(es.SQL)
	if err != nil {
		return q, fmt.Errorf("%s.sql: %w", label, err)
	}
	// LIMIT exigido e dentro do teto. `LIMIT ?` não existe na v1 (o parser o
	// recusa de propósito: parametrizá-lo obrigaria a replanejar por execução),
	// então o número está sempre aqui para ser conferido.
	if !stmt.HasLimit {
		return q, fmt.Errorf("%s.sql: declare um LIMIT (no máximo %d) — o resultado vai inteiro para dentro do evento",
			label, MaxEnrichRows)
	}
	if stmt.Limit > MaxEnrichRows {
		return q, fmt.Errorf("%s.sql: LIMIT %d acima do teto de %d linhas por consulta de enriquecimento",
			label, stmt.Limit, MaxEnrichRows)
	}
	// A contagem vem do PARSER, nunca de busca textual: `'?%'` é um caractere
	// dentro de uma string literal, não um parâmetro, e contá-lo exigiria um
	// bind que a consulta não tem.
	if stmt.ParamCount != len(es.Bind) {
		return q, fmt.Errorf("%s: a consulta tem %d parâmetro(s) `?` e o bind declara %d coluna(s)",
			label, stmt.ParamCount, len(es.Bind))
	}
	if len(es.Bind) == 0 {
		return q, nil
	}
	q.binds = make([]bind, 0, len(es.Bind))
	for _, col := range es.Bind {
		b, err := resolveBind(label, col, def)
		if err != nil {
			return q, err
		}
		q.binds = append(q.binds, b)
	}
	return q, nil
}

// resolveValueColumn compila o `value:` do alarme: a coluna efetiva é a
// declarada em schema.AlarmSpec.Value, ou schema.CounterTotalColumn quando a
// chave não veio.
//
// A validação é POR NOME, e não por tipo: o groupBy aceita int64 e boolean, e
// aceitá-lo aqui tornaria o `firingValue` ambíguo entre "o total do grupo" e
// "a OLT que disparou" — a identidade do grupo já está no groupKey e nas
// Columns do evento, e não é isso que value: declara. Só schema.CounterTotalColumn,
// schema.CounterRowsColumn, as medidas e as colunas geradas pelas métricas de
// janela (`<métrica>_<avg|min|max>`) são aceitas, e a lista sai de valueColumnNames — a MESMA enumeração que
// metricColumnsOf usa para resolver as colunas do laço (runner.go), para que
// as duas listas nunca divirjam.
//
// O FieldID resolvido é o MESMO que o avaliador do `when`/`qualifyWhen` usa
// sobre esta tabela (sc.FieldByName), e o tipo vem do schema COMPILADO, nunca
// assumido: é counters.TableSchema quem decide `avg` double e `total`/`min`/
// `max` int64.
func resolveValueColumn(label string, as schema.AlarmSpec, metrics []schema.CounterMetricSpec,
	sc *schema.Compiled, measures ...counter.MeasureDefinition) (string, types.FieldID, types.FieldType, error) {
	col := as.Value
	if col == "" {
		col = schema.CounterTotalColumn
	}
	accepted := valueColumnNames(metrics, measures...)
	if !slices.Contains(accepted, col) {
		return "", 0, 0, fmt.Errorf("%s.value: alarm %q: %q is not a valid value column (accepted: %s)",
			label, as.Name, col, strings.Join(accepted, ", "))
	}
	f, ok := sc.FieldByName([]byte(col))
	if !ok {
		// Não deveria acontecer: accepted sai da mesma lista de métricas que
		// gerou o schema. Fica calado seria pior que um erro que nomeia o
		// alarme.
		return "", 0, 0, fmt.Errorf("%s.value: alarm %q: column %q has no FieldID in the counter table",
			label, as.Name, col)
	}
	return col, f.ID, f.Type, nil
}

// valueColumnNames lista os nomes aceitos por `value:`: schema.CounterTotalColumn
// e schema.CounterRowsColumn, seguidos das medidas e das colunas geradas por cada métrica de janela, na ordem declarada
// (métrica, depois agregado) — a MESMA enumeração que metricColumnsOf usa
// para resolver as colunas do laço (runner.go), para que as duas listas nunca
// divirjam.
func valueColumnNames(metrics []schema.CounterMetricSpec, measures ...counter.MeasureDefinition) []string {
	out := make([]string, 0, 2+len(measures)+len(metrics)*3)
	out = append(out, schema.CounterTotalColumn, schema.CounterRowsColumn)
	for _, m := range measures {
		out = append(out, m.Name)
	}
	for _, m := range metrics {
		out = append(out, m.Columns()...)
	}
	return out
}

// resolveBind casa um nome de coluna com a fonte do valor na linha do contador.
// Só as colunas da tabela do contador são ligáveis: um campo do dataset base
// não existe no instante do disparo, porque o que o alarme observa é o grupo, e
// não a linha que o alimentou.
func resolveBind(label, col string, def counter.Definition) (bind, error) {
	switch col {
	case schema.CounterKeyColumn:
		return bind{column: col, kind: bindGroupKey, groupIndex: -1, fieldType: types.TypeString}, nil
	case schema.CounterTotalColumn:
		return bind{null: def.DisableTotal, column: col, kind: bindTotal, groupIndex: -1, fieldType: types.TypeInt64}, nil
	case schema.CounterRowsColumn:
		return bind{column: col, kind: bindRows, groupIndex: -1, fieldType: types.TypeInt64}, nil
	}
	for i, n := range def.Names {
		if n == col {
			return bind{column: col, kind: bindGroupColumn, groupIndex: i, fieldType: def.Types[i]}, nil
		}
	}
	return bind{}, fmt.Errorf("%s.bind: %q não é coluna da tabela do contador %q (ligáveis: %s, %s, %s e as do groupBy: %v)",
		label, col, def.Name, schema.CounterKeyColumn, schema.CounterTotalColumn, schema.CounterRowsColumn, def.Names)
}

// citesMeasure identifica dependência de uma taxa, direta ou por agregado.
func citesMeasure(sc *schema.Compiled, def counter.Definition, metrics []schema.CounterMetricSpec, ids []types.FieldID) bool {
	for _, id := range ids {
		field, ok := sc.FieldByID(id)
		if !ok {
			continue
		}
		for _, measure := range def.Measures {
			if measure.Name == field.Name {
				return true
			}
		}
		for _, metric := range metrics {
			if metric.Rate != nil && slices.Contains(metric.Columns(), field.Name) {
				return true
			}
		}
	}
	return false
}
