package alarm

import (
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
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
	alarms  []compiledAlarm
}

// compiledAlarm é um alarme pronto para o laço.
type compiledAlarm struct {
	name         string
	when         string
	sustainedFor time.Duration
	// newEvaluator é FÁBRICA, e não um avaliador: o avaliador do predicado tem
	// scratch próprio e é de um dono por vez (internal/predicate). Dois donos
	// no mesmo scratch é data race.
	newEvaluator func() counter.Evaluator
	enrich       []enrichQuery
}

// enrichQuery é uma consulta de enriquecimento com os binds resolvidos.
type enrichQuery struct {
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
)

// bind liga um `?` da consulta a um valor da linha do contador.
//
// O valor é LIGADO como parâmetro, nunca interpolado no texto do SQL. É a
// diferença entre uma consulta e uma injeção: o nome de um equipamento é dado
// vindo do cliente, e um apóstrofo dentro dele reescreveria a consulta.
type bind struct {
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
		sc, err := counters.TableSchema(def)
		if err != nil {
			return nil, fmt.Errorf("%s.alarms: %w", label, err)
		}
		d := Definition{Dataset: dataset, Counter: cs.Name, alarms: make([]compiledAlarm, 0, len(cs.Alarms))}
		for j, as := range cs.Alarms {
			a, err := compileAlarm(fmt.Sprintf("%s.alarms[%d]", label, j), as, def, sc)
			if err != nil {
				return nil, err
			}
			d.alarms = append(d.alarms, a)
		}
		out = append(out, d)
	}
	return out, nil
}

func compileAlarm(label string, as schema.AlarmSpec, def counter.Definition, sc *schema.Compiled) (compiledAlarm, error) {
	a := compiledAlarm{name: as.Name, when: as.When, sustainedFor: as.SustainedFor}
	prog, slots, err := predicate.CompileCondition(label+".when", as.When, sc)
	if err != nil {
		return a, err
	}
	ids := slots.IDs
	a.newEvaluator = func() counter.Evaluator { return predicate.NewAvaliadorOfPayload(prog, ids) }
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

func compileEnrich(label string, es schema.EnrichSpec, def counter.Definition) (enrichQuery, error) {
	q := enrichQuery{sql: es.SQL}
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

// resolveBind casa um nome de coluna com a fonte do valor na linha do contador.
// Só as colunas da tabela do contador são ligáveis: um campo do dataset base
// não existe no instante do disparo, porque o que o alarme observa é o grupo, e
// não a linha que o alimentou.
func resolveBind(label, col string, def counter.Definition) (bind, error) {
	switch col {
	case schema.CounterKeyColumn:
		return bind{column: col, kind: bindGroupKey, groupIndex: -1, fieldType: types.TypeString}, nil
	case schema.CounterTotalColumn:
		return bind{column: col, kind: bindTotal, groupIndex: -1, fieldType: types.TypeInt64}, nil
	}
	for i, n := range def.Names {
		if n == col {
			return bind{column: col, kind: bindGroupColumn, groupIndex: i, fieldType: def.Types[i]}, nil
		}
	}
	return bind{}, fmt.Errorf("%s.bind: %q não é coluna da tabela do contador %q (ligáveis: %s, %s e as do groupBy: %v)",
		label, col, def.Name, schema.CounterKeyColumn, schema.CounterTotalColumn, def.Names)
}
