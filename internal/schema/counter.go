package schema

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/keyformat"

	"github.com/nishisan-dev/nishidb/internal/units"
)

// Os contadores mantidos na escrita (planning/contadores-mantidos-na-escrita.md)
// são declarados no bloco `counters:` do YAML do dataset:
//
//	counters:
//	  - name: autenticados_por_olt
//	    groupBy: [olt]
//	    where: authenticated = true      # opcional; ausente conta toda linha
//	    keyFormat: "{olt}.{slot:02}"     # opcional; molde do groupKey
//	    maxGroups: 65536                 # opcional
//	    retainEmptyGroups: 7d            # opcional; `never` mantém para sempre
//	    alarms: [...]                    # opcional; ver alarm.go
//	    metrics: [...]                   # opcional; ver counter_metric.go
//	    emit: {...}                      # opcional; ver emit.go
//
// O `keyFormat` precisa de aspas: `{` abre mapa no YAML. Ausente, o groupKey
// sai como sempre saiu — as colunas na ordem do groupBy separadas por `|`.
//
// A leitura sai por SQL em `_counters.<name>`, uma tabela por contador com a
// coluna `groupKey`, as colunas do groupBy abertas e tipadas, e `total`.
const (
	// DefaultMaxGroups é o teto de grupos vivos quando o YAML não declara. É a
	// proteção contra um groupBy declarado, por engano, sobre uma coluna de
	// alta cardinalidade: o nível de OLT cabe folgado; PON e splitter exigem
	// declarar.
	DefaultMaxGroups = 65_536
	// MaxGroupsPhysicalCeiling é o maior teto declarável: o vetor de grupos guarda
	// o id em 31 bits, e o bit restante é a condição.
	MaxGroupsPhysicalCeiling = 1<<31 - 1
	// DefaultRetainEmptyGroups é quanto tempo um grupo sem linhas continua
	// visível com total zero. O critério é operacional (planning §6.5): maior
	// que o tempo entre um equipamento apagar por inteiro e alguém agir.
	DefaultRetainEmptyGroups = 7 * 24 * time.Hour
	// CountersNamespace é o espaço de nome virtual das tabelas dos contadores.
	CountersNamespace = "_counters"
	// CounterKeyColumn é a key de toda tabela `_counters.<name>`: o texto
	// canônico do grupo. Existe porque a key da v1 é uma coluna string única, e
	// um groupBy composto não tem coluna que identifique a linha.
	CounterKeyColumn = "groupKey"
	// CounterTotalColumn é a coluna com a contagem.
	CounterTotalColumn = "total"
	// ReservedAlarmsTable é o nome da view de alarmes publicada no schema de
	// todo dataset, sob `_counters` — por isso nenhum contador, dataset ou
	// tabela histórica pode usá-lo.
	ReservedAlarmsTable = "alarms"
)

// CounterSpec é um contador validado, com defaults aplicados.
type CounterSpec struct {
	// DisableTotal preserva o default legado no valor zero dos callers Go.
	DisableTotal bool
	Measures     []CounterMeasureSpec
	Name         string
	// GroupBy são as colunas de agrupamento, na ordem declarada — a ordem dos
	// componentes da chave e das colunas da tabela.
	GroupBy []string
	// Where é a condição normalizada (espaços colapsados); "" é sem condição.
	// NÃO é validada aqui: precisa do schema compilado, como o when do TTL.
	Where string
	// MaxGroups é o teto de grupos vivos.
	MaxGroups uint32
	// RetainEmptyGroups é o prazo de visibilidade do grupo vazio; zero é never.
	RetainEmptyGroups time.Duration
	// KeyFormat é o molde do groupKey como declarado; "" é o default. Fica
	// como TEXTO e é compilado de novo em internal/counters, contra o schema
	// compilado — aqui a validação existe para recusar na subida, e guardar o
	// molde já compilado faria a spec carregar estado que ela não usa.
	KeyFormat string
	// Alarms são os alarmes declarados, na ordem; nil quando não há bloco
	// `alarms`. O When de cada um segue a mesma regra do Where: validado
	// sintaticamente aqui, compilado contra o schema em internal/alarm.
	Alarms []AlarmSpec
	// Metrics são as métricas de janela declaradas, na ordem; nil quando não
	// há bloco `metrics`. Ver counter_metric.go.
	Metrics []CounterMetricSpec
	// Emit é a emissão periódica por linha; nil quando não há bloco `emit`.
	Emit *EmitSpec
}

// groupingTypes são os tipos que o groupBy aceita. Três, e não a lista de
// indexáveis: são os únicos com renderização textual declarada para o groupKey,
// e um tipo novo aqui exige a renderização antes.
var groupingTypes = map[string]bool{"string": true, "int64": true, "boolean": true}

// rawCounter é um item do bloco como lido do YAML, antes da validação.
type rawCounter struct {
	Total             *bool
	Measures          []CounterMeasureSpec
	Name              string
	GroupBy           []string
	Where             *string
	MaxGroups         *int64
	RetainEmptyGroups string
	KeyFormat         *string
	Alarms            []rawAlarm
	Metrics           []rawMetric
	Emit              *rawEmit
	hasGroupBy        bool
}

// countersFromNode decodifica o bloco `counters`. Ausente devolve nil; lista
// vazia devolve slice vazio (e a validação recusa). Cada item passa por block()
// para que uma chave desconhecida (`wher: …`) seja recusada em vez de
// silenciosamente ignorada.
func countersFromNode(n *yaml.Node) ([]rawCounter, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if absent(n) {
		return nil, nil
	}
	if n.Kind != yaml.SequenceNode {
		return nil, specErrf("counters deve ser uma lista de contadores")
	}
	out := make([]rawCounter, 0, len(n.Content))
	for i, item := range n.Content {
		keyPath := fmt.Sprintf("counters[%d]", i)
		keys, err := block(item, keyPath, "name", "groupBy", "where", "keyFormat",
			"maxGroups", "retainEmptyGroups", "alarms", "metrics", "emit", "total", "measures")
		if err != nil {
			return nil, err
		}
		var r rawCounter
		for key, value := range keys {
			var err error
			switch key {
			case "total":
				for value.Kind == yaml.AliasNode {
					value = value.Alias
				}
				if value.Kind != yaml.ScalarNode || value.Tag != "!!bool" {
					return nil, specErrf("%s.total must be a boolean", keyPath)
				}
				var total bool
				err = decodeInto(value, keyPath+".total", &total)
				r.Total = &total
			case "measures":
				r.Measures, err = measuresFromNode(value, keyPath+".measures")
			case "name":
				err = decodeInto(value, keyPath+".name", &r.Name)
			case "groupBy":
				r.hasGroupBy = true
				err = decodeInto(value, keyPath+".groupBy", &r.GroupBy)
			case "where":
				var w string
				err = decodeInto(value, keyPath+".where", &w)
				r.Where = &w
			case "keyFormat":
				var kf string
				err = decodeInto(value, keyPath+".keyFormat", &kf)
				r.KeyFormat = &kf
			case "maxGroups":
				var m int64
				err = decodeInto(value, keyPath+".maxGroups", &m)
				r.MaxGroups = &m
			case "retainEmptyGroups":
				err = decodeInto(value, keyPath+".retainEmptyGroups", &r.RetainEmptyGroups)
			case "alarms":
				r.Alarms, err = alarmsFromNode(value, keyPath+".alarms")
			case "metrics":
				r.Metrics, err = metricsFromNode(value, keyPath+".metrics")
			case "emit":
				r.Emit, err = emitFromNode(value, keyPath+".emit")
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// validateCounters confere a lista inteira e devolve a forma validada.
//
// fields são os campos declarados; as colunas extraídas entram pelo nome que
// derivedColumnName lhes dá. key e seq são os papéis que o groupBy não pode
// usar: agrupar pela key é um grupo por entidade, e pela sequence é agrupar
// pela ordem do evento — os dois são cardinalidade sem teto por engano.
// dataset é o nome do próprio dataset: no schema declarado a tabela base
// aparece como `<schemaName>.<dataset>` (planning/schema-declarado-e-catalogo-jdbc.md),
// e é contra esse nome que o nome do contador precisa ser distinto.
// historyTable é o nome da tabela IRMÃ, ou vazio quando não há bloco
// `history`: ela herda o schema do base e é a outra tabela com que o contador
// pode colidir. É por isso que a validação dos contadores roda depois da do
// histórico (spec.go).
func validateCounters(raws []rawCounter, fields map[string]FieldSpec, key, seq, dataset, historyTable string) ([]CounterSpec, error) {
	if len(raws) == 0 {
		return nil, specErrf("counters: lista vazia — remova a chave ou declare um contador")
	}
	columns, err := declaredColumns(fields)
	if err != nil {
		return nil, err
	}
	seenNames := make(map[string]int, len(raws))
	// seenFiles casa nome de arquivo de emissão → contador que o declarou. É
	// por dataset, e não por contador, porque a colisão que importa é entre
	// dois contadores do MESMO dataset escrevendo no mesmo arquivo.
	seenFiles := make(map[string]string, len(raws))
	out := make([]CounterSpec, 0, len(raws))
	for i, r := range raws {
		keyPath := fmt.Sprintf("counters[%d]", i)
		if r.Name == "" {
			return nil, specErrf("%s.name obrigatório", keyPath)
		}
		if !isIdentName(r.Name) {
			return nil, specErrf("%s.name %q inválido (esperado [a-z0-9_]+)", keyPath, r.Name)
		}
		// O prefixo "_" não fecha colisão nenhuma hoje — `_fields`, `_counters`
		// e `_kafka` moram no schema `_meta`, não no do dataset (planning/
		// schema-declarado-e-catalogo-jdbc.md). A recusa fica de qualquer jeito,
		// como higiene: o prefixo é a reserva do produto (mesma regra do nome
		// de dataset em validate, spec.go), e um contador `_fields` seria
		// confusão gratuita mesmo sem colidir com nada.
		if strings.HasPrefix(r.Name, "_") {
			return nil, specErrf(
				"%s.name %q: o prefixo \"_\" é reservado ao produto; escolha outro nome", keyPath, r.Name)
		}
		// Esta, sim, é colisão real: no schema declarado a tabela base do
		// dataset aparece como `<schemaName>.<dataset>`, e um contador com o
		// mesmo nome apareceria no MESMO lugar — as duas tabelas disputariam
		// uma única entrada no catálogo.
		if r.Name == dataset {
			return nil, specErrf(
				"%s.name %q é o nome do próprio dataset: no schema declarado os dois seriam a mesma tabela",
				keyPath, r.Name)
		}
		// A tabela IRMÃ do histórico é a mesma colisão por outro caminho, e é
		// construível com um arquivo só: `name: events` + `history:` produz a
		// irmã `events_history`, que HERDA o schemaName do base
		// (DeriveHistory), e um contador chamado `events_history` publica o par
		// `(telecom, events_history)` uma segunda vez. O catálogo mostraria
		// duas tabelas idênticas na árvore e o SELECT que a ferramenta gera a
		// partir delas seria recusado como ambíguo em runtime — configuração
		// aceita na subida que só quebra depois, que é o oposto do que este
		// validate existe para fazer.
		//
		// A mensagem nomeia o sufixo porque ele é configurável: quem escolheu
		// `history.suffix` precisa saber qual dos dois nomes mudar.
		if historyTable != "" && r.Name == historyTable {
			return nil, specErrf(
				"%s.name %q é o nome da tabela histórica deste dataset (history.suffix): "+
					"as duas moram no mesmo schema e seriam a mesma tabela no catálogo",
				keyPath, r.Name)
		}
		// ReservedAlarmsTable é a mesma colisão pelo terceiro caminho: a view
		// de alarmes publicada sob CountersNamespace usa este nome fixo em
		// todo dataset, e um contador chamado `alarms` disputaria a mesma
		// entrada no catálogo.
		if r.Name == ReservedAlarmsTable {
			return nil, specErrf(
				"%s.name %q is reserved for the alarms view published under %s.%s",
				keyPath, r.Name, CountersNamespace, ReservedAlarmsTable)
		}
		if j, dup := seenNames[r.Name]; dup {
			return nil, specErrf("%s.name %q repetido (já declarado em counters[%d])", keyPath, r.Name, j)
		}
		seenNames[r.Name] = i
		if !r.hasGroupBy || len(r.GroupBy) == 0 {
			return nil, specErrf("%s.groupBy obrigatório (lista de colunas de agrupamento)", keyPath)
		}
		seenColumns := make(map[string]bool, len(r.GroupBy))
		for _, col := range r.GroupBy {
			if seenColumns[col] {
				return nil, specErrf("%s.groupBy: coluna %q repetida", keyPath, col)
			}
			seenColumns[col] = true
		}
		c := CounterSpec{Name: r.Name, GroupBy: r.GroupBy, MaxGroups: DefaultMaxGroups, RetainEmptyGroups: DefaultRetainEmptyGroups}
		c.Measures = r.Measures
		c.DisableTotal = r.Total != nil && !*r.Total
		if r.Alarms != nil {
			if c.Alarms, err = validateAlarms(r.Alarms, keyPath+".alarms"); err != nil {
				return nil, err
			}
		}
		if r.Metrics != nil {
			if c.Metrics, err = validateMetrics(r.Metrics, keyPath+".metrics"); err != nil {
				return nil, err
			}
		}
		if err := validateMeasureSources(keyPath, &c); err != nil {
			return nil, err
		}
		// As colisões de nome vêm ANTES da checagem de cada coluna do groupBy:
		// um groupBy `total` tem de ser recusado pela colisão com a coluna fixa,
		// e não por um sintoma dela (não estar declarado, ter o tipo errado) —
		// o sintoma some quando o operador declara a coluna, e a colisão não.
		if err := validateCounterNames(keyPath, c); err != nil {
			return nil, err
		}
		for _, col := range r.GroupBy {
			if err := validateGroupingColumn(keyPath, col, columns, key, seq); err != nil {
				return nil, err
			}
		}
		if r.Where != nil {
			w := strings.Join(strings.Fields(*r.Where), " ")
			if w == "" {
				return nil, specErrf("%s.where vazio — omita a chave para contar toda linha com grupo", keyPath)
			}
			c.Where = w
		}
		if r.KeyFormat != nil {
			if err := validateKeyFormat(keyPath, *r.KeyFormat, r.GroupBy, columns); err != nil {
				return nil, err
			}
			c.KeyFormat = *r.KeyFormat
		}
		if r.MaxGroups != nil {
			if *r.MaxGroups < 1 || *r.MaxGroups > MaxGroupsPhysicalCeiling {
				return nil, specErrf("%s.maxGroups %d fora de [1, %d]", keyPath, *r.MaxGroups, MaxGroupsPhysicalCeiling)
			}
			c.MaxGroups = uint32(*r.MaxGroups)
		}
		if r.RetainEmptyGroups != "" {
			d, err := visibilityDuration(keyPath, r.RetainEmptyGroups)
			if err != nil {
				return nil, err
			}
			c.RetainEmptyGroups = d
		}
		if r.Emit != nil {
			if c.Emit, err = validateEmit(r.Emit, keyPath+".emit", r.Name, seenFiles); err != nil {
				return nil, err
			}
			if c.Emit.HasKafka() {
				for _, col := range CounterColumns(c.GroupBy, c.Metrics, c.Measures...) {
					if col.Origin == "" { // Fixed counter columns are emitted exactly once.
						continue
					}
					switch col.Name {
					case "dataset", "counter", "groupKey", "total", "at":
						return nil, specErrf("%s.emit: column %q collides with a reserved Kafka field", keyPath, col.Name)
					}
				}
			}
		}
		out = append(out, c)
	}
	return out, nil
}

// declaredColumns devolve nome → tipo declarado de toda coluna que o groupBy
// pode referenciar: os campos de topo e as colunas extraídas. A congelada entra
// sem tipo (ela o herda na compilação) e por isso é recusada como "sem tipo
// declarado" — agrupar por uma coluna cujo tipo só se conhece depois seria
// decidir a chave do grupo tarde demais.
func declaredColumns(fields map[string]FieldSpec) (map[string]string, error) {
	out := make(map[string]string, len(fields))
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := fields[name]
		out[name] = f.Type
		for i, e := range f.Extract {
			column, err := derivedColumnName(name, i, e)
			if err != nil {
				return nil, err
			}
			out[column] = e.Type
		}
	}
	return out, nil
}

// validateGroupingColumn confere uma coluna do groupBy contra o schema do
// dataset. A colisão com as colunas fixas da tabela do contador NÃO é
// conferida aqui: ela é de validateCounterNames, que vê a lista inteira de
// colunas (CounterColumns) e roda antes.
func validateGroupingColumn(keyPath, col string, columns map[string]string, key, seq string) error {
	switch col {
	case key:
		return specErrf("%s.groupBy: coluna %q é a key do dataset — agrupar pela key é um grupo por entidade", keyPath, col)
	case seq:
		if seq != "" {
			return specErrf("%s.groupBy: coluna %q é a sequence do dataset — a ordem do evento não é um grupo", keyPath, col)
		}
	}
	typ, ok := columns[col]
	if !ok {
		return specErrf("%s.groupBy: coluna %q não declarada em schema.fields (nem como extração)", keyPath, col)
	}
	if typ == "" || typ == "auto" {
		return specErrf("%s.groupBy: coluna %q precisa de tipo declarado (é auto ou herdado) — a chave do grupo é decidida pelo tipo",
			keyPath, col)
	}
	if !groupingTypes[typ] {
		return specErrf("%s.groupBy: coluna %q tem tipo %q; groupBy aceita string, int64 e boolean", keyPath, col, typ)
	}
	return nil
}

// validateKeyFormat recusa o molde na subida, pelo MESMO parser que renderiza
// (internal/keyformat). Não é regra duplicada: é a única forma de a recusa e a
// renderização não divergirem, e é aqui que o erro sai com o caminho do YAML
// que o operador precisa para achar a linha.
func validateKeyFormat(keyPath, mold string, groupBy []string, columns map[string]string) error {
	cols := make([]keyformat.Column, len(groupBy))
	for i, name := range groupBy {
		t, ok := fieldTypeNames[columns[name]]
		if !ok {
			// validateGroupingColumn já recusou o que não é agrupável; chegar
			// aqui seria a lista de tipos aceitos ter crescido sem a
			// renderização correspondente.
			return specErrf("%s.keyFormat: coluna %q tem tipo %q, sem renderização declarada", keyPath, name, columns[name])
		}
		cols[i] = keyformat.Column{Name: name, Type: t}
	}
	if _, err := keyformat.Parse(mold, cols); err != nil {
		return specErrf("%s.%s", keyPath, err)
	}
	return nil
}

// visibilityDuration lê retainEmptyGroups com as mesmas regras do
// expireAfterWrite: `never` é zero, zero explícito é recusado apontando para
// `never`, e abaixo da resolução do coletor é recusado.
func visibilityDuration(keyPath, raw string) (time.Duration, error) {
	if raw == DurationNever {
		return 0, nil
	}
	if isZeroDuration(raw) {
		return 0, specErrf("%s.retainEmptyGroups %q não declara prazo — use %q para manter o grupo vazio para sempre",
			keyPath, raw, DurationNever)
	}
	d, err := units.DurationWithDays(raw, 0)
	if err != nil {
		return 0, specErrf("%s.retainEmptyGroups: %s", keyPath, withoutSpecPrefix(err))
	}
	if d < ExpiryResolution {
		return 0, specErrf("%s.retainEmptyGroups %s é menor que a resolução do coletor (%s) — use %q para não aposentar",
			keyPath, raw, ExpiryResolution, DurationNever)
	}
	return d, nil
}
