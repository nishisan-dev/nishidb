package schema

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
)

// As métricas de janela de um contador são declaradas no bloco `metrics:` do
// próprio contador, irmão de `alarms:` e `emit:`:
//
//	counters:
//	  - name: autenticados_por_olt
//	    groupBy: [olt]
//	    metrics:
//	      - name: auth_5m          # nome livre; as colunas saem <name>_<aggregate>
//	        type: window           # único tipo
//	        source: total          # total habilitado ou nome de uma measure
//	        window: 5m             # múltiplo de 1m, de 1m a 30d
//	        aggregates: [avg]      # avg, min e/ou max, na ordem das colunas
//
// Aqui o bloco só é lido e recusado. Os nomes das colunas geradas
// (`auth_5m_avg`) são decididos AQUI, por MetricColumnName, porque é aqui que
// a colisão com as outras colunas da tabela do contador é recusada — e a
// recusa só vale se o nome recusado for o mesmo que a tabela e as condições
// vão usar.
const (
	// MetricTypeWindow é o único tipo de métrica. A chave `type` existe mesmo
	// com um valor só para que um tipo novo entre sem mudar a forma do bloco.
	MetricTypeWindow = "window"
	// MetricSourceTotal nomeia a população do grupo quando total está habilitado.
	MetricSourceTotal = "total"
	// AggregateAvg, AggregateMin e AggregateMax são os agregados da janela.
	AggregateAvg = "avg"
	AggregateMin = "min"
	AggregateMax = "max"
	// MaxCounterMetrics é o teto de métricas por contador. É teto, e não
	// sugestão, porque cada métrica custa arquivo e estado por grupo, e o
	// número de colunas geradas de um contador precisa ter limite conhecido
	// para que a linha da tabela possa ser composta num array de tamanho fixo.
	MaxCounterMetrics = 8
	// MinMetricWindow é a menor janela declarável: dividida em
	// MetricWindowBuckets baldes, ela dá o balde de 1 s.
	MinMetricWindow = time.Minute
	// MaxMetricWindow é a maior janela declarável. A 30 d cada balde já cobre
	// 12 h; acima disso a pergunta deixa de ser "como o grupo está" e passa a
	// ser de série histórica, que é o papel do `emit:`.
	MaxMetricWindow = 30 * 24 * time.Hour
	// MetricWindowBuckets é o número de baldes em que toda janela de métrica é
	// dividida. Este é o ÚNICO lugar deste número: o storage recebe o valor de
	// quem chama e o grava no cabeçalho, nunca o redeclara.
	//
	// É por causa dele que a janela tem de ser múltipla de 1m: a largura do
	// balde é janela ÷ 60, e só assim ela sai em segundos inteiros. Nada é
	// arredondado — uma janela de 90s daria balde de 1,5 s, e arredondar para
	// 1 s ou 2 s mudaria em silêncio o intervalo que o operador declarou.
	MetricWindowBuckets = 60
)

// windowAggregates são os agregados aceitos, na ordem em que a mensagem de
// recusa os lista.
var windowAggregates = []string{AggregateAvg, AggregateMin, AggregateMax}

// CounterMetricSpec é uma métrica de janela validada de um contador.
type CounterMetricSpec struct {
	// Rate resolve a fonte quando source nomeia uma measure; nil significa total.
	Rate *CounterMeasureSpec
	// Name é o nome livre da métrica, prefixo das colunas geradas.
	Name string
	// Type é sempre MetricTypeWindow.
	Type string
	// Source é MetricSourceTotal habilitado ou o nome de uma measure.
	Source string
	// Window é a janela: entre MinMetricWindow e MaxMetricWindow, múltipla de
	// time.Minute.
	Window time.Duration
	// Aggregates são os agregados da janela, na ordem declarada, sem
	// repetição. A ordem é a das colunas geradas (Columns).
	Aggregates []string
}

// MetricColumnName é o nome da coluna que o agregado de uma métrica gera na
// tabela do contador: `<metric>_<aggregate>`. É o mesmo nome no SQL e numa
// condição de alarme — sem gramática nova, porque `a.b` já é campo aninhado.
func MetricColumnName(metric, aggregate string) string {
	return metric + "_" + aggregate
}

// Columns devolve as colunas geradas pela métrica, na ordem declarada de
// Aggregates.
func (m CounterMetricSpec) Columns() []string {
	out := make([]string, len(m.Aggregates))
	for i, a := range m.Aggregates {
		out[i] = MetricColumnName(m.Name, a)
	}
	return out
}

// CounterColumn é uma coluna da tabela `_counters.<name>` junto com o trecho
// do contador que a produz.
type CounterColumn struct {
	Name string
	// Origin é o caminho, relativo ao item de `counters[]`, do trecho que
	// produz a coluna: vazio para as colunas fixas (groupKey e total),
	// "groupBy" para as do agrupamento e "metrics[i]" para as geradas pela
	// i-ésima métrica. É o que deixa a recusa nomear as DUAS pontas de uma
	// colisão, e não só a que chegou por último.
	Origin string
}

// counterColumnOriginGroupBy é a Origin das colunas do agrupamento.
const counterColumnOriginGroupBy = "groupBy"

// String descreve a coluna para a mensagem de recusa.
func (c CounterColumn) String() string {
	switch c.Origin {
	case "":
		return fmt.Sprintf("fixed column %q of table %s.<name>", c.Name, CountersNamespace)
	case counterColumnOriginGroupBy:
		return fmt.Sprintf("groupBy column %q", c.Name)
	default:
		return fmt.Sprintf("column %q generated by %s", c.Name, c.Origin)
	}
}

// CounterColumns lista as colunas da tabela de um contador: as fixas, as do
// groupBy na ordem declarada e as geradas pelas métricas, métrica a métrica,
// na ordem de Columns.
//
// É a fonte ÚNICA dos nomes reservados da tabela — a validação do YAML e o
// schema sintético de internal/counters leem esta lista, e por isso não há uma
// segunda relação de "colunas fixas" para esquecer de atualizar.
//
// A ordem é a de PRECEDÊNCIA da recusa, e não a de projeção da tabela (lá
// `total` vem depois do groupBy): as fixas vêm primeiro para que, numa
// colisão, a coluna apontada como culpada seja sempre a declarada pelo
// operador, que é a que ele pode renomear.
func CounterColumns(groupBy []string, metrics []CounterMetricSpec, measures ...CounterMeasureSpec) []CounterColumn {
	n := 2 + len(groupBy)
	for _, m := range metrics {
		n += len(m.Aggregates)
	}
	out := make([]CounterColumn, 0, n)
	out = append(out, CounterColumn{Name: CounterKeyColumn}, CounterColumn{Name: CounterTotalColumn})
	for _, g := range groupBy {
		out = append(out, CounterColumn{Name: g, Origin: counterColumnOriginGroupBy})
	}
	for i, m := range measures {
		out = append(out, CounterColumn{Name: m.Name, Origin: fmt.Sprintf("measures[%d]", i)})
	}
	for i, m := range metrics {
		origin := fmt.Sprintf("metrics[%d]", i)
		for _, col := range m.Columns() {
			out = append(out, CounterColumn{Name: col, Origin: origin})
		}
	}
	return out
}

// FindCounterColumnCollision devolve o primeiro par de colunas da tabela do
// contador com o mesmo nome: first é a que vem antes na ordem de
// CounterColumns, second a que a repete. found é false quando os nomes são
// todos distintos.
func FindCounterColumnCollision(groupBy []string, metrics []CounterMetricSpec, measures ...CounterMeasureSpec) (first, second CounterColumn, found bool) {
	columns := CounterColumns(groupBy, metrics, measures...)
	seen := make(map[string]CounterColumn, len(columns))
	for _, col := range columns {
		if prev, dup := seen[col.Name]; dup {
			return prev, col, true
		}
		seen[col.Name] = col
	}
	return CounterColumn{}, CounterColumn{}, false
}

// rawMetric é um item do bloco como lido do YAML, antes da validação.
type rawMetric struct {
	Name          string
	Type          string
	Source        string
	Window        string
	Aggregates    []string
	hasAggregates bool
}

// metricsFromNode decodifica o bloco `metrics` de um contador. Cada item passa
// por block() para que uma chave desconhecida (`windw:`) seja recusada em vez
// de silenciosamente ignorada.
func metricsFromNode(n *yaml.Node, keyPath string) ([]rawMetric, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n == nil || n.Kind != yaml.SequenceNode {
		return nil, specErrf("%s must be a list of metrics", keyPath)
	}
	out := make([]rawMetric, 0, len(n.Content))
	for i, item := range n.Content {
		path := fmt.Sprintf("%s[%d]", keyPath, i)
		keys, err := block(item, path, "name", "type", "source", "window", "aggregates")
		if err != nil {
			return nil, err
		}
		var r rawMetric
		for key, value := range keys {
			var err error
			switch key {
			case "name":
				err = decodeInto(value, path+".name", &r.Name)
			case "type":
				err = decodeInto(value, path+".type", &r.Type)
			case "source":
				err = decodeInto(value, path+".source", &r.Source)
			case "window":
				err = decodeInto(value, path+".window", &r.Window)
			case "aggregates":
				r.hasAggregates = true
				err = decodeInto(value, path+".aggregates", &r.Aggregates)
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// isMetricName diz se o nome de métrica é aceito: `[a-z_][a-z0-9_]*`.
//
// É mais estrito que isIdentName, que valida nome de contador e de alarme,
// porque o nome da métrica vira PREFIXO de coluna (`<name>_<aggregate>`), e a
// coluna é citada sem aspas no SQL e no `when`/`qualifyWhen` — que é o uso da
// feature. O lexer SQL não aceita identificador começando com dígito
// (isIdentStart, internal/query/lexer/lexer.go): `name: 5m` geraria `5m_avg`,
// uma coluna que a condição não consegue citar. Os nomes de contador e de
// alarme continuam com a regra antiga: são contrato publicado e não viram
// coluna.
//
// O `_` inicial é aceito. O prefixo `_` é reserva de ESPAÇO DE NOME (dataset,
// contador, schemaName: `_meta`, `_counters`), não de coluna: para campo ele é
// legítimo (`_SEQ`), e os campos de sistema são lista fechada (systemNames,
// spec.go) justamente por isso. O lexer aceita `_` como início de
// identificador, e a coluna gerada continua citável.
func isMetricName(s string) bool {
	if s == "" || (s[0] != '_' && (s[0] < 'a' || s[0] > 'z')) {
		return false
	}
	return isIdentName(s)
}

// validateMetrics confere a lista de métricas de UM contador, item a item. O
// que depende dos outros nomes do contador — repetição de nome, colisão com
// coluna ou alarme — não é conferido aqui, e sim em validateCounterNames, para
// que toda colisão do contador tenha um lugar só.
func validateMetrics(raws []rawMetric, keyPath string) ([]CounterMetricSpec, error) {
	if len(raws) == 0 {
		return nil, specErrf("%s: empty list — remove the key or declare a metric", keyPath)
	}
	if len(raws) > MaxCounterMetrics {
		return nil, specErrf("%s: %d metrics declared, the ceiling is %d per counter", keyPath, len(raws), MaxCounterMetrics)
	}
	out := make([]CounterMetricSpec, 0, len(raws))
	for i, r := range raws {
		path := fmt.Sprintf("%s[%d]", keyPath, i)
		if r.Name == "" {
			return nil, specErrf("%s.name required", path)
		}
		if !isMetricName(r.Name) {
			return nil, specErrf("%s.name %q invalid (expected [a-z_][a-z0-9_]*: the generated columns are cited by name in SQL and in alarm conditions, "+
				"and an identifier cannot start with a digit)", path, r.Name)
		}
		if r.Type == "" {
			return nil, specErrf("%s.type required (the only type is %q)", path, MetricTypeWindow)
		}
		if r.Type != MetricTypeWindow {
			return nil, specErrf("%s.type %q unknown (the only type is %q)", path, r.Type, MetricTypeWindow)
		}
		if r.Source == "" {
			return nil, specErrf("%s.source required (the only source is %q)", path, MetricSourceTotal)
		}
		window, err := metricWindow(path, r.Window)
		if err != nil {
			return nil, err
		}
		if err := validateWindowAggregates(path, r); err != nil {
			return nil, err
		}
		out = append(out, CounterMetricSpec{
			Name:       r.Name,
			Type:       r.Type,
			Source:     r.Source,
			Window:     window,
			Aggregates: r.Aggregates,
		})
	}
	return out, nil
}

// metricWindow lê a janela da métrica. `never` e zero são recusados, como no
// sustainedFor: uma janela que não fecha não tem agregado, e ao contrário do
// retainEmptyGroups não existe "para sempre" que faça sentido aqui.
func metricWindow(keyPath, raw string) (time.Duration, error) {
	if raw == "" {
		return 0, specErrf("%s.window required (between %s and %s, a multiple of 1m)", keyPath, MinMetricWindow, MaxMetricWindow)
	}
	if raw == DurationNever {
		return 0, specErrf("%s.window %q is not a window: a window that never closes has no aggregate", keyPath, DurationNever)
	}
	if isZeroDuration(raw) {
		return 0, specErrf("%s.window %q declares no duration — the minimum is %s", keyPath, raw, MinMetricWindow)
	}
	d, err := units.DurationWithDays(raw, 0)
	if err != nil {
		return 0, specErrf("%s.window: %s", keyPath, withoutSpecPrefix(err))
	}
	if d < MinMetricWindow {
		return 0, specErrf("%s.window %s is below the minimum of %s", keyPath, raw, MinMetricWindow)
	}
	if d > MaxMetricWindow {
		return 0, specErrf("%s.window %s is above the maximum of %s", keyPath, raw, MaxMetricWindow)
	}
	if d%time.Minute != 0 {
		return 0, specErrf("%s.window %s is not a multiple of 1m: the window is split into %d buckets, "+
			"and only a multiple of 1m gives each bucket a whole number of seconds", keyPath, raw, MetricWindowBuckets)
	}
	return d, nil
}

// validateWindowAggregates confere os agregados de uma métrica.
func validateWindowAggregates(keyPath string, r rawMetric) error {
	known := strings.Join(windowAggregates, ", ")
	if !r.hasAggregates {
		return specErrf("%s.aggregates required (one or more of %s)", keyPath, known)
	}
	if len(r.Aggregates) == 0 {
		return specErrf("%s.aggregates empty — declare one or more of %s", keyPath, known)
	}
	seen := make(map[string]bool, len(r.Aggregates))
	for _, a := range r.Aggregates {
		if !slices.Contains(windowAggregates, a) {
			return specErrf("%s.aggregates: %q unknown (known: %s)", keyPath, a, known)
		}
		if seen[a] {
			return specErrf("%s.aggregates: %q repeated", keyPath, a)
		}
		seen[a] = true
	}
	return nil
}

// validateCounterNames é o lugar ÚNICO das colisões de nome dentro de um
// contador. Recusa:
//
//   - duas colunas da tabela com o mesmo nome (CounterColumns: fixas, groupBy e
//     geradas), nomeando as duas origens;
//   - nome de métrica repetido;
//   - nome de métrica igual a qualquer coluna da tabela, geradas incluídas, ou
//     ao nome de um alarme do mesmo contador.
//
// A regra do nome de métrica é "um nome, uma coisa dentro do contador":
// `auth_5m` numa condição, num evento ou numa tabela de metadados tem de
// apontar para um objeto só.
//
// NÃO recusa nome de alarme igual a coluna: a regra seria a mesma, mas os
// alarmes foram publicados na v0.8.0 sem ela, e uma configuração que subia
// passaria a ser recusada sem que nada nela tivesse mudado.
//
// Depende de c.GroupBy, c.Alarms e c.Metrics já validados item a item.
func validateCounterNames(keyPath string, c CounterSpec) error {
	metricAt := make(map[string]int, len(c.Metrics))
	for i, m := range c.Metrics {
		if j, dup := metricAt[m.Name]; dup {
			return specErrf("%s.metrics[%d].name %q repeated (already declared in %s.metrics[%d])", keyPath, i, m.Name, keyPath, j)
		}
		metricAt[m.Name] = i
	}
	if first, second, found := FindCounterColumnCollision(c.GroupBy, c.Metrics); found {
		hint := "rename the metric"
		if second.Origin == counterColumnOriginGroupBy {
			hint = "rename the column in the schema"
		}
		return specErrf("%s.%s: %s collides with %s; %s", keyPath, second.Origin, second, first, hint)
	}
	columns := CounterColumns(c.GroupBy, c.Metrics)
	for i, m := range c.Metrics {
		path := fmt.Sprintf("%s.metrics[%d].name", keyPath, i)
		for _, col := range columns {
			if col.Name == m.Name {
				return specErrf("%s %q collides with %s; rename the metric", path, m.Name, col)
			}
		}
		for j, a := range c.Alarms {
			if a.Name == m.Name {
				return specErrf("%s %q collides with the name of alarm %s.alarms[%d]; rename the metric", path, m.Name, keyPath, j)
			}
		}
	}
	return nil
}
